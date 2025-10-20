use super::WalPosition;
#[cfg(test)]
use crate::latch::LatchGuard;
use crate::wal::allocator::AllocationResult;
use crate::wal::mapper::WalMapper;
use crate::WalLayout;
use parking_lot::{ArcMutexGuard, Mutex, RawMutex};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;

/// WalTracker tracks "last processed position" for the wal.
///
/// When wal entry is created, WalTracker returns a guard for the given position.
/// This guard is then passed into Db instance, and Db holds to the guard until the wal position is
/// recorded in the in-memory index of the large table.
/// After the in-memory index is updated, the Db drops the wal guard.
///
/// WalTracker considers wal positions that still have non-dropped guards as unprocessed.
/// Its job is then to report the maximum wal position last_processed,
/// so that all allocated positions
/// that have offset below last_processed are included in in-memory index.
///
/// This is an essential property to avoid race conditions with an asynchronous flush
/// and the snapshot process,
/// and this is what allows us not to hold large table mutex when writing to the wal.
pub struct WalTracker {
    jh: Option<thread::JoinHandle<()>>,
    sender: Option<mpsc::Sender<WalTrackerMessage>>,
    last_processed: Arc<AtomicU64>,
    mapper: Arc<WalMapper>,
}

pub struct WalGuard {
    _guard: Rc<ArcMutexGuard<RawMutex, ()>>,
    wal_position: WalPosition,
}

pub struct WalGuardMaker {
    shared_guard: Rc<ArcMutexGuard<RawMutex, ()>>,
}

struct WalTrackerThread {
    receiver: mpsc::Receiver<WalTrackerMessage>,
    last_processed: Arc<AtomicU64>,
    state: WalTrackerState,
    mapper: Arc<WalMapper>,
    layout: WalLayout,
}

struct WalTrackerState {
    pending: BTreeMap<u64, AllocationResult>,
    last_processed: u64,
}

struct WalTrackerMessage {
    mutex: Arc<Mutex<()>>,
    kind: WalTrackerMessageKind,
}

enum WalTrackerMessageKind {
    AllocationMessage(AllocationResult),
    #[cfg(test)]
    Barrier(#[allow(dead_code)] LatchGuard),
}

impl WalTracker {
    pub fn start(layout: WalLayout, mapper: WalMapper, last_processed: u64) -> Self {
        let (sender, receiver) = mpsc::channel();
        let atomic_last_processed = Arc::new(AtomicU64::new(last_processed));
        let mapper = Arc::new(mapper);
        let thread = WalTrackerThread {
            receiver,
            state: WalTrackerState::new_empty(last_processed),
            last_processed: atomic_last_processed.clone(),
            mapper: mapper.clone(),
            layout,
        };
        let jh = thread::Builder::new()
            .name("wal-tracker".to_string())
            .spawn(move || thread.run())
            .expect("failed to start wal-tracker thread");
        Self {
            jh: Some(jh),
            sender: Some(sender),
            last_processed: atomic_last_processed,
            mapper,
        }
    }

    pub fn allocated(&self, allocation_result: AllocationResult) -> WalGuardMaker {
        let mutex = Arc::new(Mutex::new(()));
        let guard = mutex.lock_arc();
        let message = WalTrackerMessage {
            mutex,
            kind: WalTrackerMessageKind::AllocationMessage(allocation_result),
        };
        self.sender
            .as_ref()
            .expect("WalTracker already dropped")
            .send(message)
            .ok();
        WalGuardMaker {
            shared_guard: Rc::new(guard),
        }
    }

    pub fn last_processed(&self) -> u64 {
        self.last_processed.load(Ordering::SeqCst)
    }

    pub fn min_wal_position_updated(&self, watermark: u64) {
        self.mapper.min_wal_position_updated(watermark);
    }

    #[cfg(test)]
    pub fn barrier(&self) {
        use crate::latch::Latch;
        let (latch, guard) = Latch::new();
        let mutex = Arc::new(Mutex::new(()));
        let message = WalTrackerMessage {
            mutex,
            kind: WalTrackerMessageKind::Barrier(guard),
        };
        self.sender
            .as_ref()
            .expect("WalTracker already dropped")
            .send(message)
            .ok();
        latch.latch();
    }
}

impl Drop for WalTracker {
    fn drop(&mut self) {
        // Drop the sender first to signal the tracker thread to exit
        self.sender.take();
        // Wait for the tracker thread to complete with a timeout
        if let Some(jh) = self.jh.take() {
            crate::thread_util::join_thread_with_timeout(jh, "wal-tracker", 10);
        }
        // WalMapper's Drop will be called when the tracker thread exits,
        // which in turn waits for the mapper thread to complete
    }
}

impl WalGuard {
    pub fn wal_position(&self) -> &WalPosition {
        &self.wal_position
    }

    /// Consumes the guard and returns the WalPosition, immediately dropping the guard
    pub fn into_wal_position(self) -> WalPosition {
        self.wal_position
    }

    /// Create a guard for replay that doesn't track position updates
    pub fn replay_guard(position: WalPosition) -> Self {
        // Create a dummy mutex that's already locked
        let mutex = Arc::new(Mutex::new(()));
        let guard = mutex.lock_arc();
        Self {
            _guard: Rc::new(guard),
            wal_position: position,
        }
    }
}

impl WalGuardMaker {
    pub fn guard(&self, position: WalPosition) -> WalGuard {
        WalGuard {
            _guard: Rc::clone(&self.shared_guard),
            wal_position: position,
        }
    }
}

impl WalTrackerThread {
    pub fn run(mut self) {
        for message in self.receiver {
            #[allow(clippy::let_underscore_lock)]
            let _ = message.mutex.lock();
            let position = match message.kind {
                WalTrackerMessageKind::AllocationMessage(message) => {
                    self.state.add_processed(message, |result| {
                        if let Some(frag) =
                            self.layout.is_first_in_frag(result.allocated_position())
                        {
                            if let Some(prev_frag) = frag.checked_sub(1) {
                                // When the first position for frag is allocated and all positions before it are processed,
                                // Then the previous fragment can be finalized.
                                self.mapper.map_finalized(prev_frag);
                            }
                        }
                    })
                }
                #[cfg(test)]
                WalTrackerMessageKind::Barrier(_) => {
                    // Drop a barrier here
                    continue;
                }
            };
            if let Some(position) = position {
                self.last_processed.store(position, Ordering::SeqCst);
            }
        }
    }
}

impl WalTrackerState {
    pub fn new_empty(last_processed: u64) -> Self {
        Self {
            pending: Default::default(),
            last_processed,
        }
    }

    pub fn add_processed<F: FnMut(&AllocationResult)>(
        &mut self,
        mut result: AllocationResult,
        mut callback: F,
    ) -> Option<u64> {
        let previous_position = result.previous_position();
        if self.last_processed != previous_position {
            self.pending.insert(result.previous_position(), result);
            return None;
        }
        callback(&result);
        while let Some(next_result) = self.pending.remove(&result.next_position()) {
            result = next_result;
            callback(&result);
        }
        self.last_processed = result.next_position();
        Some(self.last_processed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal_allocator::WalAllocator;
    use crate::WalLayout;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_tracker_state() {
        let mut state = WalTrackerState::new_empty(0);
        let layout = WalLayout::new_simple(32);
        let allocator = WalAllocator::new(layout, 0);
        let a = allocator.allocate(12);
        let b = allocator.allocate(6);
        assert!(b.need_skip_marker().is_none());
        let c = allocator.allocate(17);
        assert!(c.need_skip_marker().is_some());

        state.add_processed(a.clone(), |_| {});
        state.add_processed(b.clone(), |_| {});
        state.add_processed(c.clone(), |_| {});
        assert_eq!(state.last_processed, c.next_position());
        assert!(state.pending.is_empty());

        let mut state = WalTrackerState::new_empty(0);
        state.add_processed(b.clone(), |_| {});
        assert_eq!(state.last_processed, 0);
        state.add_processed(a.clone(), |_| {});
        assert_eq!(state.last_processed, b.next_position());
        state.add_processed(c.clone(), |_| {});
        assert_eq!(state.last_processed, c.next_position());
        assert!(state.pending.is_empty());

        let mut state = WalTrackerState::new_empty(0);
        state.add_processed(c.clone(), |_| {});
        assert_eq!(state.last_processed, 0);
        state.add_processed(b.clone(), |_| {});
        assert_eq!(state.last_processed, 0);
        state.add_processed(a.clone(), |_| {});
        assert_eq!(state.last_processed, c.next_position());
        assert!(state.pending.is_empty());
    }

    #[test]
    fn test_multiple_guards_ordering() {
        let layout = WalLayout::new_simple(32);
        let allocator = WalAllocator::new(layout.clone(), 0);
        let a = allocator.allocate(8);
        let b = allocator.allocate(9);
        let c = allocator.allocate(10);

        let test = |tracker: WalTracker,
                    guard1: WalGuardMaker,
                    guard2: WalGuardMaker,
                    guard3: WalGuardMaker| {
            drop(guard3);
            thread::sleep(Duration::from_millis(10));
            assert_eq!(tracker.last_processed(), 0);

            drop(guard1);
            thread::sleep(Duration::from_millis(10));
            assert_eq!(tracker.last_processed(), a.next_position());

            drop(guard2);
            tracker.barrier();
            assert_eq!(tracker.last_processed(), c.next_position());
        };

        // Test when messages are sent to tracker in order positions are allocated
        let tracker = WalTracker::start(layout.clone(), WalMapper::new_unstarted().0, 0);
        let guard1 = tracker.allocated(a.clone());
        let guard2 = tracker.allocated(b.clone());
        let guard3 = tracker.allocated(c.clone());
        test(tracker, guard1, guard2, guard3);

        // Test tracker when messages are sent to tracker in different order
        let tracker = WalTracker::start(layout.clone(), WalMapper::new_unstarted().0, 0);
        let guard1 = tracker.allocated(a.clone());
        let guard3 = tracker.allocated(c.clone());
        let guard2 = tracker.allocated(b.clone());
        test(tracker, guard1, guard2, guard3);

        let tracker = WalTracker::start(layout.clone(), WalMapper::new_unstarted().0, 0);
        let guard3 = tracker.allocated(c.clone());
        let guard2 = tracker.allocated(b.clone());
        let guard1 = tracker.allocated(a.clone());

        drop(guard3);
        thread::sleep(Duration::from_millis(10));
        assert_eq!(tracker.last_processed(), 0);

        drop(guard1);
        thread::sleep(Duration::from_millis(10));
        // Even thought guard1 is dropped, because guard2
        // is sent before guard1, it blocks the tracker thread, and a message is not processed
        assert_eq!(tracker.last_processed(), 0);

        drop(guard2);
        tracker.barrier();
        // When all guards blocked the state is correct
        assert_eq!(tracker.last_processed(), c.next_position());
    }
}
