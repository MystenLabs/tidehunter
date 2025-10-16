use super::WalPosition;
#[cfg(test)]
use crate::latch::LatchGuard;
use crate::wal::allocator::AllocationResult;
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
#[derive(Clone)]
pub struct WalTracker {
    sender: mpsc::Sender<WalTrackerMessage>,
    last_processed: Arc<AtomicU64>,
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
}

struct WalTrackerState {
    pending: BTreeMap<u64, u64>,
    last_processed: u64,
}

struct WalTrackerMessage {
    mutex: Arc<Mutex<()>>,
    kind: WalTrackerMessageKind,
}

struct AllocationMessage {
    previous_position: u64,
    next_position: u64,
}

enum WalTrackerMessageKind {
    AllocationMessage(AllocationMessage),
    #[cfg(test)]
    Barrier(#[allow(dead_code)] LatchGuard),
}

impl WalTracker {
    pub fn start(last_processed: u64) -> Self {
        let (sender, receiver) = mpsc::channel();
        let atomic_last_processed = Arc::new(AtomicU64::new(last_processed));
        let thread = WalTrackerThread {
            receiver,
            state: WalTrackerState::new_empty(last_processed),
            last_processed: atomic_last_processed.clone(),
        };
        thread::spawn(move || thread.run());
        Self {
            sender,
            last_processed: atomic_last_processed,
        }
    }

    pub fn allocated(&self, allocation_result: &AllocationResult) -> WalGuardMaker {
        let mutex = Arc::new(Mutex::new(()));
        let guard = mutex.lock_arc();
        let allocation_message = AllocationMessage::from_allocation_result(allocation_result);
        let message = WalTrackerMessage {
            mutex,
            kind: WalTrackerMessageKind::AllocationMessage(allocation_message),
        };
        self.sender.send(message).ok();
        WalGuardMaker {
            shared_guard: Rc::new(guard),
        }
    }

    pub fn last_processed(&self) -> u64 {
        self.last_processed.load(Ordering::SeqCst)
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
        self.sender.send(message).ok();
        latch.latch();
    }
}

impl AllocationMessage {
    pub fn from_allocation_result(result: &AllocationResult) -> Self {
        Self {
            previous_position: result.previous_position(),
            next_position: result.next_position(),
        }
    }
}

impl WalGuard {
    pub fn wal_position(&self) -> &WalPosition {
        &self.wal_position
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
                    self.state.add_processed(&message)
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

    /// Add an allocation message to state, return new last_processed if it has changed
    pub fn add_processed(&mut self, result: &AllocationMessage) -> Option<u64> {
        let previous_position = result.previous_position();
        if self.last_processed != previous_position {
            self.pending
                .insert(result.previous_position(), result.next_position());
            return None;
        }
        let mut next_position = result.next_position();
        while let Some(position) = self.pending.remove(&next_position) {
            next_position = position;
        }
        self.last_processed = next_position;
        Some(self.last_processed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalPosition;
    use crate::wal_allocator::WalAllocator;
    use crate::WalLayout;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_tracker_state() {
        let mut state = WalTrackerState::new_empty(0);
        let layout = WalLayout::new_simple(32);
        let allocator = WalAllocator::new(layout, 0);
        let a = AllocationMessage::from_allocation_result(&allocator.allocate(12));
        let b = AllocationMessage::from_allocation_result(&allocator.allocate(6));
        assert!(b.need_skip_marker().is_none());
        let c = AllocationMessage::from_allocation_result(&allocator.allocate(17));
        assert!(c.need_skip_marker().is_some());

        state.add_processed(&a);
        state.add_processed(&b);
        state.add_processed(&c);
        assert_eq!(state.last_processed, c.next_position());
        assert!(state.pending.is_empty());

        let mut state = WalTrackerState::new_empty(0);
        state.add_processed(&b);
        assert_eq!(state.last_processed, 0);
        state.add_processed(&a);
        assert_eq!(state.last_processed, b.next_position());
        state.add_processed(&c);
        assert_eq!(state.last_processed, c.next_position());
        assert!(state.pending.is_empty());

        let mut state = WalTrackerState::new_empty(0);
        state.add_processed(&c);
        assert_eq!(state.last_processed, 0);
        state.add_processed(&b);
        assert_eq!(state.last_processed, 0);
        state.add_processed(&a);
        assert_eq!(state.last_processed, c.next_position());
        assert!(state.pending.is_empty());
    }

    // #[test]
    // fn test_wal_batch() {
    //     let tracker = WalTracker::start(0);
    //     let layout = WalLayout::new_simple(32);
    //     let allocator = WalAllocator::new(layout, 0);
    //
    //     // Create a batch
    //     let batch = tracker.allocated(200);
    //
    //     // Create guards from the batch
    //     let pos1 = WalPosition::new(150, 5);
    //     let pos2 = WalPosition::new(180, 8);
    //     let guard1 = batch.guard(pos1);
    //     let guard2 = batch.guard(pos2);
    //
    //     // Guards should contain correct positions
    //     assert_eq!(guard1.wal_position(), &pos1);
    //     assert_eq!(guard2.wal_position(), &pos2);
    //
    //     // Drop guards and wait for processing
    //     drop(guard1);
    //     drop(guard2);
    //     drop(batch);
    //     thread::sleep(Duration::from_millis(10));
    //
    //     // last_processed should be updated to the batch end position
    //     assert_eq!(tracker.last_processed(), 200);
    // }

    #[test]
    fn test_multiple_guards_ordering() {
        let layout = WalLayout::new_simple(32);
        let allocator = WalAllocator::new(layout, 0);
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
        let tracker = WalTracker::start(0);
        let guard1 = tracker.allocated(&a);
        let guard2 = tracker.allocated(&b);
        let guard3 = tracker.allocated(&c);
        test(tracker, guard1, guard2, guard3);

        // Test tracker when messages are sent to tracker in different order
        let tracker = WalTracker::start(0);
        let guard1 = tracker.allocated(&a);
        let guard3 = tracker.allocated(&c);
        let guard2 = tracker.allocated(&b);
        test(tracker, guard1, guard2, guard3);
    }
}
