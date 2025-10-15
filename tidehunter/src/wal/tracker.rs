use super::WalPosition;
#[cfg(test)]
use crate::latch::LatchGuard;
use parking_lot::{ArcMutexGuard, Mutex, RawMutex};
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
}

struct WalTrackerMessage {
    mutex: Arc<Mutex<()>>,
    kind: WalTrackerMessageKind,
}

enum WalTrackerMessageKind {
    Position(u64),
    #[cfg(test)]
    Barrier(#[allow(dead_code)] LatchGuard),
}

impl WalTracker {
    pub fn start(last_processed: u64) -> Self {
        let (sender, receiver) = mpsc::channel();
        let last_processed = Arc::new(AtomicU64::new(last_processed));
        let thread = WalTrackerThread {
            receiver,
            last_processed: last_processed.clone(),
        };
        thread::spawn(move || thread.run());
        Self {
            sender,
            last_processed,
        }
    }

    pub fn new_batch(&self, end_position: u64) -> WalGuardMaker {
        let mutex = Arc::new(Mutex::new(()));
        let guard = mutex.lock_arc();
        let message = WalTrackerMessage {
            mutex,
            kind: WalTrackerMessageKind::Position(end_position),
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
    pub fn run(self) {
        for message in self.receiver {
            // Note that messages are sent when wal position is allocated,
            // within the scope of position mutex in the wal.
            // This means messages are sent(and received!) in the same order positions are allocated in wal.
            // Therefore, we don't need to maintain BTree or similar structure here of all unprocessed wal positions,
            // And simply blocking until received guard is dropped is enough to get correct last_processed.
            #[allow(clippy::let_underscore_lock)]
            let _ = message.mutex.lock();
            match message.kind {
                WalTrackerMessageKind::Position(position) => {
                    self.last_processed.store(position, Ordering::SeqCst)
                }
                #[cfg(test)]
                WalTrackerMessageKind::Barrier(_) => {
                    // Drop a barrier here
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalPosition;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_wal_tracker_basic() {
        let tracker = WalTracker::start(0);

        // Initially last_processed should be 0
        assert_eq!(tracker.last_processed(), 0);

        // Create a batch and guard at position 100
        let batch = tracker.new_batch(100);
        let pos = WalPosition::new(100, 10);
        let guard = batch.guard(pos);

        // Guard should contain the correct position
        assert_eq!(guard.wal_position(), &pos);

        // Drop the guard and wait a bit for processing
        drop(guard);
        drop(batch);
        thread::sleep(Duration::from_millis(10));

        // last_processed should be updated
        assert_eq!(tracker.last_processed(), 100);
    }

    #[test]
    fn test_wal_batch() {
        let tracker = WalTracker::start(0);

        // Create a batch
        let batch = tracker.new_batch(200);

        // Create guards from the batch
        let pos1 = WalPosition::new(150, 5);
        let pos2 = WalPosition::new(180, 8);
        let guard1 = batch.guard(pos1);
        let guard2 = batch.guard(pos2);

        // Guards should contain correct positions
        assert_eq!(guard1.wal_position(), &pos1);
        assert_eq!(guard2.wal_position(), &pos2);

        // Drop guards and wait for processing
        drop(guard1);
        drop(guard2);
        drop(batch);
        thread::sleep(Duration::from_millis(10));

        // last_processed should be updated to the batch end position
        assert_eq!(tracker.last_processed(), 200);
    }

    #[test]
    fn test_multiple_guards_ordering() {
        let tracker = WalTracker::start(0);

        // Create batches in order (this determines channel message order)
        let batch1 = tracker.new_batch(100);
        let batch2 = tracker.new_batch(200);
        let batch3 = tracker.new_batch(300);

        // Drop in reverse order - but processing is blocked by channel ordering
        drop(batch3); // Unlocks position 300, but channel must process 100 first
        thread::sleep(Duration::from_millis(10));
        assert_eq!(tracker.last_processed(), 0); // No change, blocked on position 100

        drop(batch1); // Unlocks position 100, allows processing of 100, but blocked on 200
        thread::sleep(Duration::from_millis(10));
        assert_eq!(tracker.last_processed(), 100); // Can process 100 now

        drop(batch2); // Unlocks position 200, allows processing of 200 and 300
        thread::sleep(Duration::from_millis(50));
        assert_eq!(tracker.last_processed(), 300); // Processes 200, then 300
    }
}
