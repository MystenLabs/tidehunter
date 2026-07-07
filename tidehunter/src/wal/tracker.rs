use super::WalPosition;
use super::position::LastProcessed;
use crate::WalLayout;
use crate::latch::LatchGuard;
use crate::metrics::{MetricIntGauge, Metrics, TimerExt};
use crate::wal::allocator::AllocationResult;
use crate::wal::mapper::WalMapper;
use parking_lot::{ArcMutexGuard, Mutex, RawMutex};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
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

/// A latch pins the externally observed `last_processed` position.
///
/// While at least one latch is held, the value returned by
/// [`WalTracker::last_processed`] is not advanced past the minimum position
/// captured by any active latch, even though the tracker keeps processing
/// allocations internally. This keeps in-memory index updates above that
/// position considered unprocessed (they stay in the BTreeMap and are not
/// promoted into flat). When all latches are released, the external value
/// catches up to the internal one.
///
/// The latch is a plain token owned by the tracker: it is constructed by the
/// tracker thread, returned from [`WalTracker::latch`], and released by handing
/// it back to [`WalTracker::release_latch`].
///
/// This is a position-pinning latch and is unrelated to the synchronization
/// primitive [`crate::latch::Latch`].
#[derive(Clone)]
pub struct WalTrackerLatch {
    id: u64,
    position: LastProcessed,
}

struct WalTrackerThread {
    receiver: mpsc::Receiver<WalTrackerMessage>,
    /// Externally observed `last_processed`, read by [`WalTracker::last_processed`].
    /// Capped by active latches; equals the internal value when no latch is held.
    last_processed: Arc<AtomicU64>,
    state: WalTrackerState,
    /// Active latches keyed by `(captured position, id)`. The first element
    /// gives the minimum latched position that caps the external value.
    latches: BTreeSet<(LastProcessed, u64)>,
    next_latch_id: u64,
    /// Latch requests whose reply is deferred until `last_processed` reaches
    /// their captured frontier (so every position below the latch position is
    /// processed before the caller receives the latch).
    pending_requests: Vec<PendingLatchRequest>,
    mapper: Arc<WalMapper>,
    layout: WalLayout,
    metrics: Arc<Metrics>,
    /// Gauge handles resolved once for this tracker's fixed `kind` label, so
    /// `publish()` avoids a `with_label_values` lookup on every WAL write.
    metric_external: MetricIntGauge,
    metric_internal: MetricIntGauge,
    metric_active_latches: MetricIntGauge,
}

struct PendingLatchRequest {
    /// The captured frontier; the reply fires once `last_processed >= target`.
    target: u64,
    id: u64,
    response: mpsc::Sender<WalTrackerLatch>,
}

struct WalTrackerState {
    pending: BTreeMap<u64, AllocationResult>,
    last_processed: LastProcessed,
    /// The highest position the tracker has ever received (the end position of
    /// any seen allocation, whether already processed or still in `pending`).
    /// Equals `last_processed` when `pending` is empty, and is strictly greater
    /// while there are out-of-order allocations awaiting a gap to be filled.
    highest_seen: u64,
}

enum WalTrackerMessage {
    /// An allocation to be processed. The `mutex` is locked by `allocated()`
    /// and held (via `WalGuard`) until the Db records the position in the
    /// in-memory index; the tracker thread blocks on it before processing, so
    /// the position is only accounted once it is durably in the index.
    AllocationMessage {
        mutex: Arc<Mutex<()>>,
        result: AllocationResult,
    },
    /// Request a new latch. The tracker assigns an id, captures the current
    /// received frontier as the latch position, stores the latch, and replies
    /// with the [`WalTrackerLatch`]. The reply is deferred until every position
    /// below the frontier has been processed (`last_processed >= position`).
    LatchRequest(mpsc::Sender<WalTrackerLatch>),
    /// Release a previously acquired latch, handing it back to the tracker.
    LatchRelease(WalTrackerLatch),
    Barrier(#[allow(dead_code)] LatchGuard),
}

impl WalTracker {
    pub fn start(
        layout: WalLayout,
        mapper: WalMapper,
        last_processed: LastProcessed,
        metrics: Arc<Metrics>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel();
        let atomic_last_processed = Arc::new(AtomicU64::new(last_processed.as_u64()));
        let mapper = Arc::new(mapper);
        let kind = layout.kind.name();
        let metric_external = metrics
            .wal_last_processed_position
            .with_label_values(&[kind]);
        let metric_internal = metrics
            .wal_internal_last_processed_position
            .with_label_values(&[kind]);
        let metric_active_latches = metrics.wal_active_latches.with_label_values(&[kind]);
        let thread = WalTrackerThread {
            receiver,
            state: WalTrackerState::new_empty(last_processed),
            last_processed: atomic_last_processed.clone(),
            latches: BTreeSet::new(),
            next_latch_id: 0,
            pending_requests: Vec::new(),
            mapper: mapper.clone(),
            layout,
            metrics,
            metric_external,
            metric_internal,
            metric_active_latches,
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
        let message = WalTrackerMessage::AllocationMessage {
            mutex,
            result: allocation_result,
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

    pub fn last_processed(&self) -> LastProcessed {
        LastProcessed::new(self.last_processed.load(Ordering::SeqCst))
    }

    /// Acquires a latch that pins the externally observed `last_processed`.
    ///
    /// The latch position is the received frontier at the time the request is
    /// processed — by FIFO, every allocation submitted before this call,
    /// including any still out of order. The call blocks until all of those
    /// positions are processed internally (`last_processed` reaches the
    /// frontier), so on return every position below
    /// [`WalTrackerLatch::position`] is in the in-memory index. While the
    /// returned [`WalTrackerLatch`] is held, [`Self::last_processed`] will not
    /// advance past that position. Release it with [`Self::release_latch`].
    ///
    /// The caller must not hold a `WalGuard` for a position below the frontier,
    /// or the awaited position can never be processed and the call deadlocks.
    pub fn latch(&self) -> WalTrackerLatch {
        let (response, response_rx) = mpsc::channel();
        let message = WalTrackerMessage::LatchRequest(response);
        let sender = self.sender.as_ref().expect("WalTracker already dropped");
        sender.send(message).ok();
        response_rx
            .recv()
            .expect("WalTracker thread terminated before replying to latch request")
    }

    /// Releases a latch acquired via [`Self::latch`], handing it back to the
    /// tracker so the externally observed `last_processed` can advance past the
    /// latch's position once no other latch pins it. Best-effort: if the tracker
    /// thread is already gone there is nothing to release.
    pub fn release_latch(&self, latch: WalTrackerLatch) {
        if let Some(sender) = self.sender.as_ref() {
            sender.send(WalTrackerMessage::LatchRelease(latch)).ok();
        }
    }

    pub fn min_wal_position_updated(&self, watermark: u64) {
        self.mapper.min_wal_position_updated(watermark);
    }

    pub fn delete_files(&self, files: Vec<super::position::WalFileId>) {
        self.mapper.delete_files(files);
    }

    /// Returns `true` while the underlying mapper thread is running.
    /// Used by `WalWriter::get_writeable_map` to surface a mapper panic
    /// (which would otherwise leave the writer spinning forever) as a
    /// loud panic on the writer thread.
    pub(crate) fn is_mapper_alive(&self) -> bool {
        self.mapper.is_alive()
    }

    pub fn barrier(&self) {
        use crate::latch::Latch;
        let (latch, guard) = Latch::new();
        let message = WalTrackerMessage::Barrier(guard);
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

impl WalTrackerLatch {
    /// The captured frontier: every position below it was processed internally
    /// before the latch was returned, and the externally observed
    /// `last_processed` will not advance past it while the latch is held.
    pub fn position(&self) -> LastProcessed {
        self.position
    }
}

impl WalTrackerThread {
    pub fn run(mut self) {
        // `recv()` borrows only `self.receiver` for the duration of the call,
        // leaving the loop body free to borrow `self` (e.g. `self.publish()`
        // and `self.fulfill_pending_requests()`). The loop ends when all
        // senders are dropped and `recv()` returns `Err`.
        while let Ok(message) = self.receiver.recv() {
            let _timer = self.metrics.wal_tracker_time_mcs.clone().mcs_timer();
            match message {
                WalTrackerMessage::AllocationMessage { mutex, result } => {
                    // Wait until every WalGuard for this allocation is dropped,
                    // i.e. the position is recorded in the in-memory index.
                    #[allow(clippy::let_underscore_lock)]
                    let _ = mutex.lock();
                    let advanced = self.state.add_processed(result, |processed| {
                        if let Some(frag) =
                            self.layout.is_first_in_frag(processed.allocated_position())
                            && let Some(prev_frag) = frag.prev_map()
                        {
                            // When the first position for frag is allocated and all positions before it are processed,
                            // Then the previous fragment can be finalized.
                            self.mapper.map_finalized(prev_frag);
                        }
                    });
                    // Only an allocation that advances last_processed can
                    // change the external value or satisfy a deferred request.
                    if advanced.is_some() {
                        if !self.pending_requests.is_empty() {
                            self.fulfill_pending_requests();
                        }
                        self.publish();
                    }
                }
                WalTrackerMessage::LatchRequest(response) => {
                    let id = self.next_latch_id;
                    self.next_latch_id += 1;
                    // Capture the received frontier. The latch is inserted now
                    // (not at reply time) so it caps the external value during
                    // the wait; the reply is deferred until last_processed
                    // reaches the frontier. `highest_seen` is monotonic, so the
                    // inserted position never lowers the existing latch minimum.
                    let target = self.state.highest_seen;
                    let position = LastProcessed::new(target);
                    self.latches.insert((position, id));
                    if self.state.last_processed.as_u64() >= target {
                        response.send(WalTrackerLatch { id, position }).ok();
                    } else {
                        self.pending_requests.push(PendingLatchRequest {
                            target,
                            id,
                            response,
                        });
                    }
                    self.publish();
                }
                WalTrackerMessage::LatchRelease(latch) => {
                    self.latches.remove(&(latch.position, latch.id));
                    self.publish();
                }
                WalTrackerMessage::Barrier(_) => {
                    // Drop the barrier guard to unblock the caller. A barrier
                    // changes no state, so it does not publish.
                }
            }
        }
    }

    /// Recomputes the externally observed `last_processed` and updates the
    /// atomic and gauges.
    ///
    /// The external value is `min(internal, min position of any active latch)`.
    /// Because latch positions are captured from the monotonic `highest_seen`,
    /// the latch minimum is non-decreasing; combined with the monotonic
    /// internal value, the external value is monotonically non-decreasing.
    fn publish(&self) {
        let internal = self.state.last_processed;
        let external = self
            .latches
            .first()
            .map_or(internal, |(min_position, _)| internal.min(*min_position));
        self.last_processed
            .store(external.as_u64(), Ordering::SeqCst);
        self.metric_external.set(external.as_u64() as i64);
        self.metric_internal.set(internal.as_u64() as i64);
        self.metric_active_latches.set(self.latches.len() as i64);
    }

    /// Replies to any deferred latch request whose captured frontier is now
    /// fully processed (`target <= last_processed`). Called after each
    /// allocation that advances `last_processed`.
    fn fulfill_pending_requests(&mut self) {
        let last_processed = self.state.last_processed.as_u64();
        self.pending_requests.retain(|request| {
            if request.target <= last_processed {
                request
                    .response
                    .send(WalTrackerLatch {
                        id: request.id,
                        position: LastProcessed::new(request.target),
                    })
                    .ok();
                false
            } else {
                true
            }
        });
    }
}

impl WalTrackerState {
    pub fn new_empty(last_processed: LastProcessed) -> Self {
        Self {
            pending: Default::default(),
            last_processed,
            highest_seen: last_processed.as_u64(),
        }
    }

    pub fn add_processed<F: FnMut(&AllocationResult)>(
        &mut self,
        mut result: AllocationResult,
        mut callback: F,
    ) -> Option<LastProcessed> {
        self.highest_seen = self.highest_seen.max(result.next_position());
        let previous_position = result.previous_position();
        if self.last_processed.as_u64() != previous_position {
            self.pending.insert(result.previous_position(), result);
            return None;
        }
        callback(&result);
        while let Some(next_result) = self.pending.remove(&result.next_position()) {
            result = next_result;
            callback(&result);
        }
        self.last_processed = LastProcessed::new(result.next_position());
        Some(self.last_processed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WalLayout;
    use crate::wal_allocator::WalAllocator;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_tracker_state() {
        let mut state = WalTrackerState::new_empty(LastProcessed::new(0));
        let layout = WalLayout::new_simple(32);
        let allocator = WalAllocator::new(layout, 0);
        let a = allocator.allocate(12);
        let b = allocator.allocate(6);
        assert!(b.need_skip_marker().is_none());
        let c = allocator.allocate(17);
        assert!(c.need_skip_marker().is_some());

        state.add_processed(a.clone(), |_| {});
        assert!(state.last_processed.is_processed(&a));
        assert!(!state.last_processed.is_processed(&b));
        assert!(!state.last_processed.is_processed(&c));

        state.add_processed(b.clone(), |_| {});
        assert!(state.last_processed.is_processed(&a));
        assert!(state.last_processed.is_processed(&b));
        assert!(!state.last_processed.is_processed(&c));

        state.add_processed(c.clone(), |_| {});
        assert!(state.last_processed.is_processed(&a));
        assert!(state.last_processed.is_processed(&b));
        assert!(state.last_processed.is_processed(&c));

        assert_eq!(state.last_processed.as_u64(), c.next_position());
        assert!(state.pending.is_empty());

        let mut state = WalTrackerState::new_empty(LastProcessed::new(0));
        state.add_processed(b.clone(), |_| {});
        assert_eq!(state.last_processed.as_u64(), 0);
        state.add_processed(a.clone(), |_| {});
        assert_eq!(state.last_processed.as_u64(), b.next_position());
        state.add_processed(c.clone(), |_| {});
        assert_eq!(state.last_processed.as_u64(), c.next_position());
        assert!(state.pending.is_empty());

        let mut state = WalTrackerState::new_empty(LastProcessed::new(0));
        state.add_processed(c.clone(), |_| {});
        assert_eq!(state.last_processed.as_u64(), 0);
        state.add_processed(b.clone(), |_| {});
        assert_eq!(state.last_processed.as_u64(), 0);
        state.add_processed(a.clone(), |_| {});
        assert_eq!(state.last_processed.as_u64(), c.next_position());
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
            assert_eq!(tracker.last_processed().as_u64(), 0);

            drop(guard1);
            thread::sleep(Duration::from_millis(10));
            assert_eq!(tracker.last_processed().as_u64(), a.next_position());

            drop(guard2);
            tracker.barrier();
            assert_eq!(tracker.last_processed().as_u64(), c.next_position());
        };

        // Test when messages are sent to tracker in order positions are allocated
        let tracker = WalTracker::start(
            layout.clone(),
            WalMapper::new_unstarted().0,
            LastProcessed::new(0),
            Metrics::new(),
        );
        let guard1 = tracker.allocated(a.clone());
        let guard2 = tracker.allocated(b.clone());
        let guard3 = tracker.allocated(c.clone());
        test(tracker, guard1, guard2, guard3);

        // Test tracker when messages are sent to tracker in different order
        let tracker = WalTracker::start(
            layout.clone(),
            WalMapper::new_unstarted().0,
            LastProcessed::new(0),
            Metrics::new(),
        );
        let guard1 = tracker.allocated(a.clone());
        let guard3 = tracker.allocated(c.clone());
        let guard2 = tracker.allocated(b.clone());
        test(tracker, guard1, guard2, guard3);

        let tracker = WalTracker::start(
            layout.clone(),
            WalMapper::new_unstarted().0,
            LastProcessed::new(0),
            Metrics::new(),
        );
        let guard3 = tracker.allocated(c.clone());
        let guard2 = tracker.allocated(b.clone());
        let guard1 = tracker.allocated(a.clone());

        drop(guard3);
        thread::sleep(Duration::from_millis(10));
        assert_eq!(tracker.last_processed().as_u64(), 0);

        drop(guard1);
        thread::sleep(Duration::from_millis(10));
        // Even thought guard1 is dropped, because guard2
        // is sent before guard1, it blocks the tracker thread, and a message is not processed
        assert_eq!(tracker.last_processed().as_u64(), 0);

        drop(guard2);
        tracker.barrier();
        // When all guards blocked the state is correct
        assert_eq!(tracker.last_processed().as_u64(), c.next_position());
    }

    #[test]
    fn test_no_latch_external_tracks_internal() {
        let layout = WalLayout::new_simple(32);
        let allocator = WalAllocator::new(layout.clone(), 0);
        let a = allocator.allocate(8);
        let b = allocator.allocate(9);

        let tracker = WalTracker::start(
            layout.clone(),
            WalMapper::new_unstarted().0,
            LastProcessed::new(0),
            Metrics::new(),
        );
        assert_eq!(tracker.last_processed().as_u64(), 0);

        drop(tracker.allocated(a.clone()));
        tracker.barrier();
        assert_eq!(tracker.last_processed().as_u64(), a.next_position());

        drop(tracker.allocated(b.clone()));
        tracker.barrier();
        assert_eq!(tracker.last_processed().as_u64(), b.next_position());
    }

    #[test]
    fn test_latch_pins_external_last_processed() {
        let layout = WalLayout::new_simple(32);
        let allocator = WalAllocator::new(layout.clone(), 0);
        let a = allocator.allocate(8);
        let b = allocator.allocate(9);
        let c = allocator.allocate(10);

        let tracker = WalTracker::start(
            layout.clone(),
            WalMapper::new_unstarted().0,
            LastProcessed::new(0),
            Metrics::new(),
        );

        // Process `a`: internal and external advance to a.next_position().
        drop(tracker.allocated(a.clone()));
        tracker.barrier();
        assert_eq!(tracker.last_processed().as_u64(), a.next_position());

        // Take a latch: it captures the current internal last_processed.
        let latch = tracker.latch();
        assert_eq!(latch.position().as_u64(), a.next_position());
        assert_eq!(tracker.last_processed().as_u64(), a.next_position());

        // Process `b` and `c`: internal advances, but external is pinned by the latch.
        drop(tracker.allocated(b.clone()));
        drop(tracker.allocated(c.clone()));
        tracker.barrier();
        assert_eq!(tracker.last_processed().as_u64(), a.next_position());

        // Release the latch: external catches up to the internal value.
        tracker.release_latch(latch);
        tracker.barrier();
        assert_eq!(tracker.last_processed().as_u64(), c.next_position());
    }

    #[test]
    fn test_multiple_latches() {
        let layout = WalLayout::new_simple(32);
        let allocator = WalAllocator::new(layout.clone(), 0);
        let a = allocator.allocate(8);
        let b = allocator.allocate(9);
        let c = allocator.allocate(10);

        let tracker = WalTracker::start(
            layout.clone(),
            WalMapper::new_unstarted().0,
            LastProcessed::new(0),
            Metrics::new(),
        );

        // internal -> a.next_position(); take the older latch l0 here.
        drop(tracker.allocated(a.clone()));
        tracker.barrier();
        let l0 = tracker.latch();
        assert_eq!(l0.position().as_u64(), a.next_position());

        // internal -> b.next_position(); external pinned at l0's position.
        drop(tracker.allocated(b.clone()));
        tracker.barrier();
        assert_eq!(tracker.last_processed().as_u64(), a.next_position());

        // Take the newer latch l1 at b.next_position(); external still pinned at l0.
        let l1 = tracker.latch();
        assert_eq!(l1.position().as_u64(), b.next_position());
        assert_eq!(tracker.last_processed().as_u64(), a.next_position());

        // internal -> c.next_position(); external still pinned at l0.
        drop(tracker.allocated(c.clone()));
        tracker.barrier();
        assert_eq!(tracker.last_processed().as_u64(), a.next_position());

        // Release the older latch: external moves to min(internal, l1.position).
        tracker.release_latch(l0);
        tracker.barrier();
        assert_eq!(tracker.last_processed().as_u64(), b.next_position());

        // Release the last latch: external catches up to internal.
        tracker.release_latch(l1);
        tracker.barrier();
        assert_eq!(tracker.last_processed().as_u64(), c.next_position());
    }

    #[test]
    fn test_latch_waits_for_prior_positions() {
        use std::sync::atomic::AtomicBool;

        let layout = WalLayout::new_simple(32);
        let allocator = WalAllocator::new(layout.clone(), 0);
        let a = allocator.allocate(8);
        let b = allocator.allocate(9);
        let c = allocator.allocate(10);

        let tracker = WalTracker::start(
            layout.clone(),
            WalMapper::new_unstarted().0,
            LastProcessed::new(0),
            Metrics::new(),
        );

        // Process `a`, then deliver `c` out of order so it sits in `pending`
        // with a hole at `b`. The received frontier is now `c.next_position()`
        // but last_processed is only `a.next_position()`.
        drop(tracker.allocated(a.clone()));
        tracker.barrier();
        drop(tracker.allocated(c.clone()));
        tracker.barrier();
        assert_eq!(tracker.last_processed().as_u64(), a.next_position());

        let done = Arc::new(AtomicBool::new(false));
        std::thread::scope(|s| {
            let thread_done = done.clone();
            let tracker = &tracker;
            let handle = s.spawn(move || {
                // Blocks until the hole at `b` is filled and last_processed
                // reaches the captured frontier (c.next_position()).
                let latch = tracker.latch();
                thread_done.store(true, Ordering::SeqCst);
                let position = latch.position().as_u64();
                tracker.release_latch(latch);
                position
            });

            // The latch request is now deferred: the hole at `b` is not filled,
            // so the call must not have returned.
            thread::sleep(Duration::from_millis(50));
            assert!(
                !done.load(Ordering::SeqCst),
                "latch returned before the hole below its frontier was filled"
            );

            // Fill the hole: `b` drains `b` and `c`, last_processed reaches the
            // frontier, and the deferred latch request is answered.
            drop(tracker.allocated(b.clone()));
            tracker.barrier();

            let position = handle.join().unwrap();
            assert!(done.load(Ordering::SeqCst));
            assert_eq!(position, c.next_position());
        });
    }
}
