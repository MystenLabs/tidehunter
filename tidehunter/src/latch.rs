use parking_lot::{ArcMutexGuard, Mutex, RawMutex};
use std::sync::Arc;

pub struct Latch(Arc<Mutex<()>>);

pub struct LatchGuard(#[allow(dead_code)] ArcMutexGuard<RawMutex, ()>);

impl Latch {
    pub fn new() -> (Latch, LatchGuard) {
        let mutex = Arc::new(Mutex::new(()));
        let guard = mutex.lock_arc();
        (Latch(mutex), LatchGuard(guard))
    }
}

impl Latch {
    // This call blocks until the associated LatchGuard is dropped
    pub fn latch(&self) {
        let _ = self.0.lock();
    }
}

// Rather than enable send_guard feature globally in parking_lot,
// using this just for the latch
unsafe impl Send for LatchGuard {}
