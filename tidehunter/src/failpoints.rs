use crate::latch::Latch;
use rand::Rng;
use rand::prelude::ThreadRng;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

pub(crate) struct FailPoint {
    fp: Box<dyn Fn() -> () + Send + Sync + 'static>,
}

impl Default for FailPoint {
    fn default() -> Self {
        Self {
            fp: Box::new(|| {}),
        }
    }
}

impl FailPoint {
    pub fn sleep(range: Range<Duration>) -> Self {
        let fp = Box::new(move || {
            let mut rng = ThreadRng::default();
            let delay = rng.gen_range(range.clone());
            thread::sleep(delay)
        });
        Self { fp }
    }

    /// Failpoint that blocks until provided latch is unlocked
    pub fn latch(latch: Latch) -> Self {
        let fp = Box::new(move || {
            latch.latch();
        });
        Self { fp }
    }

    /// Failpoint that panics after being called N times
    pub fn panic_after_n_calls(n: usize) -> Self {
        let counter = AtomicUsize::new(0);
        let fp = Box::new(move || {
            let count = counter.fetch_add(1, Ordering::SeqCst);
            if count >= n {
                panic!("failpoint: panic_after_n_calls triggered after {} calls", n);
            }
        });
        Self { fp }
    }

    pub fn fp(&self) {
        (self.fp)()
    }
}
