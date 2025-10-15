use crate::latch::Latch;
use rand::prelude::ThreadRng;
use rand::Rng;
use std::ops::Range;
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

    pub fn fp(&self) {
        (self.fp)()
    }
}
