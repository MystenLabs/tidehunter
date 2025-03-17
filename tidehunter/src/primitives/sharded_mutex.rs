use parking_lot::{Mutex, MutexGuard};
use prometheus::Histogram;
use std::time::Instant;

pub struct ShardedMutex<V>(Box<[Mutex<V>]>);

impl<V> ShardedMutex<V> {
    pub fn from_iterator(v: impl Iterator<Item = V>) -> Self {
        let arr = v.map(Mutex::new).collect::<Vec<_>>().into_boxed_slice();
        Self(arr)
    }

    pub fn lock(&self, n: usize, metric: &Histogram) -> MutexGuard<'_, V> {
        let mutex = &self.0[n % self.0.len()];
        if let Some(lock) = mutex.try_lock() {
            return lock;
        }
        let now = Instant::now();
        // todo move tokio dep under a feature
        let lock = tokio::task::block_in_place(|| mutex.lock());
        metric.observe(now.elapsed().as_micros() as f64);
        lock
    }

    pub fn mutexes(&self) -> &[Mutex<V>] {
        &self.0
    }
}

impl<V> AsRef<[Mutex<V>]> for ShardedMutex<V> {
    fn as_ref(&self) -> &[Mutex<V>] {
        &self.0
    }
}
