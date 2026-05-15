pub mod rocks;
pub mod tidehunter;

use minibytes::Bytes;

pub trait Storage: Sync + Send + 'static {
    fn insert(&self, k: Bytes, v: Bytes);

    fn get(&self, k: &[u8]) -> Option<Bytes>;

    fn get_lt(&self, k: &[u8], iterations: usize) -> Vec<Bytes>;

    fn exists(&self, k: &[u8]) -> bool;

    fn delete(&self, k: Bytes);

    fn name(&self) -> &'static str;

    /// Diagnostic hook used by the R6 missing-key investigation. When the
    /// recovery benchmark observes a `MISSING_KEY`, it asks the backend for a
    /// per-cell debug string so we can correlate the loss against the
    /// snapshot's claimed coverage. Backends that don't have this info return
    /// `None`.
    fn debug_miss(&self, _k: &[u8]) -> Option<String> {
        None
    }

    /// WAL replay window for the most recent open: `(replay_from, tail)`.
    /// Only meaningful for the tidehunter backend; others return `None`.
    fn recovery_wal_window(&self) -> Option<(u64, u64)> {
        None
    }
}
