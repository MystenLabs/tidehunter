use crate::wal::WalLayout;
use rand::Rng;
use std::cmp;

// todo - remove pub
#[cfg_attr(test, derive(Clone))] // Look for Config::clone(...) for usages
pub struct Config {
    pub frag_size: u64,
    pub max_maps: usize,
    /// Maximum number of dirty keys per LargeTable entry before it's counted as loaded
    pub max_dirty_keys: usize,
    /// How often to take snapshot depending on the number of entries written to the wal
    pub snapshot_written_bytes: u64,
    /// Force unload dirty entry if it's distance from wal tail exceeds given value
    pub snapshot_unload_threshold: u64,
    /// Percentage for the unload jitter
    pub unload_jitter_pct: usize,
    /// Use O_DIRECT when working with wal
    pub direct_io: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            frag_size: 128 * 1024 * 1024,
            max_maps: 16, // Max 2 Gb mapped space
            max_dirty_keys: 16 * 1024,
            snapshot_written_bytes: 2 * 1024 * 1024 * 1024, // 2 Gb
            snapshot_unload_threshold: 2 * 2 * 1024 * 1024 * 1024, // 4 Gb
            unload_jitter_pct: 10,
            direct_io: false,
        }
    }
}

impl Config {
    pub fn small() -> Self {
        Self {
            frag_size: 1024 * 1024,
            max_maps: 16,
            max_dirty_keys: 32,
            snapshot_written_bytes: 128 * 1024 * 1024, // 128 Mb
            snapshot_unload_threshold: 2 * 128 * 1024 * 1024, // 256 Mb
            unload_jitter_pct: 10,
            direct_io: false,
        }
    }

    pub fn frag_size(&self) -> u64 {
        self.frag_size
    }

    pub(crate) fn wal_layout(&self) -> WalLayout {
        WalLayout {
            frag_size: self.frag_size,
            max_maps: self.max_maps,
            direct_io: self.direct_io,
        }
    }

    pub fn snapshot_written_bytes(&self) -> u64 {
        self.snapshot_written_bytes
    }

    pub fn gen_dirty_keys_jitter(&self, rng: &mut impl Rng) -> usize {
        rng.gen_range(0..self.max_dirty_keys_jitter())
    }

    fn max_dirty_keys_jitter(&self) -> usize {
        cmp::max(1, self.max_dirty_keys * self.unload_jitter_pct / 100)
    }

    pub fn snapshot_unload_threshold(&self) -> u64 {
        self.snapshot_unload_threshold
    }

    #[inline]
    pub fn excess_dirty_keys(&self, dirty_keys_count: usize) -> bool {
        dirty_keys_count > self.max_dirty_keys
    }

    pub fn direct_io(&self) -> bool {
        self.direct_io
    }
}
