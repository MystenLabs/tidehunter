use crate::relocation::RelocationStrategy;
use crate::wal::layout::{WalKind, WalLayout};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp;

// todo - remove pub
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Config {
    pub frag_size: u64,
    pub max_maps: usize,
    /// The maximum number of dirty keys per LargeTable entry before it's counted as loaded
    /// This can be overwritten for individual key space via KeySpaceConfig::max_dirty_keys
    pub max_dirty_keys: usize,
    /// How often to take snapshot depending on the number of entries written to the wal
    pub snapshot_written_bytes: u64,
    /// Force unload dirty entry if it's distance from wal tail exceeds given value
    pub snapshot_unload_threshold: u64,
    /// Percentage for the unload jitter
    pub unload_jitter_pct: usize,
    /// Use O_DIRECT when working with wal
    pub direct_io: bool,
    /// Number of background flusher threads for handling index flushes
    pub num_flusher_threads: usize,
    /// Whether to perform flushing synchronously instead of async (default: false)
    pub sync_flush: bool,
    /// Maximum size of a single WAL file
    pub wal_file_size: u64,
    /// Strategy to use for relocation (WalBased or IndexBased)
    #[serde(default)]
    pub relocation_strategy: RelocationStrategy,
    /// Enable Tidehunter runtime metrics collection
    #[serde(default = "default_metrics_enabled")]
    pub metrics_enabled: bool,
}

fn default_metrics_enabled() -> bool {
    true
}

impl Default for Config {
    fn default() -> Self {
        Self {
            frag_size: 128 * 1024 * 1024,
            max_maps: 16, // Max 2 Gb mapped space
            max_dirty_keys: 16 * 1024,
            snapshot_written_bytes: 128 * 1024 * 1024 * 1024,
            snapshot_unload_threshold: 64 * 1024 * 1024 * 1024,
            unload_jitter_pct: 30,
            direct_io: false,
            num_flusher_threads: 1,
            sync_flush: false,
            wal_file_size: 10 * (1 << 30), // 10Gb
            relocation_strategy: RelocationStrategy::default(),
            metrics_enabled: true,
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
            num_flusher_threads: 1,
            sync_flush: false,
            wal_file_size: 4 * 1024 * 1024,
            relocation_strategy: RelocationStrategy::default(),
            metrics_enabled: true,
        }
    }

    pub fn frag_size(&self) -> u64 {
        self.frag_size
    }

    #[doc(hidden)] // Used by tools/wal_inspector to get WAL configuration
    pub fn wal_layout(&self, kind: WalKind) -> WalLayout {
        WalLayout {
            frag_size: self.frag_size,
            max_maps: self.max_maps,
            direct_io: self.direct_io,
            wal_file_size: self.wal_file_size,
            kind,
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

    pub fn direct_io(&self) -> bool {
        self.direct_io
    }

    pub fn metrics_enabled(&self) -> bool {
        self.metrics_enabled
    }
}
