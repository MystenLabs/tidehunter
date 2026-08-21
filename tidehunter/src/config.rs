use crate::compressed_batch::BatchCodec;
use crate::crc::CrcFrame;
use crate::db::WalEntry;
use crate::relocation::RelocationStrategy;
use crate::wal::layout::{WalKind, WalLayout};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp;
use std::time::Duration;

// todo - remove pub
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Config {
    pub frag_size: u64,
    pub max_maps: usize,
    /// Maximum number of memory-mapped regions for index WAL files.
    /// If set, overrides `max_maps` for index maps specifically.
    #[serde(default)]
    pub max_index_maps: Option<usize>,
    /// The maximum number of dirty keys per LargeTable entry before it's counted as loaded
    /// This can be overwritten for individual key space via KeySpaceConfig::max_dirty_keys
    pub max_dirty_keys: usize,
    /// Override for the L0 promotion threshold (in keys). When a flush's merged L0
    /// would exceed this, the flusher promotes L0 into L1 instead of writing a new
    /// L0. `None` falls back to `max_dirty_keys * 8` (see `KsContext::l0_max_entries`).
    /// Can be overridden per key space via `KeySpaceConfig::l0_max_entries`.
    #[serde(default)]
    pub l0_max_entries: Option<usize>,
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
    /// Maximum percentage of disk space that relocation can reclaim in a single run (0-100)
    #[serde(default = "default_relocation_max_reclaim_pct")]
    pub relocation_max_reclaim_pct: u8,
    /// Maximum size (bytes) of an in-memory batch held during WAL-based relocation.
    ///
    /// Bounds memory consumed by the relocator thread while relocating data for a single
    /// cell: once the batch reaches this size it is flushed and a fresh batch is started
    /// for the remaining entries in the same cell. Higher values consume more memory but
    /// result in fewer intermediate index rewrites; lower values cap peak memory at the
    /// cost of extra flushes.
    ///
    /// `None` (default) resolves to `frag_size * 2`.
    #[serde(default)]
    pub relocation_batch_max_bytes: Option<usize>,
    /// Minimum live-byte occupancy (percent of `wal_file_size`) for an index WAL file to be
    /// kept as-is. Files below this threshold (excluding the most recently written file) get
    /// their cells force-relocated on the next snapshot, freeing the file for GC. 0 disables
    /// occupancy-based force-relocation entirely.
    #[serde(default = "default_index_min_occupancy_pct")]
    pub index_min_occupancy_pct: u8,
    /// Enable Tidehunter runtime metrics collection
    #[serde(default = "default_metrics_enabled")]
    pub metrics_enabled: bool,
    /// Number of threads in the batch commit pool(0 = disabled).
    /// If this feature is used batch commit uses thread pool for operations that can be parallelized.
    /// Using this feature does not change batch atomicity and isolation guarantees.
    #[serde(default)]
    pub commit_pool_size: usize,
    /// Number of event-driven pending-promotion threads. Each thread owns a consistent
    /// subset of mutex shards (`mutex_idx % num_pending_promotion_threads == shard_idx`),
    /// eliminating contention between promotion threads on different keyspace shards.
    #[serde(default = "default_num_pending_promotion_threads")]
    pub num_pending_promotion_threads: usize,
    /// TideHunter enforces exclusive access: opening a database path that is already in use
    /// returns an error immediately. This timeout allows `Db::open` to wait for a previous
    /// instance at the same path to fully close before giving up.
    #[serde(default = "default_open_lock_retry_timeout")]
    pub open_lock_retry_timeout: Duration,
    /// Opt-in to per-cell L1 auto-sharding. `Some(threshold)` means the
    /// flusher splits a cell's L1 across multiple WAL blobs (one per key
    /// range) whenever the promoted index's serialized size exceeds
    /// `threshold` bytes. `None` (default) disables auto-sharding entirely;
    /// behavior is identical to a database written without the feature.
    /// Set via [`Config::with_index_auto_sharding`] (uses `frag_size / 2`)
    /// or [`Config::with_index_auto_shard_threshold`] for an explicit
    /// value. `Db::open` re-validates the value against the WAL fragment
    /// ceiling, so a struct-literal bypass of the builders is still caught.
    #[serde(default)]
    pub index_auto_shard_threshold: Option<usize>,
    /// Opt-in: pack each committed `WriteBatch` into a single compressed WAL
    /// entry. `None` (default) preserves the original per-record framing
    /// (`BatchStart` + N `Record`/`Remove` frames). `Some(codec)` writes one
    /// `WalEntry::CompressedBatch` per commit and gives every key in the
    /// batch the same `WalPosition`. Point reads decompress and linear-scan
    /// the batch; tombstones inside a batch are visited only during replay.
    ///
    /// Once a database has committed compressed batches, do not reopen it
    /// with `batch_codec: None` while those frames are still live: the
    /// empty-value read optimization in `Db::read_record_check_key` is
    /// gated on this being `None` and can misread a compressed frame whose
    /// payload length collides with an empty record's.
    #[serde(default)]
    pub batch_codec: Option<BatchCodec>,
    /// Opt-in cache of decompressed `CompressedBatch` bodies, keyed by the
    /// WAL position of the batch frame. `Some(bytes)` bounds the total
    /// decompressed bytes retained (must be non-zero; a body larger than
    /// the whole budget is never cached — `decompress_cache_rejected`
    /// counts those). `None` (default) disables the cache and every batch
    /// read decompresses the frame anew. Enable together with
    /// `batch_codec`.
    #[serde(default)]
    pub compressed_batch_cache_bytes: Option<usize>,
}

fn default_open_lock_retry_timeout() -> Duration {
    Duration::from_secs(5)
}

fn default_metrics_enabled() -> bool {
    true
}

fn default_num_pending_promotion_threads() -> usize {
    4
}

fn default_relocation_max_reclaim_pct() -> u8 {
    5
}

fn default_index_min_occupancy_pct() -> u8 {
    15
}

impl Default for Config {
    fn default() -> Self {
        Self {
            frag_size: 128 * 1024 * 1024,
            max_maps: 16, // Max 2 Gb mapped space
            max_index_maps: None,
            max_dirty_keys: 16 * 1024,
            l0_max_entries: None,
            snapshot_written_bytes: 128 * 1024 * 1024 * 1024,
            snapshot_unload_threshold: 64 * 1024 * 1024 * 1024,
            unload_jitter_pct: 30,
            direct_io: false,
            num_flusher_threads: 1,
            sync_flush: false,
            wal_file_size: 10 * (1 << 30), // 10Gb
            relocation_strategy: RelocationStrategy::default(),
            relocation_max_reclaim_pct: default_relocation_max_reclaim_pct(),
            relocation_batch_max_bytes: None,
            index_min_occupancy_pct: default_index_min_occupancy_pct(),
            metrics_enabled: true,
            commit_pool_size: 0,
            num_pending_promotion_threads: default_num_pending_promotion_threads(),
            open_lock_retry_timeout: default_open_lock_retry_timeout(),
            index_auto_shard_threshold: None,
            batch_codec: None,
            compressed_batch_cache_bytes: None,
        }
    }
}

impl Config {
    pub fn small() -> Self {
        Self {
            frag_size: 1024 * 1024,
            max_maps: 16,
            max_index_maps: None,
            max_dirty_keys: 32,
            l0_max_entries: None,
            snapshot_written_bytes: 128 * 1024 * 1024, // 128 Mb
            snapshot_unload_threshold: 2 * 128 * 1024 * 1024, // 256 Mb
            unload_jitter_pct: 10,
            direct_io: false,
            num_flusher_threads: 1,
            sync_flush: false,
            wal_file_size: 4 * 1024 * 1024,
            relocation_strategy: RelocationStrategy::default(),
            metrics_enabled: true,
            relocation_max_reclaim_pct: 100,
            relocation_batch_max_bytes: None,
            index_min_occupancy_pct: default_index_min_occupancy_pct(),
            commit_pool_size: 0,
            num_pending_promotion_threads: default_num_pending_promotion_threads(),
            open_lock_retry_timeout: default_open_lock_retry_timeout(),
            index_auto_shard_threshold: None,
            batch_codec: None,
            compressed_batch_cache_bytes: None,
        }
    }

    pub fn frag_size(&self) -> u64 {
        self.frag_size
    }

    /// Enable auto-sharding with an explicit per-shard byte threshold. The
    /// flusher splits a promoted L1 when its serialized size exceeds
    /// `threshold`. Panics if `threshold` cannot fit in a single WAL
    /// fragment (the resulting frame would overflow `frag_size`).
    pub fn with_index_auto_shard_threshold(&mut self, threshold: usize) {
        self.index_auto_shard_threshold = Some(threshold);
        self.validate();
    }

    /// Enable auto-sharding with the default `frag_size / 2` threshold —
    /// half the fragment leaves comfortable room for WAL framing and
    /// alignment so each shard's frame stays well under the ceiling.
    pub fn with_index_auto_sharding(&mut self) {
        let half = (self.frag_size / 2) as usize;
        self.with_index_auto_shard_threshold(half);
    }

    /// Panics if any field holds a value that violates a cross-field
    /// invariant. Called by the builders (early feedback) and by
    /// `Db::open` (catches struct-literal / deserialize bypasses).
    pub(crate) fn validate(&self) {
        if let Some(cache_bytes) = self.compressed_batch_cache_bytes {
            assert!(
                cache_bytes > 0,
                "compressed_batch_cache_bytes must be non-zero; use None to disable the cache"
            );
            assert!(
                self.batch_codec.is_some(),
                "compressed_batch_cache_bytes requires batch_codec: the cache only holds \
                 decompressed CompressedBatch bodies, and running with the codec off while \
                 compressed frames are live is unsafe (see batch_codec docs)"
            );
        }
        let Some(threshold) = self.index_auto_shard_threshold else {
            return;
        };
        // Largest serialized index blob that still fits in one WAL fragment,
        // after CRC frame header, WAL entry prefix, and worst-case alignment.
        let alignment = if self.direct_io { 512 } else { 8 };
        let overhead = CrcFrame::CRC_HEADER_LENGTH + WalEntry::INDEX_PREFIX_SIZE + alignment;
        let max = (self.frag_size as usize).saturating_sub(overhead);
        assert!(
            threshold <= max,
            "index_auto_shard_threshold {threshold} exceeds the WAL fragment ceiling \
             {max} (frag_size={}, direct_io={})",
            self.frag_size,
            self.direct_io,
        );
    }

    #[doc(hidden)] // Used by tools/tideconsole to get WAL configuration
    pub fn wal_layout(&self, kind: WalKind) -> WalLayout {
        let max_maps = match kind {
            WalKind::Index => self.max_index_maps.unwrap_or(self.max_maps),
            WalKind::Replay => self.max_maps,
        };
        WalLayout {
            frag_size: self.frag_size,
            max_maps,
            direct_io: self.direct_io,
            wal_file_size: self.wal_file_size,
            kind,
        }
    }

    pub fn snapshot_written_bytes(&self) -> u64 {
        self.snapshot_written_bytes
    }

    pub fn relocation_batch_max_bytes(&self) -> usize {
        self.relocation_batch_max_bytes
            .unwrap_or(self.frag_size as usize * 2)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(expected = "compressed_batch_cache_bytes must be non-zero")]
    fn validate_rejects_zero_cache_budget() {
        let mut config = Config::small();
        config.batch_codec = Some(BatchCodec::Lz4);
        config.compressed_batch_cache_bytes = Some(0);
        config.validate();
    }

    #[test]
    #[should_panic(expected = "compressed_batch_cache_bytes requires batch_codec")]
    fn validate_rejects_cache_without_codec() {
        let mut config = Config::small();
        config.compressed_batch_cache_bytes = Some(1024);
        config.validate();
    }
}
