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
    /// WAL range one stripe of a WAL-based relocation run covers, rounded down
    /// to whole WAL files and at least one file.
    ///
    /// A run copies the live entries below one stripe's end, stores a control
    /// region and reclaims the WAL below that end before it starts the next
    /// stripe. Disk use above the steady state is therefore bounded by one
    /// stripe of copies, however far the run reaches. Each stripe reads the
    /// index of every cell of the key spaces it copies and rewrites the index
    /// of every cell holding entries in it, so smaller stripes read and write
    /// more index. `u64::MAX` covers the whole run in one stripe.
    #[serde(default = "default_relocation_stripe_bytes")]
    pub relocation_stripe_bytes: u64,
    /// Minimum share of live bytes (percent of `wal_file_size`) for a
    /// side-WAL file to be kept as it is. At the end of a WAL-based
    /// relocation run, the live records of files below this threshold are
    /// copied to the tail of their side WAL and the files are deleted, the
    /// emptiest first, up to one `relocation_stripe_bytes` of copies per
    /// run. A higher value keeps less dead data on disk and copies more;
    /// with 50, a file is rewritten once half of it is dead, and the side
    /// WAL holds at most twice its live data. 0 disables reclamation.
    #[serde(default = "default_side_wal_min_occupancy_pct")]
    pub side_wal_min_occupancy_pct: u8,
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
    /// Per-cell L1 auto-sharding: the flusher splits a cell's L1 across
    /// multiple WAL blobs (one per key range) whenever the promoted index's
    /// serialized size exceeds the threshold. `None` (default) uses
    /// `frag_size / 2`; `Some(usize::MAX)` disables auto-sharding, so a
    /// cell's L1 is always one blob; any other `Some(threshold)` is an
    /// explicit byte threshold. Set via
    /// [`Config::with_index_auto_shard_threshold`],
    /// [`Config::with_index_auto_sharding`] or
    /// [`Config::disable_index_auto_sharding`]; read via
    /// [`Config::auto_shard_threshold`]. `Db::open` re-validates the value
    /// against the WAL fragment ceiling, so a struct-literal bypass of the
    /// builders is still caught.
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
    /// Opt-in cache of whole index blobs that have aged out of the index
    /// WAL's mmap window, keyed by WAL position (see `index_blob_cache`).
    /// `Some(bytes)` bounds the total blob bytes retained; must be non-zero
    /// and at least `index_blob_cache_min_bytes`, since a blob larger than
    /// the whole budget is never admitted (`index_blob_cache_rejected`
    /// counts those). Size it at about twice the largest blob expected to
    /// qualify: a cell's L0 and L1 can both qualify, fills in flight
    /// overshoot the budget until their eviction sweeps settle, and blobs
    /// orphaned by a synchronous flush on unload (`sync_flush`) or by
    /// dropping cells are reclaimed only by the sweep. `None` (default)
    /// disables the cache, and every syscall-path lookup copies its
    /// micro-cell section out of the file.
    #[serde(default)]
    pub index_blob_cache_bytes: Option<usize>,
    /// Admission floor for the index blob cache: a blob read through the
    /// syscall path is cached only if its frame payload (the bytes the index
    /// reader serves) is at least this many bytes. Blobs below the floor
    /// keep the plain section read, which is cheap for a normally sized
    /// cell; only blobs inflated by key skew (one micro-cell holding
    /// millions of entries) are worth retaining. Mapped blobs are never
    /// admitted regardless of size. Default 64 MiB; 0 admits every
    /// syscall-path blob that fits the budget. Ignored while
    /// `index_blob_cache_bytes` is `None`.
    #[serde(default = "default_index_blob_cache_min_bytes")]
    pub index_blob_cache_min_bytes: usize,
    /// Total number of WAL generations (must be >= 1). Generation 0 is the
    /// normal user-write WAL; generations 1..N are side WALs that relocation
    /// will eventually promote surviving records into. With `wal_generations`
    /// = 1 the database behaves exactly as before — no encoding bits are
    /// stolen from WAL positions and no side WALs are opened.
    #[serde(default = "default_wal_generations")]
    pub wal_generations: u8,
}

fn default_index_blob_cache_min_bytes() -> usize {
    64 * 1024 * 1024
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

fn default_relocation_stripe_bytes() -> u64 {
    64 * 1024 * 1024 * 1024
}

fn default_side_wal_min_occupancy_pct() -> u8 {
    50
}

fn default_index_min_occupancy_pct() -> u8 {
    15
}

fn default_wal_generations() -> u8 {
    1
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
            relocation_stripe_bytes: default_relocation_stripe_bytes(),
            side_wal_min_occupancy_pct: default_side_wal_min_occupancy_pct(),
            index_min_occupancy_pct: default_index_min_occupancy_pct(),
            metrics_enabled: true,
            commit_pool_size: 0,
            num_pending_promotion_threads: default_num_pending_promotion_threads(),
            open_lock_retry_timeout: default_open_lock_retry_timeout(),
            index_auto_shard_threshold: None,
            batch_codec: None,
            compressed_batch_cache_bytes: None,
            index_blob_cache_bytes: None,
            index_blob_cache_min_bytes: default_index_blob_cache_min_bytes(),
            wal_generations: default_wal_generations(),
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
            relocation_stripe_bytes: default_relocation_stripe_bytes(),
            side_wal_min_occupancy_pct: default_side_wal_min_occupancy_pct(),
            index_min_occupancy_pct: default_index_min_occupancy_pct(),
            commit_pool_size: 0,
            num_pending_promotion_threads: default_num_pending_promotion_threads(),
            open_lock_retry_timeout: default_open_lock_retry_timeout(),
            index_auto_shard_threshold: None,
            batch_codec: None,
            compressed_batch_cache_bytes: None,
            index_blob_cache_bytes: None,
            index_blob_cache_min_bytes: default_index_blob_cache_min_bytes(),
            wal_generations: default_wal_generations(),
        }
    }

    /// Number of high offset bits reserved to encode a non-G0 generation id.
    /// Returns 0 when there is at most one non-G0 generation (no disambiguation
    /// needed). The top offset bit is always the gen-flag and is not counted
    /// here.
    pub fn generation_bits(&self) -> u8 {
        Self::generation_bits_for(self.wal_generations)
    }

    /// [`Self::generation_bits`] of a configuration with `wal_generations`
    /// generations.
    pub fn generation_bits_for(wal_generations: u8) -> u8 {
        let n = wal_generations as u32;
        assert!(n >= 1, "wal_generations must be >= 1");
        let non_g0 = n.saturating_sub(1);
        if non_g0 <= 1 {
            0
        } else {
            // ceil(log2(non_g0))
            (32 - (non_g0 - 1).leading_zeros()) as u8
        }
    }

    pub fn frag_size(&self) -> u64 {
        self.frag_size
    }

    /// Sets an explicit per-shard byte threshold for auto-sharding: the
    /// flusher splits a promoted L1 when its serialized size exceeds
    /// `threshold`. `usize::MAX` disables auto-sharding. Panics if any other
    /// `threshold` is zero or cannot fit in a single WAL fragment (the
    /// resulting frame would overflow `frag_size`).
    pub fn with_index_auto_shard_threshold(&mut self, threshold: usize) {
        self.index_auto_shard_threshold = Some(threshold);
        self.validate();
    }

    /// Restores the default auto-sharding threshold, `frag_size / 2` —
    /// half the fragment leaves comfortable room for WAL framing and
    /// alignment so each shard's frame stays well under the ceiling.
    pub fn with_index_auto_sharding(&mut self) {
        self.index_auto_shard_threshold = None;
    }

    /// Disables auto-sharding: a cell's L1 is always written as one blob,
    /// however large it grows.
    pub fn disable_index_auto_sharding(&mut self) {
        self.index_auto_shard_threshold = Some(usize::MAX);
    }

    /// The per-shard byte threshold auto-sharding splits at; `usize::MAX`
    /// when auto-sharding is disabled, so no blob ever exceeds it.
    pub fn auto_shard_threshold(&self) -> usize {
        self.index_auto_shard_threshold
            .unwrap_or((self.frag_size / 2) as usize)
    }

    /// Panics if any field holds a value that violates a cross-field
    /// invariant. Called by the builders (early feedback) and by
    /// `Db::open` (catches struct-literal / deserialize bypasses).
    pub(crate) fn validate(&self) {
        assert!(
            self.side_wal_min_occupancy_pct <= 100,
            "side_wal_min_occupancy_pct is a percentage, given {}",
            self.side_wal_min_occupancy_pct
        );
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
        if let Some(cache_bytes) = self.index_blob_cache_bytes {
            assert!(
                cache_bytes > 0,
                "index_blob_cache_bytes must be non-zero; use None to disable the cache"
            );
            assert!(
                self.index_blob_cache_min_bytes <= cache_bytes,
                "index_blob_cache_min_bytes {} exceeds index_blob_cache_bytes {cache_bytes}: \
                 every blob that qualified for admission would be rejected as oversized",
                self.index_blob_cache_min_bytes,
            );
        }
        let threshold = self.auto_shard_threshold();
        if threshold == usize::MAX {
            return; // auto-sharding disabled
        }
        assert!(
            threshold > 0,
            "index_auto_shard_threshold must be positive; usize::MAX disables auto-sharding"
        );
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
            label: kind.name().to_string(),
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

    #[test]
    #[should_panic(expected = "index_blob_cache_bytes must be non-zero")]
    fn validate_rejects_zero_index_blob_cache_budget() {
        let mut config = Config::small();
        config.index_blob_cache_bytes = Some(0);
        config.validate();
    }

    #[test]
    #[should_panic(
        expected = "index_blob_cache_min_bytes 1024 exceeds index_blob_cache_bytes 1023"
    )]
    fn validate_rejects_index_blob_floor_above_budget() {
        let mut config = Config::small();
        config.index_blob_cache_bytes = Some(1023);
        config.index_blob_cache_min_bytes = 1024;
        config.validate();
    }

    #[test]
    fn validate_accepts_index_blob_cache_with_default_floor() {
        let mut config = Config::small();
        config.index_blob_cache_bytes = Some(default_index_blob_cache_min_bytes());
        config.validate();
    }

    #[test]
    fn auto_shard_threshold_defaults_to_half_a_fragment() {
        let mut config = Config::small();
        let half = (config.frag_size / 2) as usize;
        assert_eq!(config.auto_shard_threshold(), half);
        config.with_index_auto_shard_threshold(4096);
        assert_eq!(config.auto_shard_threshold(), 4096);
        config.with_index_auto_sharding();
        assert_eq!(config.auto_shard_threshold(), half);
        config.disable_index_auto_sharding();
        assert_eq!(config.auto_shard_threshold(), usize::MAX);
        config.validate();
    }

    #[test]
    #[should_panic(expected = "exceeds the WAL fragment ceiling")]
    fn validate_rejects_auto_shard_threshold_above_fragment() {
        let mut config = Config::small();
        config.with_index_auto_shard_threshold(config.frag_size as usize);
    }

    #[test]
    #[should_panic(expected = "must be positive")]
    fn validate_rejects_zero_auto_shard_threshold() {
        let mut config = Config::small();
        config.with_index_auto_shard_threshold(0);
    }

    fn bits_for(n: u8) -> u8 {
        let mut c = Config::small();
        c.wal_generations = n;
        c.generation_bits()
    }

    #[test]
    fn test_wal_layout_label_defaults_to_kind_name() {
        let config = Config::small();
        assert_eq!(config.wal_layout(WalKind::Replay).label, "wal");
        assert_eq!(config.wal_layout(WalKind::Index).label, "index");
    }

    #[test]
    fn test_generation_bits_table() {
        // (wal_generations, expected_bits) — bits encode the non-G0 generation id
        // and only need to disambiguate among `wal_generations - 1` values.
        let cases = [
            (1u8, 0u8), // no non-G0 generations
            (2, 0),     // 1 non-G0 gen, no bits needed
            (3, 1),     // 2 non-G0 gens -> 1 bit
            (4, 2),     // 3 non-G0 gens -> 2 bits
            (5, 2),     // 4 non-G0 gens -> 2 bits
            (6, 3),
            (8, 3), // 7 non-G0 gens -> 3 bits
            (9, 3), // 8 non-G0 gens -> 3 bits
            (10, 4),
            (17, 4), // 16 non-G0 gens -> 4 bits
            (18, 5),
        ];
        for (n, expected) in cases {
            assert_eq!(bits_for(n), expected, "wal_generations = {n}");
        }
    }

    #[test]
    fn test_generation_bits_fit_encoding() {
        // For each supported N, the bit count must be enough to represent every
        // non-G0 generation id (1..=N-1).
        for n in 1u8..=32 {
            let bits = bits_for(n);
            let non_g0 = (n as u32).saturating_sub(1);
            if non_g0 <= 1 {
                assert_eq!(bits, 0);
            } else {
                let capacity = 1u32 << bits;
                assert!(
                    capacity >= non_g0,
                    "bits={bits} can encode {capacity} values, need {non_g0} for N={n}",
                );
                // And it must be the *minimum* such bit count.
                if bits > 0 {
                    let smaller = 1u32 << (bits - 1);
                    assert!(smaller < non_g0, "bits={bits} is not minimal for N={n}",);
                }
            }
        }
    }

    #[test]
    #[should_panic(expected = "wal_generations must be >= 1")]
    fn test_generation_bits_zero_panics() {
        let mut c = Config::small();
        c.wal_generations = 0;
        let _ = c.generation_bits();
    }
}
