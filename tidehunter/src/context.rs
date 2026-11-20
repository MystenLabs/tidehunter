use crate::config::Config;
use crate::key_shape::{KeySpace, KeySpaceDesc};
use crate::large_table::GetResult;
use crate::metrics::{MetricHistogram, MetricIntCounter, MetricIntGauge, Metrics, TimerExt};
use crate::wal::position::{LastProcessed, WalPosition};
use crate::wal::tracker::SharedLastProcessed;
use std::array;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use strum::{AsRefStr, EnumCount, EnumIter, FromRepr};

#[derive(Clone)]
pub struct KsContext {
    inner: Arc<KsContextInner>,
}

pub struct KsContextInner {
    pub config: Arc<Config>,
    pub ks_config: KeySpaceDesc,
    pub metrics: Arc<Metrics>,
    /// Externally observed WAL `last_processed`, published by the tracker.
    pub last_processed: SharedLastProcessed,
    pub loaded_key_bytes: MetricIntGauge,
    pub flat_index_bytes: MetricIntGauge,
    pub dirty_keys: MetricIntGauge,
    pub large_table_contention: MetricHistogram,
    pub pending_table_len: MetricIntGauge,
    pub value_cache_size: MetricIntGauge,
    pub read_decompress_count: MetricIntCounter,
    pub read_decompress_mcs: MetricIntCounter,
    pub read_decompress_cache_hits: MetricIntCounter,
    // Operation metrics indexed by DbOpKind
    db_op_metrics: [MetricHistogram; DbOpKind::COUNT],
    // WAL written bytes metrics indexed by WalEntryKind
    wal_written_metrics: [MetricIntCounter; WalEntryKind::COUNT],
    // Lookup result metrics indexed by [LookupResult][LookupSource]
    lookup_result_metrics: [[MetricIntCounter; LookupSource::COUNT]; LookupResult::COUNT], // 2 for Found/NotFound
    // Lookup timing metrics indexed by ReadType
    lookup_mcs_metrics: [MetricHistogram; ReadType::COUNT],
    // Read metrics indexed by [WalEntryKind][ReadType]
    read_metrics: [[MetricIntCounter; ReadType::COUNT]; WalEntryKind::COUNT],
    // Read bytes metrics indexed by [WalEntryKind][ReadType]
    read_bytes_metrics: [[MetricIntCounter; ReadType::COUNT]; WalEntryKind::COUNT],
    /// Set once a record of this key space is written to a side WAL; never
    /// cleared. See [`KsContext::has_promoted`].
    promoted: AtomicBool,
}

#[derive(Clone, Copy, Debug, EnumIter, EnumCount, AsRefStr, FromRepr)]
#[repr(usize)]
#[strum(serialize_all = "snake_case")]
pub enum DbOpKind {
    Insert,
    Remove,
    Get,
    Exists,
    NextEntry,
    NextCell,
    UpdateFlushedIndex,
}

#[derive(Clone, Copy, Debug, EnumCount, AsRefStr, FromRepr)]
#[repr(usize)]
#[strum(serialize_all = "snake_case")]
pub enum LookupResult {
    Found,
    NotFound,
}

#[derive(Clone, Copy, Debug, EnumIter, EnumCount, AsRefStr, FromRepr)]
#[repr(usize)]
#[strum(serialize_all = "snake_case")]
pub enum LookupSource {
    Lru,
    Bloom,
    Cache,
    /// On-disk lookup that was resolved at L0 (the freshest blob) — either
    /// hit, tombstone-shadowed, or missed on a single-level cell.
    #[strum(serialize = "index_0")]
    Index0,
    /// On-disk lookup that walked past L0 and resolved at L1 (hit or miss).
    /// Non-zero `index_1` counts indicate the two-level LSM is being
    /// exercised on the read path.
    #[strum(serialize = "index_1")]
    Index1,
    Prefix,
}

impl LookupSource {
    /// Maps the level at which a miss-chain walk terminated to a
    /// `LookupSource`. Levels beyond 1 are folded into `Index1` — the
    /// flusher currently produces at most two levels, so this is only hit
    /// if the schema grows.
    pub fn for_level(level: usize) -> Self {
        match level {
            0 => LookupSource::Index0,
            _ => LookupSource::Index1,
        }
    }
}

#[derive(Clone, Copy, Debug, EnumIter, EnumCount, AsRefStr, FromRepr)]
#[repr(usize)]
#[strum(serialize_all = "snake_case")]
pub enum WalEntryKind {
    Record,
    Tombstone,
    Index,
    CompressedBatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, EnumIter, EnumCount, AsRefStr, FromRepr)]
#[repr(usize)]
#[strum(serialize_all = "snake_case")]
pub enum ReadType {
    /// Served from a memory-mapped WAL fragment.
    Mapped,
    /// Read from the WAL file with a syscall. For index blobs this includes
    /// the one whole-blob read that fills the index blob cache.
    Syscall,
    /// Served from the index blob cache (see `index_blob_cache`): resident
    /// at probe time, or supplied by another lookup's in-flight fill.
    Cached,
}

impl KsContext {
    pub fn new(
        config: Arc<Config>,
        ks_config: KeySpaceDesc,
        metrics: Arc<Metrics>,
        last_processed: SharedLastProcessed,
    ) -> Self {
        let ks_name = ks_config.name();
        let loaded_key_bytes = metrics.loaded_key_bytes.with_label_values(&[ks_name]);
        let flat_index_bytes = metrics.flat_index_bytes.with_label_values(&[ks_name]);
        let dirty_keys = metrics.dirty_keys.with_label_values(&[ks_name]);
        let large_table_contention = metrics.large_table_contention.with_label_values(&[ks_name]);
        let pending_table_len = metrics.pending_table_len.with_label_values(&[ks_name]);
        let value_cache_size = metrics.value_cache_size.with_label_values(&[ks_name]);
        let read_decompress_count = metrics.read_decompress_count.with_label_values(&[ks_name]);
        let read_decompress_mcs = metrics.read_decompress_mcs.with_label_values(&[ks_name]);
        let read_decompress_cache_hits = metrics
            .read_decompress_cache_hits
            .with_label_values(&[ks_name]);

        let db_op_metrics = array::from_fn(|i| {
            let op = DbOpKind::from_repr(i).expect("Invalid DbOpKind index");
            metrics.db_op_mcs.with_label_values(&[op.as_ref(), ks_name])
        });

        let wal_written_metrics = array::from_fn(|i| {
            let kind = WalEntryKind::from_repr(i).expect("Invalid WalEntryKind index");
            metrics
                .wal_written_bytes_type
                .with_label_values(&[kind.as_ref(), ks_name])
        });

        // Initialize lookup result metrics as a 2D array [result][source]
        let lookup_result_metrics = array::from_fn(|result_idx| {
            let result = LookupResult::from_repr(result_idx).expect("Invalid LookupResult index");
            array::from_fn(|source_idx| {
                let source =
                    LookupSource::from_repr(source_idx).expect("Invalid LookupSource index");
                metrics.lookup_result.with_label_values(&[
                    ks_name,
                    result.as_ref(),
                    source.as_ref(),
                ])
            })
        });

        let lookup_mcs_metrics = array::from_fn(|i| {
            let read_type = ReadType::from_repr(i).expect("Invalid ReadType index");
            metrics
                .lookup_mcs
                .with_label_values(&[read_type.as_ref(), ks_name])
        });

        let read_metrics = array::from_fn(|kind_idx| {
            let kind = WalEntryKind::from_repr(kind_idx).expect("Invalid WalEntryKind index");
            array::from_fn(|read_idx| {
                let read_type = ReadType::from_repr(read_idx).expect("Invalid ReadType index");
                metrics
                    .read
                    .with_label_values(&[ks_name, kind.as_ref(), read_type.as_ref()])
            })
        });

        let read_bytes_metrics = array::from_fn(|kind_idx| {
            let kind = WalEntryKind::from_repr(kind_idx).expect("Invalid WalEntryKind index");
            array::from_fn(|read_idx| {
                let read_type = ReadType::from_repr(read_idx).expect("Invalid ReadType index");
                metrics
                    .read_bytes
                    .with_label_values(&[ks_name, kind.as_ref(), read_type.as_ref()])
            })
        });

        let inner = KsContextInner {
            config,
            ks_config,
            metrics,
            last_processed,
            loaded_key_bytes,
            flat_index_bytes,
            dirty_keys,
            large_table_contention,
            pending_table_len,
            value_cache_size,
            read_decompress_count,
            read_decompress_mcs,
            read_decompress_cache_hits,
            db_op_metrics,
            wal_written_metrics,
            lookup_result_metrics,
            lookup_mcs_metrics,
            read_metrics,
            read_bytes_metrics,
            promoted: AtomicBool::new(false),
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    /// Whether a record of this key space was ever promoted to a side WAL.
    /// Only then can its index levels hold a generation-flagged position.
    /// The flag is stored with every control region (`SideWalState`) and
    /// restored from it at open, so it outlives the key space's
    /// `promote_during_relocation` setting.
    pub fn has_promoted(&self) -> bool {
        self.inner.promoted.load(Ordering::SeqCst)
    }

    /// Records that a record of this key space is about to be written to a
    /// side WAL. Called before the write, so that a control region able to
    /// point at the copy already sees the flag.
    pub fn mark_promoted(&self) {
        self.inner.promoted.store(true, Ordering::SeqCst);
    }

    /// The externally observed WAL `last_processed` position: every write
    /// below it has been applied to the in-memory index. Same value as
    /// `Loader::last_processed_wal_position` on the `Db`, readable without
    /// a loader.
    pub fn last_processed_wal_position(&self) -> LastProcessed {
        self.inner.last_processed.get()
    }

    pub fn db_op_timer(&self, op: DbOpKind) -> impl Drop {
        self.db_op_metrics[op as usize].clone().mcs_timer()
    }

    pub fn inc_wal_written(&self, kind: WalEntryKind, bytes: u64) {
        self.wal_written_metrics[kind as usize].inc_by(bytes);
    }

    pub fn inc_lookup_result(&self, result: LookupResult, source: LookupSource) {
        self.lookup_result_metrics[result as usize][source as usize].inc();
    }

    pub fn lookup_mcs_histogram(&self, read_type: ReadType) -> &MetricHistogram {
        &self.lookup_mcs_metrics[read_type as usize]
    }

    pub fn inc_read(&self, kind: WalEntryKind, read_type: ReadType) {
        self.read_metrics[kind as usize][read_type as usize].inc();
    }

    pub fn inc_read_bytes(&self, kind: WalEntryKind, read_type: ReadType, bytes: u64) {
        self.read_bytes_metrics[kind as usize][read_type as usize].inc_by(bytes);
    }

    pub fn name(&self) -> &str {
        self.ks_config.name()
    }

    pub fn id(&self) -> KeySpace {
        self.ks_config.id()
    }

    /// True if this key space is a destroyed tombstone — its WAL frames are
    /// skipped on replay and it holds no data.
    pub fn destroyed(&self) -> bool {
        self.ks_config.destroyed()
    }

    pub fn max_dirty_keys(&self) -> usize {
        self.ks_config
            .max_dirty_keys()
            .unwrap_or(self.config.max_dirty_keys)
    }

    pub fn excess_dirty_keys(&self, dirty_keys_count: usize) -> bool {
        dirty_keys_count > self.max_dirty_keys()
    }

    /// L0 capacity, in keys. When a flush's merged L0 would exceed this,
    /// the flusher promotes L0 into L1 instead of writing it as a new L0.
    /// Optimal `M ≈ √(2N/D)`; 8 is a conservative default that wins at
    /// cell sizes ≥ ~64k keys and doesn't lose much at smaller sizes.
    ///
    /// Resolution order: per-ks override → global `Config::l0_max_entries` →
    /// `max_dirty_keys * 8` fallback.
    pub fn l0_max_entries(&self) -> usize {
        self.ks_config
            .l0_max_entries()
            .or(self.config.l0_max_entries)
            .unwrap_or_else(|| self.max_dirty_keys().saturating_mul(8))
    }

    /// Returns fixed key size or None if variable keys are configured for this key space.
    pub fn index_key_size(&self) -> Option<usize> {
        self.ks_config.index_key_size()
    }

    pub fn report_lookup_result(&self, v: Option<WalPosition>, source: LookupSource) -> GetResult {
        let result = if v.is_some() {
            LookupResult::Found
        } else {
            LookupResult::NotFound
        };
        self.inc_lookup_result(result, source);
        match v {
            None => GetResult::NotFound,
            Some(w) => GetResult::WalPosition(w),
        }
    }
}

impl Deref for KsContext {
    type Target = KsContextInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
