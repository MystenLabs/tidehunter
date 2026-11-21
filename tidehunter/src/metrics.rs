use crate::config::Config;
use crate::position_cache::PositionCacheMetrics;
use prometheus::{
    Histogram as PromHistogram, HistogramVec as PromHistogramVec, IntCounter as PromIntCounter,
    IntCounterVec as PromIntCounterVec, IntGauge as PromIntGauge, IntGaugeVec as PromIntGaugeVec,
    Registry, exponential_buckets, linear_buckets,
};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Instant;

// Global dummy wrappers to avoid per-call allocations when metrics are disabled
static DUMMY_HISTOGRAM: MetricHistogram = MetricHistogram { inner: None };
static DUMMY_COUNTER: MetricIntCounter = MetricIntCounter { inner: None };
static DUMMY_GAUGE: MetricIntGauge = MetricIntGauge { inner: None };

#[derive(Clone)]
pub struct MetricHistogram {
    inner: Option<PromHistogram>,
}

impl MetricHistogram {
    pub fn observe(&self, v: f64) {
        if let Some(inner) = &self.inner {
            inner.observe(v);
        }
    }
}

#[derive(Clone)]
pub struct MetricHistogramVec {
    inner: Option<PromHistogramVec>,
}

impl MetricHistogramVec {
    pub fn with_label_values(&self, labels: &[&str]) -> MetricHistogram {
        match &self.inner {
            Some(vec) => MetricHistogram {
                inner: Some(vec.with_label_values(labels)),
            },
            None => DUMMY_HISTOGRAM.clone(),
        }
    }
}

#[derive(Clone)]
pub struct MetricIntCounter {
    inner: Option<PromIntCounter>,
}

impl MetricIntCounter {
    pub fn inc(&self) {
        if let Some(inner) = &self.inner {
            inner.inc();
        }
    }
    pub fn inc_by(&self, v: u64) {
        if let Some(inner) = &self.inner {
            inner.inc_by(v);
        }
    }
    pub fn get(&self) -> u64 {
        match &self.inner {
            Some(inner) => inner.get(),
            None => 0,
        }
    }
}

impl fmt::Debug for MetricIntCounter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            Some(inner) => inner.fmt(f),
            None => f.write_str("noop"),
        }
    }
}

#[derive(Clone)]
pub struct MetricIntCounterVec {
    inner: Option<PromIntCounterVec>,
}

impl MetricIntCounterVec {
    pub fn with_label_values(&self, labels: &[&str]) -> MetricIntCounter {
        match &self.inner {
            Some(vec) => MetricIntCounter {
                inner: Some(vec.with_label_values(labels)),
            },
            None => DUMMY_COUNTER.clone(),
        }
    }
    pub fn get_metric_with_label_values(
        &self,
        labels: &[&str],
    ) -> Result<PromIntCounter, prometheus::Error> {
        match &self.inner {
            Some(vec) => vec.get_metric_with_label_values(labels),
            None => Ok(PromIntCounter::new("noop", "noop").unwrap()),
        }
    }
}

#[derive(Clone)]
pub struct MetricIntGauge {
    inner: Option<PromIntGauge>,
}

impl MetricIntGauge {
    pub fn set(&self, v: i64) {
        if let Some(inner) = &self.inner {
            inner.set(v);
        }
    }
    pub fn add(&self, v: i64) {
        if let Some(inner) = &self.inner {
            inner.add(v);
        }
    }
    pub fn get(&self) -> i64 {
        match &self.inner {
            Some(inner) => inner.get(),
            None => 0,
        }
    }
}

#[derive(Clone)]
pub struct MetricIntGaugeVec {
    inner: Option<PromIntGaugeVec>,
}

impl MetricIntGaugeVec {
    pub fn with_label_values(&self, labels: &[&str]) -> MetricIntGauge {
        match &self.inner {
            Some(vec) => MetricIntGauge {
                inner: Some(vec.with_label_values(labels)),
            },
            None => DUMMY_GAUGE.clone(),
        }
    }

    #[cfg(test)]
    pub fn label_count(&self) -> usize {
        use prometheus::core::Collector;
        match &self.inner {
            Some(vec) => vec.collect().first().map_or(0, |f| f.get_metric().len()),
            None => 0,
        }
    }
}

pub struct Metrics {
    pub replayed_wal_records: MetricIntCounter,
    pub index_size: MetricHistogram,
    pub max_index_size: AtomicUsize,
    pub max_index_size_metric: MetricIntGauge,
    pub wal_written_bytes: MetricIntGauge,
    pub wal_written_bytes_type: MetricIntCounterVec,
    pub unload: MetricIntCounterVec,
    pub entry_state: MetricIntGaugeVec,
    pub compacted_keys: MetricIntCounterVec,
    pub read: MetricIntCounterVec,
    pub read_bytes: MetricIntCounterVec,
    pub loaded_key_bytes: MetricIntGaugeVec,
    pub flat_index_bytes: MetricIntGaugeVec,
    pub index_distance_from_tail: MetricIntGaugeVec,

    pub lookup_mcs: MetricHistogramVec,
    pub lookup_result: MetricIntCounterVec,
    pub lookup_iterations: MetricHistogram,
    pub lookup_scan_mcs: MetricIntCounter,
    pub lookup_io_mcs: MetricIntCounter,
    pub lookup_io_bytes: MetricIntCounter,

    /// Per-ks count of batch-body decompressions performed on the read
    /// path. With the decompressed-batch cache enabled this counts only
    /// cache misses; hit rate is
    /// `read_decompress_cache_hits / (read_decompress_cache_hits + read_decompress_count)`.
    /// Attributed to the requesting keyspace, not the keyspaces of inner
    /// entries.
    pub read_decompress_count: MetricIntCounterVec,
    /// Per-ks cumulative microseconds spent decompressing batch bodies on
    /// the read path. Covers the decompression only — the record scan over
    /// the decompressed body is not included (whole-read latency lives in
    /// `db_op_mcs`). Attribution matches `read_decompress_count`.
    pub read_decompress_mcs: MetricIntCounterVec,
    /// Per-ks count of batch reads that did not pay a decompression: served
    /// from a cached body, or by waiting on a concurrent reader's in-flight
    /// decompression of the same frame. Attribution matches
    /// `read_decompress_count`.
    pub read_decompress_cache_hits: MetricIntCounterVec,
    /// Total decompressed bytes currently retained by the decompressed-batch
    /// cache, across all shards. Stays 0 when the cache is disabled.
    pub decompress_cache_bytes: MetricIntGauge,
    /// Decompressed bodies returned to a reader but not cached because they
    /// exceed `Config::compressed_batch_cache_bytes`. A non-zero rate means
    /// the budget is smaller than the workload's batch bodies and the cache
    /// is not retaining them.
    pub decompress_cache_rejected: MetricIntCounter,
    /// Cumulative microseconds readers spent blocked on another thread's
    /// in-flight decompression of the same frame (single-flight waits).
    pub decompress_cache_wait_mcs: MetricIntCounter,
    /// Time to acquire a contended decompressed-batch-cache shard lock —
    /// the counterpart of `large_table_contention` for the cache's shards.
    /// Uncontended acquisitions are not recorded.
    pub decompress_cache_contention: MetricHistogram,
    /// Bodies the decompressed-batch cache evicted to stay under its budget.
    pub decompress_cache_evictions: MetricIntCounter,
    /// Total bytes of index blobs currently retained by the index blob
    /// cache (`Config::index_blob_cache_bytes`). Stays 0 when the cache is
    /// disabled or no blob has qualified for admission.
    pub index_blob_cache_bytes: MetricIntGauge,
    /// Index reader opens on syscall-path blobs at or above the admission
    /// floor that were not admitted because the blob exceeds the whole
    /// budget; those lookups stay on the plain section-read path. A
    /// non-zero rate means the budget is smaller than the workload's largest
    /// blob — the cell the cache exists for is silently still paying the
    /// full section copy per lookup.
    pub index_blob_cache_rejected: MetricIntCounter,
    /// Fills whose frame failed CRC validation or could not be read. The
    /// blob is served raw, as on the uncached path, and never retained.
    /// Non-zero means an index blob on disk is corrupt.
    pub index_blob_cache_unreadable: MetricIntCounter,
    /// Cumulative microseconds lookups spent blocked on another thread's
    /// in-flight fill of the same index blob (single-flight waits).
    pub index_blob_cache_wait_mcs: MetricIntCounter,
    /// Time to acquire a contended index-blob-cache shard lock. Uncontended
    /// acquisitions are not recorded.
    pub index_blob_cache_contention: MetricHistogram,
    /// Probes answered from a cached blob: index readers (resident at probe
    /// time, or supplied by another thread's fill the reader waited on) and
    /// `Db::load` calls from the flusher and relocation.
    pub index_blob_cache_hits: MetricIntCounter,
    /// Whole-blob reads run to fill the index blob cache — one per admitted
    /// blob each time it enters the cache (first syscall-path lookup after
    /// it leaves the mmap window, or after an eviction).
    pub index_blob_cache_fills: MetricIntCounter,
    /// Cumulative microseconds spent in those fill reads.
    pub index_blob_cache_fill_mcs: MetricIntCounter,
    /// Cached index blobs dropped explicitly because a flush or relocation
    /// replaced them in their cell's levels.
    pub index_blob_cache_drops: MetricIntCounter,
    /// Index blobs the cache evicted to stay under its budget.
    pub index_blob_cache_evictions: MetricIntCounter,

    pub large_table_contention: MetricHistogramVec,
    pub wal_write_wait: MetricIntCounter,
    pub wal_synced_position: MetricIntGaugeVec,
    pub wal_last_processed_position: MetricIntGaugeVec,
    /// Internal `last_processed` tracked by the WAL tracker. While a latch is
    /// held this keeps advancing even though `wal_last_processed_position`
    /// (the externally observed value) is pinned; the gap is the latch lag.
    pub wal_internal_last_processed_position: MetricIntGaugeVec,
    /// Number of latches currently held on the WAL tracker.
    pub wal_active_latches: MetricIntGaugeVec,
    pub db_op_mcs: MetricHistogramVec,
    pub map_time_mcs: MetricHistogram,
    pub wal_mapper_time_mcs: MetricIntCounter,
    pub wal_tracker_time_mcs: MetricIntCounter,
    pub wal_unlinker_time_mcs: MetricIntCounterVec,
    /// Number of times the unlink worker's `Arc::try_unwrap` failed because a
    /// reader (or anything else) still held a clone of `Arc<File>`. One
    /// increment per failed attempt, not per file.
    pub wal_unlinker_close_retry_count: MetricIntCounterVec,
    pub write_batch_times: MetricIntCounterVec,
    pub write_batch_operations: MetricIntCounterVec,
    pub skip_stale_update: MetricIntCounterVec,

    pub snapshot_lock_time_mcs: MetricHistogram,
    pub snapshot_force_unload: MetricIntCounterVec,
    pub snapshot_forced_relocation: MetricIntCounterVec,
    pub snapshot_written_bytes: MetricIntCounter,
    pub relocation_written_bytes: MetricIntCounter,
    pub rebuild_control_region_time_mcs: MetricHistogram,
    pub large_table_init_mcs: MetricIntCounterVec,

    pub flush_time_mcs: MetricIntCounterVec,
    pub flush_count: MetricIntCounterVec,
    pub flush_update: MetricIntCounterVec,
    pub flushed_keys: MetricIntCounterVec,
    pub flushed_bytes: MetricIntCounterVec,
    pub flush_pending: MetricIntGauge,

    // Two-level LSM accounting.
    pub l0_bytes_written: MetricIntCounterVec,
    pub l1_bytes_written: MetricIntCounterVec,
    pub promote_total: MetricIntCounterVec,
    /// Number of L1 shard blobs written by the flusher, one increment per
    /// blob: initial sharding, re-sharding promotes, relocation
    /// compare-and-set rewrites, and snapshot forced-relocation reflushes.
    pub l1_shards_total: MetricIntCounterVec,
    /// Number of times a shard write produced more than one L1 shard blob:
    /// the initial sharding of a promoted L1, and a shard splitting while a
    /// re-sharding promote or a relocation compare-and-set rewrites it.
    pub l1_shard_split_total: MetricIntCounterVec,
    /// Number of existing shards rewritten by the flusher: by a re-sharding
    /// promote folding L0 keys into them, by a relocation compare-and-set
    /// re-pointing keys inside them, or by a snapshot forced relocation
    /// reflushing them out of a low-occupancy index file. Stays at zero
    /// while no cell is sharded.
    pub l1_shard_rewritten_total: MetricIntCounterVec,

    pub relocation_target_position: MetricIntGauge,
    /// Stripes a WAL-based relocation run completed: each stored a control
    /// region and reclaimed the WAL below its end, or below the control
    /// region's replay position when that is lower.
    pub relocation_stripes: MetricIntCounter,
    pub relocation_terminal_position: MetricIntGauge,
    /// Bytes of live records in each side WAL (`wal` label) below its tail
    /// file, as counted by the last relocation run.
    pub side_wal_live_bytes: MetricIntGaugeVec,
    /// Bytes the files of each side WAL below its tail file take on disk,
    /// at the same moment as `side_wal_live_bytes`.
    pub side_wal_file_bytes: MetricIntGaugeVec,
    /// Bytes of live records copied out of side-WAL files being reclaimed.
    pub side_wal_compacted_bytes: MetricIntCounter,
    /// Side-WAL files deleted after reclamation.
    pub side_wal_reclaimed_files: MetricIntCounter,
    pub gc_position: MetricIntGaugeVec,
    pub relocation_kept: MetricIntCounterVec,
    /// Records a WAL-based relocation pass copied that were themselves the
    /// copy an earlier pass made: they survived a whole relocation cycle.
    /// `relocation_kept` counts them too.
    pub relocation_recopied: MetricIntCounterVec,
    /// Records a relocation pass promoted to a side WAL. `relocation_kept`
    /// counts them too.
    pub relocation_promoted: MetricIntCounterVec,
    pub relocation_removed: MetricIntCounterVec,

    // Index-based relocation metrics
    pub relocation_cells_processed: MetricIntCounterVec,
    pub relocation_current_keyspace: MetricIntGauge,

    pub wal_mmap_bytes: MetricIntGauge,

    pub dirty_keys: MetricIntGaugeVec,

    pub memory_estimate: MetricIntGaugeVec,
    pub value_cache_size: MetricIntGaugeVec,
    pub pending_table_len: MetricIntGaugeVec,
    pub pending_promotion_job_remaining: MetricIntGaugeVec,
    pub pending_promotion_job_time_mcs: MetricIntCounter,
    pub flat_promotion_job_time_mcs: MetricIntCounter,
    pub promote_flat_arc_miss: MetricIntCounter,
}

macro_rules! gauge (
    ($name:expr, $r:expr, $en:expr) => {
        $crate::metrics::MetricIntGauge {
            inner: if $en {
                Some(prometheus::register_int_gauge_with_registry!($name, $name, $r).unwrap())
            } else {
                None
            }
        }
    };
);
macro_rules! counter (
    ($name:expr, $r:expr, $en:expr) => {
        $crate::metrics::MetricIntCounter {
            inner: if $en {
                Some(prometheus::register_int_counter_with_registry!($name, $name, $r).unwrap())
            } else {
                None
            }
        }
    };
);
macro_rules! counter_vec (
    ($name:expr, $b:expr, $r:expr, $en:expr) => {
        $crate::metrics::MetricIntCounterVec {
            inner: if $en {
                Some(prometheus::register_int_counter_vec_with_registry!($name, $name, $b, $r).unwrap())
            } else {
                None
            }
        }
    };
);
macro_rules! gauge_vec (
    ($name:expr, $b:expr, $r:expr, $en:expr) => {
        $crate::metrics::MetricIntGaugeVec {
            inner: if $en {
                Some(prometheus::register_int_gauge_vec_with_registry!($name, $name, $b, $r).unwrap())
            } else {
                None
            }
        }
    };
);
macro_rules! histogram (
    ($name:expr, $buck:expr, $r:expr, $en:expr) => {
        $crate::metrics::MetricHistogram {
            inner: if $en {
                Some(prometheus::register_histogram_with_registry!($name, $name, $buck, $r).unwrap())
            } else {
                None
            }
        }
    }
);
macro_rules! histogram_vec (
    ($name:expr, $labels:expr, $buck:expr, $r:expr, $en:expr) => {
        $crate::metrics::MetricHistogramVec {
            inner: if $en {
                Some(prometheus::register_histogram_vec_with_registry!($name, $name, $labels, $buck, $r).unwrap())
            } else {
                None
            }
        }
    };
);
impl Metrics {
    /// Metric handles for the decompressed-batch `PositionCache`.
    pub(crate) fn decompress_cache_metrics(&self) -> PositionCacheMetrics {
        PositionCacheMetrics {
            bytes: self.decompress_cache_bytes.clone(),
            rejected: self.decompress_cache_rejected.clone(),
            wait_mcs: self.decompress_cache_wait_mcs.clone(),
            contention: self.decompress_cache_contention.clone(),
            evictions: self.decompress_cache_evictions.clone(),
        }
    }

    /// Metric handles for the index blob `PositionCache`.
    pub(crate) fn index_blob_cache_metrics(&self) -> PositionCacheMetrics {
        PositionCacheMetrics {
            bytes: self.index_blob_cache_bytes.clone(),
            rejected: self.index_blob_cache_rejected.clone(),
            wait_mcs: self.index_blob_cache_wait_mcs.clone(),
            contention: self.index_blob_cache_contention.clone(),
            evictions: self.index_blob_cache_evictions.clone(),
        }
    }

    pub fn new() -> Arc<Self> {
        Self::new_in(&Registry::default())
    }

    pub fn new_in(registry: &Registry) -> Arc<Self> {
        Self::new_in_enabled(registry, true)
    }

    pub fn from_config_registry(registry: &Registry, config: &Config) -> Arc<Self> {
        Self::new_in_enabled(registry, config.metrics_enabled())
    }

    pub fn new_in_enabled(registry: &Registry, enabled: bool) -> Arc<Self> {
        let index_size_buckets = exponential_buckets(100., 2., 20).unwrap();
        let snapshot_buckets = exponential_buckets(500., 2., 12).unwrap();
        let rebuild_buckets = exponential_buckets(2000., 2., 12).unwrap();
        let lookup_buckets = exponential_buckets(5., 1.4, 25).unwrap();
        let db_op_buckets = exponential_buckets(5., 1.4, 25).unwrap();
        let lock_buckets = exponential_buckets(1., 1.5, 12).unwrap();
        let lookup_iterations_buckets = linear_buckets(1., 1.0, 10).unwrap();

        let this = Metrics {
            replayed_wal_records: counter!("replayed_wal_records", registry, enabled),
            max_index_size: AtomicUsize::new(0),
            index_size: histogram!("index_size", index_size_buckets, registry, enabled),
            max_index_size_metric: gauge!("max_index_size", registry, enabled),
            wal_written_bytes: gauge!("wal_written_bytes", registry, enabled),
            wal_written_bytes_type: counter_vec!(
                "wal_written_bytes_type",
                &["type", "ks"],
                registry,
                enabled
            ),
            unload: counter_vec!("unload", &["kind"], registry, enabled),
            entry_state: gauge_vec!("entry_state", &["ks", "state"], registry, enabled),
            compacted_keys: counter_vec!("compacted_keys", &["ks"], registry, enabled),
            read: counter_vec!("read", &["ks", "kind", "type"], registry, enabled),
            read_bytes: counter_vec!("read_bytes", &["ks", "kind", "type"], registry, enabled),
            loaded_key_bytes: gauge_vec!("loaded_key_bytes", &["ks"], registry, enabled),
            flat_index_bytes: gauge_vec!("flat_index_bytes", &["ks"], registry, enabled),
            index_distance_from_tail: gauge_vec!(
                "index_distance_from_tail",
                &["ks"],
                registry,
                enabled
            ),

            lookup_mcs: histogram_vec!(
                "lookup_mcs",
                &["type", "ks"],
                lookup_buckets.clone(),
                registry,
                enabled
            ),
            lookup_result: counter_vec!(
                "lookup_result",
                &["ks", "result", "source"],
                registry,
                enabled
            ),
            lookup_iterations: histogram!(
                "lookup_iterations",
                lookup_iterations_buckets,
                registry,
                enabled
            ),
            lookup_scan_mcs: counter!("lookup_scan_mcs", registry, enabled),
            lookup_io_mcs: counter!("lookup_io_mcs", registry, enabled),
            lookup_io_bytes: counter!("lookup_io_bytes", registry, enabled),
            read_decompress_count: counter_vec!(
                "read_decompress_count",
                &["ks"],
                registry,
                enabled
            ),
            read_decompress_mcs: counter_vec!("read_decompress_mcs", &["ks"], registry, enabled),
            read_decompress_cache_hits: counter_vec!(
                "read_decompress_cache_hits",
                &["ks"],
                registry,
                enabled
            ),
            decompress_cache_bytes: gauge!("decompress_cache_bytes", registry, enabled),
            decompress_cache_rejected: counter!("decompress_cache_rejected", registry, enabled),
            decompress_cache_wait_mcs: counter!("decompress_cache_wait_mcs", registry, enabled),
            decompress_cache_contention: histogram!(
                "decompress_cache_contention",
                lock_buckets.clone(),
                registry,
                enabled
            ),
            decompress_cache_evictions: counter!("decompress_cache_evictions", registry, enabled),
            index_blob_cache_bytes: gauge!("index_blob_cache_bytes", registry, enabled),
            index_blob_cache_rejected: counter!("index_blob_cache_rejected", registry, enabled),
            index_blob_cache_unreadable: counter!("index_blob_cache_unreadable", registry, enabled),
            index_blob_cache_wait_mcs: counter!("index_blob_cache_wait_mcs", registry, enabled),
            index_blob_cache_contention: histogram!(
                "index_blob_cache_contention",
                lock_buckets.clone(),
                registry,
                enabled
            ),
            index_blob_cache_hits: counter!("index_blob_cache_hits", registry, enabled),
            index_blob_cache_fills: counter!("index_blob_cache_fills", registry, enabled),
            index_blob_cache_fill_mcs: counter!("index_blob_cache_fill_mcs", registry, enabled),
            index_blob_cache_drops: counter!("index_blob_cache_drops", registry, enabled),
            index_blob_cache_evictions: counter!("index_blob_cache_evictions", registry, enabled),
            large_table_contention: histogram_vec!(
                "large_table_contention",
                &["ks"],
                lock_buckets.clone(),
                registry,
                enabled
            ),
            wal_write_wait: counter!("wal_write_wait", registry, enabled),
            wal_synced_position: gauge_vec!("wal_synced_position", &["kind"], registry, enabled),
            wal_last_processed_position: gauge_vec!(
                "wal_last_processed_position",
                &["kind"],
                registry,
                enabled
            ),
            wal_internal_last_processed_position: gauge_vec!(
                "wal_internal_last_processed_position",
                &["kind"],
                registry,
                enabled
            ),
            wal_active_latches: gauge_vec!("wal_active_latches", &["kind"], registry, enabled),
            db_op_mcs: histogram_vec!("db_op", &["op", "ks"], db_op_buckets, registry, enabled),
            map_time_mcs: histogram!("map_time_mcs", lookup_buckets.clone(), registry, enabled),
            wal_mapper_time_mcs: counter!("wal_mapper_time_mcs", registry, enabled),
            wal_tracker_time_mcs: counter!("wal_tracker_time_mcs", registry, enabled),
            wal_unlinker_time_mcs: counter_vec!(
                "wal_unlinker_time_mcs",
                &["kind"],
                registry,
                enabled
            ),
            wal_unlinker_close_retry_count: counter_vec!(
                "wal_unlinker_close_retry_count",
                &["kind"],
                registry,
                enabled
            ),
            write_batch_times: counter_vec!(
                "write_batch_times",
                &["tag", "kind"],
                registry,
                enabled
            ),
            write_batch_operations: counter_vec!(
                "write_batch_operations",
                &["tag", "kind"],
                registry,
                enabled
            ),
            // Writes ordered behind a newer write to the same key
            // (`IndexTable::checked_insert`, `WriteOrder::Stale`). The metric
            // name is kept for dashboard continuity.
            skip_stale_update: counter_vec!("skip_stale_update", &["ks", "op"], registry, enabled),

            snapshot_lock_time_mcs: histogram!(
                "snapshot_lock_time_mcs",
                snapshot_buckets,
                registry,
                enabled
            ),
            snapshot_force_unload: counter_vec!(
                "snapshot_force_unload",
                &["ks"],
                registry,
                enabled
            ),
            snapshot_forced_relocation: counter_vec!(
                "snapshot_forced_relocation",
                &["ks"],
                registry,
                enabled
            ),
            snapshot_written_bytes: counter!("snapshot_written_bytes", registry, enabled),
            relocation_written_bytes: counter!("relocation_written_bytes", registry, enabled),
            rebuild_control_region_time_mcs: histogram!(
                "rebuild_control_region_time_mcs",
                rebuild_buckets,
                registry,
                enabled
            ),
            large_table_init_mcs: counter_vec!("large_table_init_mcs", &["ks"], registry, enabled),

            flush_time_mcs: counter_vec!("flush_time_mcs", &["thread_id"], registry, enabled),
            flush_count: counter_vec!("flush_count", &["ks"], registry, enabled),
            flush_update: counter_vec!("flush_update", &["kind"], registry, enabled),
            flushed_keys: counter_vec!("flushed_keys", &["ks"], registry, enabled),
            flushed_bytes: counter_vec!("flushed_bytes", &["ks"], registry, enabled),
            flush_pending: gauge!("flush_pending", registry, enabled),

            l0_bytes_written: counter_vec!("l0_bytes_written", &["ks"], registry, enabled),
            l1_bytes_written: counter_vec!("l1_bytes_written", &["ks"], registry, enabled),
            promote_total: counter_vec!("promote_total", &["ks"], registry, enabled),
            l1_shards_total: counter_vec!("l1_shards_total", &["ks"], registry, enabled),
            l1_shard_split_total: counter_vec!("l1_shard_split_total", &["ks"], registry, enabled),
            l1_shard_rewritten_total: counter_vec!(
                "l1_shard_rewritten_total",
                &["ks"],
                registry,
                enabled
            ),

            relocation_target_position: gauge!("relocation_target_position", registry, enabled),
            relocation_stripes: counter!("relocation_stripes", registry, enabled),
            relocation_terminal_position: gauge!("relocation_terminal_position", registry, enabled),
            side_wal_live_bytes: gauge_vec!("side_wal_live_bytes", &["wal"], registry, enabled),
            side_wal_file_bytes: gauge_vec!("side_wal_file_bytes", &["wal"], registry, enabled),
            side_wal_compacted_bytes: counter!("side_wal_compacted_bytes", registry, enabled),
            side_wal_reclaimed_files: counter!("side_wal_reclaimed_files", registry, enabled),
            gc_position: gauge_vec!("gc_position", &["kind"], registry, enabled),
            relocation_kept: counter_vec!("relocation_kept", &["ks"], registry, enabled),
            relocation_recopied: counter_vec!("relocation_recopied", &["ks"], registry, enabled),
            relocation_promoted: counter_vec!("relocation_promoted", &["ks"], registry, enabled),
            relocation_removed: counter_vec!("relocation_removed", &["ks"], registry, enabled),

            // Index-based relocation metrics
            relocation_cells_processed: counter_vec!(
                "relocation_cells_processed",
                &["ks"],
                registry,
                enabled
            ),
            relocation_current_keyspace: gauge!("relocation_current_keyspace", registry, enabled),

            wal_mmap_bytes: gauge!("wal_mmap_bytes", registry, enabled),

            dirty_keys: gauge_vec!("dirty_keys", &["ks"], registry, enabled),

            memory_estimate: gauge_vec!("memory_estimate", &["ks", "kind"], registry, enabled),
            value_cache_size: gauge_vec!("value_cache_size", &["ks"], registry, enabled),
            pending_table_len: gauge_vec!("pending_table_len", &["ks"], registry, enabled),
            pending_promotion_job_remaining: gauge_vec!(
                "pending_promotion_job_remaining",
                &["ks"],
                registry,
                enabled
            ),
            pending_promotion_job_time_mcs: counter!(
                "pending_promotion_job_time_mcs",
                registry,
                enabled
            ),
            flat_promotion_job_time_mcs: counter!("flat_promotion_job_time_mcs", registry, enabled),
            promote_flat_arc_miss: counter!("promote_flat_arc_miss", registry, enabled),
        };
        Arc::new(this)
    }
}

pub trait TimerExt {
    fn mcs_timer(self) -> impl Drop;
}

pub struct McsHistogramTimer {
    histogram: MetricHistogram,
    start: Option<Instant>,
}

pub struct McsCounterTimer {
    counter: MetricIntCounter,
    start: Option<Instant>,
}

impl TimerExt for MetricHistogram {
    fn mcs_timer(self) -> impl Drop {
        McsHistogramTimer {
            histogram: self.clone(),
            start: if self.inner.is_some() {
                Some(Instant::now())
            } else {
                None
            },
        }
    }
}

impl Drop for McsHistogramTimer {
    fn drop(&mut self) {
        if let Some(start) = self.start.take() {
            self.histogram.observe(start.elapsed().as_micros() as f64)
        }
    }
}

impl TimerExt for MetricIntCounter {
    fn mcs_timer(self) -> impl Drop {
        McsCounterTimer {
            counter: self.clone(),
            start: if self.inner.is_some() {
                Some(Instant::now())
            } else {
                None
            },
        }
    }
}

impl Drop for McsCounterTimer {
    fn drop(&mut self) {
        if let Some(start) = self.start.take() {
            self.counter.inc_by(start.elapsed().as_micros() as u64)
        }
    }
}

pub fn print_histogram_stats(histogram: &MetricHistogram) {
    if let Some(inner) = &histogram.inner {
        println!("Histogram stats: {inner:?}");
    }
}

#[doc(hidden)] // Used by benchmarks for computing histogram averages
pub fn get_histogram_avg(histogram: &MetricHistogram) -> Option<f64> {
    histogram.inner.as_ref().and_then(|inner| {
        let count = inner.get_sample_count();
        if count > 0 {
            Some(inner.get_sample_sum() / count as f64)
        } else {
            None
        }
    })
}

#[doc(hidden)] // Used by benchmarks for computing histogram sum and count
pub fn get_histogram_sum_count(histogram: &MetricHistogram) -> Option<(f64, u64)> {
    histogram
        .inner
        .as_ref()
        .map(|inner| (inner.get_sample_sum(), inner.get_sample_count()))
}
