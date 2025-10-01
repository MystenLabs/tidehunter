use prometheus::{
    exponential_buckets, linear_buckets, Histogram, HistogramVec, IntCounter, IntCounterVec,
    IntGauge, IntGaugeVec, Registry,
};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Instant;

pub struct Metrics {
    pub replayed_wal_records: IntCounter,
    pub index_size: Histogram,
    pub max_index_size: AtomicUsize,
    pub max_index_size_metric: IntGauge,
    pub wal_written_bytes: IntGauge,
    pub wal_written_bytes_type: IntCounterVec,
    pub unload: IntCounterVec,
    pub entry_state: IntGaugeVec,
    pub compacted_keys: IntCounterVec,
    pub read: IntCounterVec,
    pub read_bytes: IntCounterVec,
    pub loaded_key_bytes: IntGaugeVec,
    pub index_distance_from_tail: IntGaugeVec,

    pub lookup_mcs: HistogramVec,
    pub lookup_result: IntCounterVec,
    pub lookup_iterations: Histogram,
    pub lookup_scan_mcs: IntCounter,
    pub lookup_io_mcs: IntCounter,
    pub lookup_io_bytes: IntCounter,

    pub large_table_contention: HistogramVec,
    pub wal_contention: Histogram,
    pub wal_synced_position: IntGauge,
    pub db_op_mcs: HistogramVec,
    pub map_time_mcs: Histogram,
    pub wal_mapper_time_mcs: IntCounter,
    pub write_batch_times: IntCounterVec,
    pub write_batch_operations: IntCounterVec,
    pub skip_stale_update: IntCounterVec,

    pub snapshot_lock_time_mcs: Histogram,
    pub snapshot_force_unload: IntCounterVec,
    pub snapshot_forced_relocation: IntCounterVec,
    pub snapshot_written_bytes: IntCounter,
    pub rebuild_control_region_time_mcs: Histogram,
    pub large_table_init_mcs: IntCounterVec,

    pub flush_time_mcs: IntCounterVec,
    pub flush_count: IntCounterVec,
    pub flush_update: IntCounterVec,
    pub flushed_keys: IntCounterVec,
    pub flushed_bytes: IntCounterVec,
    pub flush_pending: IntGauge,

    pub relocation_position: IntGauge,
    pub gc_position: IntGaugeVec,
    pub relocation_kept: IntCounterVec,
    pub relocation_removed: IntCounterVec,
    pub relocation_bloom_filter_build_time_mcs: IntCounter,

    // Index-based relocation metrics
    pub relocation_cells_processed: IntCounterVec,
    pub relocation_current_keyspace: IntGauge,

    pub memory_estimate: IntGaugeVec,
    pub value_cache_size: IntGaugeVec,
}

#[macro_export]
macro_rules! gauge (
    ($name:expr, $r:expr) => {prometheus::register_int_gauge_with_registry!($name, $name, $r).unwrap()};
);
#[macro_export]
macro_rules! counter (
    ($name:expr, $r:expr) => {prometheus::register_int_counter_with_registry!($name, $name, $r).unwrap()};
);
#[macro_export]
macro_rules! counter_vec (
    ($name:expr, $b:expr, $r:expr) => {prometheus::register_int_counter_vec_with_registry!($name, $name, $b, $r).unwrap()};
);
#[macro_export]
macro_rules! gauge_vec (
    ($name:expr, $b:expr, $r:expr) => {prometheus::register_int_gauge_vec_with_registry!($name, $name, $b, $r).unwrap()};
);
#[macro_export]
macro_rules! histogram (
    ($name:expr, $buck:expr, $r:expr) => {prometheus::register_histogram_with_registry!($name, $name, $buck, $r).unwrap()}
);
#[macro_export]
macro_rules! histogram_vec (
    ($name:expr, $labels:expr, $buck:expr, $r:expr) => {prometheus::register_histogram_vec_with_registry!($name, $name, $labels, $buck, $r).unwrap()};
);
impl Metrics {
    pub fn new() -> Arc<Self> {
        Self::new_in(&Registry::default())
    }

    pub fn new_in(registry: &Registry) -> Arc<Self> {
        let index_size_buckets = exponential_buckets(100., 2., 20).unwrap();
        let snapshot_buckets = exponential_buckets(500., 2., 12).unwrap();
        let rebuild_buckets = exponential_buckets(2000., 2., 12).unwrap();
        let lookup_buckets = exponential_buckets(5., 1.4, 25).unwrap();
        let db_op_buckets = exponential_buckets(5., 1.4, 25).unwrap();
        let lock_buckets = exponential_buckets(1., 1.5, 12).unwrap();
        let lookup_iterations_buckets = linear_buckets(1., 1.0, 10).unwrap();

        let this = Metrics {
            replayed_wal_records: counter!("replayed_wal_records", registry),
            max_index_size: AtomicUsize::new(0),
            index_size: histogram!("index_size", index_size_buckets, registry),
            max_index_size_metric: gauge!("max_index_size", registry),
            wal_written_bytes: gauge!("wal_written_bytes", registry),
            wal_written_bytes_type: counter_vec!(
                "wal_written_bytes_type",
                &["type", "ks"],
                registry
            ),
            unload: counter_vec!("unload", &["kind"], registry),
            entry_state: gauge_vec!("entry_state", &["ks", "state"], registry),
            compacted_keys: counter_vec!("compacted_keys", &["ks"], registry),
            read: counter_vec!("read", &["ks", "kind", "type"], registry),
            read_bytes: counter_vec!("read_bytes", &["ks", "kind", "type"], registry),
            loaded_key_bytes: gauge_vec!("loaded_key_bytes", &["ks"], registry),
            index_distance_from_tail: gauge_vec!("index_distance_from_tail", &["ks"], registry),

            lookup_mcs: histogram_vec!(
                "lookup_mcs",
                &["type", "ks"],
                lookup_buckets.clone(),
                registry
            ),
            lookup_result: counter_vec!("lookup_result", &["ks", "result", "source"], registry),
            lookup_iterations: histogram!("lookup_iterations", lookup_iterations_buckets, registry),
            lookup_scan_mcs: counter!("lookup_scan_mcs", registry),
            lookup_io_mcs: counter!("lookup_io_mcs", registry),
            lookup_io_bytes: counter!("lookup_io_bytes", registry),
            large_table_contention: histogram_vec!(
                "large_table_contention",
                &["ks"],
                lock_buckets.clone(),
                registry
            ),
            wal_contention: histogram!("wal_contention", lock_buckets.clone(), registry),
            wal_synced_position: gauge!("wal_synced_position", registry),
            db_op_mcs: histogram_vec!("db_op", &["op", "ks"], db_op_buckets, registry),
            map_time_mcs: histogram!("map_time_mcs", lookup_buckets.clone(), registry),
            wal_mapper_time_mcs: counter!("wal_mapper_time_mcs", registry),
            write_batch_times: counter_vec!("write_batch_times", &["tag", "kind"], registry),
            write_batch_operations: counter_vec!(
                "write_batch_operations",
                &["tag", "kind"],
                registry
            ),
            skip_stale_update: counter_vec!("skip_stale_update", &["ks", "op"], registry),

            snapshot_lock_time_mcs: histogram!(
                "snapshot_lock_time_mcs",
                snapshot_buckets,
                registry
            ),
            snapshot_force_unload: counter_vec!("snapshot_force_unload", &["ks"], registry),
            snapshot_forced_relocation: counter_vec!(
                "snapshot_forced_relocation",
                &["ks"],
                registry
            ),
            snapshot_written_bytes: counter!("snapshot_written_bytes", registry),
            rebuild_control_region_time_mcs: histogram!(
                "rebuild_control_region_time_mcs",
                rebuild_buckets,
                registry
            ),
            large_table_init_mcs: counter_vec!("bloom_filter_restore_time_mcs", &["ks"], registry),

            flush_time_mcs: counter_vec!("flush_time_mcs", &["thread_id"], registry),
            flush_count: counter_vec!("flush_count", &["ks"], registry),
            flush_update: counter_vec!("flush_update", &["kind"], registry),
            flushed_keys: counter_vec!("flushed_keys", &["ks"], registry),
            flushed_bytes: counter_vec!("flushed_bytes", &["ks"], registry),
            flush_pending: gauge!("flush_pending", registry),

            relocation_position: gauge!("relocation_position", registry),
            gc_position: gauge_vec!("gc_position", &["kind"], registry),
            relocation_kept: counter_vec!("relocation_kept", &["ks"], registry),
            relocation_removed: counter_vec!("relocation_removed", &["ks"], registry),
            relocation_bloom_filter_build_time_mcs: counter!(
                "relocation_bloom_filter_build_time_mcs",
                registry
            ),

            // Index-based relocation metrics
            relocation_cells_processed: counter_vec!(
                "relocation_cells_processed",
                &["ks"],
                registry
            ),
            relocation_current_keyspace: gauge!("relocation_current_keyspace", registry),

            memory_estimate: gauge_vec!("memory_estimate", &["ks", "kind"], registry),
            value_cache_size: gauge_vec!("value_cache_size", &["ks"], registry),
        };
        Arc::new(this)
    }
}

pub trait TimerExt {
    fn mcs_timer(self) -> impl Drop;
}

pub struct McsHistogramTimer {
    histogram: Histogram,
    start: Instant,
}

pub struct McsCounterTimer {
    counter: IntCounter,
    start: Instant,
}

impl TimerExt for Histogram {
    fn mcs_timer(self) -> impl Drop {
        McsHistogramTimer {
            histogram: self,
            start: Instant::now(),
        }
    }
}

impl Drop for McsHistogramTimer {
    fn drop(&mut self) {
        self.histogram
            .observe(self.start.elapsed().as_micros() as f64)
    }
}

impl TimerExt for IntCounter {
    fn mcs_timer(self) -> impl Drop {
        McsCounterTimer {
            counter: self,
            start: Instant::now(),
        }
    }
}

impl Drop for McsCounterTimer {
    fn drop(&mut self) {
        self.counter.inc_by(self.start.elapsed().as_micros() as u64)
    }
}

pub fn print_histogram_stats(histogram: &Histogram) {
    println!("Histogram: {:?}", histogram);
}
