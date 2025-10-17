use crate::config::Config;
use prometheus::{
    exponential_buckets, linear_buckets, Histogram as PromHistogram,
    HistogramVec as PromHistogramVec, IntCounter as PromIntCounter,
    IntCounterVec as PromIntCounterVec, IntGauge as PromIntGauge, IntGaugeVec as PromIntGaugeVec,
    Registry,
};
use std::fmt;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
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
    pub index_distance_from_tail: MetricIntGaugeVec,

    pub lookup_mcs: MetricHistogramVec,
    pub lookup_result: MetricIntCounterVec,
    pub lookup_iterations: MetricHistogram,
    pub lookup_scan_mcs: MetricIntCounter,
    pub lookup_io_mcs: MetricIntCounter,
    pub lookup_io_bytes: MetricIntCounter,

    pub large_table_contention: MetricHistogramVec,
    pub wal_write_wait: MetricIntCounter,
    pub wal_synced_position: MetricIntGauge,
    pub db_op_mcs: MetricHistogramVec,
    pub map_time_mcs: MetricHistogram,
    pub wal_mapper_time_mcs: MetricIntCounter,
    pub write_batch_times: MetricIntCounterVec,
    pub write_batch_operations: MetricIntCounterVec,
    pub skip_stale_update: MetricIntCounterVec,

    pub snapshot_lock_time_mcs: MetricHistogram,
    pub snapshot_force_unload: MetricIntCounterVec,
    pub snapshot_forced_relocation: MetricIntCounterVec,
    pub snapshot_written_bytes: MetricIntCounter,
    pub rebuild_control_region_time_mcs: MetricHistogram,
    pub large_table_init_mcs: MetricIntCounterVec,

    pub flush_time_mcs: MetricIntCounterVec,
    pub flush_count: MetricIntCounterVec,
    pub flush_update: MetricIntCounterVec,
    pub flushed_keys: MetricIntCounterVec,
    pub flushed_bytes: MetricIntCounterVec,
    pub flush_pending: MetricIntGauge,

    pub relocation_target_position: MetricIntGauge,
    pub gc_position: MetricIntGaugeVec,
    pub relocation_kept: MetricIntCounterVec,
    pub relocation_removed: MetricIntCounterVec,

    // Index-based relocation metrics
    pub relocation_cells_processed: MetricIntCounterVec,
    pub relocation_current_keyspace: MetricIntGauge,

    pub memory_estimate: MetricIntGaugeVec,
    pub value_cache_size: MetricIntGaugeVec,
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
            large_table_contention: histogram_vec!(
                "large_table_contention",
                &["ks"],
                lock_buckets.clone(),
                registry,
                enabled
            ),
            wal_write_wait: counter!("wal_write_wait", registry, enabled),
            wal_synced_position: gauge!("wal_synced_position", registry, enabled),
            db_op_mcs: histogram_vec!("db_op", &["op", "ks"], db_op_buckets, registry, enabled),
            map_time_mcs: histogram!("map_time_mcs", lookup_buckets.clone(), registry, enabled),
            wal_mapper_time_mcs: counter!("wal_mapper_time_mcs", registry, enabled),
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

            relocation_target_position: gauge!("relocation_target_position", registry, enabled),
            gc_position: gauge_vec!("gc_position", &["kind"], registry, enabled),
            relocation_kept: counter_vec!("relocation_kept", &["ks"], registry, enabled),
            relocation_removed: counter_vec!("relocation_removed", &["ks"], registry, enabled),

            // Index-based relocation metrics
            relocation_cells_processed: counter_vec!(
                "relocation_cells_processed",
                &["ks"],
                registry,
                enabled
            ),
            relocation_current_keyspace: gauge!("relocation_current_keyspace", registry, enabled),

            memory_estimate: gauge_vec!("memory_estimate", &["ks", "kind"], registry, enabled),
            value_cache_size: gauge_vec!("value_cache_size", &["ks"], registry, enabled),
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
        println!("Histogram stats: {:?}", inner);
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
