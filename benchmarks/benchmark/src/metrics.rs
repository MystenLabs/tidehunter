use crate::configs::StressTestConfigs;
use prometheus::{Gauge, HistogramVec, IntCounterVec, IntGauge, Registry, exponential_buckets};
use std::sync::Arc;

pub struct BenchmarkMetrics {
    pub bench_reads: HistogramVec,
    pub bench_writes: HistogramVec,
    pub get_lt_result: IntCounterVec,

    // Parameter gauge metrics
    pub param_read_percentage: IntGauge,
    pub param_zipf_exponent: Gauge,
    pub param_write_threads: IntGauge,
    pub param_mixed_threads: IntGauge,
    pub param_write_size: IntGauge,
    pub param_key_len: IntGauge,
    pub param_mixed_duration_secs: IntGauge,
    pub param_writes: IntGauge,
    pub param_background_writes: IntGauge,
    pub param_direct_io: IntGauge,
    pub param_num_flusher_threads: IntGauge,
    pub param_frag_size: IntGauge,
    pub param_max_maps: IntGauge,
    pub param_max_dirty_keys: IntGauge,
}

impl BenchmarkMetrics {
    pub fn new_in(registry: &Registry, config: &StressTestConfigs) -> Arc<Self> {
        let latency_buckets = exponential_buckets(5., 1.4, 30).unwrap();
        let this = Self {
            bench_reads: prometheus::register_histogram_vec_with_registry!(
                "bench_reads",
                "bench_reads",
                &["db"],
                latency_buckets.clone(),
                registry
            )
            .unwrap(),
            bench_writes: prometheus::register_histogram_vec_with_registry!(
                "bench_writes",
                "bench_writes",
                &["db"],
                latency_buckets.clone(),
                registry
            )
            .unwrap(),
            get_lt_result: prometheus::register_int_counter_vec_with_registry!(
                "get_lt_result",
                "get_lt_result",
                &["result"],
                registry
            )
            .unwrap(),

            // Register parameter gauges
            param_read_percentage: prometheus::register_int_gauge_with_registry!(
                "benchmark_param_read_percentage",
                "Percentage of reads in mixed phase (0-100)",
                registry
            )
            .unwrap(),
            param_zipf_exponent: prometheus::register_gauge_with_registry!(
                "benchmark_param_zipf_exponent",
                "Zipf distribution exponent for key selection",
                registry
            )
            .unwrap(),
            param_write_threads: prometheus::register_int_gauge_with_registry!(
                "benchmark_param_write_threads",
                "Number of write threads",
                registry
            )
            .unwrap(),
            param_mixed_threads: prometheus::register_int_gauge_with_registry!(
                "benchmark_param_mixed_threads",
                "Number of mixed read/write threads",
                registry
            )
            .unwrap(),
            param_write_size: prometheus::register_int_gauge_with_registry!(
                "benchmark_param_write_size",
                "Size of each write in bytes",
                registry
            )
            .unwrap(),
            param_key_len: prometheus::register_int_gauge_with_registry!(
                "benchmark_param_key_len",
                "Length of keys in bytes",
                registry
            )
            .unwrap(),
            param_mixed_duration_secs: prometheus::register_int_gauge_with_registry!(
                "benchmark_param_mixed_duration_secs",
                "Duration of mixed phase in seconds",
                registry
            )
            .unwrap(),
            param_writes: prometheus::register_int_gauge_with_registry!(
                "benchmark_param_writes",
                "Number of writes per thread",
                registry
            )
            .unwrap(),
            param_background_writes: prometheus::register_int_gauge_with_registry!(
                "benchmark_param_background_writes",
                "Background writes per second",
                registry
            )
            .unwrap(),
            param_direct_io: prometheus::register_int_gauge_with_registry!(
                "benchmark_param_direct_io",
                "Direct IO enabled (1) or disabled (0)",
                registry
            )
            .unwrap(),
            param_num_flusher_threads: prometheus::register_int_gauge_with_registry!(
                "benchmark_param_num_flusher_threads",
                "Number of flusher threads",
                registry
            )
            .unwrap(),
            param_frag_size: prometheus::register_int_gauge_with_registry!(
                "benchmark_param_frag_size",
                "Fragment size for tidehunter",
                registry
            )
            .unwrap(),
            param_max_maps: prometheus::register_int_gauge_with_registry!(
                "benchmark_param_max_maps",
                "Maximum memory maps",
                registry
            )
            .unwrap(),
            param_max_dirty_keys: prometheus::register_int_gauge_with_registry!(
                "benchmark_param_max_dirty_keys",
                "Maximum dirty keys threshold",
                registry
            )
            .unwrap(),
        };

        // Set the gauge values from config
        this.param_read_percentage
            .set(config.stress_client_parameters.read_percentage as i64);
        this.param_zipf_exponent
            .set(config.stress_client_parameters.zipf_exponent);
        this.param_write_threads
            .set(config.stress_client_parameters.write_threads as i64);
        this.param_mixed_threads
            .set(config.stress_client_parameters.mixed_threads as i64);
        this.param_write_size
            .set(config.stress_client_parameters.write_size as i64);
        this.param_key_len
            .set(config.stress_client_parameters.key_len as i64);
        this.param_mixed_duration_secs
            .set(config.stress_client_parameters.mixed_duration_secs as i64);
        this.param_writes
            .set(config.stress_client_parameters.writes as i64);
        this.param_background_writes
            .set(config.stress_client_parameters.background_writes as i64);
        this.param_direct_io
            .set(if config.db_parameters.direct_io { 1 } else { 0 });
        this.param_num_flusher_threads
            .set(config.db_parameters.num_flusher_threads as i64);
        this.param_frag_size
            .set(config.db_parameters.frag_size as i64);
        this.param_max_maps
            .set(config.db_parameters.max_maps as i64);
        this.param_max_dirty_keys
            .set(config.db_parameters.max_dirty_keys as i64);

        Arc::new(this)
    }
}
