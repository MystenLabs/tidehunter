use prometheus::{exponential_buckets, HistogramVec, IntCounterVec, Registry};
use std::sync::Arc;

pub struct BenchmarkMetrics {
    pub bench_reads: HistogramVec,
    pub bench_writes: HistogramVec,
    pub get_lt_result: IntCounterVec,
}

impl BenchmarkMetrics {
    pub fn new_in(registry: &Registry) -> Arc<Self> {
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
        };
        Arc::new(this)
    }
}
