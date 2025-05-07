use prometheus::{exponential_buckets, HistogramVec, IntCounterVec, Registry};
use std::sync::Arc;
use tidehunter::{counter_vec, histogram_vec};

pub struct BenchmarkMetrics {
    pub bench_reads: HistogramVec,
    pub bench_writes: HistogramVec,
    pub get_lt_result: IntCounterVec,
}

impl BenchmarkMetrics {
    pub fn new_in(registry: &Registry) -> Arc<Self> {
        let latency_buckets = exponential_buckets(5., 1.4, 30).unwrap();
        let this = Self {
            bench_reads: histogram_vec!("bench_reads", &["db"], latency_buckets.clone(), registry),
            bench_writes: histogram_vec!(
                "bench_writes",
                &["db"],
                latency_buckets.clone(),
                registry
            ),
            get_lt_result: counter_vec!("get_lt_result", &["result"], registry),
        };
        Arc::new(this)
    }
}
