use prometheus::{exponential_buckets, HistogramVec, Registry};
use std::sync::Arc;
use tidehunter::histogram_vec;

pub struct BenchmarkMetrics {
    pub bench_reads: HistogramVec,
    pub bench_writes: HistogramVec,
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
        };
        Arc::new(this)
    }
}
