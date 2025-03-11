pub mod batch;
#[cfg(any(
    feature = "stress",
    feature = "random_access_speed_test",
    feature = "read_window_test",
    feature = "index_benchmark_generate",
    feature = "index_benchmark_run"
))]
pub mod benchmarks;
pub mod config;
mod control;
mod crc;
pub mod db;
mod file_reader;
mod flusher;
mod index;
pub mod iterators;
mod key_shape;
mod large_table;
mod lookup;
mod math;
pub mod metrics;
mod primitives;
#[cfg(feature = "stress")]
mod prometheus;
mod wal;
mod wal_syncer;

fn main() {
    #[cfg(feature = "random_access_speed_test")]
    benchmarks::random_access_speed_test::random_access_speed_test();
    #[cfg(feature = "stress")]
    benchmarks::stress::main();
    #[cfg(feature = "read_window_test")]
    benchmarks::read_window_test::main();
    #[cfg(feature = "index_benchmark_generate")]
    benchmarks::index_benchmark::generate_benchmark_files();
    #[cfg(feature = "index_benchmark_run")]
    benchmarks::index_benchmark::run_benchmarks();
}

#[allow(dead_code)]
fn mutex_speed_test() {
    use parking_lot::Mutex;
    use std::time::Instant;
    const C: usize = 1_000_000;
    let mut v = Vec::with_capacity(C);
    for i in 0..C {
        v.push(Mutex::new(i));
    }
    let start = Instant::now();
    for m in &v {
        *m.lock() += 1;
    }
    println!("Duration {:?}", start.elapsed());
}
