pub mod batch;
pub mod config;
mod control;
mod crc;
pub mod db;
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
#[cfg(feature = "stress")]
mod stress;

#[cfg(feature = "random_access_speed_test")]
mod random_access_speed_test;
#[cfg(feature = "read_window_test")]
mod read_window_test;
mod wal;
mod wal_syncer;

fn main() {
    #[cfg(feature = "random_access_speed_test")]
    random_access_speed_test::random_access_speed_test();
    #[cfg(feature = "stress")]
    stress::main();
    #[cfg(feature = "read_window_test")]
    read_window_test::main();
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
