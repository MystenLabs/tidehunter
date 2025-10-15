use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use std::thread;
use tidehunter::wal_allocator::WalAllocator;
use tidehunter::{WalKind, WalLayout};

fn benchmark_wal_allocator(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_allocator");

    // Create a layout with 1GB fragment size
    let layout = WalLayout {
        frag_size: 1024 * 1024 * 1024, // 1GB
        max_maps: 10,
        direct_io: false,
        wal_file_size: 10 * 1024 * 1024 * 1024, // 10GB
        kind: WalKind::Replay,
    };

    // Test with different thread counts: 1, 4, 8, 16, 32
    for thread_count in [1, 4, 8, 16, 32].iter() {
        group.throughput(Throughput::Elements(*thread_count as u64));
        group.bench_with_input(
            BenchmarkId::new("threads", thread_count),
            thread_count,
            |b, &thread_count| {
                b.iter_custom(|iters| {
                    let allocator = Arc::new(WalAllocator::new(layout.clone(), 0));
                    let start = std::time::Instant::now();

                    // Number of operations per thread
                    let ops_per_thread = iters;

                    // Spawn threads
                    let handles: Vec<_> = (0..thread_count)
                        .map(|_| {
                            let allocator = Arc::clone(&allocator);
                            thread::spawn(move || {
                                for _ in 0..ops_per_thread {
                                    // Allocate 1024 bytes
                                    let result = allocator.allocate(black_box(1024));
                                    black_box(result);
                                }
                            })
                        })
                        .collect();

                    // Wait for all threads to finish
                    for handle in handles {
                        handle.join().unwrap();
                    }

                    start.elapsed()
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, benchmark_wal_allocator);
criterion_main!(benches);
