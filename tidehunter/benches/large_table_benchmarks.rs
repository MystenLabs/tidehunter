use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;
use tempdir::TempDir;
use tidehunter::{
    config::Config,
    db::Db,
    key_shape::{KeyShapeBuilder, KeyType},
    metrics::Metrics,
};

fn benchmark_large_table_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_table_insert");

    // Test with different key sizes: 8, 32, 48, 96 bytes
    for key_size in [8, 32, 48, 96].iter() {
        group.bench_with_input(
            BenchmarkId::new("key_size", key_size),
            key_size,
            |b, &key_size| {
                // Setup: Create a new database for each benchmark iteration group
                let dir = TempDir::new("bench_large_table_insert").unwrap();

                // Create key shape with a single keyspace matching the key size
                let mut builder = KeyShapeBuilder::new();
                let ks = builder.add_key_space("bench", key_size, 16, KeyType::uniform(16));
                let key_shape = builder.build();

                // Use default config
                let config = Arc::new(Config::default());

                // Open database
                let db = Db::open(
                    dir.path(),
                    key_shape.clone(),
                    config.clone(),
                    Metrics::new(),
                )
                .unwrap();

                // Create test data with fixed value size of 256 bytes
                let value = vec![42u8; 256];
                let mut key_counter = 0u64;

                b.iter(|| {
                    // Generate a unique key of the specified size
                    let mut key = vec![0u8; key_size];
                    let counter_bytes = key_counter.to_be_bytes();
                    // Copy counter bytes to the beginning of the key
                    let copy_len = counter_bytes.len().min(key_size);
                    key[..copy_len].copy_from_slice(&counter_bytes[..copy_len]);
                    key_counter += 1;

                    // Benchmark the insert operation
                    db.insert(ks, black_box(key), black_box(value.clone()))
                        .unwrap();
                });

                // Gracefully shutdown the database
                db.wait_for_background_threads_to_finish();
            },
        );
    }

    group.finish();
}

fn benchmark_large_table_insert_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_table_insert_batch");

    // Test with different batch sizes and key sizes
    for key_size in [8, 32, 48].iter() {
        for batch_size in [10, 50, 100].iter() {
            group.bench_with_input(
                BenchmarkId::new(format!("key_{}_batch", key_size), batch_size),
                batch_size,
                |b, &batch_size| {
                    // Setup: Create a new database for each benchmark iteration group
                    let dir = TempDir::new("bench_large_table_insert_batch").unwrap();

                    // Create key shape with a single keyspace
                    let mut builder = KeyShapeBuilder::new();
                    let ks = builder.add_key_space("bench", *key_size, 16, KeyType::uniform(16));
                    let key_shape = builder.build();

                    // Use default config
                    let config = Arc::new(Config::default());

                    // Open database
                    let db = Db::open(
                        dir.path(),
                        key_shape.clone(),
                        config.clone(),
                        Metrics::new(),
                    )
                    .unwrap();

                    // Create test data with fixed value size of 256 bytes
                    let value = vec![42u8; 256];
                    let mut key_counter = 0u64;

                    b.iter(|| {
                        // Insert a batch of items
                        for _ in 0..batch_size {
                            let mut key = vec![0u8; *key_size];
                            let counter_bytes = key_counter.to_be_bytes();
                            let copy_len = counter_bytes.len().min(*key_size);
                            key[..copy_len].copy_from_slice(&counter_bytes[..copy_len]);
                            key_counter += 1;
                            db.insert(ks, black_box(key), black_box(value.clone()))
                                .unwrap();
                        }
                    });

                    // Gracefully shutdown the database
                    db.wait_for_background_threads_to_finish();
                },
            );
        }
    }

    group.finish();
}

fn benchmark_large_table_insert_with_overwrites(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_table_insert_overwrites");

    for key_size in [8, 32, 48].iter() {
        group.bench_with_input(
            BenchmarkId::new("50pct_overwrites_key", key_size),
            key_size,
            |b, &key_size| {
                // Setup: Create a new database
                let dir = TempDir::new("bench_large_table_overwrites").unwrap();

                // Create key shape with a single keyspace
                let mut builder = KeyShapeBuilder::new();
                let ks = builder.add_key_space("bench", key_size, 16, KeyType::uniform(16));
                let key_shape = builder.build();

                // Use default config
                let config = Arc::new(Config::default());

                // Open database
                let db = Db::open(
                    dir.path(),
                    key_shape.clone(),
                    config.clone(),
                    Metrics::new(),
                )
                .unwrap();

                // Pre-populate with some keys
                let value = vec![42u8; 256];
                for i in 0u64..1000 {
                    let mut key = vec![0u8; key_size];
                    let counter_bytes = i.to_be_bytes();
                    let copy_len = counter_bytes.len().min(key_size);
                    key[..copy_len].copy_from_slice(&counter_bytes[..copy_len]);
                    db.insert(ks, key, value.clone()).unwrap();
                }

                let mut key_counter = 0u64;

                b.iter(|| {
                    // 50% chance of overwriting existing key vs inserting new key
                    let key_num = if key_counter % 2 == 0 {
                        key_counter % 1000 // Overwrite existing key
                    } else {
                        1000 + key_counter // New key
                    };
                    key_counter += 1;

                    let mut key = vec![0u8; key_size];
                    let counter_bytes = key_num.to_be_bytes();
                    let copy_len = counter_bytes.len().min(key_size);
                    key[..copy_len].copy_from_slice(&counter_bytes[..copy_len]);
                    db.insert(ks, black_box(key), black_box(value.clone()))
                        .unwrap();
                });

                // Gracefully shutdown the database
                db.wait_for_background_threads_to_finish();
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_large_table_insert,
    benchmark_large_table_insert_batch,
    benchmark_large_table_insert_with_overwrites
);
criterion_main!(benches);
