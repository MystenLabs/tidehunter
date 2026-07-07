use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use parking_lot::Mutex;
use rhai::{Dynamic, Scope};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tideconsole::engine::{ConsoleContext, create_engine};
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::key_shape::{KeyShapeBuilder, KeyType};
use tidehunter::test_utils::Metrics;

// ---------------------------------------------------------------------------
// DB setup
// ---------------------------------------------------------------------------

struct BenchDb {
    _dir: TempDir,
    path: PathBuf,
}

/// Write `record_count` records to a fresh DB in batches of 100.
fn build_bench_db(record_count: usize) -> BenchDb {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("db");
    std::fs::create_dir_all(&path).unwrap();

    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("ks", 8, 32, KeyType::uniform(8));
    let key_shape = builder.build();

    let config = Arc::new(Config::default());
    let metrics = Metrics::new();
    let db = Db::open(&path, key_shape, config, metrics).unwrap();
    let ks = db.ks("ks");

    let batch_size = 100;
    let mut i = 0;
    while i < record_count {
        let end = (i + batch_size).min(record_count);
        let mut batch = db.write_batch();
        for j in i..end {
            let key = format!("{j:08x}").into_bytes();
            batch.write(ks, key, vec![(j & 0xff) as u8; 32]);
        }
        batch.commit().unwrap();
        i = end;
    }
    drop(db);

    BenchDb { _dir: dir, path }
}

/// Build a Rhai engine+scope with the DB already opened as `db`.
fn open_rhai(db: &BenchDb) -> (rhai::Engine, Scope<'static>) {
    let ctx = Arc::new(Mutex::new(ConsoleContext {
        print_fn: Arc::new(|_| {}),
    }));
    let engine = create_engine(ctx);
    let mut scope = Scope::new();
    let path = db.path.display().to_string();
    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(&mut scope, &format!("let db = open(\"{path}\")"))
        .unwrap();
    (engine, scope)
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

/// Measures the time to walk the entire WAL via the Rhai `db.walk_wal` method,
/// counting every entry.  Parameterised by total record count.
fn bench_wal_walk_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_walk/count_entries");

    for &record_count in &[1_000usize, 10_000, 100_000] {
        let db = build_bench_db(record_count);
        let (engine, mut scope) = open_rhai(&db);

        group.bench_with_input(
            BenchmarkId::from_parameter(record_count),
            &record_count,
            |b, _| {
                b.iter(|| {
                    let count: i64 = engine
                        .eval_with_scope(
                            &mut scope,
                            "let count = 0; db.walk_wal(|entry| { count += 1; }); count",
                        )
                        .unwrap();
                    assert!(count > 0);
                });
            },
        );
    }

    group.finish();
}

/// Same walk but also reads `entry.key` on every entry (exercises hex encoding
/// and Rhai property access).
fn bench_wal_walk_read_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_walk/read_key");

    for &record_count in &[1_000usize, 10_000, 100_000] {
        let db = build_bench_db(record_count);
        let (engine, mut scope) = open_rhai(&db);

        group.bench_with_input(
            BenchmarkId::from_parameter(record_count),
            &record_count,
            |b, _| {
                b.iter(|| {
                    let count: i64 = engine
                        .eval_with_scope(
                            &mut scope,
                            r#"
                            let count = 0;
                            db.walk_wal(|entry| {
                                let k = entry.key;
                                count += 1;
                            });
                            count
                            "#,
                        )
                        .unwrap();
                    assert!(count > 0);
                });
            },
        );
    }

    group.finish();
}

/// Same bare-count walk but using db.walk_wal_unchecked (CRC skipped).
fn bench_wal_walk_skip_crc(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_walk/skip_crc");

    for &record_count in &[1_000usize, 10_000, 100_000] {
        let db = build_bench_db(record_count);
        let (engine, mut scope) = open_rhai(&db);

        group.bench_with_input(
            BenchmarkId::from_parameter(record_count),
            &record_count,
            |b, _| {
                b.iter(|| {
                    let count: i64 = engine
                        .eval_with_scope(
                            &mut scope,
                            "let count = 0; db.walk_wal_unchecked(|entry| { count += 1; }); count",
                        )
                        .unwrap();
                    assert!(count > 0);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_wal_walk_count,
    bench_wal_walk_read_key,
    bench_wal_walk_skip_crc
);
criterion_main!(benches);
