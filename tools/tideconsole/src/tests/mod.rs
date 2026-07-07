pub mod control_region_tests;
pub mod rhai_generated_tests;
pub mod wal_tests;

use crate::engine::{ConsoleContext, create_engine};
use parking_lot::Mutex;
use rhai::{Dynamic, Scope};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::key_shape::{KeyShapeBuilder, KeyType};
use tidehunter::test_utils::Metrics;

// ---------------------------------------------------------------------------
// Rhai test file runner
// ---------------------------------------------------------------------------

/// Run the `.rhai` file at `src/tests/rhai/<filename>`.
/// A fresh temp directory is created for each test and its path is injected
/// as `__db_path__`; `__common__.rhai` (setup functions) is prepended so
/// every script can call `setup_db()` / `setup_cr_db()` directly.
pub fn run_rhai_test(filename: &str) {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().display().to_string();

    let ctx = Arc::new(Mutex::new(ConsoleContext {
        print_fn: Arc::new(|_| {}),
    }));
    let engine = create_engine(ctx);
    let mut scope = Scope::new();
    scope.push_constant("__db_path__", db_path);

    let common = include_str!("rhai/__common__.rhai");
    let test_path = format!("{}/src/tests/rhai/{filename}", env!("CARGO_MANIFEST_DIR"));
    let test_script = std::fs::read_to_string(&test_path)
        .unwrap_or_else(|e| panic!("cannot read {test_path}: {e}"));
    let combined = format!("{common}\n{test_script}");

    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(&mut scope, &combined)
        .unwrap_or_else(|e| panic!("Test {filename} failed:\n{e}"));
}

// ---------------------------------------------------------------------------
// Rust-level helpers (used by tests that can't be expressed in Rhai)
// ---------------------------------------------------------------------------

pub struct TestDb {
    pub _dir: TempDir,
    pub path: PathBuf,
}

/// Standard two-keyspace database for Rust-only tests (e.g. output-capture tests).
pub fn setup_db() -> TestDb {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("db");
    std::fs::create_dir_all(&path).unwrap();

    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("ks", 8, 16, KeyType::uniform(8));
    builder.add_key_space("ks2", 8, 16, KeyType::uniform(8));
    let key_shape = builder.build();

    let db = Db::open(
        &path,
        key_shape,
        Arc::new(Config::default()),
        Metrics::new(),
    )
    .unwrap();
    let ks = db.ks("ks");
    let ks2 = db.ks("ks2");

    let mut batch = db.write_batch();
    for i in 0..5u8 {
        batch.write(ks, format!("key{i:02}___").into_bytes(), vec![i; 16]);
    }
    for i in 0..3u8 {
        batch.write(ks2, format!("key{i:02}___").into_bytes(), vec![i + 10; 8]);
    }
    batch.commit().unwrap();

    let mut batch = db.write_batch();
    batch.delete(ks, b"key00___".to_vec());
    batch.delete(ks, b"key01___".to_vec());
    batch.commit().unwrap();

    drop(db);
    TestDb { _dir: dir, path }
}

/// CR database for Rust-only tests (e.g. output-capture, multi-db tests).
pub fn setup_db_with_cr() -> (TempDir, PathBuf) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("db");
    std::fs::create_dir_all(&path).unwrap();

    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("objects", 8, 16, KeyType::uniform(8));
    builder.add_key_space("metadata", 8, 16, KeyType::uniform(8));
    let key_shape = builder.build();

    let db = Db::open(
        &path,
        key_shape,
        Arc::new(Config::default()),
        Metrics::new(),
    )
    .unwrap();
    let ks = db.ks("objects");
    let ks2 = db.ks("metadata");

    let mut batch = db.write_batch();
    for i in 0..5u8 {
        batch.write(ks, format!("key{i:02}___").into_bytes(), vec![i; 16]);
    }
    for i in 0..3u8 {
        batch.write(ks2, format!("key{i:02}___").into_bytes(), vec![i + 10; 8]);
    }
    batch.commit().unwrap();

    db.force_rebuild_control_region().unwrap();
    drop(db);
    (dir, path)
}
