use super::setup_db;
use crate::engine::{ConsoleContext, create_engine, is_complete};
use parking_lot::Mutex;
use rhai::{Array, Dynamic, Scope};
use std::sync::Arc;
use tempfile::TempDir;
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::key_shape::{KeyShapeBuilder, KeyType};
use tidehunter::test_utils::Metrics;

// ---------------------------------------------------------------------------
// is_complete (pure, no DB needed)
// ---------------------------------------------------------------------------

#[test]
fn test_is_complete_single_line() {
    assert!(is_complete("1 + 2"));
    assert!(is_complete("db.walk_wal(|e| { print(e.key); });"));
}

#[test]
fn test_is_complete_incomplete() {
    assert!(!is_complete("db.walk_wal(|entry| {"));
    assert!(!is_complete("let x = (1 +"));
}

#[test]
fn test_is_complete_multiline_complete() {
    let input = "db.walk_wal(|entry| {\n    print(entry.key);\n});";
    assert!(is_complete(input));
}

// ---------------------------------------------------------------------------
// list_wal_files — custom DB setup (tiny wal_file_size)
// ---------------------------------------------------------------------------

#[test]
fn test_list_wal_files() {
    // Build a DB with a tiny wal_file_size so writes spill across multiple files.
    let wal_file_size: u64 = 1024;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("db");
    std::fs::create_dir_all(&path).unwrap();

    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("ks", 8, 16, KeyType::uniform(8));
    let key_shape = builder.build();

    let config = Arc::new(Config {
        frag_size: wal_file_size,
        wal_file_size,
        ..Config::default()
    });
    let metrics = Metrics::new();
    let db = Db::open(&path, key_shape, config, metrics).unwrap();
    let ks = db.ks("ks");

    // Write enough records to span several WAL files.
    for i in 0..100u8 {
        let mut batch = db.write_batch();
        batch.write(ks, format!("key{i:02}__x").into_bytes(), vec![i; 8]);
        batch.commit().unwrap();
    }
    drop(db);

    let ctx = Arc::new(Mutex::new(ConsoleContext {
        print_fn: Arc::new(|_| {}),
    }));
    let engine = create_engine(ctx);
    let mut scope = Scope::new();
    let db_path = path.display().to_string();
    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(&mut scope, &format!("let db = open(\"{db_path}\")"))
        .unwrap();

    let files: Array = engine
        .eval_with_scope(&mut scope, "db.list_wal_files()")
        .unwrap();

    // Must have produced multiple WAL files.
    assert!(
        files.len() >= 2,
        "expected multiple WAL files, got {}",
        files.len()
    );

    for (i, entry) in files.iter().enumerate() {
        let map = entry.clone().cast::<rhai::Map>();

        let name = map["name"].clone().cast::<String>();
        assert!(name.starts_with("wal_"), "unexpected file name: {name}");

        let expected_start = (i as i64) * (wal_file_size as i64);
        assert_eq!(
            map["start_pos"].clone().cast::<i64>(),
            expected_start,
            "file {i} start_pos mismatch"
        );

        assert!(
            map["size"].clone().cast::<i64>() > 0,
            "file {i} size should be positive"
        );

        let created = map["created"].clone().cast::<i64>();
        assert!(
            created > 946_684_800,
            "file {i} created timestamp looks wrong: {created}"
        );
    }

    let second_start = files[1].clone().cast::<rhai::Map>()["start_pos"]
        .clone()
        .cast::<i64>();
    let snippet = format!(
        r#"
        let full = 0;
        let partial = 0;
        db.walk_wal(|entry| {{ full += 1; }});
        db.walk_wal({second_start}, |entry| {{ partial += 1; }});
        [full, partial]
        "#
    );
    let counts: Array = engine.eval_with_scope(&mut scope, &snippet).unwrap();
    let full = counts[0].clone().cast::<i64>();
    let partial = counts[1].clone().cast::<i64>();
    assert!(
        partial < full,
        "walking from second file should yield fewer entries"
    );
    assert!(
        partial > 0,
        "walking from second file should still yield entries"
    );
}

// ---------------------------------------------------------------------------
// wal_stats — output capture test
// ---------------------------------------------------------------------------

#[test]
fn test_wal_stats_output() {
    let db = setup_db();
    let output: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let output_clone = output.clone();
    let ctx = Arc::new(Mutex::new(ConsoleContext {
        print_fn: Arc::new(move |s| output_clone.lock().push(s.to_string())),
    }));
    let engine = create_engine(ctx);
    let mut scope = Scope::new();

    let path = db.path.display().to_string();
    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(&mut scope, &format!("let db = open(\"{path}\")"))
        .unwrap();
    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(&mut scope, "db.wal_stats()")
        .unwrap();

    let lines = output.lock().join("\n");
    assert!(
        lines.contains("Record"),
        "output should mention Record type"
    );
    assert!(
        lines.contains("Remove"),
        "output should mention Remove type"
    );
    assert!(lines.contains('8'), "output should contain record count 8");
    assert!(lines.contains('2'), "output should contain remove count 2");
    assert!(
        lines.contains("Key Statistics"),
        "output should include Key Statistics section"
    );
    assert!(
        lines.contains("Per-keyspace"),
        "output should include Per-keyspace section"
    );
}

// ---------------------------------------------------------------------------
// load_index — custom DB setup (requires force_rebuild_control_region)
// ---------------------------------------------------------------------------

#[test]
fn test_load_index_returns_entries() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("db");
    std::fs::create_dir_all(&path).unwrap();

    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("objects", 8, 16, KeyType::uniform(8));
    let key_shape = builder.build();
    let config = Arc::new(Config::default());
    let db = Db::open(&path, key_shape, config, Metrics::new()).unwrap();
    let ks = db.ks("objects");

    let mut batch = db.write_batch();
    for i in 0..3u8 {
        batch.write(ks, format!("key{i:02}___").into_bytes(), vec![i; 16]);
    }
    batch.commit().unwrap();
    db.force_rebuild_control_region().unwrap();
    drop(db);

    let ctx = Arc::new(Mutex::new(ConsoleContext {
        print_fn: Arc::new(|_| {}),
    }));
    let engine = create_engine(ctx);
    let mut scope = Scope::new();
    let db_path = path.display().to_string();
    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(&mut scope, &format!("let db = open(\"{db_path}\")"))
        .unwrap();

    let script = r#"
        let cr = db.load_cr();
        let idx_offset = -1;
        for ks in cr.keyspaces {
            if ks.name == "objects" {
                for c in ks.cells {
                    if c.levels.len() > 0 {
                        idx_offset = c.levels[0].offset;
                        break;
                    }
                }
            }
        }
        idx_offset
    "#;
    let idx_offset: i64 = engine.eval_with_scope(&mut scope, script).unwrap();
    assert!(
        idx_offset >= 0,
        "expected at least one cell with a valid index offset after snapshot"
    );

    let entries: Array = engine
        .eval_with_scope(&mut scope, &format!("db.load_index({idx_offset})"))
        .unwrap();

    assert!(
        !entries.is_empty(),
        "expected at least one entry in the index at offset {idx_offset}"
    );
    let first = entries[0].clone().cast::<rhai::Map>();
    assert!(
        first.contains_key("key"),
        "each entry should have a 'key' field"
    );
    assert!(
        first.contains_key("wal_position"),
        "each entry should have a 'wal_position' field"
    );
    assert!(
        first.contains_key("payload_len"),
        "each entry should have a 'payload_len' field"
    );
    let key_hex = first["key"].clone().cast::<String>();
    assert!(!key_hex.is_empty(), "key hex string should be non-empty");
    let wal_pos: i64 = first["wal_position"].clone().cast::<i64>();
    assert!(wal_pos >= 0, "wal_position should be non-negative");
    let payload_len: i64 = first["payload_len"].clone().cast::<i64>();
    assert!(payload_len >= 0, "payload_len should be non-negative");
}
