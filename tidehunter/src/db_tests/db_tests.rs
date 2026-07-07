use super::super::*;
use crate::compressed_batch::BatchCodec;
use crate::config::Config;
use crate::crc::CrcFrame;
use crate::failpoints::FailPoint;
use crate::index::index_format::IndexFormatType;
use crate::index::uniform_lookup::UniformLookupIndex;
use crate::key_shape::{
    Compactor, KeyIndexing, KeyShape, KeyShapeBuilder, KeySpace, KeySpaceConfig, KeySpaces, KeyType,
};
use crate::latch::Latch;
use crate::metrics::Metrics;
use hex_literal::hex;
use minibytes::Bytes;
use rand::rngs::{StdRng, ThreadRng};
use rand::{Rng, SeedableRng};
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// see generate.py
pub(super) fn db_test(key_shape: KeyShape) {
    let dir = tempdir::TempDir::new("test-wal").unwrap();
    let config = Arc::new(Config::small());
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        let ks = db.single_ks();
        db.insert(ks, vec![1, 2, 3, 4], vec![5, 6]).unwrap();
        db.insert(ks, vec![3, 4, 5, 6], vec![7]).unwrap();
        assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
        assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
    }
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            force_unload_config(&config),
            Metrics::new(),
        )
        .unwrap();
        let ks = db.single_ks();
        assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
        assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
        thread::sleep(Duration::from_millis(10)); // todo replace this with wal tracker barrier
        db.rebuild_control_region().unwrap();
        assert!(
            db.large_table.is_all_clean(),
            "Some entries are not clean after snapshot"
        );
    }
    {
        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();
        let ks = db.single_ks();
        // nothing replayed from wal since we just rebuilt the control region
        assert_eq!(metrics.replayed_wal_records.get(), 0);
        assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
        assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
        db.insert(ks, vec![3, 4, 5, 6], vec![8]).unwrap();
        assert_eq!(Some(vec![8].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
    }
    {
        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();
        let ks = db.single_ks();
        assert_eq!(metrics.replayed_wal_records.get(), 1);
        assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
        assert_eq!(Some(vec![8].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
    }
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new().clone(),
        )
        .unwrap();
        let ks = db.single_ks();
        db.insert(ks, vec![3, 4, 5, 6], vec![9]).unwrap();
        assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
        assert_eq!(Some(vec![9].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
    }
}

#[test]
fn test_db_lock() {
    use std::env;
    use std::process::Command;

    // Helper subprocess mode
    if let Ok(mode) = env::var("TEST_DB_LOCK_HELPER") {
        let db_path_str = env::var("TEST_DB_PATH").unwrap();
        let db_path = Path::new(&db_path_str);
        let config = Arc::new(Config::small());
        let key_shape = KeyShape::new_single(8, 16, KeyType::uniform(16));

        let result = Db::open(db_path, key_shape, config, Metrics::new());
        std::process::exit(match (mode.as_str(), result) {
            ("locked", Err(DbError::Io(e))) if e.kind() == std::io::ErrorKind::AlreadyExists => 1,
            ("unlocked", Ok(_)) => 0,
            _ => 2,
        });
    }

    let dir = tempdir::TempDir::new("test-lock").unwrap();
    let config = Arc::new(Config::small());
    let key_shape = KeyShape::new_single(8, 16, KeyType::uniform(16));

    let run_subprocess = |mode: &str| {
        Command::new(env::current_exe().unwrap())
            .args(["db::tests::db_tests::test_db_lock", "--exact"])
            .env("TEST_DB_LOCK_HELPER", mode)
            .env("TEST_DB_PATH", dir.path().to_str().unwrap())
            .output()
            .unwrap()
            .status
            .code()
    };

    // Test with lock held
    {
        let db = Db::open(dir.path(), key_shape.clone(), config, Metrics::new()).unwrap();
        assert_eq!(run_subprocess("locked"), Some(1), "Should be locked");
        db.wait_for_background_threads_to_finish();
    }

    // Test after lock released
    assert_eq!(run_subprocess("unlocked"), Some(0), "Should be unlocked");
}

#[test]
fn test_multi_thread_write() {
    let dir = tempdir::TempDir::new("test-batch").unwrap();
    let config = Config::small();
    let config = Arc::new(config);
    let key_shape = KeyShape::new_single(8, 16, KeyType::uniform(16));
    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
    let ks = db.single_ks();
    let threads = 8u64;
    let mut jhs = Vec::with_capacity(threads as usize);
    let iterations = 256u64;
    for t in 0..threads {
        let db = db.clone();
        let jh = thread::spawn(move || {
            for i in 0..iterations {
                let key = (t << 16) + i;
                let value = (i << 16) + t;
                db.insert(ks, key.to_be_bytes().to_vec(), value.to_be_bytes().to_vec())
                    .unwrap();
            }
        });
        jhs.push(jh);
    }
    for jh in jhs {
        jh.join().unwrap();
    }
    for t in 0..threads {
        for i in 0..iterations {
            let key = (t << 16) + i;
            let expected_value = (i << 16) + t;
            let expected_value = expected_value.to_be_bytes();
            let value = db.get(ks, &key.to_be_bytes()).unwrap();
            let value = value.unwrap();
            assert_eq!(&expected_value, value.as_ref());
        }
    }
}

// `force_rebuild_control_region` must include all recent writes in the snapshot,
// leaving the table fully clean even when the async WAL tracker lags behind writes
// under load (a lagging tracker used to leave flushed entries stuck dirty).
#[test]
fn test_force_rebuild_clean_under_load() {
    // Spinners keep all cores busy so the WAL tracker lags behind writes.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut spinners = Vec::new();
    for _ in 0..8 {
        let stop = stop.clone();
        spinners.push(thread::spawn(move || {
            let mut x = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
                std::hint::black_box(x);
            }
        }));
    }

    for iter in 0..60 {
        let dir = tempdir::TempDir::new("test-rebuild-clean").unwrap();
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("objects", 8, 16, KeyType::uniform(8));
        builder.add_key_space("metadata", 8, 16, KeyType::uniform(8));
        let key_shape = builder.build();
        let db = Db::open(
            dir.path(),
            key_shape,
            Arc::new(Config::default()),
            Metrics::new(),
        )
        .unwrap();
        let objects = db.ks("objects");
        let metadata = db.ks("metadata");
        for i in 0..5u8 {
            db.insert(objects, format!("key{i:02}___").into_bytes(), vec![i; 16])
                .unwrap();
        }
        for i in 0..3u8 {
            db.insert(
                metadata,
                format!("key{i:02}___").into_bytes(),
                vec![i + 10; 8],
            )
            .unwrap();
        }
        db.force_rebuild_control_region().unwrap();
        assert!(
            db.large_table.is_all_clean(),
            "iter {iter}: table not clean after force_rebuild_control_region",
        );
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for s in spinners {
        s.join().unwrap();
    }
}

#[test]
fn test_batch() {
    test_batch_impl(Config::small());
}

#[test]
fn test_batch_commit_pool() {
    let mut config = Config::small();
    config.commit_pool_size = 4;
    test_batch_impl(config);
}

fn test_batch_impl(config: Config) {
    let dir = tempdir::TempDir::new("test-batch").unwrap();
    let config = Arc::new(config);
    let key_shape = KeyShape::new_single(4, 16, KeyType::uniform(16));
    let metrics = Metrics::new();
    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
    let ks = db.single_ks();
    let mut batch = db.write_batch();
    batch.write(ks, vec![5, 6, 7, 8], vec![15]);
    batch.write(ks, vec![6, 7, 8, 9], vec![17]);

    // Check pending_table_len after writes but before commit
    // With deferred pending table updates, pending entries are only added on commit
    let pending_len = metrics.pending_table_len.with_label_values(&["root"]).get();
    assert_eq!(
        pending_len, 0,
        "Should have 0 pending entries before commit (deferred behavior)"
    );

    batch.commit().unwrap();

    // Check pending_table_len after commit — promotion threads may have already processed some
    // entries, so the count is at most 2, not necessarily exactly 2.
    let pending_len = metrics.pending_table_len.with_label_values(&["root"]).get();
    assert!(
        pending_len <= 2,
        "Should have at most 2 pending entries after commit, got {pending_len}"
    );

    assert_eq!(Some(vec![15].into()), db.get(ks, &[5, 6, 7, 8]).unwrap());

    // Check pending_table_len after first get (promote_pending is called for that cell)
    let pending_len = metrics.pending_table_len.with_label_values(&["root"]).get();
    // Could be 0, 1, or 2 depending on whether keys are in same cell
    // If keys are in different cells, only one cell's pending_table is cleared
    let pending_after_first_get = pending_len;

    assert_eq!(Some(vec![17].into()), db.get(ks, &[6, 7, 8, 9]).unwrap());

    // After both gets, all pending entries should be promoted
    let pending_len = metrics.pending_table_len.with_label_values(&["root"]).get();
    assert_eq!(
        pending_len, 0,
        "Should have 0 pending entries after both gets (was {} after first get)",
        pending_after_first_get
    );
}

#[test]
fn test_batch_lru() {
    let dir = tempdir::TempDir::new("test-batch-lru").unwrap();
    let config = Arc::new(Config::small());
    let metrics = Metrics::new();

    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new().with_value_cache_size(100);
    ksb.add_key_space_config("ks", 4, 16, KeyType::uniform(16), ksc);
    let key_shape = ksb.build();

    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
    let ks = db.ks("ks");

    // Test batch writes populate LRU cache during promote_pending
    let mut batch = db.write_batch();
    batch.write(ks, vec![1, 2, 3, 4], vec![10]);
    batch.write(ks, vec![2, 3, 4, 5], vec![20]);
    batch.commit().unwrap();

    // First access: promote_pending is called which populates LRU, then LRU is checked
    assert_eq!(Some(vec![10].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
    assert_eq!(Some(vec![20].into()), db.get(ks, &[2, 3, 4, 5]).unwrap());

    // Second access: promote_pending does nothing (already promoted), then LRU is checked and hits
    assert_eq!(Some(vec![10].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
    assert_eq!(Some(vec![20].into()), db.get(ks, &[2, 3, 4, 5]).unwrap());

    // Check that LRU cache was hit on all four get() calls
    // (promote_pending populates LRU, then subsequent gets hit the cache)
    let lru_hits = metrics
        .lookup_result
        .with_label_values(&["ks", "found", "lru"])
        .get();
    assert_eq!(
        lru_hits, 4,
        "All four get() calls should have been served from LRU cache"
    );

    // Test overwrite in batch updates LRU cache
    let mut batch = db.write_batch();
    batch.write(ks, vec![1, 2, 3, 4], vec![30]); // Overwrite with new value
    batch.commit().unwrap();

    // Access the overwritten key - should get the new value from LRU
    assert_eq!(Some(vec![30].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());

    // Verify LRU was hit (not read from disk/index)
    let lru_hits_after_overwrite = metrics
        .lookup_result
        .with_label_values(&["ks", "found", "lru"])
        .get();
    assert_eq!(
        lru_hits_after_overwrite, 5,
        "Overwritten value should be served from updated LRU cache"
    );

    // Test delete in batch removes from LRU cache
    let mut batch = db.write_batch();
    batch.delete(ks, vec![1, 2, 3, 4]);
    batch.commit().unwrap();

    // Key should be deleted and not in LRU cache
    assert_eq!(None, db.get(ks, &[1, 2, 3, 4]).unwrap());
}

#[test]
fn test_batch_replay() {
    test_batch_replay_impl(Config::small());
}

#[test]
fn test_batch_replay_compressed() {
    let mut config = Config::small();
    config.batch_codec = Some(BatchCodec::Lz4);
    test_batch_replay_impl(config);
}

fn test_batch_replay_impl(config: Config) {
    let dir = tempdir::TempDir::new("test_batch_replay").unwrap();
    let config = Arc::new(config);
    let key_shape = KeyShape::new_single(4, 16, KeyType::uniform(16));
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        let ks = db.single_ks();
        let mut batch = db.write_batch();
        batch.write(ks, vec![5, 6, 7, 8], vec![15]);
        batch.write(ks, vec![6, 7, 8, 9], vec![17]);
        batch.commit().unwrap();
    }
    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
    let ks = db.single_ks();
    assert_eq!(Some(vec![15].into()), db.get(ks, &[5, 6, 7, 8]).unwrap());
    assert_eq!(Some(vec![17].into()), db.get(ks, &[6, 7, 8, 9]).unwrap());
}

/// With `batch_codec = Some(_)`, every entry in a batch shares one WAL
/// position. Duplicate ops on the same key in one batch used to crash the
/// promotion thread (`Index WAL position must be increasing`). The dedup
/// pass should now collapse them to last-wins. Covers both repeated
/// inserts and an insert-then-delete pair, with verification across
/// reopen so replay also exercises the deduped frame.
#[test]
fn test_compressed_batch_dedups_duplicate_keys() {
    let dir = tempdir::TempDir::new("test_compressed_dedup").unwrap();
    let mut config = Config::small();
    config.batch_codec = Some(BatchCodec::Lz4);
    let config = Arc::new(config);
    let key_shape = KeyShape::new_single(4, 16, KeyType::uniform(16));

    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        let ks = db.single_ks();

        // Repeated inserts — last value wins.
        let mut batch = db.write_batch();
        batch.write(ks, vec![1, 2, 3, 4], vec![10]);
        batch.write(ks, vec![1, 2, 3, 4], vec![20]);
        batch.write(ks, vec![1, 2, 3, 4], vec![30]);
        batch.commit().unwrap();
        assert_eq!(Some(vec![30].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());

        // Insert-then-delete in one batch — delete wins.
        let mut batch = db.write_batch();
        batch.write(ks, vec![5, 6, 7, 8], vec![55]);
        batch.delete(ks, vec![5, 6, 7, 8]);
        batch.commit().unwrap();
        assert_eq!(None, db.get(ks, &[5, 6, 7, 8]).unwrap());

        // Delete-then-insert — insert wins.
        let mut batch = db.write_batch();
        batch.delete(ks, vec![9, 9, 9, 9]); // tombstone for a missing key (no-op)
        batch.write(ks, vec![9, 9, 9, 9], vec![99]);
        batch.commit().unwrap();
        assert_eq!(Some(vec![99].into()), db.get(ks, &[9, 9, 9, 9]).unwrap());
    }

    // Reopen and re-verify — replay walks the same deduped frames.
    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
    let ks = db.single_ks();
    assert_eq!(Some(vec![30].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
    assert_eq!(None, db.get(ks, &[5, 6, 7, 8]).unwrap());
    assert_eq!(Some(vec![99].into()), db.get(ks, &[9, 9, 9, 9]).unwrap());
}

/// Reproduce the production crash window where a writer wrote a skip
/// marker that terminates fragment M but the process died before
/// fragment M+1's file was created. The relevant code window is in
/// `WalWriter::multi_write` (`wal/mod.rs:81-122`) between the
/// `write_skip_marker` call and the subsequent `get_writeable_map`,
/// during which the mapper thread is asked to create the new file.
///
/// On replay this manifests as: iterator reads through fragment M,
/// hits the skip marker, advances `self.position` into fragment M+1,
/// then `make_map` returns `EndOfWal` because the file is missing.
/// Without the fix, `WalIterator::into_writer` then tripped two
/// "position must be in current map" assertions. With the fix the
/// recovered `WalWriter` must:
///   1. Open the Db without panicking.
///   2. Surface only the records that survived (record A in fragment M).
///   3. Be able to materialise fragment M+1 on a fresh insert and
///      land it at the start of that fragment.
///   4. Persist that fresh insert across another close/open cycle.
#[test]
fn test_replay_recovers_from_missing_next_fragment_file() {
    let dir =
        tempdir::TempDir::new("test_replay_recovers_from_missing_next_fragment_file").unwrap();
    // One fragment per WAL file so the missing fragment maps to a
    // single, easy-to-remove on-disk file.
    let mut config = Config::small();
    config.frag_size = 8 * 1024;
    config.wal_file_size = 8 * 1024;
    let config = Arc::new(config);
    let key_shape = KeyShape::new_single(16, 16, KeyType::uniform(16));

    let key_a = vec![0x01u8; 16];
    let key_b = vec![0x02u8; 16];
    let key_c = vec![0x03u8; 16];
    // 5000-byte values: one record's frame fits in an 8 KiB fragment,
    // two don't — `multi_write` writes a skip marker and places the
    // second record at the start of fragment 1.
    let value_a = vec![0xAAu8; 5000];
    let value_b = vec![0xBBu8; 5000];
    let value_c = vec![0xCCu8; 32];

    // Phase 1: write two large records. Drop the Db so background
    // threads (mapper, tracker, syncer, flusher) join cleanly before we
    // touch on-disk WAL files.
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        let ks = db.single_ks();
        db.insert(ks, key_a.clone(), value_a.clone()).unwrap();
        db.insert(ks, key_b.clone(), value_b.clone()).unwrap();
    }

    let file_0 = dir.path().join("wal_0000000000000000");
    let file_1 = dir.path().join("wal_0000000000000001");
    assert!(file_0.exists(), "file 0 should exist after phase 1");
    assert!(file_1.exists(), "file 1 should exist after phase 1");

    // Phase 2: simulate the production crash window by truncating all
    // WAL files past file 0. We must remove *every* file >= 1, not
    // just file 1, because the live writer's mapper had already
    // pre-created `INITIAL_MAPS_BUFFER` files ahead — leaving any of
    // those on disk would produce a sparse `{0, 2, 3, ...}` layout that
    // a real crash cannot produce (writers create files strictly
    // sequentially; GC only deletes prefixes). The post-crash on-disk
    // layout is always a contiguous prefix.
    let entries: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
    for entry in entries {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let name = name.to_str().unwrap_or("");
        if let Some(id_str) = name.strip_prefix("wal_")
            && let Ok(id) = u64::from_str_radix(id_str, 16)
            && id >= 1
        {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }
    assert!(file_0.exists(), "file 0 should still exist after deletion");
    assert!(!file_1.exists(), "file 1 should have been removed");

    // Phase 3: re-open. Replay walks file 0, hits the skip marker,
    // tries to load fragment 1 and gets `EndOfWal` from `make_map`.
    // Previously `WalIterator::into_writer` panicked here; with the
    // fix, the Db opens, surfaces only record A (record B's file is
    // gone), and accepts a fresh insert that materialises fragment 1
    // and lands at its start.
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        let ks = db.single_ks();
        assert_eq!(
            db.get(ks, &key_a).unwrap(),
            Some(value_a.clone().into()),
            "record A in fragment 0 should survive the crash"
        );
        assert_eq!(
            db.get(ks, &key_b).unwrap(),
            None,
            "record B was in the deleted fragment 1 — must be absent",
        );
        db.insert(ks, key_c.clone(), value_c.clone()).unwrap();
        assert_eq!(
            db.get(ks, &key_c).unwrap(),
            Some(value_c.clone().into()),
            "fresh insert after recovery must be readable",
        );
    }
    assert!(
        file_1.exists(),
        "writer should have materialised fragment 1's file when accepting the fresh insert",
    );

    // Phase 4: re-open one more time. The fresh insert in phase 3 must
    // be durable across another close/replay cycle; record A still
    // survives; record B is still absent.
    {
        let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
        let ks = db.single_ks();
        assert_eq!(db.get(ks, &key_a).unwrap(), Some(value_a.into()));
        assert_eq!(db.get(ks, &key_b).unwrap(), None);
        assert_eq!(db.get(ks, &key_c).unwrap(), Some(value_c.into()));
    }
}

#[test]
fn test_corrupted_batch_replay() {
    let dir = tempdir::TempDir::new("test_corrupted_batch_replay").unwrap();
    let config = Arc::new(Config::small());
    let (key_a, key_b) = (vec![5, 6, 7, 8], vec![6, 7, 8, 9]);
    let (value_a, value_b) = (vec![15], vec![17]);

    let key_shape = KeyShape::new_single(4, 16, KeyType::uniform(16));
    let (position, file) = {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        let ks = db.single_ks();
        let mut batch = db.write_batch();
        batch.write(ks, key_a.clone(), value_a.clone());
        batch.write(ks, key_b.clone(), value_b.clone());
        batch.commit().unwrap();
        let mut batch = db.write_batch();
        batch.write(ks, key_a.clone(), vec![20]);
        batch.write(ks, key_b.clone(), vec![23]);
        batch.commit().unwrap();

        let position = db.wal_writer.position();
        let record_length = CrcFrame::CRC_HEADER_LENGTH as u64 + 4 + 1 + 4;
        let offset = config.wal_layout(WalKind::Replay).align(record_length) - record_length;
        let file = db.wal.file().try_clone().unwrap();

        (position - offset - 1, file)
    };
    // Corrupt the last byte of the final entry in the last batch
    let mut data = [0u8; 1];
    file.read_exact_at(&mut data, position).unwrap();
    data[0] = !data[0];
    file.write_all_at(&data, position).unwrap();

    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
    let ks = db.single_ks();
    assert_eq!(Some(value_a.into()), db.get(ks, &key_a).unwrap());
    assert_eq!(Some(value_b.into()), db.get(ks, &key_b).unwrap());
}

#[test]
fn test_concurrent_batch() {
    let dir = tempdir::TempDir::new("test_concurrent_batch").unwrap();
    let config = Arc::new(Config::small());
    let ksc = KeySpaceConfig::new().with_value_cache_size(10);
    let key_shape = KeyShape::new_single_config(1, 16, KeyType::uniform(16), ksc);
    let (key_a, key_b, key_c) = (vec![15], vec![16], vec![17]);
    let ks = KeySpaces::from_key_shape(&key_shape).single();
    let get_value = |db: &Arc<Db>, key: _| {
        let bytes = db.get(ks, key).unwrap().unwrap();
        usize::from_be_bytes(bytes.as_ref().try_into().unwrap())
    };
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        let num_threads = 1000;
        let mut handles = Vec::with_capacity(num_threads);
        for thread_id in 0..num_threads {
            let db = db.clone();
            let (key_a, key_b, key_c) = (key_a.clone(), key_b.clone(), key_c.clone());
            let handle = thread::spawn(move || {
                let mut batch = db.write_batch();
                let (a, b) = (thread_id, thread_id * 2);
                batch.write(ks, key_a, thread_id.to_be_bytes().to_vec());
                batch.write(ks, key_b, (thread_id * 2).to_be_bytes().to_vec());
                batch.write(ks, key_c, (a + b).to_be_bytes().to_vec());
                batch.commit().unwrap();
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
        let (a, b, c) = (
            get_value(&db, &key_a),
            get_value(&db, &key_b),
            get_value(&db, &key_c),
        );
        assert_eq!(a + b, c);
    }
    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();

    let (a, b, c) = (
        get_value(&db, &key_a),
        get_value(&db, &key_b),
        get_value(&db, &key_c),
    );
    // verify that no matter which batch is last, the state remains consistent
    assert_eq!(a + b, c);
}

// see generate.py
pub(super) fn test_remove(key_shape: KeyShape) {
    let dir = tempdir::TempDir::new("test-remove").unwrap();
    let config = Arc::new(Config::small());
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        let ks = db.single_ks();
        db.insert(ks, vec![1, 2, 3, 4], vec![5, 6]).unwrap();
        db.insert(ks, vec![3, 4, 5, 6], vec![7]).unwrap();
        assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
        assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
        db.remove(ks, vec![1, 2, 3, 4]).unwrap();
        assert_eq!(None, db.get(ks, &[1, 2, 3, 4]).unwrap());
        db.remove(ks, vec![1, 2, 3, 4]).unwrap();
        assert_eq!(None, db.get(ks, &[1, 2, 3, 4]).unwrap());
    }
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            force_unload_config(&config),
            Metrics::new(),
        )
        .unwrap();
        let ks = db.single_ks();
        assert_eq!(None, db.get(ks, &[1, 2, 3, 4]).unwrap());
        db.insert(ks, vec![1, 2, 3, 4], vec![9, 10]).unwrap();
        assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
        assert_eq!(Some(vec![9, 10].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
        thread::sleep(Duration::from_millis(100)); // todo replace this with wal tracker barrier
        db.rebuild_control_region().unwrap();
        db.remove(ks, vec![1, 2, 3, 4]).unwrap();
        assert_eq!(None, db.get(ks, &[1, 2, 3, 4]).unwrap());
    }
    {
        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();
        let ks = db.single_ks();
        assert_eq!(metrics.replayed_wal_records.get(), 1);
        assert_eq!(None, db.get(ks, &[1, 2, 3, 4]).unwrap());
        assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
    }
}

// see generate.py
pub(super) fn test_iterator(key_shape: KeyShape) {
    let dir = tempdir::TempDir::new("test-iterator").unwrap();
    let config = Arc::new(Config::small());
    let mut data = Vec::with_capacity(1024);
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        let ks = db.single_ks();
        let mut it = db.iterator(ks);
        assert!(it.next().is_none());
        for v in 0..1024u32 {
            let v = v * 3;
            let k = ku32(v);
            let v = vu32(v);
            data.push((k.clone(), v.clone()));
            db.insert(ks, k, v).unwrap();
        }
        let it = db.iterator(ks);
        let s: DbResult<Vec<_>> = it.collect();
        let s = s.unwrap();
        assert_eq!(s, data);
    }
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        let ks = db.single_ks();
        let it = db.iterator(ks);
        let s: DbResult<Vec<_>> = it.collect();
        let s = s.unwrap();
        assert_eq!(s, data);

        let mut it = db.iterator(ks);
        it.set_lower_bound(ku32(6));
        assert_eq!((ku32(6), vu32(6)), it.next().unwrap().unwrap());

        let mut it = db.iterator(ks);
        it.set_lower_bound(ku32(7));
        assert_eq!((ku32(9), vu32(9)), it.next().unwrap().unwrap());

        let mut it = db.iterator(ks);
        it.set_lower_bound(ku32(1024 * 3));
        assert!(it.next().is_none());

        let mut it = db.iterator(ks);
        it.set_lower_bound(ku32(12));
        it.set_upper_bound(ku32(16));
        assert_eq!((ku32(12), vu32(12)), it.next().unwrap().unwrap());
        assert_eq!((ku32(15), vu32(15)), it.next().unwrap().unwrap());
        assert!(it.next().is_none());

        let mut it = db.iterator(ks);
        it.set_lower_bound(ku32(12));
        it.set_upper_bound(ku32(15));
        assert_eq!((ku32(12), vu32(12)), it.next().unwrap().unwrap());
        assert!(it.next().is_none());

        // Reverse iterator
        let mut it = db.iterator(ks);
        it.set_lower_bound(ku32(12));
        it.set_upper_bound(ku32(15));
        it.reverse();
        assert_eq!((ku32(12), vu32(12)), it.next().unwrap().unwrap());
        assert!(it.next().is_none());
    }
}

#[test]
fn test_iterator_gen() {
    let sequential = Vec::from_iter(125u128..1125);
    let mut random = sequential.clone();
    ThreadRng::default().fill(&mut random[..]);
    random.sort();
    for reduced in [true, false] {
        let key_indexing = if reduced {
            // For the sequential test we reduce key to last 8 bytes,
            // since they are the only ones that are different
            KeyIndexing::key_reduction(16, 8..16)
        } else {
            KeyIndexing::fixed(16)
        };
        println!("Starting sequential test, reduced={reduced}");
        test_iterator_run(sequential.clone(), key_indexing.clone());

        let key_indexing = if reduced {
            // For the random test, we reduce key to first 8 bytes as they are most significant
            KeyIndexing::key_reduction(16, 0..8)
        } else {
            KeyIndexing::fixed(16)
        };
        println!("Starting random test, reduced={reduced}");
        test_iterator_run(random.clone(), key_indexing);
    }
}

fn test_iterator_run(data: Vec<u128>, key_indexing: KeyIndexing) {
    let dir = tempdir::TempDir::new("test-iterator").unwrap();
    let config = Arc::new(Config::small());
    let key_shape = KeyShape::new_single_config_indexing(
        key_indexing,
        4,
        KeyType::uniform(4),
        KeySpaceConfig::default(),
    );
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();
    let ks = db.single_ks();
    for (i, k) in data.iter().enumerate() {
        if i % 2 == 0 {
            db.insert(ks, ku128(*k), vu128(*k)).unwrap();
        } else {
            // Write some values with batch write to make sure there is no difference with regular write
            let mut batch = db.write_batch();
            batch.write(ks, ku128(*k), vu128(*k));
            batch.commit().unwrap();
        }
    }
    let mut rng = ThreadRng::default();
    for reverse in [true, false] {
        println!("Testing with reverse={reverse}");
        for _ in 0..128 {
            let from = rng.gen_range(0..data.len() - 1);
            let to = rng.gen_range(from + 1..data.len());
            test_iterator_slice(&db, ks, &data[from..to], reverse);
        }
    }
}

fn test_iterator_slice(db: &Arc<Db>, ks: KeySpace, slice: &[u128], reverse: bool) {
    let mut iterator = db.iterator(ks);
    iterator.set_lower_bound(ku128(slice[0]));
    iterator.set_upper_bound(ku128(slice[slice.len() - 1] + 1));
    if reverse {
        iterator.reverse();
    }
    let data: Vec<_> = iterator.collect::<DbResult<_>>().unwrap();
    assert_eq!(data.len(), slice.len());
    let slice_iter: Box<dyn Iterator<Item = &u128>> = if reverse {
        Box::new(slice.iter().rev())
    } else {
        Box::new(slice.iter())
    };
    for ((key, value), expected) in data.into_iter().zip(slice_iter) {
        assert_eq!(key, ku128(*expected));
        assert_eq!(value, vu128(*expected));
    }
}

#[test]
#[ignore = "long test"]
fn test_extensive_iterator_random_ranges() {
    test_extensive_iterator_random_ranges_for_key_type(KeyType::uniform(1));
    test_extensive_iterator_random_ranges_for_key_type(KeyType::uniform(16));
    test_extensive_iterator_random_ranges_for_key_type(KeyType::prefix_uniform(1, 0));
    test_extensive_iterator_random_ranges_for_key_type(KeyType::prefix_uniform(1, 4));
    test_extensive_iterator_random_ranges_for_key_type(KeyType::prefix_uniform(1, 3));
    test_extensive_iterator_random_ranges_for_key_type(KeyType::prefix_uniform(2, 0));
    test_extensive_iterator_random_ranges_for_key_type(KeyType::prefix_uniform(2, 4));
    test_extensive_iterator_random_ranges_for_key_type(KeyType::prefix_uniform(2, 3));
}

fn test_extensive_iterator_random_ranges_for_key_type(key_type: KeyType) {
    println!("Testing extensive iterator with KeyType: {:?}", key_type);
    let dir = tempdir::TempDir::new("test-extensive-iterator").unwrap();
    let config = Arc::new(Config::small());
    let key_shape = KeyShape::new_single(2, 16, key_type);

    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();
    let ks = db.single_ks();

    // Fill a database with values
    const MAX: u16 = 0xffff;
    for i in 0u16..MAX {
        let key = i.to_be_bytes().to_vec();
        let value = format!("value_{:04x}", i).into_bytes();
        db.insert(ks, key, value).unwrap();
    }

    // Test iterator 1000 times with random ranges
    let mut rng = StdRng::seed_from_u64(42); // Use seeded RNG for reproducibility

    for iteration in 0..10000 {
        // Generate random range bounds
        let start = rng.gen_range(0u16..MAX);
        let end = rng.gen_range(start..=std::cmp::min(start.saturating_add(1000), MAX));

        // Randomly decide forward or reverse iteration
        let reverse = rng.gen_bool(0.5);

        // Create iterator with bounds
        let mut iterator = db.iterator(ks);
        iterator.set_lower_bound(start.to_be_bytes().to_vec());
        iterator.set_upper_bound(end.to_be_bytes().to_vec());

        if reverse {
            iterator.reverse();
        }

        // Collect all items from iterator
        let items: Vec<_> = iterator.collect::<DbResult<_>>().unwrap();

        // Verify results
        let expected_count = (end - start) as usize;

        assert_eq!(
            items.len(),
            expected_count,
            "Iteration {}: range [{:04x}, {:04x}), reverse={}, expected {} items but got {}",
            iteration,
            start,
            end,
            reverse,
            expected_count,
            items.len()
        );

        // Verify actual key-value pairs
        if reverse {
            // In reverse mode, we expect keys from (end-1) down to start
            for (idx, (key, value)) in items.iter().enumerate() {
                let expected_key_num = end - 1 - idx as u16;
                let expected_value = format!("value_{:04x}", expected_key_num).into_bytes();

                // First check the key value as u16 for better error messages
                let actual_key_num = u16::from_be_bytes([key[0], key[1]]);
                assert_eq!(
                    actual_key_num, expected_key_num,
                    "Iteration {} (reverse): Wrong key at position {}. Expected {:04x}, got {:04x}",
                    iteration, idx, expected_key_num, actual_key_num
                );

                assert_eq!(
                    value.as_ref(),
                    &expected_value[..],
                    "Iteration {} (reverse): Wrong value for key {:04x} at position {}",
                    iteration,
                    expected_key_num,
                    idx
                );
            }
        } else {
            // In forward mode, we expect keys from start to (end-1)
            for (idx, (key, value)) in items.iter().enumerate() {
                let expected_key_num = start + idx as u16;
                let expected_value = format!("value_{:04x}", expected_key_num).into_bytes();

                // First check the key value as u16 for better error messages
                let actual_key_num = u16::from_be_bytes([key[0], key[1]]);
                assert_eq!(
                    actual_key_num, expected_key_num,
                    "Iteration {} (forward): Wrong key at position {}. Expected {:04x}, got {:04x}",
                    iteration, idx, expected_key_num, actual_key_num
                );

                assert_eq!(
                    value.as_ref(),
                    &expected_value[..],
                    "Iteration {} (forward): Wrong value for key {:04x} at position {}",
                    iteration,
                    expected_key_num,
                    idx
                );
            }
        }

        // Additional check: verify that all returned keys are within the range
        for (key, _) in &items {
            let key_num = u16::from_be_bytes([key[0], key[1]]);
            assert!(
                key_num >= start && key_num < end,
                "Iteration {}: Key {:04x} is outside the range [{:04x}, {:04x})",
                iteration,
                key_num,
                start,
                end
            );
        }
    }

    // Additional edge case tests

    // Test empty range
    let mut iterator = db.iterator(ks);
    iterator.set_lower_bound(0x0500u16.to_be_bytes().to_vec());
    iterator.set_upper_bound(0x0500u16.to_be_bytes().to_vec());
    let items: Vec<_> = iterator.collect::<DbResult<_>>().unwrap();
    assert_eq!(items.len(), 0, "Empty range should return no items");

    // Test single item range
    let mut iterator = db.iterator(ks);
    iterator.set_lower_bound(0x0500u16.to_be_bytes().to_vec());
    iterator.set_upper_bound(0x0501u16.to_be_bytes().to_vec());
    let items: Vec<_> = iterator.collect::<DbResult<_>>().unwrap();
    assert_eq!(items.len(), 1, "Single item range should return 1 item");
    assert_eq!(items[0].0.as_ref(), &0x0500u16.to_be_bytes()[..]);

    // Test full range forward
    let mut iterator = db.iterator(ks);
    iterator.set_lower_bound(0x0000u16.to_be_bytes().to_vec());
    let items: Vec<_> = iterator.collect::<DbResult<_>>().unwrap();
    assert_eq!(
        items.len(),
        MAX as usize,
        "Full range forward should return all items"
    );

    // Test full range reverse
    let mut iterator = db.iterator(ks);
    iterator.set_lower_bound(0x0000u16.to_be_bytes().to_vec());
    iterator.reverse();
    let items: Vec<_> = iterator.collect::<DbResult<_>>().unwrap();
    assert_eq!(
        items.len(),
        MAX as usize,
        "Full range reverse should return all items"
    );

    // Verify first and last items in reverse
    assert_eq!(
        items[0].0.as_ref(),
        &(MAX - 1).to_be_bytes()[..],
        "First item in reverse is not correct"
    );
    assert_eq!(
        items[(MAX - 1) as usize].0.as_ref(),
        &0x0000u16.to_be_bytes()[..],
        "Last item in reverse should be 0x0000"
    );
}

#[test]
fn test_ordered_iterator() {
    let dir = tempdir::TempDir::new("test-ordered-iterator").unwrap();
    let config = Arc::new(Config::small());
    let key_shape = KeyShape::new_single(5, 16, KeyType::uniform(16));
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        let ks = db.single_ks();
        let mut it = db.iterator(ks);
        assert!(it.next().is_none());
        db.insert(ks, vec![1, 2, 3, 4, 6], vec![1]).unwrap();
        db.insert(ks, vec![1, 2, 3, 4, 5], vec![2]).unwrap();
        db.insert(ks, vec![1, 2, 3, 4, 10], vec![3]).unwrap();
        db.insert(ks, vec![3, 4, 5, 6, 11], vec![7]).unwrap();
        let mut it = db.iterator(ks);
        it.set_lower_bound(vec![1, 2, 3, 4, 0]);
        it.set_upper_bound(vec![1, 2, 3, 4, 10]);
        let v: DbResult<Vec<_>> = it.collect();
        let v = v.unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(
            v.first().unwrap(),
            &(vec![1, 2, 3, 4, 5].into(), vec![2].into())
        );
        assert_eq!(
            v.get(1).unwrap(),
            &(vec![1, 2, 3, 4, 6].into(), vec![1].into())
        );
    }
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        let ks = db.single_ks();
        let mut it = db.iterator(ks);
        it.set_lower_bound(vec![1, 2, 3, 4, 0]);
        it.set_upper_bound(vec![1, 2, 3, 4, 10]);
        let v: DbResult<Vec<_>> = it.collect();
        let v = v.unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(
            v.first().unwrap(),
            &(vec![1, 2, 3, 4, 5].into(), vec![2].into())
        );
        assert_eq!(
            v.get(1).unwrap(),
            &(vec![1, 2, 3, 4, 6].into(), vec![1].into())
        );
    }
}

#[test]
fn test_iterator_with_tombstones() {
    let dir = tempdir::TempDir::new("test-insert-while-iterating").unwrap();
    let config = Arc::new(Config::small());
    let key_shape = KeyShape::new_single(2, 16, KeyType::uniform(16));
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();
    let ks = db.single_ks();
    db.insert(ks, vec![1, 2], vec![1]).unwrap();
    db.insert(ks, vec![1, 3], vec![2]).unwrap();
    db.insert(ks, vec![1, 4], vec![3]).unwrap();
    db.remove(ks, vec![1, 3]).unwrap();
    let mut it = db.iterator(ks);
    assert_eq!(
        it.next().unwrap().unwrap(),
        (vec![1, 2].into(), vec![1].into())
    );
    assert_eq!(
        it.next().unwrap().unwrap(),
        (vec![1, 4].into(), vec![3].into())
    );
    assert!(it.next().is_none());
}

#[test]
fn test_insert_while_iterating() {
    let dir = tempdir::TempDir::new("test-insert-while-iterating").unwrap();
    let config = Arc::new(Config::small());
    let key_shape = KeyShape::new_single(5, 16, KeyType::uniform(16));
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();
    let ks = db.single_ks();
    db.insert(ks, vec![1, 2, 3, 4, 5], vec![1]).unwrap();
    db.insert(ks, vec![1, 2, 3, 4, 8], vec![2]).unwrap();
    let mut it = db.iterator(ks);

    let (k, _) = it.next().unwrap().unwrap();
    assert_eq!(k, vec![1, 2, 3, 4, 5]);

    db.insert(ks, vec![1, 2, 3, 4, 6], vec![3]).unwrap();

    let (k, _) = it.next().unwrap().unwrap();
    assert_eq!(k, vec![1, 2, 3, 4, 6]);

    let (k, _) = it.next().unwrap().unwrap();
    assert_eq!(k, vec![1, 2, 3, 4, 8]);
}

#[test]
fn test_iterator_bounds_no_reduction() {
    let dir = tempdir::TempDir::new("test-iterator-bounds").unwrap();
    let config = Arc::new(Config::small());
    let key_shape = KeyShape::new_single(4, 16, KeyType::uniform(16));

    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();
    let ks = db.single_ks();

    db.insert(ks, vec![0, 0, 0, 0], vec![1]).unwrap();
    db.insert(ks, vec![0, 0, 0, 1], vec![1]).unwrap();
    db.insert(ks, vec![255, 255, 255, 254], vec![2]).unwrap();
    db.insert(ks, vec![255, 255, 255, 255], vec![2]).unwrap();

    // forward iterator from 0
    let mut it = db.iterator(ks);
    it.set_lower_bound(vec![0, 0, 0, 0]);
    it.set_upper_bound(vec![0, 0, 0, 1]);
    let (k, v) = it.next().unwrap().unwrap();
    assert_eq!(k, vec![0, 0, 0, 0]);
    assert_eq!(v, vec![1]);
    assert!(it.next().is_none());

    // forward iterator from 1
    let mut it = db.iterator(ks);
    it.set_lower_bound(vec![0, 0, 0, 1]);
    it.set_upper_bound(vec![0, 0, 0, 2]);
    let (k, v) = it.next().unwrap().unwrap();
    assert_eq!(k, vec![0, 0, 0, 1]);
    assert_eq!(v, vec![1]);
    assert!(it.next().is_none());

    // reverse iterator to 0
    let mut it = db.iterator(ks);
    it.set_lower_bound(vec![0, 0, 0, 0]);
    it.set_upper_bound(vec![0, 0, 0, 1]);
    it.reverse();
    let (k, v) = it.next().unwrap().unwrap();
    assert_eq!(k, vec![0, 0, 0, 0]);
    assert_eq!(v, vec![1]);
    assert!(it.next().is_none());

    // reverse iterator to 1
    let mut it = db.iterator(ks);
    it.set_lower_bound(vec![0, 0, 0, 1]);
    it.set_upper_bound(vec![0, 0, 0, 2]);
    it.reverse();
    let (k, v) = it.next().unwrap().unwrap();
    assert_eq!(k, vec![0, 0, 0, 1]);
    assert_eq!(v, vec![1]);
    assert!(it.next().is_none());

    // forward iterator to 255
    let mut it = db.iterator(ks);
    it.set_lower_bound(vec![255, 255, 255, 254]);
    it.set_upper_bound(vec![255, 255, 255, 255]);
    let (k, v) = it.next().unwrap().unwrap();
    assert_eq!(k, vec![255, 255, 255, 254]);
    assert_eq!(v, vec![2]);
    assert!(it.next().is_none());

    // forward iterator to 254
    let mut it = db.iterator(ks);
    it.set_lower_bound(vec![255, 255, 255, 253]);
    it.set_upper_bound(vec![255, 255, 255, 254]);
    assert!(it.next().is_none());

    // reverse iterator from 255
    let mut it = db.iterator(ks);
    it.set_lower_bound(vec![255, 255, 255, 254]);
    it.set_upper_bound(vec![255, 255, 255, 255]);
    it.reverse();
    let (k, v) = it.next().unwrap().unwrap();
    assert_eq!(k, vec![255, 255, 255, 254]);
    assert_eq!(v, vec![2]);
    assert!(it.next().is_none());

    // reverse iterator from 255
    let mut it = db.iterator(ks);
    it.set_lower_bound(vec![255, 255, 255, 253]);
    it.set_upper_bound(vec![255, 255, 255, 254]);
    it.reverse();
    assert!(it.next().is_none());
}

#[test]
fn test_iterator_bounds_with_reduction() {
    let dir = tempdir::TempDir::new("test-iterator-bounds-with-reduction").unwrap();
    let config = Arc::new(Config::small());
    let key_indexing = KeyIndexing::key_reduction(4, 0..2);
    let key_shape = KeyShape::new_single_config_indexing(
        key_indexing,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::default(),
    );
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();
    let ks = db.single_ks();

    db.insert(ks, vec![0, 0, 0, 0], vec![1]).unwrap();
    db.insert(ks, vec![255, 255, 255, 253], vec![2]).unwrap();
    db.insert(ks, vec![255, 255, 255, 254], vec![2]).unwrap();

    // forward iterator from 0
    let mut it = db.iterator(ks);
    it.set_lower_bound(vec![0, 0, 0, 0]);
    it.set_upper_bound(vec![0, 0, 0, 1]);
    let (k, v) = it.next().unwrap().unwrap();
    assert_eq!(k, vec![0, 0, 0, 0]);
    assert_eq!(v, vec![1]);
    assert!(it.next().is_none());

    // forward iterator from 1
    let mut it = db.iterator(ks);
    it.set_lower_bound(vec![0, 0, 0, 1]);
    it.set_upper_bound(vec![0, 0, 0, 2]);
    assert!(it.next().is_none());

    // reverse iterator to 0
    let mut it = db.iterator(ks);
    it.set_lower_bound(vec![0, 0, 0, 0]);
    it.set_upper_bound(vec![0, 0, 0, 1]);
    it.reverse();
    let (k, v) = it.next().unwrap().unwrap();
    assert_eq!(k, vec![0, 0, 0, 0]);
    assert_eq!(v, vec![1]);
    assert!(it.next().is_none());

    // reverse iterator to 1
    let mut it = db.iterator(ks);
    it.set_lower_bound(vec![0, 0, 0, 1]);
    it.set_upper_bound(vec![0, 0, 0, 2]);
    it.reverse();
    assert!(it.next().is_none());

    // forward iterator to 255
    let mut it = db.iterator(ks);
    it.set_lower_bound(vec![255, 255, 255, 254]);
    it.set_upper_bound(vec![255, 255, 255, 255]);
    let (k, v) = it.next().unwrap().unwrap();
    assert_eq!(k, vec![255, 255, 255, 254]);
    assert_eq!(v, vec![2]);
    assert!(it.next().is_none());

    // reverse iterator from 255
    let mut it = db.iterator(ks);
    it.set_lower_bound(vec![255, 255, 255, 254]);
    it.set_upper_bound(vec![255, 255, 255, 255]);
    it.reverse();
    let (k, v) = it.next().unwrap().unwrap();
    assert_eq!(k, vec![255, 255, 255, 254]);
    assert_eq!(v, vec![2]);
    assert!(it.next().is_none());
}

#[test]
fn test_empty() {
    let dir = tempdir::TempDir::new("test-empty").unwrap();
    let config = Arc::new(Config::small());
    let key_shape = KeyShape::new_single(5, 16, KeyType::uniform(16));
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        let ks = db.single_ks();
        assert!(db.is_empty());
        db.insert(ks, vec![1, 2, 3, 4, 0], vec![1]).unwrap();
        assert!(!db.is_empty());
    }
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        assert!(!db.is_empty());
    }
}

#[test]
fn test_small_keys() {
    let dir = tempdir::TempDir::new("test-small-keys").unwrap();
    let config = Arc::new(Config::small());
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space("a", 0, 16, KeyType::uniform(16));
    ksb.add_key_space("b", 1, 16, KeyType::uniform(16));
    ksb.add_key_space("c", 2, 16, KeyType::uniform(16));
    ksb.add_key_space("d", 3, 16, KeyType::uniform(16));
    let key_shape = ksb.build();
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        let ks0 = db.ks("a");
        let ks1 = db.ks("b");
        let ks2 = db.ks("c");
        db.insert(ks0, vec![], vec![1]).unwrap();
        db.insert(ks1, vec![1], vec![2]).unwrap();
        db.insert(ks2, vec![1, 2], vec![3]).unwrap();
        assert_eq!(db.get(ks0, &[]).unwrap(), Some(vec![1].into()));
        assert_eq!(db.get(ks1, &[1]).unwrap(), Some(vec![2].into()));
        assert_eq!(db.get(ks2, &[1, 2]).unwrap(), Some(vec![3].into()));
    }
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        let ks0 = db.ks("a");
        let ks1 = db.ks("b");
        let ks2 = db.ks("c");
        assert_eq!(db.get(ks0, &[]).unwrap(), Some(vec![1].into()));
        assert_eq!(db.get(ks1, &[1]).unwrap(), Some(vec![2].into()));
        assert_eq!(db.get(ks2, &[1, 2]).unwrap(), Some(vec![3].into()));
    }
}

#[test]
fn test_value_cache() {
    let dir = tempdir::TempDir::new("test-value-cache").unwrap();
    let config = Arc::new(Config::small());
    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new().with_value_cache_size(512);
    ksb.add_key_space_config("k", 8, 1, KeyType::uniform(1), ksc);
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();
    let ks = db.ks("k");

    for i in 0..1024u64 {
        db.insert(ks, i.to_be_bytes().to_vec(), vec![]).unwrap();
    }
    for i in (0..1024u64).rev() {
        assert!(db.get(ks, &i.to_be_bytes()).unwrap().is_some());
    }

    let found_lru = metrics
        .lookup_result
        .with_label_values(&["k", "found", "lru"])
        .get();

    assert_eq!(found_lru, 512);
}

#[test]
fn test_bloom_filter() {
    let dir = tempdir::TempDir::new("test-bloom-filter").unwrap();
    let config = Arc::new(Config::small());
    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new().with_bloom_filter(0.01, 2000);
    ksb.add_key_space_config("k", 8, 1, KeyType::uniform(1), ksc);
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();
    let ks = db.ks("k");

    for i in 0..1000u64 {
        db.insert(ks, i.to_be_bytes().to_vec(), vec![]).unwrap();
    }

    for i in 0..1000u64 {
        assert!(db.exists(ks, &i.to_be_bytes()).unwrap());
    }

    for i in 1000..2000u64 {
        assert!(!db.exists(ks, &i.to_be_bytes()).unwrap());
    }
    let cache = metrics
        .lookup_result
        .with_label_values(&["k", "found", "cache"])
        .get();
    let i0 = metrics
        .lookup_result
        .with_label_values(&["k", "found", "index_0"])
        .get();
    let i1 = metrics
        .lookup_result
        .with_label_values(&["k", "found", "index_1"])
        .get();
    let found = cache + i0 + i1;
    let not_found_bloom = metrics
        .lookup_result
        .with_label_values(&["k", "not_found", "bloom"])
        .get();

    assert_eq!(found, 1000);
    if not_found_bloom < 900 {
        panic!("Bloom filter efficiency less then 90%");
    }
}

fn test_dirty_unloading_with_config(config: Arc<Config>) {
    let dir = tempdir::TempDir::new("test-dirty-unloading").unwrap();
    let key_shape = KeyShape::new_single(5, 2, KeyType::uniform(1024));
    let ks = KeySpaces::from_key_shape(&key_shape).single();
    #[track_caller]
    fn check_all(db: &Db, ks: KeySpace, last: u8) {
        for i in 5u8..=last {
            assert_eq!(db.get(ks, &[1, 2, 3, 4, i]).unwrap(), Some(vec![i].into()));
        }
    }
    #[track_caller]
    fn check_metrics(metrics: &Metrics, unmerge: u64, flush: u64, merge_flush: u64, clean: u64) {
        assert_eq!(
            metrics
                .unload
                .get_metric_with_label_values(&["unmerge"])
                .unwrap()
                .get(),
            unmerge,
            "unmerge metric does not match"
        );
        assert_eq!(
            metrics
                .unload
                .get_metric_with_label_values(&["flush"])
                .unwrap()
                .get(),
            flush,
            "flush metric does not match"
        );
        assert_eq!(
            metrics
                .unload
                .get_metric_with_label_values(&["merge_flush"])
                .unwrap()
                .get(),
            merge_flush,
            "merge_flush metric does not match"
        );
        assert_eq!(
            metrics
                .unload
                .get_metric_with_label_values(&["clean"])
                .unwrap()
                .get(),
            clean,
            "clean metric does not match"
        );
    }
    let other_key = vec![129, 2, 3, 4, 5];
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        {
            // todo rewrite
        }

        db.insert(ks, other_key.clone(), vec![5]).unwrap(); // fill one
        db.insert(ks, vec![1, 2, 3, 4, 5], vec![5]).unwrap();
        db.insert(ks, vec![1, 2, 3, 4, 6], vec![6]).unwrap();
        db.insert(ks, vec![1, 2, 3, 4, 7], vec![7]).unwrap();
        db.insert(ks, vec![1, 2, 3, 4, 8], vec![8]).unwrap();
        db.large_table.flusher.barrier();
        check_metrics(&db.metrics, 0, 1, 0, 0);
        db.insert(ks, vec![1, 2, 3, 4, 9], vec![9]).unwrap();
        db.insert(ks, vec![1, 2, 3, 4, 10], vec![10]).unwrap();
        check_all(&db, ks, 10);
        db.large_table.flusher.barrier();
        check_metrics(&db.metrics, 0, 1, 1, 0);
    }
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        check_all(&db, ks, 10);
        check_metrics(&db.metrics, 0, 0, 0, 0);
    }
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        db.insert(ks, vec![1, 2, 3, 4, 11], vec![11]).unwrap();
        db.insert(ks, vec![1, 2, 3, 4, 12], vec![12]).unwrap();
        db.large_table.flusher.barrier();
        check_metrics(&db.metrics, 0, 1, 0, 0);
        check_all(&db, ks, 12);
        db.insert(ks, vec![1, 2, 3, 4, 13], vec![13]).unwrap();
        db.get(ks, &other_key).unwrap().unwrap();
        check_metrics(&db.metrics, 0, 1, 0, 0);
    }
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            force_unload_config(&config),
            Metrics::new(),
        )
        .unwrap();
        check_all(&db, ks, 13);
        check_metrics(&db.metrics, 0, 0, 0, 0);
        db.rebuild_control_region().unwrap(); // this puts all entries into clean state
        assert!(
            db.large_table.is_all_clean(),
            "Some entries are not clean after snapshot"
        );
        db.get(ks, &other_key).unwrap().unwrap();
        check_metrics(&db.metrics, 0, 2, 0, 0);
    }
}

#[test]
#[ignore = "Test is flaky due to async WalTracker timing issue. Similar to test_dirty_unloading_sync_flush, \
when guards are dropped, last_processed isn't updated immediately, causing the flush to capture stale values \
and potentially skip entries that should be flushed. This needs to be fixed by either using guard position \
directly or implementing synchronous update mode for WalTracker."]
fn test_dirty_unloading() {
    let mut config = Config::small();
    config.max_dirty_keys = 2;
    test_dirty_unloading_with_config(Arc::new(config));
}

#[test]
#[ignore = "Test fails due to async WalTracker timing issue. When guards are dropped, \
last_processed isn't updated immediately, causing flush to capture 0 and skip everything. \
This needs to be fixed by either using guard position directly or implementing synchronous \
update mode for WalTracker."]
fn test_dirty_unloading_sync_flush() {
    let mut config = Config::small();
    config.max_dirty_keys = 2;
    config.sync_flush = true;
    test_dirty_unloading_with_config(Arc::new(config));
}

#[test]
fn test_value_cache_update_remove() {
    let dir = tempdir::TempDir::new("test-value-cache-update-remove").unwrap();
    let config = Arc::new(Config::small());
    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new().with_value_cache_size(10);
    ksb.add_key_space_config("k", 1, 1, KeyType::uniform(1), ksc);
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();
    let ks = db.ks("k");
    db.insert(ks, vec![1], vec![2]).unwrap();
    db.insert(ks, vec![2], vec![3]).unwrap();
    assert_eq!(&db.get(ks, &[1]).unwrap().unwrap(), &[2]);
    assert_eq!(&db.get(ks, &[2]).unwrap().unwrap(), &[3]);
    assert_eq!(
        2,
        metrics
            .lookup_result
            .with_label_values(&["k", "found", "lru"])
            .get()
    );
    db.insert(ks, vec![1], vec![4]).unwrap();
    assert_eq!(&db.get(ks, &[1]).unwrap().unwrap(), &[4]);
    db.remove(ks, vec![1]).unwrap();
    assert_eq!(db.get(ks, &[1]).unwrap(), None);
    assert_eq!(3, lru_lookups("k", &metrics));
}

#[test]
// This test verifies that the last value written into the large table
// cache matches the last value written to wal.
// Because wal write and write into large table are not done under single mutex,
// there can be race condition unless special measures are taken.
fn test_concurrent_single_value_update() {
    test_concurrent_single_value_update_impl(0, Default::default());
}

#[test]
// Same as test_concurrent_single_value_update but also randomly removes value.
// Makes sure that removal treated same way as update with regard to concurrency/ordering.
fn test_concurrent_single_value_update_remove() {
    test_concurrent_single_value_update_impl(70, Default::default());
}

#[test]
fn test_concurrent_single_value_update_lru() {
    let ks_config = KeySpaceConfig::default().with_value_cache_size(1000);
    test_concurrent_single_value_update_impl(0, ks_config);
}

#[test]
fn test_concurrent_single_value_update_remove_lru() {
    let ks_config = KeySpaceConfig::default().with_value_cache_size(1000);
    test_concurrent_single_value_update_impl(70, ks_config);
}

fn test_concurrent_single_value_update_impl(remove_chance_pct: u32, ks_config: KeySpaceConfig) {
    let num_threads = 8;
    let mut threads = Vec::with_capacity(num_threads);
    for i in 0..num_threads {
        let ks_config = ks_config.clone();
        let jh = thread::spawn(move || {
            for _ in 0..16 {
                test_concurrent_single_value_update_iteration(
                    i,
                    remove_chance_pct,
                    ks_config.clone(),
                )
            }
        });
        threads.push(jh);
    }
    for jh in threads {
        jh.join().unwrap();
    }
}
fn test_concurrent_single_value_update_iteration(
    i: usize,
    remove_chance_pct: u32,
    ks_config: KeySpaceConfig,
) {
    let dir = tempdir::TempDir::new(&format!("test-concurrent-single-value-update-{i}")).unwrap();
    let config = Arc::new(Config::small());
    let key_shape = KeyShape::new_single_config(4, 1, KeyType::uniform(1), ks_config);
    let ks = KeySpaces::from_key_shape(&key_shape).single();
    let cached_value;
    let key = Bytes::from(15u32.to_be_bytes().to_vec());
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        db.large_table.fp.0.write().fp_insert_before_lock =
            FailPoint::sleep(Duration::ZERO..Duration::from_millis(1));
        db.large_table.fp.0.write().fp_remove_before_lock =
            FailPoint::sleep(Duration::ZERO..Duration::from_millis(1));
        let num_threads = 16;
        let mut threads = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            let db = db.clone();
            let jh = thread::spawn(move || {
                let mut rng = ThreadRng::default();
                let key = Bytes::from(15u32.to_be_bytes().to_vec());
                for _ in 0..16 {
                    if rng.gen_range(0..100u32) < remove_chance_pct {
                        db.remove(ks, key.clone()).unwrap()
                    } else {
                        let value: u32 = rng.r#gen();
                        db.insert(ks, key.clone(), value.to_be_bytes().to_vec())
                            .unwrap();
                    }
                }
            });
            threads.push(jh);
        }
        for jh in threads {
            jh.join().unwrap();
        }
        cached_value = db.get(ks, &key).unwrap();
        if remove_chance_pct == 0 {
            assert!(cached_value.is_some());
        }
    }
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        let replay_value = db.get(ks, &key).unwrap();
        assert_eq!(replay_value, cached_value);
    }
}

#[test]
fn test_key_reduction() {
    let dir = tempdir::TempDir::new("test_key_reduction").unwrap();
    let config = Arc::new(Config::small());
    let key_indexing = KeyIndexing::key_reduction(4, 0..2);
    let key_shape = KeyShape::new_single_config_indexing(
        key_indexing,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::default(),
    );
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();
    let ks = db.single_ks();

    db.insert(ks, vec![1, 2, 3, 4], vec![1]).unwrap();
    db.insert(ks, vec![1, 3, 3, 4], vec![2]).unwrap();
    db.insert(ks, vec![1, 5, 3, 4], vec![3]).unwrap();

    // Simple get tests
    assert_eq!(db.get(ks, &[1, 2, 3, 4]).unwrap().unwrap().as_ref(), &[1]);
    assert_eq!(db.get(ks, &[1, 3, 3, 4]).unwrap().unwrap().as_ref(), &[2]);
    assert_eq!(db.get(ks, &[1, 5, 3, 4]).unwrap().unwrap().as_ref(), &[3]);
    assert!(db.get(ks, &[1, 6, 3, 4]).unwrap().is_none());
    assert!(db.get(ks, &[1, 5, 4, 4]).unwrap().is_none());

    // Iterator test (forward direction)
    let mut iterator = db.iterator(ks);
    iterator.set_lower_bound(vec![1, 3, 3, 4]);
    iterator.set_upper_bound(vec![1, 3, 3, 5]);
    let (k, v) = iterator.next().unwrap().unwrap();
    assert_eq!(k.as_ref(), &[1, 3, 3, 4]);
    assert_eq!(v.as_ref(), &[2]);
    assert!(iterator.next().is_none());

    let mut iterator = db.iterator(ks);
    iterator.set_lower_bound(vec![1, 3, 3, 5]);
    iterator.set_upper_bound(vec![1, 3, 3, 6]);
    assert!(iterator.next().is_none());

    let mut iterator = db.iterator(ks);
    iterator.set_lower_bound(vec![1, 3, 3, 3]);
    iterator.set_upper_bound(vec![1, 3, 3, 4]);
    assert!(iterator.next().is_none());

    // Iterator test (reverse direction)
    let mut iterator = db.iterator(ks);
    iterator.set_lower_bound(vec![1, 3, 3, 4]);
    iterator.set_upper_bound(vec![1, 3, 3, 5]);
    iterator.reverse();
    let (k, v) = iterator.next().unwrap().unwrap();
    assert_eq!(k.as_ref(), &[1, 3, 3, 4]);
    assert_eq!(v.as_ref(), &[2]);
    assert!(iterator.next().is_none());

    let mut iterator = db.iterator(ks);
    iterator.set_lower_bound(vec![1, 3, 3, 5]);
    iterator.set_upper_bound(vec![1, 3, 3, 6]);
    iterator.reverse();
    assert!(iterator.next().is_none());

    let mut iterator = db.iterator(ks);
    iterator.set_lower_bound(vec![1, 3, 3, 3]);
    iterator.set_upper_bound(vec![1, 3, 3, 4]);
    iterator.reverse();
    assert!(iterator.next().is_none());

    // Remove test
    db.remove(ks, vec![1, 3, 3, 4]).unwrap();
    assert_eq!(db.get(ks, &[1, 3, 3, 4]).unwrap(), None);
}

#[test]
fn test_key_reduction_lru() {
    let dir = tempdir::TempDir::new("test_key_reduction_lru").unwrap();
    let config = Arc::new(Config::small());
    let key_indexing = KeyIndexing::key_reduction(4, 0..2);
    let ks_config = KeySpaceConfig::new().with_value_cache_size(2);
    let key_shape =
        KeyShape::new_single_config_indexing(key_indexing, 1, KeyType::uniform(1), ks_config);
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();
    let ks = db.single_ks();

    db.insert(ks, vec![1, 2, 3, 4], vec![1]).unwrap();
    assert_eq!(db.get(ks, &[1, 2, 3, 4]).unwrap().unwrap().as_ref(), &[1]);
    assert_eq!(1, lru_lookups("root", &metrics));

    db.insert(ks, vec![1, 3, 3, 4], vec![2]).unwrap();
    assert_eq!(db.get(ks, &[1, 3, 3, 4]).unwrap().unwrap().as_ref(), &[2]);
    assert_eq!(2, lru_lookups("root", &metrics));

    db.insert(ks, vec![1, 5, 3, 4], vec![3]).unwrap();
    assert_eq!(db.get(ks, &[1, 5, 3, 4]).unwrap().unwrap().as_ref(), &[3]);
    assert_eq!(3, lru_lookups("root", &metrics));

    // First key was evicted, so lru lookup metric does not increment
    assert_eq!(db.get(ks, &[1, 2, 3, 4]).unwrap().unwrap().as_ref(), &[1]);
    assert_eq!(3, lru_lookups("root", &metrics));
    // Since we just fetched this key, and it should be populated to lru,
    // the next lookup comes from the lru cache
    assert_eq!(db.get(ks, &[1, 2, 3, 4]).unwrap().unwrap().as_ref(), &[1]);
    assert_eq!(4, lru_lookups("root", &metrics));
}

#[test]
fn test_cluster_bits_sequence_choice() {
    test_cluster_bits(true)
}

#[test]
fn test_cluster_bits_choice_sequence() {
    test_cluster_bits(false)
}

fn test_cluster_bits(sc: bool) {
    let dir = tempdir::TempDir::new(&format!("test_cluster_bits_{sc}")).unwrap();
    let config = Arc::new(Config::small());
    let key_type = if sc {
        KeyType::prefix_uniform(8, 4)
    } else {
        KeyType::prefix_uniform(15, 4)
    };
    let key_shape = KeyShape::new_single(32, 16, key_type);
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();
    let ks = db.single_ks();
    let mut rng = StdRng::from_seed(Default::default());

    for i in 0..0xffff {
        let mut key = vec![0; 32];
        let i = i * 121;
        if sc {
            key[..8].copy_from_slice(&u64::to_be_bytes(i / 256));
            key[8..16].copy_from_slice(&u64::to_be_bytes(i % 256));
        } else {
            key[..8].copy_from_slice(&u64::to_be_bytes(i % 256));
            key[8..16].copy_from_slice(&u64::to_be_bytes(i / 256));
        }
        rng.fill(&mut key[16..]);
        db.insert(ks, key, vec![]).unwrap();
    }
    db.large_table
        .each_entry(|entry| println!("Dirty {}", entry.data.len()));
}

pub(super) fn default_key_shape() -> KeyShape {
    KeyShape::new_single(4, 16, KeyType::uniform(16))
}

pub(super) fn prefix_key_shape() -> KeyShape {
    KeyShape::new_single(4, 16, KeyType::prefix_uniform(2, 0))
}

pub(super) fn hashed_index_key_shape() -> KeyShape {
    KeyShape::new_single_config_indexing(
        KeyIndexing::hash(),
        16,
        KeyType::prefix_uniform(2, 0),
        KeySpaceConfig::default(),
    )
}

fn lru_lookups(ks: &str, metrics: &Metrics) -> u64 {
    metrics
        .lookup_result
        .with_label_values(&[ks, "found", "lru"])
        .get()
}

fn force_unload_config(config: &Config) -> Arc<Config> {
    let mut config2 = Config::clone(config);
    config2.snapshot_unload_threshold = 0;
    Arc::new(config2)
}

fn ku32(k: u32) -> Bytes {
    k.to_be_bytes().to_vec().into()
}

fn vu32(v: u32) -> Bytes {
    v.to_le_bytes().to_vec().into()
}

fn ku128(k: u128) -> Bytes {
    k.to_be_bytes().to_vec().into()
}

fn vu128(v: u128) -> Bytes {
    v.to_le_bytes().to_vec().into()
}

pub(super) fn uniform_two_key_spaces() -> (KeyShape, KeySpace, KeySpace) {
    // Create a key shape with two key spaces using different index formats
    let mut builder = KeyShapeBuilder::new();

    // First key space with default LookupHeader index format
    builder.add_key_space("lookup_header", 4, 16, KeyType::uniform(16));

    // Second key space with UniformLookup index format
    let uniform_index = UniformLookupIndex::new();
    let ks2_config =
        KeySpaceConfig::default().with_index_format(IndexFormatType::Uniform(uniform_index));
    builder.add_key_space_config("uniform_lookup", 4, 16, KeyType::uniform(16), ks2_config);

    let key_shape = builder.build();
    let kss = KeySpaces::from_key_shape(&key_shape);
    let ks1 = kss.ks("lookup_header");
    let ks2 = kss.ks("uniform_lookup");
    (key_shape, ks1, ks2)
}

pub(super) fn prefix_two_key_spaces() -> (KeyShape, KeySpace, KeySpace) {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("lookup_header", 4, 16, KeyType::prefix_uniform(2, 0));
    // Second key space with UniformLookup index format
    let uniform_index = UniformLookupIndex::new();
    let ks2_config =
        KeySpaceConfig::default().with_index_format(IndexFormatType::Uniform(uniform_index));
    builder.add_key_space_config(
        "prefix_lookup",
        4,
        16,
        KeyType::prefix_uniform(2, 0),
        ks2_config,
    );
    let key_shape = builder.build();
    let kss = KeySpaces::from_key_shape(&key_shape);
    let ks1 = kss.ks("lookup_header");
    let ks2 = kss.ks("prefix_lookup");
    (key_shape, ks1, ks2)
}

pub(super) fn test_multiple_index_formats((key_shape, ks1, ks2): (KeyShape, KeySpace, KeySpace)) {
    let dir = tempdir::TempDir::new("test-index-formats").unwrap();
    let config = Arc::new(Config::small());
    let metrics = Metrics::new();

    // First session: insert data into both key spaces
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();

        // Insert into first key space (LookupHeader)
        db.insert(ks1, vec![1, 2, 3, 4], vec![10, 11]).unwrap();
        db.insert(ks1, vec![5, 6, 7, 8], vec![12, 13]).unwrap();

        // Insert into second key space (UniformLookup)
        db.insert(ks2, vec![1, 2, 3, 4], vec![20, 21]).unwrap();
        db.insert(ks2, vec![5, 6, 7, 8], vec![22, 23]).unwrap();

        // Verify we can read the data back
        assert_eq!(
            Some(vec![10, 11].into()),
            db.get(ks1, &[1, 2, 3, 4]).unwrap()
        );
        assert_eq!(
            Some(vec![12, 13].into()),
            db.get(ks1, &[5, 6, 7, 8]).unwrap()
        );
        assert_eq!(
            Some(vec![20, 21].into()),
            db.get(ks2, &[1, 2, 3, 4]).unwrap()
        );
        assert_eq!(
            Some(vec![22, 23].into()),
            db.get(ks2, &[5, 6, 7, 8]).unwrap()
        );
    }

    // Second session: reopen the DB and verify the data persisted
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();

        // Verify data from both key spaces
        assert_eq!(
            Some(vec![10, 11].into()),
            db.get(ks1, &[1, 2, 3, 4]).unwrap()
        );
        assert_eq!(
            Some(vec![12, 13].into()),
            db.get(ks1, &[5, 6, 7, 8]).unwrap()
        );
        assert_eq!(
            Some(vec![20, 21].into()),
            db.get(ks2, &[1, 2, 3, 4]).unwrap()
        );
        assert_eq!(
            Some(vec![22, 23].into()),
            db.get(ks2, &[5, 6, 7, 8]).unwrap()
        );

        // Update some values
        db.insert(ks1, vec![1, 2, 3, 4], vec![14, 15]).unwrap();
        db.insert(ks2, vec![1, 2, 3, 4], vec![24, 25]).unwrap();

        // Verify updates
        assert_eq!(
            Some(vec![14, 15].into()),
            db.get(ks1, &[1, 2, 3, 4]).unwrap()
        );
        assert_eq!(
            Some(vec![24, 25].into()),
            db.get(ks2, &[1, 2, 3, 4]).unwrap()
        );

        // Force a snapshot to ensure data is flushed
        db.rebuild_control_region().unwrap();
    }

    // Third session: verify updates after control region rebuild
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();

        // Verify all data including updates
        assert_eq!(
            Some(vec![14, 15].into()),
            db.get(ks1, &[1, 2, 3, 4]).unwrap()
        );
        assert_eq!(
            Some(vec![12, 13].into()),
            db.get(ks1, &[5, 6, 7, 8]).unwrap()
        );
        assert_eq!(
            Some(vec![24, 25].into()),
            db.get(ks2, &[1, 2, 3, 4]).unwrap()
        );
        assert_eq!(
            Some(vec![22, 23].into()),
            db.get(ks2, &[5, 6, 7, 8]).unwrap()
        );

        // Remove from one key space, update in another
        db.remove(ks1, vec![1, 2, 3, 4]).unwrap();
        db.insert(ks2, vec![5, 6, 7, 8], vec![26, 27]).unwrap();

        // Verify changes
        assert_eq!(None, db.get(ks1, &[1, 2, 3, 4]).unwrap());
        assert_eq!(
            Some(vec![26, 27].into()),
            db.get(ks2, &[5, 6, 7, 8]).unwrap()
        );
    }

    // Fourth session: final verification
    {
        let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();

        // Verify final state
        assert_eq!(None, db.get(ks1, &[1, 2, 3, 4]).unwrap());
        assert_eq!(
            Some(vec![12, 13].into()),
            db.get(ks1, &[5, 6, 7, 8]).unwrap()
        );
        assert_eq!(
            Some(vec![24, 25].into()),
            db.get(ks2, &[1, 2, 3, 4]).unwrap()
        );
        assert_eq!(
            Some(vec![26, 27].into()),
            db.get(ks2, &[5, 6, 7, 8]).unwrap()
        );
    }
}

#[test]
fn test_value_corruption() {
    let dir = tempdir::TempDir::new("test_value_corruption").unwrap();
    let config = Arc::new(Config::small());
    let key_shape = KeyShape::new_single(2, 16, KeyType::uniform(16));
    let ks = KeySpaces::from_key_shape(&key_shape).single();

    let (key_1, value_1) = (vec![1, 1], vec![1, 11]);
    let (key_2, value_2) = (vec![2, 2], vec![2, 12]);
    let (key_3, value_3) = (vec![3, 3], vec![3, 13]);
    let (key_4, value_4) = (vec![4, 4], vec![4, 14]);

    // Open the db and insert some data. Record the position of the last entry
    let (last_position, file) = {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();

        db.insert(ks, key_1.clone(), value_1.clone()).unwrap();
        db.insert(ks, key_2.clone(), value_2.clone()).unwrap();
        let position = db.wal_writer.position();
        db.insert(ks, key_3.clone(), value_3.clone()).unwrap();

        let file = db.wal.file().try_clone().unwrap();
        (position, file)
    };

    // Insert a corruption in the last byte of the last database entry
    let mut data = [0u8; 1];
    let position = last_position + CrcFrame::CRC_HEADER_LENGTH as u64;
    file.read_exact_at(&mut data, position).unwrap();
    data[0] = !data[0];
    file.write_all_at(&data, position).unwrap();

    // Re-open the database and insert some new data
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();

        db.insert(ks, key_4.clone(), value_4.clone()).unwrap();
    }

    // Re-open the database; verify that the corrupt data is not accessible
    // and all other data is intact
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();

        assert_eq!(Some(value_1.into()), db.get(ks, &key_1).unwrap());
        assert_eq!(Some(value_2.into()), db.get(ks, &key_2).unwrap());
        assert_eq!(None, db.get(ks, &key_3).unwrap());
        assert_eq!(Some(value_4.into()), db.get(ks, &key_4).unwrap());
    }
}

#[test]
fn test_header_corruption() {
    let dir = tempdir::TempDir::new("test_header_corruption").unwrap();
    let config = Arc::new(Config::small());
    let key_shape = KeyShape::new_single(2, 16, KeyType::uniform(16));
    let ks = KeySpaces::from_key_shape(&key_shape).single();

    let (key_1, value_1) = (vec![1, 1], vec![1, 11]);
    let (key_2, value_2) = (vec![2, 2], vec![2, 12]);
    let (key_3, value_3) = (vec![3, 3], vec![3, 13]);
    let (key_4, value_4) = (vec![4, 4], vec![4, 14]);

    // Open the db and insert some data. Record the position of the last entry
    let (last_position, file) = {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();

        db.insert(ks, key_1.clone(), value_1.clone()).unwrap();
        db.insert(ks, key_2.clone(), value_2.clone()).unwrap();
        let position = db.wal_writer.position();
        db.insert(ks, key_3.clone(), value_3.clone()).unwrap();

        let file = db.wal.file().try_clone().unwrap();
        (position, file)
    };

    // Insert a corruption in the first byte of the last database entry
    let mut data = [0u8; 1];
    file.read_exact_at(&mut data, last_position).unwrap();
    data[0] = !data[0];
    file.write_all_at(&data, last_position).unwrap();

    // Re-open the database and insert some new data
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();

        db.insert(ks, key_4.clone(), value_4.clone()).unwrap();
    }

    // Re-open the database; verify that the corrupt data is not accessible
    // and all other data is intact
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();

        assert_eq!(Some(value_1.into()), db.get(ks, &key_1).unwrap());
        assert_eq!(Some(value_2.into()), db.get(ks, &key_2).unwrap());
        assert_eq!(None, db.get(ks, &key_3).unwrap());
        assert_eq!(Some(value_4.into()), db.get(ks, &key_4).unwrap());
    }
}

#[test]
fn test_max_value_header_corruption() {
    let dir = tempdir::TempDir::new("test_max_value_header_corruption").unwrap();
    let config = Arc::new(Config::small());
    let key_shape = KeyShape::new_single(2, 16, KeyType::uniform(16));
    let ks = KeySpaces::from_key_shape(&key_shape).single();

    let (key_1, value_1) = (vec![1, 1], vec![1, 11]);
    let (key_2, value_2) = (vec![2, 2], vec![2, 12]);
    let (key_3, value_3) = (vec![3, 3], vec![3, 13]);
    let (key_4, value_4) = (vec![4, 4], vec![4, 14]);

    // Open the db and insert some data. Record the position of the last entry
    let (last_position, file) = {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();

        db.insert(ks, key_1.clone(), value_1.clone()).unwrap();
        db.insert(ks, key_2.clone(), value_2.clone()).unwrap();
        let position = db.wal_writer.position();
        db.insert(ks, key_3.clone(), value_3.clone()).unwrap();

        let file = db.wal.file().try_clone().unwrap();
        (position, file)
    };

    // Insert a corruption in the first byte of the last database entry
    let data = [0xffu8; 8];
    file.write_all_at(&data, last_position).unwrap();

    // Re-open the database and insert some new data
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();

        db.insert(ks, key_4.clone(), value_4.clone()).unwrap();
    }

    // Re-open the database; verify that the corrupt data is not accessible
    // and all other data is intact
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();

        assert_eq!(Some(value_1.into()), db.get(ks, &key_1).unwrap());
        assert_eq!(Some(value_2.into()), db.get(ks, &key_2).unwrap());
        assert_eq!(None, db.get(ks, &key_3).unwrap());
        assert_eq!(Some(value_4.into()), db.get(ks, &key_4).unwrap());
    }
}

#[test]
fn test_state_snapshot() {
    let db_path = tempdir::TempDir::new("test-state-snapshot-db").unwrap();
    let snapshot_path = tempdir::TempDir::new("test-state-snapshot-saved").unwrap();
    let config = Arc::new(Config::small());
    let key_shape = KeyShape::new_single(2, 16, KeyType::uniform(16));
    let ks = KeySpaces::from_key_shape(&key_shape).single();

    let (key_1, value_1) = (vec![1, 1], vec![1, 11]);
    let (key_2, value_2) = (vec![2, 2], vec![2, 12]);
    let (key_3, value_3) = (vec![3, 3], vec![3, 13]);
    let (key_4, value_4) = (vec![4, 4], vec![4, 14]);
    let (key_5, value_5) = (vec![5, 5], vec![5, 15]);
    let (key_6, value_6) = (vec![6, 6], vec![6, 16]);

    // Create a new database and insert some data
    let last_position = {
        let db = Db::open(
            db_path.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();

        db.insert(ks, key_1.clone(), value_1.clone()).unwrap();
        db.insert(ks, key_2.clone(), value_2.clone()).unwrap();
        db.insert(ks, key_3.clone(), value_3.clone()).unwrap();

        let last_position = db.wal_writer.position();

        db.rebuild_control_region().unwrap();

        // Create a state snapshot
        db.create_state_snapshot(PathBuf::from(snapshot_path.path()))
            .unwrap();

        // Insert more data after the snapshot
        db.insert(ks, key_4.clone(), value_4.clone()).unwrap();
        db.insert(ks, key_5.clone(), value_5.clone()).unwrap();

        db.rebuild_control_region().unwrap();

        last_position
    };

    // Restore the database from the snapshot
    let db = Db::restore_state_snapshot(
        PathBuf::from(snapshot_path.path()),
        PathBuf::from(db_path.path()),
        key_shape,
        config,
        Metrics::new(),
    )
    .unwrap();

    // Check that the last position in the WAL matches the last position before snapshot
    let recovered_position = db.wal_writer.position();
    assert_eq!(last_position, recovered_position);

    // Check that the data before the snapshot is still present
    assert_eq!(Some(value_1.into()), db.get(ks, &key_1).unwrap());
    assert_eq!(Some(value_2.into()), db.get(ks, &key_2).unwrap());
    assert_eq!(Some(value_3.into()), db.get(ks, &key_3).unwrap());

    // Check that the data after the snapshot is not present
    assert_eq!(None, db.get(ks, &key_4).unwrap());
    assert_eq!(None, db.get(ks, &key_5).unwrap());

    // Insert new data after restoring from snapshot
    db.insert(ks, key_6.clone(), value_6.clone()).unwrap();
    assert_eq!(Some(value_6.into()), db.get(ks, &key_6).unwrap());
}

#[test]
fn test_state_snapshot_empty() {
    let db_path = tempdir::TempDir::new("test-state-snapshot-empty-db").unwrap();
    let snapshot_path = tempdir::TempDir::new("test-state-snapshot-empty-saved").unwrap();
    let config = Arc::new(Config::small());
    let key_shape = KeyShape::new_single(4, 16, KeyType::uniform(16));

    // Create a new database
    let last_position = {
        let db = Db::open(
            db_path.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();

        db.rebuild_control_region().unwrap();

        // Create a state snapshot
        db.create_state_snapshot(PathBuf::from(snapshot_path.path()))
            .unwrap();

        db.wal_writer.position()
    };

    // Restore the database from the snapshot
    let db = Db::restore_state_snapshot(
        PathBuf::from(snapshot_path.path()),
        PathBuf::from(db_path.path()),
        key_shape,
        config,
        Metrics::new(),
    )
    .unwrap();
    let ks = db.single_ks();

    // Check that the last position in the WAL matches the last position before snapshot
    let recovered_position = db.wal_writer.position();
    assert_eq!(last_position, recovered_position);

    // Insert new data after restoring from snapshot
    db.insert(ks, vec![1, 2, 3, 4], vec![6]).unwrap();
    assert_eq!(Some(vec![6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
}

#[test]
fn test_bloom_filter_restore() {
    let dir = tempdir::TempDir::new("test_bloom_filter_restore").unwrap();
    let config = Arc::new(Config::small());
    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new().with_bloom_filter(0.01, 2000);
    ksb.add_key_space_config("k", 8, 1, KeyType::uniform(1), ksc);
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            force_unload_config(&config),
            metrics.clone(),
        )
        .unwrap();
        let ks = db.ks("k");
        for i in 0..10u64 {
            db.insert(ks, i.to_be_bytes().to_vec(), vec![0, 1, 2])
                .unwrap();
        }
        thread::sleep(Duration::from_millis(10)); // todo replace this with wal tracker barrier
        db.rebuild_control_region().unwrap();
        assert!(db.large_table.is_all_clean());
    }
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();
    let ks = db.ks("k");
    for key in 0..10u64 {
        assert!(db.exists(ks, &key.to_be_bytes()).unwrap());
    }
    for key in 10..20u64 {
        assert!(!db.exists(ks, &key.to_be_bytes()).unwrap());
    }
    let found = metrics
        .lookup_result
        .with_label_values(&["k", "found", "cache"])
        .get()
        + metrics
            .lookup_result
            .with_label_values(&["k", "found", "index_0"])
            .get()
        + metrics
            .lookup_result
            .with_label_values(&["k", "found", "index_1"])
            .get();
    let not_found_bloom = metrics
        .lookup_result
        .with_label_values(&["k", "not_found", "bloom"])
        .get();
    assert_eq!(found, 10);
    if not_found_bloom < 9 {
        panic!("Bloom filter efficiency less then 90%");
    }
}

#[test]
fn test_variable_length_keys() {
    // Aside from testing variable length keys, this test can expose a replay bug
    for _ in 0..100 {
        test_variable_length_keys_it();
    }
}

fn test_variable_length_keys_it() {
    let dir = tempdir::TempDir::new("test_variable_length_keys").unwrap();
    let mut config = Config::small();
    config.sync_flush = false;
    let config = Arc::new(config);
    let metrics = Metrics::new();
    let ks_config = KeySpaceConfig::default().with_max_dirty_keys(1);
    let key_shape = KeyShape::new_single_config_indexing(
        KeyIndexing::VariableLength,
        16,
        KeyType::uniform(1),
        ks_config,
    );
    let ks = KeySpaces::from_key_shape(&key_shape).single();
    let key1 = vec![];
    let key2 = vec![1u8];
    let key3 = vec![2u8, 3];
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();

        assert!(db.get(ks, &key1).unwrap().is_none());
        assert!(db.get(ks, &key2).unwrap().is_none());
        assert!(db.get(ks, &key3).unwrap().is_none());

        db.insert(ks, key1.clone(), vec![1]).unwrap();
        db.insert(ks, key2.clone(), vec![2]).unwrap();
        db.insert(ks, key3.clone(), vec![3]).unwrap();

        assert_eq!(Some(vec![1].into()), db.get(ks, &key1).unwrap());
        assert_eq!(Some(vec![2].into()), db.get(ks, &key2).unwrap());
        assert_eq!(Some(vec![3].into()), db.get(ks, &key3).unwrap());

        db.large_table.flusher.barrier();
        db.rebuild_control_region().unwrap();

        let mut it = db.iterator(ks);
        assert_eq!(
            (key1.clone().into(), vec![1].into()),
            it.next().unwrap().unwrap()
        );
        assert_eq!(
            (key2.clone().into(), vec![2].into()),
            it.next().unwrap().unwrap()
        );
        assert_eq!(
            (key3.clone().into(), vec![3].into()),
            it.next().unwrap().unwrap()
        );
        assert!(it.next().is_none());
        // This, small max_dirty_keys and unloaded_iterator enabled
        // will force iterator in the next code block to be an unloaded iterator
    }
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();
        assert_eq!(Some(vec![1].into()), db.get(ks, &key1).unwrap());
        assert_eq!(Some(vec![2].into()), db.get(ks, &key2).unwrap());
        assert_eq!(Some(vec![3].into()), db.get(ks, &key3).unwrap());

        let mut it = db.iterator(ks);
        assert_eq!((key1.into(), vec![1].into()), it.next().unwrap().unwrap());
        assert_eq!((key2.into(), vec![2].into()), it.next().unwrap().unwrap());
        assert_eq!((key3.into(), vec![3].into()), it.next().unwrap().unwrap());
        assert!(it.next().is_none());
    }
}

/// Tests variable-length key lookup with enough keys to span multiple micro-cells,
/// exercising binary search on the flushed on-disk index. Also covers absent-key
/// lookups after flush and a reopen round-trip.
#[test]
fn test_variable_length_keys_many() {
    let dir = tempdir::TempDir::new("test_variable_length_keys_many").unwrap();
    let mut config = Config::small();
    config.sync_flush = false;
    let config = Arc::new(config);
    let metrics = Metrics::new();
    // max_dirty_keys=1 forces every insert to flush, so lookups after the first
    // insert hit the on-disk binary-search path.
    let ks_config = KeySpaceConfig::default().with_max_dirty_keys(1);
    let key_shape = KeyShape::new_single_config_indexing(
        KeyIndexing::VariableLength,
        16,
        KeyType::uniform(1),
        ks_config,
    );
    let ks = KeySpaces::from_key_shape(&key_shape).single();

    // Build keys of varying lengths to spread across many micro-cells.
    // Single-byte keys [0x00]..[0xff] naturally cover the full key-space prefix range.
    let keys: Vec<Vec<u8>> = (0u8..=255)
        .map(|b| vec![b])
        .chain((0u8..=255).map(|b| vec![b, b]))
        .collect();

    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();

        for (i, key) in keys.iter().enumerate() {
            db.insert(ks, key.clone(), vec![i as u8]).unwrap();
        }

        // Wait for the WAL tracker to catch up before flushing — otherwise
        // each flush captures a stale `last_processed` and `retain_unprocessed`
        // leaves recent inserts in `self.data`, leaving cells DirtyUnloaded.
        db.wal_writer.wal_tracker_barrier();
        db.large_table.flusher.barrier();
        db.force_rebuild_control_region().unwrap();

        // Do absent-key lookups first, before present-key lookups warm the cell
        // cache. Cells are unloaded at this point, so each absent lookup reaches
        // the on-disk index. Without a bloom filter nothing is intercepted early,
        // so on_disk_not_found must equal exactly the number of absent lookups.
        const ABSENT_KEYS: &[&[u8]] = &[&[], &[0, 1], &[1, 0], &[0xab, 0xcd, 0xef]];
        for key in ABSENT_KEYS {
            assert!(db.get(ks, key).unwrap().is_none());
        }

        // All inserted keys must be found with correct values.
        for (i, key) in keys.iter().enumerate() {
            assert_eq!(
                Some(vec![i as u8].into()),
                db.get(ks, key).unwrap(),
                "lookup failed for key {key:?}",
            );
        }

        let on_disk_found = metrics
            .lookup_result
            .with_label_values(&["root", "found", "index_0"])
            .get()
            + metrics
                .lookup_result
                .with_label_values(&["root", "found", "index_1"])
                .get();
        let cache_found = metrics
            .lookup_result
            .with_label_values(&["root", "found", "cache"])
            .get();
        // Every inserted key must be accounted for.
        assert_eq!(
            keys.len() as u64,
            on_disk_found + cache_found,
            "total found lookups should equal number of inserted keys",
        );
        // At least half must have hit the on-disk binary-search path.
        assert!(
            on_disk_found >= keys.len() as u64 / 2,
            "expected at least {} on-disk found lookups, got {on_disk_found}",
            keys.len() / 2,
        );
        // Without a bloom filter every absent-key lookup reaches the index.
        // Cells are unloaded when we do these lookups (done before the present-key
        // loop above), so they all land in the on-disk not_found bucket.
        let on_disk_not_found = metrics
            .lookup_result
            .with_label_values(&["root", "not_found", "index_0"])
            .get()
            + metrics
                .lookup_result
                .with_label_values(&["root", "not_found", "index_1"])
                .get();
        assert_eq!(
            ABSENT_KEYS.len() as u64,
            on_disk_not_found,
            "each absent-key lookup should produce exactly one on-disk not_found",
        );
    }

    // Reopen and verify all lookups still work from disk.
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();

        for (i, key) in keys.iter().enumerate() {
            assert_eq!(
                Some(vec![i as u8].into()),
                db.get(ks, key).unwrap(),
                "post-reopen lookup failed for key {key:?}",
            );
        }
        assert!(db.get(ks, &[]).unwrap().is_none());
        assert!(db.get(ks, &[0xab, 0xcd, 0xef]).unwrap().is_none());
    }
}

/// Tests that the unloaded iterator works correctly for variable-length key spaces:
/// forward and backward iteration across multiple micro-cells, O(1) sequential
/// fast path, and correct ordering after a reopen (all cells start unloaded).
#[test]
fn test_variable_length_keys_unloaded_iterator() {
    let dir = tempdir::TempDir::new("test_variable_length_keys_unloaded_iterator").unwrap();
    let mut config = Config::small();
    config.sync_flush = false;
    let config = Arc::new(config);
    let metrics = Metrics::new();
    // max_dirty_keys=1 forces every insert to flush so cells are unloaded on reopen.
    let ks_config = KeySpaceConfig::default().with_max_dirty_keys(1);
    let key_shape = KeyShape::new_single_config_indexing(
        KeyIndexing::VariableLength,
        16,
        KeyType::uniform(1),
        ks_config,
    );
    let ks = KeySpaces::from_key_shape(&key_shape).single();

    // 256 single-byte keys spread across all micro-cells.
    let keys: Vec<Vec<u8>> = (0u8..=255).map(|b| vec![b]).collect();

    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();

        for (i, key) in keys.iter().enumerate() {
            db.insert(ks, key.clone(), vec![i as u8]).unwrap();
        }
        db.large_table.flusher.barrier();
        db.force_rebuild_control_region().unwrap();
    }

    // Reopen: all cells start unloaded, so every iterator step exercises
    // next_entry_unloaded_varlen including micro-cell boundary crossings.
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();

        // Confirm cells are unloaded at the start.
        db.large_table.each_entry(|entry| {
            assert!(
                !entry.is_index_loaded(),
                "cells should start unloaded after reopen"
            );
        });

        // Forward iteration: keys must come out in lexicographic order.
        let collected: Vec<(Bytes, Bytes)> = db.iterator(ks).map(|r| r.unwrap()).collect();
        assert_eq!(collected.len(), keys.len(), "forward: wrong key count");
        for (i, (k, v)) in collected.iter().enumerate() {
            assert_eq!(k.as_ref(), keys[i].as_slice(), "forward: wrong key at {i}");
            assert_eq!(v.as_ref(), &[i as u8], "forward: wrong value at {i}");
        }

        // Backward iteration: keys must come out in reverse lexicographic order.
        let mut rev_it = db.iterator(ks);
        rev_it.reverse();
        let collected_rev: Vec<(Bytes, Bytes)> = rev_it.map(|r| r.unwrap()).collect();
        assert_eq!(collected_rev.len(), keys.len(), "backward: wrong key count");
        for (i, (k, v)) in collected_rev.iter().enumerate() {
            let expected_idx = keys.len() - 1 - i;
            assert_eq!(
                k.as_ref(),
                keys[expected_idx].as_slice(),
                "backward: wrong key at position {i}"
            );
            assert_eq!(
                v.as_ref(),
                &[expected_idx as u8],
                "backward: wrong value at position {i}"
            );
        }
    }
}

#[test]
fn test_reverse_iterator_without_bounds() {
    // Test that reverse iterator works without setting any bounds
    let dir = tempdir::TempDir::new("test-reverse-no-bounds").unwrap();
    let config = Arc::new(Config::small());
    let key_shape = KeyShape::new_single(8, 16, KeyType::uniform(16));

    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();
    let ks = db.single_ks();

    // Insert some test data with 8-byte keys
    let test_data = vec![
        (b"key00001".to_vec(), b"value1".to_vec()),
        (b"key00002".to_vec(), b"value2".to_vec()),
        (b"key00003".to_vec(), b"value3".to_vec()),
    ];

    for (key, value) in &test_data {
        db.insert(ks, key.clone(), value.clone()).unwrap();
    }

    // Test forward iterator without bounds - this should work
    let forward_iterator = db.iterator(ks);
    let forward_results: Vec<_> = forward_iterator
        .collect::<DbResult<Vec<_>>>()
        .expect("Forward iterator should work without bounds");

    assert_eq!(
        forward_results.len(),
        3,
        "Forward iterator should find all 3 keys"
    );

    // Test reverse iterator without bounds - this is what we're testing
    let mut reverse_iterator = db.iterator(ks);
    reverse_iterator.reverse();

    let reverse_results: Vec<_> = reverse_iterator
        .collect::<DbResult<Vec<_>>>()
        .expect("Reverse iterator should work without bounds");

    // The issue: reverse iterator returns no results without bounds
    assert_eq!(
        reverse_results.len(),
        3,
        "Reverse iterator should find all 3 keys without needing bounds, but found {}",
        reverse_results.len()
    );

    // Verify the keys are the same (just in reverse order)
    let forward_keys: Vec<_> = forward_results.iter().map(|(k, _)| k.clone()).collect();
    let mut reverse_keys: Vec<_> = reverse_results.iter().map(|(k, _)| k.clone()).collect();
    reverse_keys.reverse();

    assert_eq!(
        forward_keys, reverse_keys,
        "Reverse iterator should return same keys as forward iterator (in reverse order)"
    );
}

#[test]
fn test_rebuild_replay_from_monotonic_across_keyspaces() {
    let dir = tempdir::TempDir::new("test-replay-monotonic").unwrap();
    let config = Arc::new(Config::small());
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("ks1", 8, 1, KeyType::uniform(8));
    builder.add_key_space("ks2", 8, 1, KeyType::uniform(8));
    let key_shape = builder.build();

    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();
    let ks1 = db.ks("ks1");
    let ks2 = db.ks("ks2");

    db.insert(ks1, 1u64.to_be_bytes().to_vec(), vec![1])
        .unwrap();
    db.wal_writer.wal_tracker_barrier();
    let replay1 = db.force_rebuild_control_region().unwrap();
    assert!(replay1 > 0, "replay1 must not be 0 after force_rebuild");

    db.insert(ks2, 2u64.to_be_bytes().to_vec(), vec![2])
        .unwrap();
    let replay2 = db.rebuild_control_region().unwrap();

    assert!(
        replay2 >= replay1,
        "replay_from regressed: replay1={replay1}, replay2={replay2}"
    );
}

#[ignore]
#[test]
fn test_force_rebuild_control_region() {
    let dir = tempdir::TempDir::new("test-force-rebuild").unwrap();
    let config = Arc::new(Config::small());
    let key_shape = KeyShape::new_single(4, 16, KeyType::uniform(16));

    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();
    let ks = db.single_ks();

    // Insert some data
    db.insert(ks, vec![1, 2, 3, 4], vec![5, 6]).unwrap();
    db.insert(ks, vec![7, 8, 9, 10], vec![11, 12]).unwrap();

    // Initially, should have dirty entries
    assert!(
        !db.is_all_clean(),
        "Should have dirty entries after inserts"
    );

    // Force rebuild control region - should flush all dirty entries
    db.force_rebuild_control_region().unwrap();

    // After force rebuild, all entries should be clean
    assert!(
        db.is_all_clean(),
        "All entries should be clean after force_rebuild_control_region"
    );

    // Verify data is still accessible
    assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
    assert_eq!(
        Some(vec![11, 12].into()),
        db.get(ks, &[7, 8, 9, 10]).unwrap()
    );

    // Insert more data
    db.insert(ks, vec![13, 14, 15, 16], vec![17, 18]).unwrap();

    // Should have dirty entries again
    assert!(
        !db.is_all_clean(),
        "Should have dirty entries after new insert"
    );

    // Force rebuild again
    db.force_rebuild_control_region().unwrap();

    // All should be clean again
    assert!(
        db.is_all_clean(),
        "All entries should be clean after second force_rebuild_control_region"
    );
}

#[test]
fn db_test_snapshot_unload_threshold() {
    let dir = tempdir::TempDir::new("test_unload_threshold").unwrap();
    let mut config = Config::small();
    // Set snapshot_unload_threshold to 4KB
    config.snapshot_unload_threshold = 4 * 1024;
    let config = Arc::new(config);

    // Use KeyShapeBuilder instead of KeyShape::new_single
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("test", 8, 16, KeyType::uniform(16));
    // Make sure ks that only was written once does not affect forced snapshot
    builder.add_key_space("small", 8, 16, KeyType::uniform(16));
    // Make sure empty key space does not affect forced snapshot
    builder.add_key_space("empty", 8, 16, KeyType::uniform(16));
    let key_shape = builder.build();

    let metrics = Metrics::new();

    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .expect("open failed");
    let ks = db.ks("test");
    let ks2 = db.ks("small");

    // Write 20 values, each approximately 1KB
    let value_size = 1024;
    let large_value = vec![0xAB; value_size];
    db.insert(ks2, 0u64.to_be_bytes().to_vec(), large_value.clone())
        .expect("insert failed");

    for i in 0u64..20 {
        db.insert(ks, i.to_be_bytes().to_vec(), large_value.clone())
            .expect("insert failed");
    }

    thread::sleep(Duration::from_millis(10));

    // Get the current WAL position before snapshot
    let wal_position_before = db.wal_writer.position();
    println!("WAL position before snapshot: {}", wal_position_before);

    let replay_position = db
        .rebuild_control_region()
        .expect("force_rebuild_control_region failed");
    println!("  - WAL position: {}", wal_position_before);
    println!("  - Replay position in control region: {}", replay_position);

    assert_eq!(replay_position, wal_position_before);
}

/// Reproduces the bug where ForceRelocate on a dirty entry loses the dirty overlay.
///
/// Scenario:
/// 1. Write to a rarely-updated entry + many bulk entries, snapshot to flush all.
/// 2. Write more bulk data to advance WAL, run several snapshots so the
///    rarely-updated entry's `last_processed` advances while its on-disk position stays old.
/// 3. Write to the rarely-updated entry (becomes dirty).
/// 4. Snapshot — the old on-disk position falls below `force_relocate_below`.
///    The buggy code uses ForceRelocate which copies the stale on-disk index
///    (missing the dirty write) and advances `last_processed` past the dirty write.
/// 5. Simulate crash recovery (drop + reopen). The dirty write should be present.
///
/// Validates the file-occupancy index GC path end-to-end.
///
/// Tiny index files (8 KiB) guarantee that a moderate number of flushes fills
/// several files, and that each file's live bytes sit well below the configured
/// threshold. With `index_min_occupancy_pct` disabled (0), low-occupancy files
/// accumulate and `min_wal_position` stays pinned. With the threshold enabled,
/// ForceRelocate fires during snapshots and drains the old files so the GC
/// watermark advances.
///
/// The comparison is the core correctness signal: same workload, same knobs —
/// the only difference is whether occupancy-based relocation runs.
#[test]
fn test_index_gc_low_occupancy_files_relocated() {
    fn run(min_occupancy_pct: u8) -> (u64, u64) {
        let dir = tempdir::TempDir::new("test_index_gc_low_occupancy").unwrap();
        let mut config = Config::small();
        config.frag_size = 8 * 1024;
        config.wal_file_size = 8 * 1024;
        config.index_min_occupancy_pct = min_occupancy_pct;
        config.snapshot_unload_threshold = 0;
        config.max_dirty_keys = 100_000;
        // Keep the index map LRU small so finalized maps for old files drop
        // out quickly. `delete_files` will not reclaim a file while its map
        // is still in `WalMaps`, so without this the GC assertion below races
        // the LRU rather than the reclaim itself. 3 is the minimum allowed
        // (`INITIAL_MAPS_BUFFER + 1`).
        config.max_index_maps = Some(3);
        let config = Arc::new(config);

        let mut builder = KeyShapeBuilder::new();
        // "rare" — written once and never again. Its blob sits in the earliest
        // index file and pins `min_wal_position` unless ForceRelocate moves it.
        builder.add_key_space("rare", 8, 1, KeyType::uniform(1));
        builder.add_key_space("bulk", 8, 4, KeyType::uniform(4));
        let key_shape = builder.build();

        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .expect("open failed");
        let ks_rare = db.ks("rare");
        let ks_bulk = db.ks("bulk");

        let bulk_key = |i: u32| -> Vec<u8> {
            let mut k = [0u8; 8];
            k[..4].copy_from_slice(&i.to_be_bytes());
            k.to_vec()
        };
        let rare_key = 0u64.to_be_bytes().to_vec();

        db.insert(ks_rare, rare_key.clone(), vec![0x11; 16])
            .expect("insert rare");

        let num_bulk: u32 = 64;
        let step = u32::MAX / num_bulk;

        // Several rounds of rewrite-then-snapshot. Bulk cells get re-flushed
        // every round, so their blobs keep moving into the newest file; rare
        // is never touched and stays in whatever file it was first flushed to.
        for round in 0..4u8 {
            for i in 0..num_bulk {
                db.insert(ks_bulk, bulk_key(i * step), vec![round; 16])
                    .expect("insert bulk");
            }
            db.wal_writer.wal_tracker_barrier();
            db.force_rebuild_control_region().expect("snapshot");
        }

        // Sanity: rare's original value and the last-round bulk values survive.
        let v = db.get(ks_rare, &rare_key).expect("get rare");
        assert_eq!(v.as_deref(), Some(&[0x11; 16][..]));
        for i in 0..num_bulk {
            let v = db.get(ks_bulk, &bulk_key(i * step)).expect("get bulk");
            assert_eq!(v.as_deref(), Some(&[3u8; 16][..]));
        }

        // Wait for the mapper thread to process any pending GC messages.
        thread::sleep(Duration::from_millis(200));

        let forced = metrics
            .snapshot_forced_relocation
            .with_label_values(&["rare"])
            .get();
        (db.indexes.min_wal_position(), forced)
    }

    let (min_pos_off, forced_off) = run(0);
    let (min_pos_on, forced_on) = run(99);

    assert_eq!(
        min_pos_off, 0,
        "with occupancy GC disabled, rare blob pins file 0 and min_wal_position stays at 0",
    );
    assert_eq!(
        forced_off, 0,
        "with occupancy GC disabled, ForceRelocate should never fire for rare",
    );
    assert!(
        min_pos_on > 0,
        "with occupancy GC enabled, rare should be relocated and min_wal_position should advance; got {min_pos_on}",
    );
    assert!(
        forced_on > 0,
        "with occupancy GC enabled, ForceRelocate should fire for rare; got {forced_on}",
    );
}

/// Sparse index-WAL GC: empty middle files are reclaimed even while an old
/// blob pins the earliest file.
///
/// Workload:
/// - `rare` keyspace writes one value, then never again — its blob lives in
///   file 0 and pins it.
/// - `bulk` keyspace runs many rewrite-then-snapshot rounds — each round's
///   flush replaces all bulk cells' L0 positions, leaving earlier rounds'
///   positions unreferenced.
/// - `index_min_occupancy_pct = 0` disables ForceRelocate so the rare blob
///   is never moved out of file 0.
///
/// Expectation: after the rounds, file 0 is alive (rare), the writer-tail
/// file is alive, and at least one file id in the middle has been deleted
/// from disk.
#[test]
fn test_sparse_gc_deletes_empty_middle_index_files() {
    let dir = tempdir::TempDir::new("test_sparse_gc_middle").unwrap();
    let mut config = Config::small();
    // Tiny index files so each round fills a fresh file.
    config.frag_size = 8 * 1024;
    config.wal_file_size = 8 * 1024;
    // Critical: disable ForceRelocate so the rare blob stays pinned in file 0.
    // Sparse GC must reclaim empty middle files without help from relocation.
    config.index_min_occupancy_pct = 0;
    config.snapshot_unload_threshold = 0;
    config.max_dirty_keys = 100_000;
    let config = Arc::new(config);

    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("rare", 8, 1, KeyType::uniform(1));
    builder.add_key_space("bulk", 8, 16, KeyType::uniform(8));
    let key_shape = builder.build();

    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .expect("open failed");
    let ks_rare = db.ks("rare");
    let ks_bulk = db.ks("bulk");

    let rare_key = 0u64.to_be_bytes().to_vec();
    db.insert(ks_rare, rare_key.clone(), vec![0x11; 16])
        .expect("insert rare");

    let bulk_key = |i: u32| -> Vec<u8> {
        let mut k = [0u8; 8];
        k[..4].copy_from_slice(&i.to_be_bytes());
        k.to_vec()
    };
    let num_bulk: u32 = 128;
    let step = u32::MAX / num_bulk;
    let rounds: u8 = 8;

    for round in 0..rounds {
        for i in 0..num_bulk {
            db.insert(ks_bulk, bulk_key(i * step), vec![round; 128])
                .expect("insert bulk");
        }
        db.wal_writer.wal_tracker_barrier();
        db.force_rebuild_control_region().expect("snapshot");
    }

    // Sanity: data is intact for both keyspaces.
    let v = db.get(ks_rare, &rare_key).expect("get rare");
    assert_eq!(v.as_deref(), Some(&[0x11; 16][..]));
    let last_round = rounds - 1;
    for i in 0..num_bulk {
        let v = db.get(ks_bulk, &bulk_key(i * step)).expect("get bulk");
        assert_eq!(v.as_deref(), Some(&vec![last_round; 128][..]));
    }

    // Drop the DB so the unlink worker drains its queue before we scan.
    drop(db);

    // Scan disk for surviving index_* files.
    let mut present: Vec<u64> = std::fs::read_dir(dir.path())
        .expect("read dir")
        .filter_map(|e| {
            let p = e.ok()?.path();
            let name = p.file_name()?.to_str()?.to_string();
            let id_str = name.strip_prefix("index_")?;
            u64::from_str_radix(id_str, 16).ok()
        })
        .collect();
    present.sort();
    assert!(!present.is_empty(), "no index files on disk");

    let min_id = *present.first().unwrap();
    let max_id = *present.last().unwrap();
    assert_eq!(
        min_id, 0,
        "rare must keep file 0 pinned (ForceRelocate disabled); present={present:?}",
    );
    assert!(
        max_id >= 2,
        "test workload must span at least 3 files so a middle exists; \
         present={present:?}. Increase rounds or num_bulk.",
    );
    let full_range_len = (max_id - min_id + 1) as usize;
    assert!(
        present.len() < full_range_len,
        "expected at least one middle file to be sparse-GC'd; \
         present={present:?}, full range 0..={max_id} would have {full_range_len} files",
    );
}

/// After sparse GC creates a multi-file gap between the last live val
/// position and the writer's current file, reopening the DB must not panic.
/// Pre-fix: on reopen the writer rewinds to `max(live_valpos)` and the
/// mapper's INITIAL_MAPS_BUFFER lookahead tries to mmap the very next file,
/// which sparse GC deleted — panic in `WalMapperThread::make_map` at
/// `attempt to access non existing file WalFileId(_)`.
#[test]
fn test_sparse_gc_reopen_with_dead_window() {
    let dir = tempdir::TempDir::new("test_sparse_gc_reopen").unwrap();
    let mut config = Config::small();
    // One map per file so INITIAL_MAPS_BUFFER lookahead crosses file
    // boundaries on the very first iteration.
    config.frag_size = 8 * 1024;
    config.wal_file_size = 8 * 1024;
    // Disable occupancy-based relocation so dead writes stay in place.
    config.index_min_occupancy_pct = 0;
    config.snapshot_unload_threshold = 0;
    config.max_dirty_keys = 100_000;
    let config = Arc::new(config);

    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("ks", 8, 1, KeyType::uniform(1));
    let key_shape = builder.build();

    let rare_key = 0u64.to_be_bytes().to_vec();
    let rare_value: Vec<u8> = vec![0xAA; 16];

    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .expect("open failed");
        let ks = db.ks("ks");

        // Single insert + snapshot pins the only live index valpos in file 0.
        db.insert(ks, rare_key.clone(), rare_value.clone())
            .expect("insert");
        db.wal_writer.wal_tracker_barrier();
        db.force_rebuild_control_region().expect("first snapshot");

        // Push the index writer far past file 0 with raw writes. The returned
        // WalGuards are dropped, so no cell ever references these positions —
        // they form a multi-file dead window between max(live_valpos) (file 0)
        // and the writer's current file.
        let payload = vec![0u8; config.frag_size as usize - 16];
        for _ in 0..16 {
            db.index_writer
                .write(&PreparedWalWrite::new(&payload))
                .expect("direct index write");
        }
        db.index_writer.wal_tracker_barrier();

        // Sparse GC runs as part of this snapshot and unlinks the dead
        // middle files, leaving on-disk layout = {0, writer-current-file}.
        db.force_rebuild_control_region().expect("second snapshot");
    }

    // Sanity: writer advanced past file 1, and no file the writer must
    // traverse on restart was reclaimed. Specifically, every file id from
    // `max(live_valpos)`'s file (= 0 here) through the writer's current
    // file must still be on disk — that is the invariant sparse GC must
    // not violate.
    let mut present: Vec<u64> = std::fs::read_dir(dir.path())
        .expect("read dir")
        .filter_map(|e| {
            let p = e.ok()?.path();
            let name = p.file_name()?.to_str()?.to_string();
            let id_str = name.strip_prefix("index_")?;
            u64::from_str_radix(id_str, 16).ok()
        })
        .collect();
    present.sort();
    let max_id = *present.last().unwrap();
    assert!(
        max_id >= 2,
        "dead-window setup must advance the writer past file 1; max_id={max_id}",
    );
    let expected: Vec<u64> = (0..=max_id).collect();
    assert_eq!(
        present, expected,
        "sparse GC must not punch holes between max(live_valpos) and writer file",
    );

    // Reopen must not panic — this is the regression guard.
    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).expect("reopen failed");
    let ks = db.ks("ks");
    let v = db.get(ks, &rare_key).expect("read after reopen");
    assert_eq!(v.as_deref(), Some(&rare_value[..]));
}

#[test]
fn test_force_relocate_dirty_entry_preserves_data() {
    let dir = tempdir::TempDir::new("test_force_relocate_dirty").unwrap();
    let mut config = Config::small();
    // Use a small threshold so that entries flush eagerly during snapshots
    config.snapshot_unload_threshold = 0;
    // Prevent automatic inline flushing due to dirty key limits
    config.max_dirty_keys = 100_000;
    // Tiny index files so the per-file occupancy GC fires quickly: each round
    // of bulk flushes spans multiple files, leaving the rare entry's blob in
    // an old file by itself (≪ threshold).
    config.frag_size = 8 * 1024;
    config.wal_file_size = 8 * 1024;
    config.index_min_occupancy_pct = 99;
    let config = Arc::new(config);

    // "rare" keyspace: 1 cell — the rarely-updated entry
    // "bulk" keyspace: 128 cells — used to advance WAL and establish high positions
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("rare", 8, 1, KeyType::uniform(1));
    builder.add_key_space("bulk", 8, 16, KeyType::uniform(8));
    let key_shape = builder.build();

    let rare_key = 0u64.to_be_bytes().to_vec();
    let initial_value: Vec<u8> = vec![0x01; 128];
    let updated_value: Vec<u8> = vec![0x02; 128];
    let bulk_value: Vec<u8> = vec![0xBB; 512];

    // Helper: generate bulk keys that spread across all cells.
    // Keys are 8 bytes; the cell is determined by the first 4 bytes (starting_u32).
    // Use the upper 32 bits to distribute evenly across cells.
    let bulk_key = |i: u32| -> Vec<u8> {
        let mut key = [0u8; 8];
        key[..4].copy_from_slice(&i.to_be_bytes());
        key.to_vec()
    };

    // --- Phase 1: Write initial data and flush everything ---
    {
        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .expect("open failed");
        let ks_rare = db.ks("rare");
        let ks_bulk = db.ks("bulk");

        // Write to rare entry
        db.insert(ks_rare, rare_key.clone(), initial_value.clone())
            .expect("insert failed");

        // Write to many bulk cells so they establish on-disk positions.
        // Use keys that spread evenly across all 128 cells.
        let num_bulk_keys: u32 = 256;
        let step = u32::MAX / num_bulk_keys;
        for i in 0..num_bulk_keys {
            db.insert(ks_bulk, bulk_key(i * step), bulk_value.clone())
                .expect("insert failed");
        }
        thread::sleep(Duration::from_millis(50));

        // Snapshot 1: flush everything (threshold=0 means all dirty entries get flushed)
        db.force_rebuild_control_region()
            .expect("snapshot 1 failed");

        // Verify initial value is readable
        let val = db.get(ks_rare, &rare_key).expect("get failed");
        assert_eq!(
            val,
            Some(Bytes::from(initial_value.clone())),
            "initial value should be readable"
        );

        // --- Phase 2: Advance WAL far past the rare entry, run snapshots to advance last_processed ---
        // Write bulk data in several rounds with snapshots in between.
        // This advances the rare entry's last_processed (it's clean) while its
        // on-disk position stays at the original low WAL offset.
        for round in 0..5 {
            for i in 0..num_bulk_keys {
                let v = vec![(round + 2) as u8; 512];
                db.insert(ks_bulk, bulk_key(i * step), v)
                    .expect("insert failed");
            }
            thread::sleep(Duration::from_millis(50));

            // Each snapshot: bulk entries get flushed at new high positions,
            // rare entry stays clean → last_processed advanced to current WAL frontier.
            db.force_rebuild_control_region().expect("snapshot failed");
        }

        // --- Phase 3: Write to the rare entry (becomes dirty) ---
        db.insert(ks_rare, rare_key.clone(), updated_value.clone())
            .expect("insert failed");
        thread::sleep(Duration::from_millis(50));

        // Verify updated value is readable in memory
        let val = db.get(ks_rare, &rare_key).expect("get failed");
        assert_eq!(
            val,
            Some(Bytes::from(updated_value.clone())),
            "updated value should be readable before snapshot"
        );

        // --- Phase 4: Snapshot ---
        // The rare entry is dirty going into the snapshot. In the two-pass
        // design, pass 1 issues a normal flush of the dirty overlay (preserving
        // the new value) before pass 2 ever considers force-relocation. So
        // the historical bug — ForceRelocate copying the stale on-disk index
        // and dropping the dirty overlay — is structurally impossible here.
        // The crash-recovery check below verifies the dirty write survives.
        db.force_rebuild_control_region()
            .expect("final snapshot failed");

        // Verify value is still correct in memory after snapshot
        let val = db.get(ks_rare, &rare_key).expect("get failed");
        assert_eq!(
            val,
            Some(Bytes::from(updated_value.clone())),
            "updated value should still be readable after snapshot"
        );
    }

    // --- Phase 5: Crash recovery — reopen DB and verify dirty write survived ---
    {
        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .expect("reopen failed");
        let ks_rare = db.ks("rare");

        let val = db.get(ks_rare, &rare_key).expect("get failed");
        assert_eq!(
            val,
            Some(Bytes::from(updated_value.clone())),
            "updated value must survive crash recovery — \
             if this fails, ForceRelocate dropped the dirty overlay"
        );
    }
}

#[test]
// This test simulates a situation
// where index wal file is deleted while index is being read by another thread.
// We use latch fail point to emulate race condition where thread reading from the db is blocked
// after it reads the index but before it does IO to the index, while the file is being deleted.
// This test will fail if index reader is acquired after the row mutex is dropped in LargeTable::get
fn test_concurrent_index_reclaim() {
    let dir = tempdir::TempDir::new("test-concurrent-index-reclaim").unwrap();
    let mut config = Config::small();
    config.wal_file_size = config.frag_size;
    // `delete_files` skips files whose map is still in `WalMaps`; cap the
    // index map LRU at its minimum so file 0's map drops out within the
    // handful of file advances this test performs.
    config.max_index_maps = Some(3);
    let config = Arc::new(config);
    let key_shape = KeyShape::new_single(2, 1, KeyType::uniform(1));

    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();
    let ks = db.single_ks();

    const SLEEP: u64 = 200;

    db.insert(ks, vec![1, 2], vec![5, 6]).unwrap();
    db.wal_writer.wal_tracker_barrier();
    db.force_rebuild_control_region().unwrap();
    assert!(db.large_table.is_all_clean());
    // Snapshot keeps entries loaded in memory; force-unload clean entries so that the
    // subsequent get() goes through the disk-read path and hits the fail point.
    db.large_table.force_unload_clean();
    let (lookup_latch, lookup_latch_guard) = Latch::new();
    db.large_table.fp.0.write().fp_lookup_after_lock_drop = FailPoint::latch(lookup_latch);
    let lookup_thread = {
        let db = db.clone();
        thread::spawn(move || db.get(ks, &[1, 2]))
    };
    thread::sleep(Duration::from_millis(SLEEP));
    assert!(!lookup_thread.is_finished());
    // Write big buffer into index wal to force it to go to next wal file
    db.index_writer
        .write(&PreparedWalWrite::new(&vec![
            0;
            config.frag_size as usize - 16
        ]))
        .unwrap();
    db.insert(ks, vec![3, 4], vec![6, 7]).unwrap();
    db.index_writer.wal_tracker_barrier();
    db.force_rebuild_control_region().unwrap();
    db.insert(ks, vec![3, 4], vec![6, 7]).unwrap();
    db.index_writer.wal_tracker_barrier();
    db.force_rebuild_control_region().unwrap();
    // Write big buffer into index wal to force it to go to next wal file, which also trigger file reclaim in WalMapper thread
    db.index_writer
        .write(&PreparedWalWrite::new(&vec![
            0;
            config.frag_size as usize - 16
        ]))
        .unwrap();
    thread::sleep(Duration::from_millis(SLEEP));
    // Assert that at least one file was deleted
    assert!(db.indexes.min_wal_position() > 0);
    assert!(!lookup_thread.is_finished());
    drop(lookup_latch_guard);

    assert_eq!(
        Some(vec![5, 6].into()),
        lookup_thread.join().unwrap().unwrap()
    );
}

#[test]
fn test_empty_value_read_optimization() {
    // This test verifies that when reading keys with empty values from keyspaces
    // that use Hash indexing, we can avoid WAL reads by checking the payload length.
    // Hash indexing means need_check_index_key() returns false, enabling the optimization.
    let dir = tempdir::TempDir::new("test-empty-value-optimization").unwrap();
    let config = Arc::new(Config::small());

    // Use hash indexing which allows the optimization (need_check_index_key() == false)
    let key_shape = hashed_index_key_shape();
    let metrics = Metrics::new();

    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();
    let ks = db.single_ks();

    // Insert keys with empty values - these should benefit from the optimization
    for i in 0..10u32 {
        db.insert(ks, i.to_be_bytes().to_vec(), vec![]).unwrap();
    }

    // Insert keys with non-empty values - these will require WAL reads
    for i in 10..20u32 {
        db.insert(ks, i.to_be_bytes().to_vec(), vec![i as u8])
            .unwrap();
    }

    // Flush to ensure everything is written
    db.large_table.flusher.barrier();

    // Get initial read count
    let initial_reads = metrics
        .read
        .with_label_values(&["root", "record", "mapped"])
        .get()
        + metrics
            .read
            .with_label_values(&["root", "record", "syscall"])
            .get();
    assert_eq!(initial_reads, 0);

    // Read all keys with empty values - these should NOT increment the read metric
    // due to the optimization
    for i in 0..10u32 {
        let val = db.get(ks, &i.to_be_bytes()).unwrap();
        assert_eq!(val, Some(Bytes::new()));
    }

    let reads_after_empty = metrics
        .read
        .with_label_values(&["root", "record", "mapped"])
        .get()
        + metrics
            .read
            .with_label_values(&["root", "record", "syscall"])
            .get();

    // The optimization should have avoided all WAL reads for empty values
    assert_eq!(
        initial_reads, reads_after_empty,
        "Empty value optimization should avoid WAL reads"
    );

    // Now read keys with non-empty values - these SHOULD increment the read metric
    for i in 10..20u32 {
        let val = db.get(ks, &i.to_be_bytes()).unwrap();
        assert_eq!(val, Some(vec![i as u8].into()));
    }

    let reads_after_nonempty = metrics
        .read
        .with_label_values(&["root", "record", "mapped"])
        .get()
        + metrics
            .read
            .with_label_values(&["root", "record", "syscall"])
            .get();

    // Non-empty values should have caused WAL reads
    assert_eq!(
        reads_after_nonempty - reads_after_empty,
        10,
        "Non-empty values should require WAL reads"
    );
}

/// Helper function to set up a database with an incomplete batch in the WAL
/// This simulates a crash during batch write, leaving the WAL in a partially written state
fn setup_corrupted_db(
    dir: &std::path::Path,
    key_shape: &KeyShape,
    config: &Arc<Config>,
    ks: KeySpace,
) {
    let db = Db::open(dir, key_shape.clone(), config.clone(), Metrics::new()).unwrap();

    // Set up WAL failpoint to panic after 2 writes
    use crate::wal::WalFailPointsInner;
    db.wal_writer.fp.0.store(Arc::new(WalFailPointsInner {
        fp_multi_write_before_write_buf: FailPoint::panic_after_n_calls(2),
    }));

    // Create a batch with 3 records
    let mut batch = db.write_batch();
    batch.write(ks, vec![1, 2, 3, 4], vec![10]);
    batch.write(ks, vec![2, 3, 4, 5], vec![20]);
    batch.write(ks, vec![3, 4, 5, 6], vec![30]);

    // Attempt to write the batch - this should panic on the 3rd write
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        batch.commit().unwrap();
    }));

    assert!(result.is_err(), "Expected panic during batch write");
    db.wait_for_background_threads_to_finish();
}

#[test]
fn test_batch_after_incomplete_batch() {
    let dir = tempdir::TempDir::new("test_wal_failpoint_panic_during_batch").unwrap();
    let config = Arc::new(Config::small());
    let key_shape = KeyShape::new_single(4, 16, KeyType::uniform(16));
    let ks = KeySpaces::from_key_shape(&key_shape).single();

    // Set up database with incomplete batch
    setup_corrupted_db(dir.path(), &key_shape, &config, ks);

    // Now reopen the database - it should open without issues
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();

        // Verify that none of the keys from the failed batch are accessible
        // Since the batch write is atomic, all 3 writes should have been rolled back
        assert_eq!(None, db.get(ks, &[1, 2, 3, 4]).unwrap());
        assert_eq!(None, db.get(ks, &[2, 3, 4, 5]).unwrap());
        assert_eq!(None, db.get(ks, &[3, 4, 5, 6]).unwrap());

        // Now write the batch again without the failpoint
        let mut batch = db.write_batch();
        batch.write(ks, vec![1, 2, 3, 4], vec![10]);
        batch.write(ks, vec![2, 3, 4, 5], vec![20]);
        batch.write(ks, vec![3, 4, 5, 6], vec![30]);
        batch.commit().unwrap();
        db.wait_for_background_threads_to_finish();
    }

    // Reopen the database again and verify all keys are accessible
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();

        // Verify all keys are now accessible with correct values
        assert_eq!(Some(vec![10].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
        assert_eq!(Some(vec![20].into()), db.get(ks, &[2, 3, 4, 5]).unwrap());
        assert_eq!(Some(vec![30].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
    }
}

#[test]
fn test_standalone_write_after_incomplete_batch() {
    let dir = tempdir::TempDir::new("test_wal_failpoint_standalone_write").unwrap();
    let config = Arc::new(Config::small());
    let key_shape = KeyShape::new_single(4, 16, KeyType::uniform(16));
    let ks = KeySpaces::from_key_shape(&key_shape).single();

    // Set up database with incomplete batch
    setup_corrupted_db(dir.path(), &key_shape, &config, ks);

    // Reopen the database - replay should stop at CRC error
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();

        // Verify that none of the keys from the failed batch are accessible
        assert_eq!(None, db.get(ks, &[1, 2, 3, 4]).unwrap());
        assert_eq!(None, db.get(ks, &[2, 3, 4, 5]).unwrap());
        assert_eq!(None, db.get(ks, &[3, 4, 5, 6]).unwrap());

        // Now write a STANDALONE record (not a batch)
        // This will overwrite the garbage space left by the incomplete batch
        db.insert(ks, vec![4, 5, 6, 7], vec![40]).unwrap();

        // Verify the standalone write is accessible before reopen
        assert_eq!(Some(vec![40].into()), db.get(ks, &[4, 5, 6, 7]).unwrap());
    }

    // Reopen the database again
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();

        assert_eq!(
            Some(vec![40].into()),
            db.get(ks, &[4, 5, 6, 7]).unwrap(),
            "Standalone write after incomplete batch should be accessible"
        );
    }
}

#[test]
fn test_drop_cells_in_range_uniform() {
    let dir = tempdir::TempDir::new("test-drop-cells").unwrap();
    let key_shape = KeyShape::new_single(10, 2, KeyType::uniform(2));
    let ks = KeySpaces::from_key_shape(&key_shape).single();
    let config = Arc::new(Config::small());

    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();

    // Insert data across multiple cells
    // Cell 0: 0x00000000..0x40000000
    db.insert(ks, hex!("00000000000000000000"), vec![1])
        .unwrap();
    db.insert(ks, hex!("10000000000000000000"), vec![2])
        .unwrap();
    // Cell 1: 0x40000000..0x80000000
    db.insert(ks, hex!("40000000000000000000"), vec![3])
        .unwrap();
    db.insert(ks, hex!("50000000000000000000"), vec![4])
        .unwrap();
    // Cell 2: 0x80000000..0xC0000000
    db.insert(ks, hex!("80000000000000000000"), vec![5])
        .unwrap();

    // Verify all data is present
    assert_eq!(
        Some(vec![1].into()),
        db.get(ks, &hex!("00000000000000000000")).unwrap()
    );
    assert_eq!(
        Some(vec![3].into()),
        db.get(ks, &hex!("40000000000000000000")).unwrap()
    );
    assert_eq!(
        Some(vec![5].into()),
        db.get(ks, &hex!("80000000000000000000")).unwrap()
    );

    // Get boundary keys for cells 0 and 1
    let ksd = db.key_shape.ks(ks);
    let (first_key, _) = ksd.cell_range(&crate::cell::CellId::Integer(0));
    let (_, last_key) = ksd.cell_range(&crate::cell::CellId::Integer(1));

    // Drop cells 0 and 1
    db.drop_cells_in_range(ks, &first_key, &last_key).unwrap();

    // Data from cells 0 and 1 should be gone from memory
    assert_eq!(None, db.get(ks, &hex!("00000000000000000000")).unwrap());
    assert_eq!(None, db.get(ks, &hex!("40000000000000000000")).unwrap());
    // But data from cell 2 should still be there
    assert_eq!(
        Some(vec![5].into()),
        db.get(ks, &hex!("80000000000000000000")).unwrap()
    );

    // Reopen the database - dropped cells should remain dropped after WAL replay
    drop(db);
    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();

    // Data from cells 0 and 1 should still be gone
    assert_eq!(None, db.get(ks, &hex!("00000000000000000000")).unwrap());
    assert_eq!(None, db.get(ks, &hex!("40000000000000000000")).unwrap());
    // Data from cell 2 should still be there
    assert_eq!(
        Some(vec![5].into()),
        db.get(ks, &hex!("80000000000000000000")).unwrap()
    );
}

#[test]
fn test_drop_cells_clears_value_cache() {
    // Regression: drop_cells_in_range must invalidate the per-cell value LRU.
    // For Uniform (Array-backed) keyspaces the entry is cleared in place rather
    // than removed, and `get` consults the value LRU before the Empty-state
    // check — so a retained cache entry would serve a stale value for a key
    // whose cell was dropped.
    let dir = tempdir::TempDir::new("test-drop-cells-value-cache").unwrap();
    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new().with_value_cache_size(512);
    ksb.add_key_space_config("k", 10, 2, KeyType::uniform(2), ksc);
    let key_shape = ksb.build();
    let config = Arc::new(Config::small());
    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
    let ks = db.ks("k");

    // Cell 0 and cell 2; cell 2 is the control that must survive.
    db.insert(ks, hex!("00000000000000000000"), vec![1])
        .unwrap();
    db.insert(ks, hex!("80000000000000000000"), vec![5])
        .unwrap();
    // Read both so the value LRU is populated.
    assert_eq!(
        Some(vec![1].into()),
        db.get(ks, &hex!("00000000000000000000")).unwrap()
    );
    assert_eq!(
        Some(vec![5].into()),
        db.get(ks, &hex!("80000000000000000000")).unwrap()
    );

    let ksd = db.key_shape.ks(ks);
    let (first_key, _) = ksd.cell_range(&crate::cell::CellId::Integer(0));
    let (_, last_key) = ksd.cell_range(&crate::cell::CellId::Integer(1));
    db.drop_cells_in_range(ks, &first_key, &last_key).unwrap();

    // The dropped key must not be served stale from the value LRU.
    assert_eq!(None, db.get(ks, &hex!("00000000000000000000")).unwrap());
    // The untouched cell still serves its cached value.
    assert_eq!(
        Some(vec![5].into()),
        db.get(ks, &hex!("80000000000000000000")).unwrap()
    );
}

#[test]
fn test_drop_cells_in_range_prefixed_uniform() {
    let dir = tempdir::TempDir::new("test-drop-cells-prefixed").unwrap();
    let key_shape = KeyShape::new_single(10, 16, KeyType::from_prefix_bits(16));
    let ks = KeySpaces::from_key_shape(&key_shape).single();
    let config = Arc::new(Config::small());

    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();

    // Insert data across multiple cells (different prefixes)
    db.insert(ks, hex!("12340000000000000000"), vec![1])
        .unwrap();
    db.insert(ks, hex!("1234AAAAAAAAAAAAAAAA"), vec![2])
        .unwrap();
    db.insert(ks, hex!("56780000000000000000"), vec![3])
        .unwrap();
    db.insert(ks, hex!("5678BBBBBBBBBBBBBBBB"), vec![4])
        .unwrap();
    db.insert(ks, hex!("9ABC0000000000000000"), vec![5])
        .unwrap();

    // Verify all data is present
    assert_eq!(
        Some(vec![1].into()),
        db.get(ks, &hex!("12340000000000000000")).unwrap()
    );
    assert_eq!(
        Some(vec![3].into()),
        db.get(ks, &hex!("56780000000000000000")).unwrap()
    );
    assert_eq!(
        Some(vec![5].into()),
        db.get(ks, &hex!("9ABC0000000000000000")).unwrap()
    );

    // Get boundary keys for cell [0x12, 0x34] only (single cell)
    // Note: Testing single cell drop for PrefixedUniform to avoid issues with
    // next_cell traversal over non-existent cells in the BTreeMap
    let ksd = db.key_shape.ks(ks);
    let cell1 = crate::cell::CellId::Bytes(smallvec::SmallVec::from_slice(&[0x12, 0x34]));
    let (first_key, last_key) = ksd.cell_range(&cell1);

    // Drop single cell [0x12, 0x34]
    db.drop_cells_in_range(ks, &first_key, &last_key).unwrap();

    // Data from cell [0x12, 0x34] should be gone from memory
    assert_eq!(None, db.get(ks, &hex!("12340000000000000000")).unwrap());
    // But data from other cells should still be there
    assert_eq!(
        Some(vec![3].into()),
        db.get(ks, &hex!("56780000000000000000")).unwrap()
    );
    assert_eq!(
        Some(vec![5].into()),
        db.get(ks, &hex!("9ABC0000000000000000")).unwrap()
    );

    // Reopen the database - dropped cells should remain dropped after WAL replay
    drop(db);
    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();

    // Data from cell [0x12, 0x34] should still be gone
    assert_eq!(None, db.get(ks, &hex!("12340000000000000000")).unwrap());
    // Data from other cells should still be there
    assert_eq!(
        Some(vec![3].into()),
        db.get(ks, &hex!("56780000000000000000")).unwrap()
    );
    assert_eq!(
        Some(vec![5].into()),
        db.get(ks, &hex!("9ABC0000000000000000")).unwrap()
    );
}

#[test]
fn test_drop_db() {
    drop_db(default_key_shape())
}

pub(super) fn drop_db(key_shape: KeyShape) {
    let dir = tempdir::TempDir::new("test-drop-db").unwrap();
    let path = dir.path().to_path_buf();
    let config = Arc::new(Config::small());

    let db = Db::open(&path, key_shape, config, Metrics::new()).unwrap();
    let ks = db.single_ks();
    db.insert(ks, vec![1, 2, 3, 4], vec![5, 6]).unwrap();

    // DB is open — drop_db must fail
    let err = Db::drop_db(&path).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
    assert!(path.exists());

    // DB is closed — drop_db must remove the directory
    drop(db);
    Db::drop_db(&path).unwrap();
    assert!(!path.exists());

    // Prevent TempDir from trying to delete the already-removed directory
    std::mem::forget(dir);
}

// `compact_with` must preserve `Removed` entries — they are the only thing
// shadowing matching keys in deeper levels (see `flusher.rs:333-354`: in the
// non-over_threshold branch, `existing_l1.is_some()` skips `clean_self`, so
// tombstones in the new L0 are load-bearing). Without that invariant, the
// shallow flush below would drop the in-memory tombstone, the next read would
// fall through to L1, and the deleted key would resurface.
#[test]
fn test_compact_with_preserves_tombstones_shadowing_l1() {
    use std::collections::HashSet;

    let dir = tempdir::TempDir::new("test-compact-with-preserves-tombstones").unwrap();
    let mut config = Config::small();
    config.sync_flush = false;
    let config = Arc::new(config);

    // Keep-one-per-prefix compactor. The closure's return value is not what
    // matters here — what matters is that `run_compactor` runs during the
    // shallow flush below. Key layout: prefix(1) || suffix(1).
    let compactor: Compactor = Box::new(|iter: &mut dyn DoubleEndedIterator<Item = &Bytes>| {
        let mut retain: HashSet<Bytes> = HashSet::new();
        let mut previous: Option<Bytes> = None;
        const PREFIX_SIZE: usize = 1;
        for key in iter.rev() {
            if let Some(prev) = &previous
                && prev[..PREFIX_SIZE] == key[..PREFIX_SIZE]
            {
                continue;
            }
            previous = Some(key.clone());
            retain.insert(key.clone());
        }
        retain
    });

    // One cell, two-byte keys. l0_max_entries=4 means a flush with
    // merged_l0.len() > 4 promotes into L1.
    let ks_config = KeySpaceConfig::new()
        .with_compactor(compactor)
        .with_l0_max_entries(4);
    let key_shape = KeyShape::new_single_config(2, 1, KeyType::uniform(1), ks_config);

    let db = Db::open(dir.path(), key_shape, config.clone(), Metrics::new()).unwrap();
    let ks = db.single_ks();

    // Phase 1: write 8 distinct-prefix keys (one is the victim we will later
    // delete) and force a flush. 8 > l0_max_entries=4 → `over_threshold`
    // branch promotes everything into L1, cell ends as [INVALID, L1].
    let victim_prefix: u8 = 0x42;
    let victim_key: Vec<u8> = vec![victim_prefix, 0x01];
    let victim_val: Vec<u8> = vec![0xAA, 0xBB];

    db.insert(ks, victim_key.clone(), victim_val.clone())
        .unwrap();
    for i in 0u8..8 {
        if i == victim_prefix {
            continue;
        }
        db.insert(ks, vec![i, 0x00], vec![i]).unwrap();
    }
    db.wal_writer.wal_tracker_barrier();
    db.force_rebuild_control_region().unwrap();

    db.large_table.each_entry(|entry| {
        assert!(
            entry.levels().l0().is_none(),
            "expected post-promote L0 to be INVALID",
        );
        assert!(
            entry.levels().l1().is_some(),
            "expected L1 to hold the blob"
        );
    });

    assert_eq!(
        Some(victim_val.clone().into()),
        db.get(ks, &victim_key).unwrap(),
    );

    // Phase 2: delete the victim. Single-op `Db::remove` writes the tombstone
    // straight into `entry.data`, so the in-memory shadow is visible.
    db.remove(ks, victim_key.clone()).unwrap();
    assert_eq!(None, db.get(ks, &victim_key).unwrap());

    // Force a non-over_threshold flush. merged_l0 has a single Removed entry
    // (well under l0_max_entries=4), so the flusher writes a new L0 above the
    // existing L1. The tombstone must survive into that new L0 to keep
    // shadowing L1.
    db.wal_writer.wal_tracker_barrier();
    db.force_rebuild_control_region().unwrap();

    db.large_table.each_entry(|entry| {
        assert!(entry.levels().l1().is_some(), "L1 must still be populated");
    });

    assert_eq!(
        None,
        db.get(ks, &victim_key).unwrap(),
        "tombstone in the new L0 must shadow the L1 entry",
    );
}

#[test]
fn test_auto_sharding_concurrent() {
    use parking_lot::Mutex;
    use std::collections::HashMap;

    type Slot = Arc<Mutex<Option<Vec<u8>>>>;

    let dir = tempdir::TempDir::new("test-auto-sharding-concurrent").unwrap();

    let mut config = Config::small();
    config.frag_size = 4 * 1024;
    config.max_dirty_keys = 8;
    config.l0_max_entries = Some(16);
    // Keep index WAL files tiny so churn rolls the writer forward past the
    // most-recent file. Combined with the small `index_min_occupancy_pct`
    // window below, this lets older files qualify as force-relocation
    // candidates.
    config.wal_file_size = 8 * 1024;
    // Force dirty entries to flush on every snapshot so the alive-bytes
    // accumulator reflects post-flush positions; otherwise untouched-shard
    // positions never get re-counted against current files.
    config.snapshot_unload_threshold = 0;
    // Aggressive occupancy threshold so any old file with a partially-stale
    // mix of live blobs qualifies for force-relocation. Combined with the
    // narrowed second-half workload below, the shards covering the outer
    // quarters stay put while their hosting files drain below this threshold.
    config.index_min_occupancy_pct = 99;
    config.with_index_auto_sharding();
    let config = Arc::new(config);

    let key_shape = KeyShape::new_single(8, 2, KeyType::uniform(1));
    let ks = KeySpaces::from_key_shape(&key_shape).single();
    let metrics = Metrics::new();
    let mut db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();

    let state: Arc<Mutex<HashMap<Vec<u8>, Slot>>> = Arc::default();

    const POOL_SIZE: u64 = 4000;
    const THREADS: usize = 4;
    const OPS_PER_THREAD: usize = 500;
    let iterations: usize = std::env::var("AUTO_SHARDING_ITERATIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);

    for iter in 0..iterations {
        // First half churns the whole pool to build up sharded L1s across
        // many WAL files. Second half narrows to a sliver around the middle
        // ([7/16, 9/16) of the pool) so shards outside that band stop
        // receiving writes — their blobs age into old files that fall below
        // `index_min_occupancy_pct` and become force-relocation targets.
        let (key_lo, key_hi) = if iter < iterations / 2 {
            (0u64, POOL_SIZE)
        } else {
            (POOL_SIZE * 7 / 16, POOL_SIZE * 9 / 16)
        };

        let mut handles = vec![];
        for tid in 0..THREADS {
            let db = db.clone();
            let state = state.clone();
            handles.push(thread::spawn(move || {
                let seed = (iter * THREADS + tid) as u64;
                let mut rng = StdRng::seed_from_u64(seed);
                for op_idx in 0..OPS_PER_THREAD {
                    // Mid-iteration snapshot from thread 0 — races writers in
                    // the other threads so the flush+relocate path runs while
                    // dirty overlays are in motion.
                    if tid == 0 && op_idx == OPS_PER_THREAD / 2 && iter >= iterations / 2 {
                        db.force_rebuild_control_region().unwrap();
                    }
                    let key = rng.gen_range(key_lo..key_hi).to_le_bytes().to_vec();

                    let slot = state
                        .lock()
                        .entry(key.clone())
                        .or_insert_with(|| Arc::new(Mutex::new(None)))
                        .clone();
                    let mut slot_guard = slot.lock();

                    if rng.gen_bool(0.75) {
                        let value: Vec<u8> = (0..8).map(|_| rng.r#gen()).collect();
                        db.insert(ks, key.clone(), value.clone()).unwrap();
                        *slot_guard = Some(value);
                    } else {
                        db.remove(ks, key.clone()).unwrap();
                        *slot_guard = None;
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let snap: HashMap<Vec<u8>, Vec<u8>> = state
            .lock()
            .iter()
            .filter_map(|(k, s)| s.lock().clone().map(|v| (k.clone(), v)))
            .collect();

        for (k, v) in &snap {
            let got = db.get(ks, k).unwrap();
            assert_eq!(
                got.as_deref(),
                Some(v.as_slice()),
                "get mismatch at iter {iter} for key {k:?}",
            );
        }

        let from_iter: HashMap<Vec<u8>, Vec<u8>> = db
            .iterator(ks)
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.as_ref().to_vec(), v.as_ref().to_vec())
            })
            .collect();
        assert_eq!(from_iter, snap, "iterator/shadow mismatch at iter {iter}");

        // Reverse iteration must visit the same key set in descending order so
        // the sharded reverse picker is exercised across the same shards as
        // the forward sweep.
        let mut reverse_iter = db.iterator(ks);
        reverse_iter.reverse();
        let reverse_pairs: Vec<(Vec<u8>, Vec<u8>)> = reverse_iter
            .map(|r| {
                let (k, v) = r.unwrap();
                (k.as_ref().to_vec(), v.as_ref().to_vec())
            })
            .collect();
        for window in reverse_pairs.windows(2) {
            assert!(
                window[0].0 > window[1].0,
                "reverse iterator must yield strictly descending keys at iter {iter}",
            );
        }
        let from_reverse_iter: HashMap<Vec<u8>, Vec<u8>> = reverse_pairs.into_iter().collect();
        assert_eq!(
            from_reverse_iter, snap,
            "reverse iterator/shadow mismatch at iter {iter}",
        );

        // Spot-check iteration starting from a random key in the middle of
        // the pool so the cell-seek path for both directions is exercised at
        // positions other than the keyspace boundaries.
        let mut start_rng = StdRng::seed_from_u64((iter as u64).wrapping_mul(0x9E3779B97F4A7C15));
        for check_idx in 0..2 {
            let start_key = start_rng.gen_range(0..POOL_SIZE).to_le_bytes().to_vec();

            let mut fwd_iter = db.iterator(ks);
            fwd_iter.set_lower_bound(start_key.clone());
            let fwd_pairs: Vec<(Vec<u8>, Vec<u8>)> = fwd_iter
                .map(|r| {
                    let (k, v) = r.unwrap();
                    (k.as_ref().to_vec(), v.as_ref().to_vec())
                })
                .collect();
            for window in fwd_pairs.windows(2) {
                assert!(
                    window[0].0 < window[1].0,
                    "forward iterator from random key must yield strictly ascending keys \
                     at iter {iter} check {check_idx}",
                );
            }
            let mut expected_fwd: Vec<(Vec<u8>, Vec<u8>)> = snap
                .iter()
                .filter(|(k, _)| k.as_slice() >= start_key.as_slice())
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            expected_fwd.sort_by(|a, b| a.0.cmp(&b.0));
            assert_eq!(
                fwd_pairs, expected_fwd,
                "forward iterator from random key mismatch at iter {iter} check {check_idx} \
                 start_key={start_key:?}",
            );

            let mut rev_iter = db.iterator(ks);
            rev_iter.set_upper_bound(start_key.clone());
            rev_iter.reverse();
            let rev_pairs: Vec<(Vec<u8>, Vec<u8>)> = rev_iter
                .map(|r| {
                    let (k, v) = r.unwrap();
                    (k.as_ref().to_vec(), v.as_ref().to_vec())
                })
                .collect();
            for window in rev_pairs.windows(2) {
                assert!(
                    window[0].0 > window[1].0,
                    "reverse iterator from random key must yield strictly descending keys \
                     at iter {iter} check {check_idx}",
                );
            }
            let mut expected_rev: Vec<(Vec<u8>, Vec<u8>)> = snap
                .iter()
                .filter(|(k, _)| k.as_slice() < start_key.as_slice())
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            expected_rev.sort_by(|a, b| b.0.cmp(&a.0));
            assert_eq!(
                rev_pairs, expected_rev,
                "reverse iterator from random key mismatch at iter {iter} check {check_idx} \
                 start_key={start_key:?}",
            );
        }

        // After the midpoint, snapshot the control region every iteration so
        // the low-occupancy index files left behind by heavy churn become
        // force-relocation candidates.
        if iter >= iterations / 2 {
            db.force_rebuild_control_region().unwrap();
        }

        // In the last third of iterations, drop and re-open every third
        // iteration so subsequent threads exercise reads/writes against
        // freshly-replayed sharded cells.
        let last_third_start = iterations * 2 / 3;
        if iter >= last_third_start && (iter - last_third_start).is_multiple_of(3) {
            drop(db);
            db = Db::open(
                dir.path(),
                key_shape.clone(),
                config.clone(),
                metrics.clone(),
            )
            .unwrap();
        }
    }

    let label = &["root"];
    let shards_total = metrics.l1_shards_total.with_label_values(label).get();
    let splits = metrics.l1_shard_split_total.with_label_values(label).get();
    let resharding = metrics
        .l1_shard_rewritten_total
        .with_label_values(label)
        .get();
    let forced_reloc = metrics
        .snapshot_forced_relocation
        .with_label_values(label)
        .get();
    println!(
        "auto-sharding counters: l1_shards_total={shards_total} \
         l1_shard_split_total={splits} l1_shard_rewritten_total={resharding} \
         snapshot_forced_relocation={forced_reloc}"
    );
    assert!(
        resharding > 0,
        "expected re-sharding (incremental sharded promote) to fire"
    );
}
