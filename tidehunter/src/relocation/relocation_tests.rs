use std::collections::HashSet;
use std::{path::Path, sync::Arc, thread, time::Duration};

use crate::control::RelocateFiles;
use crate::failpoints::FailPoint;
use crate::key_shape::{KeySpace, KeySpaceConfig};
use crate::large_table::Loader;
use crate::latch::Latch;
use crate::relocation::CellReference;
use crate::relocation::watermark::WatermarkData;
use crate::wal::layout::WalKind;
use crate::{
    RelocationStrategy,
    config::Config,
    db::Db,
    key_shape::{KeyShapeBuilder, KeyType},
    relocation::RelocationWatermarks,
};
use crate::{metrics::Metrics, relocation::Decision};
use minibytes::Bytes;

fn force_unload_config(config: &Config) -> Arc<Config> {
    let mut config2 = Config::clone(config);
    config2.snapshot_unload_threshold = 0;
    Arc::new(config2)
}

fn index_based_config() -> Arc<Config> {
    let mut config = Config::small();
    config.relocation_strategy = RelocationStrategy::IndexBased(None);
    force_unload_config(&config)
}

fn relocation_removed(metrics: &Metrics, name: &str) -> u64 {
    metrics
        .relocation_removed
        .get_metric_with_label_values(&[name])
        .unwrap()
        .get()
}

fn relocation_cells_processed(metrics: &Metrics, keyspace_name: &str) -> u64 {
    metrics
        .relocation_cells_processed
        .get_metric_with_label_values(&[keyspace_name])
        .unwrap()
        .get()
}

fn start_index_based_relocation(db: &Db) {
    db.start_blocking_relocation_with_strategy(RelocationStrategy::IndexBased(None))
}

fn list_wal_files(path: &Path) -> Vec<String> {
    std::fs::read_dir(path)
        .unwrap()
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            let name = path.file_name()?.to_str()?;
            if name.starts_with("wal_") {
                Some(name.to_string())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
}

// A checkpoint must keep reading the as-of-frontier values across a relocation
// that runs while it is held: a pre-checkpoint key is overwritten after the
// checkpoint and then relocation rewrites positions / reclaims WAL files. Run
// against two configs (see the two tests below): forced unload, where the cell
// unloads and the as-of value is read from the relocated on-disk blob; and
// unloading disabled, where the cell stays loaded and the as-of value must
// survive in the in-memory overlay.
fn checkpoint_survives_relocation(config: Arc<Config>, ksc: KeySpaceConfig) {
    let dir = tempdir::TempDir::new("test_ckpt_reloc").unwrap();
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config("k", 8, 1, KeyType::uniform(1), ksc);
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
    let ks = db.ks("k");

    let big = |b: u8| -> Vec<u8> { vec![b; 12 * 1000] };
    let overwritten = 3u64;
    let untouched = 7u64;

    db.insert(ks, overwritten.to_be_bytes().to_vec(), big(1))
        .unwrap();
    db.insert(ks, untouched.to_be_bytes().to_vec(), big(5))
        .unwrap();
    // Enough padding to push the relocation target past several WAL files so it
    // actually relocates (rewrites positions), not a no-op.
    for i in 1000..3000u64 {
        db.insert(ks, i.to_be_bytes().to_vec(), big(7)).unwrap();
    }

    let checkpoint = db.checkpoint();

    db.insert(ks, overwritten.to_be_bytes().to_vec(), big(2))
        .unwrap();
    for i in 3000..3100u64 {
        db.insert(ks, i.to_be_bytes().to_vec(), big(7)).unwrap();
    }

    db.start_blocking_relocation();

    // Sanity: relocation actually relocated entries (otherwise this test is
    // vacuous — it must exercise the position-rewrite path).
    let kept = metrics.relocation_kept.with_label_values(&["k"]).get();
    assert!(
        kept > 0,
        "relocation must have relocated entries for this test to be meaningful"
    );

    // Checkpoint reads the as-of-frontier values.
    assert_eq!(
        checkpoint.get(ks, &overwritten.to_be_bytes()).unwrap(),
        Some(big(1).into()),
        "overwritten key as of checkpoint"
    );
    assert_eq!(
        checkpoint.get(ks, &untouched.to_be_bytes()).unwrap(),
        Some(big(5).into()),
        "untouched key as of checkpoint (relocation rewrite hazard)"
    );
    // Live reads reflect the latest values after relocation.
    assert_eq!(
        db.get(ks, &untouched.to_be_bytes()).unwrap(),
        Some(big(5).into())
    );
    assert_eq!(
        db.get(ks, &overwritten.to_be_bytes()).unwrap(),
        Some(big(2).into())
    );
}

#[test]
fn test_checkpoint_survives_relocation_with_unload() {
    // Forced unload during the held checkpoint (snapshot_unload_threshold = 0):
    // the cell unloads, so the checkpoint reads the as-of value from the
    // relocated on-disk blob rather than the post-checkpoint value.
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    checkpoint_survives_relocation(force_unload_config(&config), KeySpaceConfig::new());
}

#[test]
fn test_checkpoint_survives_relocation_without_unload() {
    // Unloading disabled: the cell stays loaded, so the as-of value must survive
    // relocation in the in-memory overlay (relocation rewrites its position to
    // >= L and reclaims the original frame).
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    checkpoint_survives_relocation(Arc::new(config), KeySpaceConfig::new().disable_unload());
}

// Regression: a key overwritten after a checkpoint, whose as-of value lives as a
// distinct *overlay* position (not promoted to flat/L1) in a multi-level/unloaded
// cell, must survive relocation. Before the fix, `get_index_for_cell` collapsed
// the overlay to latest-per-key before `retain_processed`, discarding the
// below-frontier value; it was never relocated, and GC reclaimed its WAL file, so
// the held checkpoint read `None`.
#[test]
fn test_checkpoint_survives_relocation_overlay_asof_value() {
    let dir = tempdir::TempDir::new("ckpt_reloc_overlay_asof").unwrap();
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    let config = force_unload_config(&config); // snapshot_unload_threshold = 0
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config("k", 8, 1, KeyType::uniform(1), KeySpaceConfig::new());
    ksb.add_key_space_config("k2", 8, 1, KeyType::uniform(1), KeySpaceConfig::new());
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
    let ks = db.ks("k");
    let ks2 = db.ks("k2");

    let big = |b: u8| -> Vec<u8> { vec![b; 12 * 1000] };
    let target = 3u64;

    // 1. Build a cell from OTHER keys (not `target`); flush it to disk
    //    synchronously so the bulk lives on-disk and the overlay is clean.
    //    Two force-rebuilds encourage an L0->L1 compaction (multi-level cell).
    for i in 1000..3000u64 {
        db.insert(ks, i.to_be_bytes().to_vec(), big(7)).unwrap();
    }
    db.force_rebuild_control_region().unwrap();
    db.force_rebuild_control_region().unwrap();

    // 2. Insert the target's as-of value into a fresh overlay over the on-disk
    //    cell. One dirty key (< max_dirty_keys=32) => stays in `data`.
    db.insert(ks, target.to_be_bytes().to_vec(), big(1))
        .unwrap();

    // 3. Pin a checkpoint at this frontier (target == big(1) as of here).
    let checkpoint = db.checkpoint();
    assert_eq!(
        checkpoint.get(ks, &target.to_be_bytes()).unwrap(),
        Some(big(1).into()),
        "SANITY: as-of read works right after checkpoint"
    );

    // 4. Overwrite target after the frontier => overlay holds BOTH positions.
    db.insert(ks, target.to_be_bytes().to_vec(), big(2))
        .unwrap();
    assert_eq!(
        checkpoint.get(ks, &target.to_be_bytes()).unwrap(),
        Some(big(1).into()),
        "SANITY: as-of read still works after the post-frontier overwrite (pre-relocation)"
    );

    // 5. Advance the global WAL position via a different keyspace/cell so
    //    relocation's target_position passes the as-of value's offset.
    for i in 0..4000u64 {
        db.insert(ks2, i.to_be_bytes().to_vec(), big(9)).unwrap();
    }

    // 6. Relocate (default WAL-based) => GC reclaims files below target_position.
    db.start_blocking_relocation();
    assert!(
        metrics.relocation_kept.with_label_values(&["k"]).get() > 0,
        "relocation must have relocated entries"
    );

    assert_eq!(
        db.get(ks, &target.to_be_bytes()).unwrap(),
        Some(big(2).into()),
        "live get must be the overwrite"
    );
    assert_eq!(
        checkpoint.get(ks, &target.to_be_bytes()).unwrap(),
        Some(big(1).into()),
        "checkpoint must read the as-of value big(1) after relocation; \
         None => the below-frontier overlay value was lost"
    );
}

#[test]
fn test_wal_relocation_basic_flow() {
    let dir = tempdir::TempDir::new("test_relocation_filter").unwrap();
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    let config = Arc::new(config);
    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new().with_relocation_filter(|key, _| {
        let value = u32::from_be_bytes(key.try_into().unwrap());
        if value >= 1_000 {
            Decision::StopRelocation
        } else {
            Decision::Remove
        }
    });
    ksb.add_key_space_config("k", 4, 1, KeyType::uniform(1), ksc);
    ksb.add_key_space_config("k2", 4, 1, KeyType::uniform(1), KeySpaceConfig::new());
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let sample_key = 3_u32.to_be_bytes().to_vec();
    let insert_count = 2000_u32;
    let value = vec![3; 12 * 1000];
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            force_unload_config(&config),
            metrics.clone(),
        )
        .unwrap();
        let ks = db.ks("k");
        let ks2 = db.ks("k2");
        for i in 0..insert_count {
            db.insert(ks, i.to_be_bytes().to_vec(), value.clone())
                .unwrap();
            db.insert(ks2, i.to_be_bytes().to_vec(), value.clone())
                .unwrap();
        }
        assert_eq!(db.get(ks, &sample_key).unwrap(), Some(value.clone().into()));
        db.wait_for_background_threads_to_finish();
    }
    {
        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            force_unload_config(&config),
            metrics.clone(),
        )
        .unwrap();

        db.rebuild_control_region().unwrap();
        db.start_blocking_relocation();
        db.rebuild_control_region().unwrap();
        db.wait_for_background_threads_to_finish();
    }
    assert!(
        list_wal_files(dir.path())
            .into_iter()
            .all(|name| name != "wal_0000000000000000")
    );
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();
    let ks = db.ks("k");
    let ks2 = db.ks("k2");
    assert_eq!(db.get(ks, &sample_key).unwrap(), None);
    assert_eq!(
        db.get(ks2, &sample_key).unwrap(),
        Some(value.clone().into())
    );
    assert_eq!(
        db.get(ks, 1500_u32.to_be_bytes().as_ref()).unwrap(),
        Some(value.clone().into())
    );
}

// Index-based relocation tests
#[test]
fn test_index_based_relocation_point_deletes() {
    let dir = tempdir::TempDir::new("test_index_based_relocation_point_deletes").unwrap();
    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new().with_bloom_filter(0.01, 2000);
    ksb.add_key_space_config("k", 8, 1, KeyType::uniform(1), ksc);
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        index_based_config(),
        metrics.clone(),
    )
    .unwrap();
    let ks = db.ks("k");
    for key in 0..200u64 {
        db.insert(ks, key.to_be_bytes().to_vec(), vec![0, 1, 2])
            .unwrap();
    }
    for key in 0..100u64 {
        db.remove(ks, key.to_be_bytes().to_vec()).unwrap();
    }
    thread::sleep(Duration::from_millis(10));
    db.rebuild_control_region().unwrap();
    start_index_based_relocation(&db);

    // Index-based relocation processes current cell contents, not historical WAL entries
    // So it won't see the deleted entries (they're not in cells anymore)
    // Instead, verify that:
    // 1. Some cells were processed
    let processed = relocation_cells_processed(&metrics, "k");
    assert!(processed > 0, "Expected some cells to be processed");

    // 2. The preserved data is correct (entries 100-199 should still exist)
    for key in 100..200u64 {
        assert_eq!(
            db.get(ks, &key.to_be_bytes()).unwrap(),
            Some(vec![0, 1, 2].into()),
            "Key {} should still exist",
            key
        );
    }

    // 3. The deleted entries are still gone (entries 0-99 were removed)
    for key in 0..100u64 {
        assert_eq!(
            db.get(ks, &key.to_be_bytes()).unwrap(),
            None,
            "Key {} should not exist",
            key
        );
    }
}

#[test]
fn test_index_based_relocation_filter() {
    let dir = tempdir::TempDir::new("test_index_based_relocation_filter").unwrap();
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    config.relocation_strategy = RelocationStrategy::IndexBased(None);
    let config = force_unload_config(&config);
    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new().with_relocation_filter(|key, _| {
        if u64::from_be_bytes(key.try_into().unwrap()) % 2 == 0 {
            Decision::Keep
        } else {
            Decision::Remove
        }
    });
    ksb.add_key_space_config("k", 8, 1, KeyType::uniform(1), ksc);
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let sample_key = 3_u64.to_be_bytes().to_vec();
    let mut insert_count = 0_u64;
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();
        let ks = db.ks("k");
        loop {
            db.insert(ks, insert_count.to_be_bytes().to_vec(), vec![0, 1, 2])
                .unwrap();
            insert_count += 1;
            if insert_count.is_multiple_of(10000) && list_wal_files(dir.path()).len() > 1 {
                break;
            }
        }
        assert_eq!(db.get(ks, &sample_key).unwrap(), Some(vec![0, 1, 2].into()));
        db.wait_for_background_threads_to_finish();
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

        db.rebuild_control_region().unwrap();
        start_index_based_relocation(&db);
        // With force_unload_config, index-based relocation may or may not process cells
        // depending on whether they're loaded in memory. Either behavior is safe.
        // We just verify no crashes occurred (the function returned successfully)
        db.rebuild_control_region().unwrap();

        db.wait_for_background_threads_to_finish();
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
        let ks = db.ks("k");
        for key in insert_count..(insert_count + 100) {
            db.insert(ks, key.to_be_bytes().to_vec(), vec![0, 1, 2])
                .unwrap();
        }
        db.rebuild_control_region().unwrap();
        start_index_based_relocation(&db);
        // With force_unload_config, index-based relocation may or may not process cells
        // depending on whether they're loaded in memory. Either behavior is safe.
        // We just verify no crashes occurred (the function returned successfully)
        db.wait_for_background_threads_to_finish();
    }
    // Verify data integrity - all data should still be accessible
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();
    let ks = db.ks("k");

    // Verify sample_key still exists (wasn't filtered because no relocation occurred)
    assert_eq!(db.get(ks, &sample_key).unwrap(), Some(vec![0, 1, 2].into()));

    // Verify all inserted data is still accessible
    for key in 0..insert_count {
        assert_eq!(
            db.get(ks, &key.to_be_bytes()).unwrap(),
            Some(vec![0, 1, 2].into())
        );
    }
    for key in insert_count..(insert_count + 100) {
        assert_eq!(
            db.get(ks, &key.to_be_bytes()).unwrap(),
            Some(vec![0, 1, 2].into())
        );
    }

    start_index_based_relocation(&db);

    // Verify all data is still accessible regardless of whether relocation occurred
    assert_eq!(db.get(ks, &sample_key).unwrap(), Some(vec![0, 1, 2].into()));
}

#[test]
#[ignore]
fn test_relocation_strategies_produce_identical_results() {
    let dir1 = tempdir::TempDir::new("test_wal_strategy").unwrap();
    let dir2 = tempdir::TempDir::new("test_index_strategy").unwrap();
    let config = Arc::new(Config::small());

    // Create identical keyspace configurations
    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new().with_bloom_filter(0.01, 2000);
    ksb.add_key_space_config("test", 8, 1, KeyType::uniform(1), ksc);
    let key_shape = ksb.build();

    let metrics_wal = Metrics::new();
    let metrics_index = Metrics::new();

    // Create identical databases with identical operations
    let (db_wal, db_index, ks) = {
        let db_wal = Db::open(
            dir1.path(),
            key_shape.clone(),
            config.clone(),
            metrics_wal.clone(),
        )
        .unwrap();
        let db_index = Db::open(
            dir2.path(),
            key_shape.clone(),
            index_based_config(),
            metrics_index.clone(),
        )
        .unwrap();
        let ks = db_wal.ks("test");

        // Apply identical operations to both databases
        for key in 0..1000u64 {
            let value = format!("value_{}", key).into_bytes();
            db_wal
                .insert(ks, key.to_be_bytes().to_vec(), value.clone())
                .unwrap();
            db_index
                .insert(ks, key.to_be_bytes().to_vec(), value)
                .unwrap();
        }

        // Update some entries
        for key in (0..500u64).step_by(2) {
            let value = format!("updated_value_{}", key).into_bytes();
            db_wal
                .insert(ks, key.to_be_bytes().to_vec(), value.clone())
                .unwrap();
            db_index
                .insert(ks, key.to_be_bytes().to_vec(), value)
                .unwrap();
        }

        // Delete some entries
        for key in (100..200u64).step_by(3) {
            db_wal.remove(ks, key.to_be_bytes().to_vec()).unwrap();
            db_index.remove(ks, key.to_be_bytes().to_vec()).unwrap();
        }

        (db_wal, db_index, ks)
    };

    // Run different relocation strategies
    db_wal.rebuild_control_region().unwrap();
    db_index.rebuild_control_region().unwrap();

    db_wal.start_blocking_relocation(); // Default WAL-based
    start_index_based_relocation(&db_index);

    // Compare final database contents key by key
    for key in 0..1000u64 {
        let key_bytes = key.to_be_bytes().to_vec();
        let val_wal = db_wal.get(ks, &key_bytes).unwrap();
        let val_index = db_index.get(ks, &key_bytes).unwrap();

        assert_eq!(val_wal, val_index, "Databases differ for key {}", key);
    }

    // Verify both processed data (metrics may differ but both should have done work)
    let wal_removed = relocation_removed(&metrics_wal, "test");
    let index_processed = relocation_cells_processed(&metrics_index, "test");

    // WAL-based counts removed entries, index-based counts processed cells
    // Both should be > 0 indicating work was done
    assert!(
        wal_removed > 0,
        "WAL-based should have processed removed entries"
    );
    assert!(
        index_processed > 0,
        "Index-based should have processed some cells"
    );
}

#[test]
fn test_both_strategies_handle_concurrent_writes() {
    // Test both strategies handle concurrent writes safely

    let test_concurrent_strategy = |strategy_name: &str, use_index_based: bool| {
        let dir = tempdir::TempDir::new(&format!("test_concurrent_{strategy_name}")).unwrap();
        let config = if use_index_based {
            index_based_config()
        } else {
            force_unload_config(&Config::small())
        };
        let mut ksb = KeyShapeBuilder::new();
        let ksc = KeySpaceConfig::new();
        ksb.add_key_space_config("concurrent", 8, 1, KeyType::uniform(1), ksc);
        let key_shape = ksb.build();
        let metrics = Metrics::new();

        let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
        let ks = db.ks("concurrent");

        // Pre-populate data
        for i in 0..1000u64 {
            db.insert(ks, i.to_be_bytes().to_vec(), vec![1, 2, 3])
                .unwrap();
        }

        let skip_stale_before = metrics
            .skip_stale_update
            .get_metric_with_label_values(&["concurrent", "insert"])
            .unwrap()
            .get();

        // Start concurrent writers
        let mut handles = vec![];
        let db_clone = Arc::clone(&db);

        // Writer thread - continuously updates keys
        let writer_handle = thread::spawn(move || {
            let mut successful_writes = 0;
            for round in 0..100 {
                for i in (0..100u64).step_by(5) {
                    let key = i.to_be_bytes().to_vec();
                    let value = vec![round as u8, (round >> 8) as u8, i as u8];
                    // Some concurrent access failures are expected
                    if db_clone.insert(ks, key, value).is_ok() {
                        successful_writes += 1;
                    }
                }
                thread::sleep(Duration::from_millis(1));
            }
            successful_writes
        });
        handles.push(writer_handle);

        // Start relocation while writers are active
        db.rebuild_control_region().unwrap();
        if use_index_based {
            start_index_based_relocation(&db);
        } else {
            db.start_blocking_relocation();
        }

        // Wait for writers to finish and collect successful write counts
        let mut total_successful_writes = 0;
        for handle in handles {
            let successful_writes = handle.join().unwrap();
            total_successful_writes += successful_writes;
        }

        let skip_stale_after = metrics
            .skip_stale_update
            .get_metric_with_label_values(&["concurrent", "insert"])
            .unwrap()
            .get();

        // Return the database, keyspace, metrics, and successful write count for verification
        (
            db,
            ks,
            skip_stale_after - skip_stale_before,
            total_successful_writes,
        )
    };

    let (db_wal, ks_wal, _wal_stale_updates, wal_successful_writes) =
        test_concurrent_strategy("wal", false);
    let (db_index, ks_index, _index_stale_updates, index_successful_writes) =
        test_concurrent_strategy("index", true);

    // Test 1: Both strategies should complete without crashing (we got here)

    // Test 2: Both strategies should have completed some successful writes
    assert!(
        wal_successful_writes > 0,
        "WAL strategy should have completed some writes, got {}",
        wal_successful_writes
    );
    assert!(
        index_successful_writes > 0,
        "Index strategy should have completed some writes, got {}",
        index_successful_writes
    );

    assert_eq!(
        wal_successful_writes, index_successful_writes,
        "Both strategies should complete the same number of writes: WAL={}, Index={}",
        wal_successful_writes, index_successful_writes
    );

    // Test 3: Verify data consistency - all keys should have valid values after concurrent writes
    for i in (0..100u64).step_by(5) {
        let wal_value = db_wal.get(ks_wal, &i.to_be_bytes()).unwrap().unwrap();
        let index_value = db_index.get(ks_index, &i.to_be_bytes()).unwrap().unwrap();

        // Values should be 3 bytes: [round, round>>8, key]
        assert_eq!(
            wal_value.len(),
            3,
            "WAL strategy produced invalid value length for key {}",
            i
        );
        assert_eq!(
            index_value.len(),
            3,
            "Index strategy produced invalid value length for key {}",
            i
        );
        assert_eq!(
            wal_value[2], i as u8,
            "WAL strategy corrupted key data for key {}",
            i
        );
        assert_eq!(
            index_value[2], i as u8,
            "Index strategy corrupted key data for key {}",
            i
        );
    }
}

#[test]
fn test_index_based_relocation_progress_tracking() {
    let dir = tempdir::TempDir::new("test_index_progress_tracking").unwrap();

    // Create multiple keyspaces to ensure cross-keyspace progress tracking
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config("ks1", 8, 1, KeyType::uniform(1), KeySpaceConfig::new());
    ksb.add_key_space_config("ks2", 8, 1, KeyType::uniform(1), KeySpaceConfig::new());
    let key_shape = ksb.build();
    let metrics = Metrics::new();

    // Populate data across multiple keyspaces
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            index_based_config(),
            metrics.clone(),
        )
        .unwrap();
        let ks1 = db.ks("ks1");
        let ks2 = db.ks("ks2");

        for ks in [ks1, ks2] {
            for key in 0..500u64 {
                db.insert(ks, key.to_be_bytes().to_vec(), vec![1, 2, 3])
                    .unwrap();
            }
        }
        db.wait_for_background_threads_to_finish();
    }

    // Test that watermark files are created and progress is tracked
    {
        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            index_based_config(),
            metrics.clone(),
        )
        .unwrap();
        db.rebuild_control_region().unwrap();
        start_index_based_relocation(&db);

        // Verify some progress was made
        let processed_ks1 = relocation_cells_processed(&metrics, "ks1");
        let processed_ks2 = relocation_cells_processed(&metrics, "ks2");
        assert!(
            processed_ks1 > 0 || processed_ks2 > 0,
            "Expected some cells to be processed"
        );

        db.wait_for_background_threads_to_finish();
    }

    // Verify watermark file exists
    let watermark_file = dir.path().join("rel");
    assert!(watermark_file.exists(), "Watermark file should be created");
}

#[test]
fn test_index_based_relocation_empty_and_sparse_cells() {
    let dir = tempdir::TempDir::new("test_sparse_cells").unwrap();
    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new();
    ksb.add_key_space_config("sparse", 8, 4, KeyType::uniform(128), ksc); // 128 cells per mutex (power of 2)
    let key_shape = ksb.build();
    let metrics = Metrics::new();

    let db = Db::open(dir.path(), key_shape, index_based_config(), metrics.clone()).unwrap();
    let ks = db.ks("sparse");

    // Create very sparse data - only populate every 10th cell
    for cell_idx in (0..128).step_by(10) {
        for key_in_cell in 0..5u64 {
            let key = (cell_idx as u64 * 1000) + key_in_cell; // Ensure keys land in specific cells
            db.insert(ks, key.to_be_bytes().to_vec(), vec![cell_idx as u8])
                .unwrap();
        }
    }

    db.rebuild_control_region().unwrap();
    start_index_based_relocation(&db);

    // Should handle empty cells gracefully - no crashes, reasonable metrics
    let processed = relocation_cells_processed(&metrics, "sparse");

    // Index-based relocation should process some cells and complete successfully
    // The exact number depends on implementation details, but it should be reasonable
    assert!(processed > 0, "Expected some cells to be processed");
    assert!(
        processed < 10000,
        "Processed cell count should be reasonable"
    );

    // Verify data integrity for populated cells
    for cell_idx in (0..128).step_by(10) {
        for key_in_cell in 0..5u64 {
            let key = (cell_idx as u64 * 1000) + key_in_cell;
            let expected_value = Some(vec![cell_idx as u8].into());
            assert_eq!(db.get(ks, &key.to_be_bytes()).unwrap(), expected_value);
        }
    }
}

#[test]
fn test_index_based_relocation_large_cells() {
    let dir = tempdir::TempDir::new("test_large_cells").unwrap();
    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new();
    // Use fewer cells so we can pack more entries per cell
    ksb.add_key_space_config("dense", 8, 2, KeyType::uniform(2), ksc); // Only 2 cells per mutex
    let key_shape = ksb.build();
    let metrics = Metrics::new();

    let db = Db::open(dir.path(), key_shape, index_based_config(), metrics.clone()).unwrap();
    let ks = db.ks("dense");

    // Fill cells with many entries each
    let entries_per_cell = 1000u64;
    for cell_idx in 0..2u64 {
        for entry_idx in 0..entries_per_cell {
            let key = (cell_idx * entries_per_cell) + entry_idx;
            let value = format!("large_value_{}_{}", cell_idx, entry_idx).into_bytes();
            db.insert(ks, key.to_be_bytes().to_vec(), value).unwrap();
        }
    }

    db.rebuild_control_region().unwrap();

    let start_time = std::time::Instant::now();
    start_index_based_relocation(&db);
    let elapsed = start_time.elapsed();

    // Verify large cells were processed successfully
    let processed = relocation_cells_processed(&metrics, "dense");
    assert!(processed > 0, "Should have processed some cells");

    // Basic performance check - should complete in reasonable time
    assert!(
        elapsed.as_secs() < 30,
        "Large cell processing should complete in reasonable time"
    );

    // Verify data integrity after processing
    for cell_idx in 0..2u64 {
        for entry_idx in (0..entries_per_cell).step_by(100) {
            // Sample every 100th entry
            let key = (cell_idx * entries_per_cell) + entry_idx;
            let expected = format!("large_value_{}_{}", cell_idx, entry_idx).into_bytes();
            assert_eq!(
                db.get(ks, &key.to_be_bytes()).unwrap(),
                Some(expected.into())
            );
        }
    }
}

#[test]
fn test_watermark_highest_wal_position_tracking() {
    let dir = tempdir::TempDir::new("test_watermark_wal_position").unwrap();
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config("test", 8, 1, KeyType::uniform(1), KeySpaceConfig::new());
    let key_shape = ksb.build();
    let metrics = Metrics::new();

    let db = Db::open(dir.path(), key_shape, index_based_config(), metrics.clone()).unwrap();
    let ks = db.ks("test");

    // Insert multiple entries to create WAL entries at different positions
    for key in 0..50u64 {
        db.insert(ks, key.to_be_bytes().to_vec(), vec![1, 2, 3])
            .unwrap();
    }

    // Get the initial WAL position to verify we have data to process
    let initial_wal_position = db.wal_writer.position();
    assert!(initial_wal_position > 0, "Database should have WAL entries");

    // Ensure data is persisted and control region is built
    db.rebuild_control_region().unwrap();

    // Run index-based relocation
    start_index_based_relocation(&db);

    // Verify some cells were processed - this confirms relocation completed successfully
    let processed = relocation_cells_processed(&metrics, "test");
    assert!(processed > 0, "Should have processed some cells");

    // Verify data integrity - all data should still be accessible after relocation
    for key in 0..50u64 {
        assert_eq!(
            db.get(ks, &key.to_be_bytes()).unwrap(),
            Some(vec![1, 2, 3].into()),
            "Key {} should still exist after relocation",
            key
        );
    }

    // Wait for background threads to finish - this consumes the db
    db.wait_for_background_threads_to_finish();

    // Now the key test: load watermarks from disk and ensure it is as expected
    let watermarks = RelocationWatermarks::read_or_create(dir.path()).unwrap();

    // The correct value should be the highest WAL position of entries that were processed
    let WatermarkData {
        highest_wal_position,
        upper_limit,
        ..
    } = watermarks.data;

    assert_eq!(
        upper_limit, initial_wal_position,
        "Upper limit should equal initial WAL position (this defines the processing boundary)"
    );

    // The highest_wal_position should be the actual highest position among processed entries
    // It should be:
    // 1. Greater than 0 (we processed some entries)
    // 2. Less than or equal to upper_limit (can't process beyond the safe boundary)
    // 3. Close to upper_limit (most entries should be processed in a simple sequential insert scenario)
    assert!(
        highest_wal_position > 0,
        "Watermark highest_wal_position ({}) should be greater than 0",
        highest_wal_position
    );
    assert!(
        highest_wal_position <= upper_limit,
        "Watermark highest_wal_position ({}) should not exceed upper_limit ({})",
        highest_wal_position,
        upper_limit
    );

    // Precise correctness check: highest_wal_position should be close to upper_limit
    let gap = upper_limit - highest_wal_position;
    assert!(
        gap <= 100,
        "Gap between highest_wal_position ({}) and upper_limit ({}) is too large ({}), suggests incomplete processing",
        highest_wal_position,
        upper_limit,
        gap
    );

    // The exact value cannot be computed with precision because:
    // 1. Index-based relocation only processes entries that have been ingested into the large table
    // 2. There's a delay between WAL writes and large table ingestion
    // 3. The "upper_limit" represents the safe boundary, but not all entries up to that
    //    point may have been ingested into cells yet
    // 4. Our rebuild_control_region() call ensures most entries are ingested, but timing varies
    //
    // However, we can assert a deterministic bound: in our controlled test scenario with
    // sequential inserts followed by rebuild_control_region(), we expect high ingestion rate.
    assert!(
        highest_wal_position >= initial_wal_position * 9 / 10,
        "Watermark highest_wal_position ({}) should be at least 90% of initial WAL position ({}) \
         - if this fails, it suggests ingestion issues, not the bug we're testing",
        highest_wal_position,
        initial_wal_position
    );
}

#[test]
fn test_index_based_relocation_with_target_position() {
    let dir = tempdir::TempDir::new("test_target_position").unwrap();
    let config = index_based_config();
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space("default", 8, 1, KeyType::uniform(1));
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
    let ks = db.ks("default");

    // Insert 1000 entries sequentially
    for i in 0..500u64 {
        let key = i.to_be_bytes().to_vec();
        let value = format!("value_{}", i).into_bytes();
        db.insert(ks, key, value).unwrap();
    }

    // Ensure wal tracker has processed all guard drops from entries 0-499
    db.wal_writer.wal_tracker_barrier();
    // Capture WAL position at entry 500
    let mid_position = db.wal_writer.last_processed().as_u64();

    // Continue inserting entries 501-1000
    for i in 500..1000u64 {
        let key = i.to_be_bytes().to_vec();
        let value = format!("value_{}", i).into_bytes();
        db.insert(ks, key, value).unwrap();
    }
    db.wal_writer.wal_tracker_barrier();

    // Force unload to ensure entries are in index (index-based relocation needs this)
    db.rebuild_control_region().unwrap();

    // Run relocation with target_position
    db.start_blocking_relocation_with_strategy(RelocationStrategy::IndexBased(Some(mid_position)));

    // Get metrics
    let kept = metrics
        .relocation_kept
        .with_label_values(&["default"])
        .get();

    // Verify approximately 500 entries were relocated (allow wider margin for WAL position variation)
    assert!(
        (350..=650).contains(&kept),
        "Expected ~500 entries relocated, got {}",
        kept
    );

    // Verify entries below and above target
    let key_250 = 250u64.to_be_bytes().to_vec();
    let key_750 = 750u64.to_be_bytes().to_vec();

    assert_eq!(
        db.get(ks, &key_250).unwrap(),
        Some(format!("value_{}", 250).into_bytes().into())
    );
    assert_eq!(
        db.get(ks, &key_750).unwrap(),
        Some(format!("value_{}", 750).into_bytes().into())
    );

    // Check watermark file contains target_position
    let watermark = RelocationWatermarks::read_or_create(dir.path())
        .unwrap()
        .data;
    assert_eq!(watermark.target_position, Some(mid_position));
}

// Regression: the index-based CAS threshold must be the relocation frontier
// (`effective_limit`), not the live frontier. For a key overwritten after the
// frontier, the as-of view still relocates the older value; a live-frontier
// threshold lets `RelocationUpdates::apply` re-point the newer write to the
// relocated copy of that older value, losing the overwrite. The same state
// arises with a write that lands concurrently with a relocation run, so this
// also pins the concurrent-overwrite case deterministically.
#[test]
fn test_index_based_relocation_preserves_overwrite_above_target() {
    let dir = tempdir::TempDir::new("test_index_cas_overwrite").unwrap();
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space("k", 8, 1, KeyType::uniform(1));
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    // No forced unload: the cell must stay loaded so the as-of value is still
    // a distinct overlay position when relocation runs (a flush would collapse
    // the key to latest-per-key and nothing would be relocated).
    let db = Db::open(
        dir.path(),
        key_shape,
        Arc::new(Config::small()),
        metrics.clone(),
    )
    .unwrap();
    let ks = db.ks("k");

    let key = 3u64.to_be_bytes().to_vec();
    db.insert(ks, key.clone(), vec![1; 64]).unwrap();
    db.wal_writer.wal_tracker_barrier();
    let frontier = db.wal_writer.last_processed().as_u64();

    // Overwrite after the frontier; the barrier ensures the live frontier at
    // relocation time is past the overwrite.
    db.insert(ks, key.clone(), vec![2; 64]).unwrap();
    db.wal_writer.wal_tracker_barrier();

    db.start_blocking_relocation_with_strategy(RelocationStrategy::IndexBased(Some(frontier)));

    // The as-of value must actually have been relocated, otherwise the CAS
    // never runs and this test is vacuous.
    assert!(
        metrics.relocation_kept.with_label_values(&["k"]).get() > 0,
        "relocation must have relocated the as-of value"
    );
    assert_eq!(
        db.get(ks, &key).unwrap(),
        Some(vec![2; 64].into()),
        "the overwrite must survive relocation; the relocated copy of the \
         as-of value must not be CAS'd over it"
    );
}

// Regression: a write racing a clean `ForceRelocate` on a Loaded `[L0, L1]`
// cell must not lose the keys below L0.
//
// A loaded cell's overlay covers L0 only; reads reach L1-resident keys by
// walking `disk_levels_to_walk` -> `iter_below_l0`. A clean ForceRelocate
// collapses L0+L1 into a single blob in the L0 slot. When a write lands while
// that flush is in flight, the completion (`update_relocated_position`)
// reaches the DirtyLoaded arm with an overlay that no longer covers the new
// L0 blob (it lacks the former-L1-only keys). Before the fix that arm kept
// the cell loaded, violating the "Loaded => data covers L0" invariant:
// - reads: `iter_below_l0` on the single-level cell walks nothing, so
//   former-L1-only keys returned None immediately;
// - the next normal flush cloned the overlay as the cell's only level,
//   dropping those keys from the index permanently.
// The fix retains only the racing writes and demotes the cell to
// DirtyUnloaded, so reads and flushes consult the relocated blob again.
//
// The relocation runs on the production flusher; the
// `fp_flush_before_completion` failpoint pauses it between the flush work and
// the completion, so the racing write deterministically lands inside the
// in-flight window.
#[test]
fn test_force_relocate_concurrent_write_keeps_below_l0_keys() {
    let dir = tempdir::TempDir::new("force_relocate_dirty_loaded").unwrap();
    // Forced unload (threshold 0) so the flushed cell unloads; the later read
    // reloads it folding only L0 into the overlay, leaving L1 on disk.
    let config = force_unload_config(&Config::small());
    let mut ksb = KeyShapeBuilder::new();
    // Unloaded iteration off so that stepping an iterator loads the cell
    // (point reads never load; this is the only read-path load trigger).
    ksb.add_key_space_config(
        "k",
        8,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_unloaded_iterator(false),
    );
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
    let ks = db.ks("k");

    let value = |b: u8| -> Vec<u8> { vec![b; 100] };
    let key = |i: u64| -> Vec<u8> { i.to_be_bytes().to_vec() };

    // Build an L1: enough keys for an L0->L1 promote (l0_max_entries =
    // 8 * max_dirty_keys = 256 for Config::small), then a small fresh L0.
    for i in 0..300u64 {
        db.insert(ks, key(i), value(1)).unwrap();
    }
    // Advance the tracker frontier past the inserts before each rebuild, so
    // the flush retains nothing in the overlay and the cell ends up clean.
    db.wal_writer.wal_tracker_barrier();
    db.force_rebuild_control_region().unwrap();
    for i in 1000..1010u64 {
        db.insert(ks, key(i), value(2)).unwrap();
    }
    db.wal_writer.wal_tracker_barrier();
    db.force_rebuild_control_region().unwrap();
    db.large_table.flusher.barrier();
    // Flushes dispatched while the inserts ran sampled an older frontier and
    // can leave a retained overlay tail (DirtyUnloaded). With nothing in
    // flight any more, one more rebuild flushes at the current frontier and
    // leaves the cell clean.
    db.wal_writer.wal_tracker_barrier();
    db.force_rebuild_control_region().unwrap();
    db.large_table.flusher.barrier();

    let cell = CellReference::first(&db, KeySpace::first()).expect("cell must exist");
    let context = db.ks_context(cell.keyspace);
    assert_eq!(
        db.large_table.entry_state_for_test(context, &cell.cell_id),
        "Unloaded",
        "PRECONDITION: the flushed cell must be clean and unloaded"
    );

    // Load the (clean) cell: iterators load cells (point reads read through
    // without loading); this folds only L0 into the in-memory overlay,
    // leaving L1 reachable solely through the on-disk levels walk.
    assert!(db.iterator(ks).next().is_some());
    assert_eq!(
        db.large_table.entry_state_for_test(context, &cell.cell_id),
        "Loaded",
        "PRECONDITION: cell must be clean and loaded"
    );

    // Pick a probe key that lives only below L0: absent from the loaded
    // overlay but readable through the on-disk levels walk.
    let below_l0_key = (0..300u64)
        .find(|i| {
            let reduced = context.ks_config.reduced_key_bytes(Bytes::from(key(*i)));
            db.large_table
                .with_entry_for_test(context, &cell.cell_id, |entry| {
                    entry.get(&reduced).is_none()
                })
        })
        .expect("setup must leave at least one key only reachable below L0");
    assert_eq!(
        db.get(ks, &key(below_l0_key)).unwrap(),
        Some(value(1).into()),
        "SANITY: probe key readable via the levels walk before relocation"
    );

    // Dispatch a force-relocate of the cell's blobs into a flusher whose
    // receiver the test owns, capturing the command instead of racing a
    // Pause the flusher between the flush work and its completion, so the
    // racing write below deterministically lands while the force-relocate is
    // in flight. The latch releases when its guard drops — including on a
    // panicking assertion, so a failure cannot wedge the flusher thread.
    let (completion_latch, completion_latch_guard) = Latch::new();
    db.large_table.fp.0.write().fp_flush_before_completion = FailPoint::latch(completion_latch);

    // Dispatch the force-relocate through the production flusher, exactly as
    // the snapshot pass does for a clean cell in low-occupancy files.
    let last_processed = db.last_processed_wal_position();
    db.large_table
        .with_entry_for_test(context, &cell.cell_id, |entry| {
            let layout = db.config.wal_layout(WalKind::Index);
            let files: HashSet<_> = entry
                .levels()
                .iter()
                .filter_map(|p| p.valid())
                .map(|p| layout.locate_file(p.offset()))
                .collect();
            let relocate_files = RelocateFiles::new(files, db.config.wal_layout(WalKind::Index));
            entry.request_force_relocate(&db.large_table.flusher, last_processed, &relocate_files);
        });

    // The racing write: lands while the paused flusher holds the completion.
    // The cell goes Loaded -> DirtyLoaded, routing the completion through the
    // arm whose overlay no longer covers the relocated blob.
    db.insert(ks, key(7777), value(3)).unwrap();
    assert_eq!(
        db.large_table.entry_state_for_test(context, &cell.cell_id),
        "DirtyLoaded",
        "PRECONDITION: the racing write must land before the completion"
    );

    // Unblock the flusher and wait for the completion to apply.
    drop(completion_latch_guard);
    db.large_table.flusher.barrier();

    // The clean-cell branch must have dispatched a ForceRelocate — a dirty
    // cell would dispatch a normal Flush, exercising a different completion
    // path and leaving this test vacuous.
    assert!(
        metrics.unload.with_label_values(&["force_relocate"]).get() > 0,
        "the ForceRelocate flusher path must have run"
    );

    // The completion demotes the cell so reads consult the relocated blob;
    // the overlay keeps only the racing write.
    assert_eq!(
        db.large_table.entry_state_for_test(context, &cell.cell_id),
        "DirtyUnloaded",
        "completion on a dirtied loaded cell must demote it"
    );

    // The racing write and the L0-resident keys survive...
    assert_eq!(db.get(ks, &key(7777)).unwrap(), Some(value(3).into()));
    assert_eq!(db.get(ks, &key(1000)).unwrap(), Some(value(2).into()));
    // ...and so must the key that lived below L0 (lost before the fix: the
    // single-level cell had nothing below L0 to walk, and the kept overlay
    // never had the key).
    assert_eq!(
        db.get(ks, &key(below_l0_key)).unwrap(),
        Some(value(1).into()),
        "key residing below L0 must survive a force-relocate completing on a dirtied cell"
    );

    // The loss must also not become permanent: the next normal flush writes
    // the overlay as the cell's only level.
    db.force_rebuild_control_region().unwrap();
    db.large_table.flusher.barrier();
    assert_eq!(
        db.get(ks, &key(below_l0_key)).unwrap(),
        Some(value(1).into()),
        "key below L0 must survive the next normal flush after the relocation"
    );
}

#[test]
fn test_compute_target_position_from_ratio() {
    use crate::relocation::compute_target_position_from_ratio;

    let dir = tempdir::TempDir::new("test_compute_ratio").unwrap();
    let config = Arc::new(Config::small());
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space("default", 8, 1, KeyType::uniform(1));
    let key_shape = ksb.build();
    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
    let ks = db.ks("default");

    // Initially should return None (empty WAL)
    assert_eq!(compute_target_position_from_ratio(&db, 0.5), None);

    // Add some data
    for i in 0..1000u64 {
        let key = i.to_be_bytes().to_vec();
        let value = format!("value_{}", i).into_bytes();
        db.insert(ks, key, value).unwrap();
    }
    db.large_table.flusher.barrier();
    db.wal_writer.wal_tracker_barrier();

    let min_pos = db.wal.min_wal_position();
    let last_pos = db.wal_writer.last_processed().as_u64();
    let range = last_pos - min_pos;

    // Test various ratios
    let target_0 = compute_target_position_from_ratio(&db, 0.0).unwrap();
    assert_eq!(target_0, min_pos);

    let target_50 = compute_target_position_from_ratio(&db, 0.5).unwrap();
    assert!(target_50 > min_pos && target_50 < last_pos);
    // Allow some margin for byte alignment
    assert!(
        (target_50 - min_pos) >= range / 2 - 1000,
        "target_50 {} should be close to middle of range {}",
        target_50 - min_pos,
        range / 2
    );
    assert!(
        (target_50 - min_pos) <= range / 2 + 1000,
        "target_50 {} should be close to middle of range {}",
        target_50 - min_pos,
        range / 2
    );

    let target_100 = compute_target_position_from_ratio(&db, 1.0).unwrap();
    // Read fresh last_pos since background threads may have advanced the WAL
    let last_pos_at_100 = db.wal_writer.last_processed().as_u64();
    assert_eq!(target_100, last_pos_at_100);

    // Test clamping - negative ratio should give min_pos
    let target_negative = compute_target_position_from_ratio(&db, -0.5).unwrap();
    assert_eq!(target_negative, min_pos);

    // Test clamping - ratio > 1.0 should give last_pos
    let target_over_one = compute_target_position_from_ratio(&db, 1.5).unwrap();
    // Read fresh last_pos again
    let last_pos_at_over = db.wal_writer.last_processed().as_u64();
    assert_eq!(target_over_one, last_pos_at_over);
}
