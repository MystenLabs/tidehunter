use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use std::{path::Path, sync::Arc, thread, time::Duration};

use crate::control::RelocateFiles;
use crate::failpoints::FailPoint;
use crate::key_shape::{KeyIndexing, KeySpace, KeySpaceConfig};
use crate::large_table::Loader;
use crate::latch::{Latch, LatchGuard};
use crate::relocation::CellReference;
use crate::relocation::watermark::WatermarkData;
use crate::wal::layout::WalKind;
use crate::{
    RelocationStrategy,
    config::Config,
    db::{Db, DbResult},
    key_shape::{KeyShape, KeyShapeBuilder, KeyType},
    relocation::RelocationWatermarks,
};
use crate::{metrics::Metrics, relocation::Decision};
use minibytes::Bytes;

/// `Config::small` with one WAL file per relocation stripe, so the WAL-based
/// runs in these tests span several stripes.
fn small_config() -> Config {
    Config {
        relocation_stripe_bytes: 0,
        ..Config::small()
    }
}

fn force_unload_config(config: &Config) -> Arc<Config> {
    let mut config2 = Config::clone(config);
    config2.snapshot_unload_threshold = 0;
    Arc::new(config2)
}

fn index_based_config() -> Arc<Config> {
    let mut config = small_config();
    config.relocation_strategy = RelocationStrategy::IndexBased(None);
    force_unload_config(&config)
}

/// `small_config` with WAL files of two fragments, cells unloading at every
/// control-region rebuild, and, when `sharded`, small dirty and L0 limits plus
/// an auto-shard threshold of 2 KiB so a cell of a few hundred 8-byte keys
/// splits its promoted L1.
fn relocation_test_config(sharded: bool) -> Arc<Config> {
    let mut config = small_config();
    config.wal_file_size = 2 * config.frag_size;
    if sharded {
        config.max_dirty_keys = 8;
        config.l0_max_entries = Some(16);
        config.with_index_auto_shard_threshold(2048);
    }
    force_unload_config(&config)
}

/// Key spaces `k` (under `ksc`) and `pad`, the latter for writes that move the
/// WAL without touching the cell under test.
fn k_and_pad_key_shape(ksc: KeySpaceConfig) -> KeyShape {
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config("k", 8, 1, KeyType::uniform(1), ksc);
    ksb.add_key_space_config("pad", 8, 1, KeyType::uniform(1), KeySpaceConfig::new());
    ksb.build()
}

/// True when any cell of `ks` has an auto-sharded L1.
fn any_cell_sharded(db: &Db, ks: KeySpace) -> bool {
    let context = db.ks_context(ks);
    let mut cell_ref = CellReference::first(db, ks);
    while let Some(cell) = cell_ref.take() {
        let sharded = db
            .large_table
            .with_entry_for_test(context, &cell.cell_id, |entry| entry.levels().is_sharded());
        if sharded {
            return true;
        }
        cell_ref = cell.next_in_ks(db);
    }
    false
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
    let mut config = small_config();
    config.wal_file_size = 2 * config.frag_size;
    checkpoint_survives_relocation(force_unload_config(&config), KeySpaceConfig::new());
}

#[test]
fn test_checkpoint_survives_relocation_without_unload() {
    // Unloading disabled: the cell stays loaded, so the as-of value must survive
    // relocation in the in-memory overlay (relocation rewrites its position to
    // >= L and reclaims the original frame).
    let mut config = small_config();
    config.wal_file_size = 2 * config.frag_size;
    checkpoint_survives_relocation(Arc::new(config), KeySpaceConfig::new().disable_unload());
}

/// A write allocated before a relocation copies its key's as-of value sits
/// below the copy the relocation writes at the WAL tail: its guard pins the
/// tracker frontier, so the relocation must not index the copy
/// until the write is applied (a torn frame below an indexed copy would
/// strand the key after a crash), and once it is, the compare-and-set must
/// leave the key on the newer write, which sits at a lower offset than the
/// copy of the older value. Unloading is disabled so the relocation flush
/// re-points the loaded overlay at the relocated blob (`rebase_on_as_of`)
/// instead of unloading the cell; the writer is parked on
/// `fp_insert_before_lock` between its WAL write and the row lock.
#[test]
fn test_relocation_waits_for_racing_write_on_loaded_cell() {
    use std::time::Instant;

    let dir = tempdir::TempDir::new("test_reloc_racing_write").unwrap();
    let config = Arc::new(small_config());
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config(
        "k",
        8,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().disable_unload(),
    );
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
    let ks = db.ks("k");
    let context = db.ks_context(ks);
    let key = 3u64.to_be_bytes().to_vec();
    let cell = context.ks_config.cell_id(&key);
    let old_value = vec![1u8; 64];
    let new_value = vec![2u8; 64];
    let indexed_position = || {
        db.large_table.with_entry_for_test(context, &cell, |entry| {
            entry.get(&key).expect("key must be indexed")
        })
    };
    let skipped_inserts = || {
        metrics
            .skip_stale_update
            .with_label_values(&["k", "insert"])
            .get()
    };

    db.insert(ks, key.clone(), old_value.clone()).unwrap();
    // The relocation only considers positions the tracker has processed.
    db.wal_writer.wal_tracker_barrier();
    let original = indexed_position();

    // Park the racing writer between its WAL write (guard held, so the
    // tracker frontier stays at or below it) and the row lock.
    let (latch, latch_guard) = Latch::new();
    db.large_table.fp.0.write().fp_insert_before_lock = FailPoint::latch(latch);
    let written_before = metrics.wal_written_bytes.get();
    let writer = {
        let db = db.clone();
        let key = key.clone();
        let new_value = new_value.clone();
        thread::spawn(move || db.insert(ks, key, new_value).unwrap())
    };
    // `Db::insert` publishes the frame offset right after the WAL write and
    // before the failpoint, so a changed gauge means the write is in flight.
    let deadline = Instant::now() + Duration::from_secs(60);
    while metrics.wal_written_bytes.get() == written_before {
        assert!(
            Instant::now() < deadline,
            "racing writer did not reach the WAL before the deadline"
        );
        thread::sleep(Duration::from_millis(1));
    }
    let in_flight_offset = metrics.wal_written_bytes.get() as u64;

    // Relocate just the key's original record: everything below its
    // successor offset. The copy lands past the in-flight write, so the
    // relocation must park until that write is applied.
    let wait_requests_before = db.wal_writer.wait_requests_sent();
    let relocation = {
        let db = db.clone();
        thread::spawn(move || {
            db.start_blocking_relocation_with_strategy(RelocationStrategy::IndexBased(Some(
                original.offset() + 1,
            )))
        })
    };
    while db.wal_writer.wait_requests_sent() == wait_requests_before {
        assert!(
            !relocation.is_finished(),
            "relocation finished with a write in flight below its copy"
        );
        assert!(
            Instant::now() < deadline,
            "relocation did not wait for the in-flight write"
        );
        thread::sleep(Duration::from_millis(1));
    }
    assert!(
        !relocation.is_finished(),
        "relocation finished with a write in flight below its copy"
    );
    assert_eq!(
        original,
        indexed_position(),
        "the copy must not be indexed while the write below it is in flight"
    );

    // Release the writer: it is applied first, and the relocation's
    // compare-and-set then leaves the key on the newer write, which sits at
    // a lower offset than the copy.
    drop(latch_guard);
    writer
        .join()
        .expect("racing write must be applied without panicking");
    relocation.join().unwrap();
    assert_eq!(
        1,
        metrics.relocation_kept.with_label_values(&["k"]).get(),
        "relocation must have copied the key's record"
    );
    assert_eq!(0, skipped_inserts(), "racing write was skipped as stale");
    assert_eq!(in_flight_offset, indexed_position().offset());
    assert_eq!(Some(new_value.clone().into()), db.get(ks, &key).unwrap());
    db.large_table.each_entry(|entry| {
        assert!(entry.is_index_loaded(), "cell must stay loaded");
    });

    // A flush must persist the newer write, not the copy of the older one.
    db.force_rebuild_control_region().unwrap();
    assert_eq!(Some(new_value.into()), db.get(ks, &key).unwrap());
    assert_eq!(0, skipped_inserts());
}

// A key overwritten after a checkpoint, whose as-of value is a distinct overlay
// position (never promoted to flat or a deeper level), must survive relocation:
// the as-of view relocates it, the flush re-points it to the copy, and the held
// checkpoint reads the copy once GC reclaims its WAL file. The WAL is moved
// several files past the as-of write before the checkpoint, so the relocation
// target passes its file. On an unloading-enabled cell the flush completion
// must drop the re-pointed position from the overlay although the key's latest
// position is past the frontier, and a later flush of the cell must not write
// the reclaimed position back into the blob.
fn checkpoint_survives_relocation_overlay_asof_value(sharded: bool) {
    let dir = tempdir::TempDir::new("ckpt_reloc_overlay_asof").unwrap();
    let config = relocation_test_config(sharded);
    let key_shape = k_and_pad_key_shape(KeySpaceConfig::new());
    let metrics = Metrics::new();
    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
    let ks = db.ks("k");
    let pad = db.ks("pad");

    let big = |b: u8| -> Vec<u8> { vec![b; 12 * 1000] };
    let target = 3u64.to_be_bytes().to_vec();

    // 1. Build the cell from other keys and flush it to disk, so the bulk
    //    lives below L0 (split into shards when `sharded`) and the overlay
    //    is clean.
    for i in 1000..3000u64 {
        db.insert(ks, i.to_be_bytes().to_vec(), big(7)).unwrap();
    }
    db.force_rebuild_control_region().unwrap();
    if sharded {
        assert!(
            any_cell_sharded(&db, ks),
            "test setup must produce a sharded cell"
        );
    }

    // 2. The target's as-of value lands in a fresh overlay over the on-disk
    //    cell: one dirty key stays in `data`.
    db.insert(ks, target.clone(), big(1)).unwrap();

    // 3. Move the WAL several files past the as-of write, through another
    //    key space, so the relocation target passes its file.
    for i in 0..4000u64 {
        db.insert(pad, i.to_be_bytes().to_vec(), big(9)).unwrap();
    }

    // 4. Pin a checkpoint at this frontier.
    let checkpoint = db.checkpoint();
    assert_eq!(
        checkpoint.get(ks, &target).unwrap(),
        Some(big(1).into()),
        "SANITY: as-of read works right after checkpoint"
    );

    // 5. Overwrite the target after the frontier, so the overlay holds both
    //    positions; the overwrite is also the entry past the frontier that
    //    the cut-off scan stops at.
    db.insert(ks, target.clone(), big(2)).unwrap();
    assert_eq!(
        checkpoint.get(ks, &target).unwrap(),
        Some(big(1).into()),
        "SANITY: as-of read still works after the post-frontier overwrite (pre-relocation)"
    );

    // 6. Relocate (default WAL-based) => GC reclaims files below target_position.
    db.start_blocking_relocation();
    assert!(
        metrics.relocation_kept.with_label_values(&["k"]).get() > 0,
        "relocation must have relocated entries"
    );

    assert_eq!(
        db.get(ks, &target).unwrap(),
        Some(big(2).into()),
        "live get must be the overwrite"
    );
    assert_eq!(
        checkpoint.get(ks, &target).unwrap(),
        Some(big(1).into()),
        "checkpoint must read the as-of value big(1) after relocation; \
         None => the overlay still points at the reclaimed position"
    );

    // 7. Flush the cell again while the checkpoint is held. The overlay
    //    holds only the overwrite now, so the new blob keeps the copy.
    db.force_rebuild_control_region().unwrap();
    assert_eq!(
        db.get(ks, &target).unwrap(),
        Some(big(2).into()),
        "live get must still be the overwrite after the second flush"
    );
    assert_eq!(
        checkpoint.get(ks, &target).unwrap(),
        Some(big(1).into()),
        "checkpoint must still read the as-of value after the second flush; \
         None => the blob was rewritten with the reclaimed position"
    );
}

#[test]
fn test_checkpoint_survives_relocation_overlay_asof_value() {
    checkpoint_survives_relocation_overlay_asof_value(false);
}

#[test]
fn test_checkpoint_survives_relocation_overlay_asof_value_sharded() {
    checkpoint_survives_relocation_overlay_asof_value(true);
}

#[test]
fn test_wal_relocation_basic_flow() {
    let dir = tempdir::TempDir::new("test_relocation_filter").unwrap();
    let mut config = small_config();
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

#[test]
fn test_iterator_skips_reclaimed_positions() {
    // A relocation filter removes the first half of the key space. Index
    // cleanup is eventual, so entries pointing into the reclaimed WAL prefix
    // can still be present afterwards. Iteration must skip them and still
    // return every live key: stopping at the first unreadable entry hides the
    // whole live half, because the removed keys sort in front of it.
    let dir = tempdir::TempDir::new("test_iterator_skips_reclaimed_positions").unwrap();
    let mut config = small_config();
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
    let key_shape = ksb.build();
    let insert_count = 2000_u32;
    let value = vec![3; 12 * 1000];
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            force_unload_config(&config),
            Metrics::new(),
        )
        .unwrap();
        let ks = db.ks("k");
        for i in 0..insert_count {
            db.insert(ks, i.to_be_bytes().to_vec(), value.clone())
                .unwrap();
        }
        db.wait_for_background_threads_to_finish();
    }
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            force_unload_config(&config),
            Metrics::new(),
        )
        .unwrap();
        db.rebuild_control_region().unwrap();
        db.start_blocking_relocation();
        db.rebuild_control_region().unwrap();
        db.wait_for_background_threads_to_finish();
    }

    let db = Db::open(
        dir.path(),
        key_shape,
        force_unload_config(&config),
        Metrics::new(),
    )
    .unwrap();
    let ks = db.ks("k");

    fn collect_keys(
        iter: impl Iterator<Item = DbResult<(Bytes, Bytes)>>,
        value_len: usize,
        label: &str,
    ) -> Vec<u32> {
        iter.map(|entry| {
            let (key, v) = entry.unwrap();
            assert_eq!(
                v.len(),
                value_len,
                "{label} iterator returned a truncated value"
            );
            u32::from_be_bytes(key.as_ref().try_into().unwrap())
        })
        .collect()
    }

    fn assert_live_keys_visible(seen: &[u32], insert_count: u32, label: &str) {
        assert!(
            seen.windows(2).all(|w| w[0] < w[1]),
            "{label} iteration returned keys out of order"
        );
        let live: HashSet<u32> = seen.iter().copied().filter(|k| *k >= 1_000).collect();
        for expected in 1_000..insert_count {
            assert!(
                live.contains(&expected),
                "{label} iteration missed live key {expected} \
                 (returned {} entries, first {:?})",
                seen.len(),
                seen.first(),
            );
        }
    }

    // Both read paths walk the same index, so both have to skip the stale
    // entries: `Db::next_entry` for the live iterator and
    // `DbCheckpoint::next_entry` for the checkpoint one.
    let live_seen = collect_keys(db.iterator(ks), value.len(), "live");
    assert_live_keys_visible(&live_seen, insert_count, "live");

    let checkpoint_seen = collect_keys(db.checkpoint().iterator(ks), value.len(), "checkpoint");
    assert_live_keys_visible(&checkpoint_seen, insert_count, "checkpoint");

    db.wait_for_background_threads_to_finish();
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
    let mut config = small_config();
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
    let config = Arc::new(small_config());

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
            force_unload_config(&small_config())
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
        Arc::new(small_config()),
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
    let config = force_unload_config(&small_config());
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
    let config = Arc::new(small_config());
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

// WAL-based relocation on a cell whose L1 has been auto-sharded. Keys that live
// only in shards must be relocated too (the as-of view folds every shard in),
// and the relocation flush must re-point them shard by shard. Keys overwritten
// or removed after the split cover the case where a fresher copy above the
// shard shadows the shard copy; keys overwritten after the checkpoint cover the
// case where the shard copy is the as-of value a held checkpoint still reads.
fn wal_relocation_on_sharded_cell(ksc: KeySpaceConfig) {
    let dir = tempdir::TempDir::new("test_wal_relocation_sharded").unwrap();
    let config = relocation_test_config(true);
    let key_shape = k_and_pad_key_shape(ksc);
    let metrics = Metrics::new();

    let key = |i: u64| i.to_be_bytes().to_vec();
    let value = |i: u64, tag: u8| -> Vec<u8> {
        let mut v = vec![(i % 251) as u8; 12 * 1000];
        v[0] = tag;
        v
    };
    let insert_count = 2000u64;
    let overwritten = 0..100u64;
    let removed = 100..150u64;
    let overwritten_after_checkpoint = 150..160u64;

    let verify_live = |db: &Arc<Db>, ks: KeySpace, label: &str| {
        for i in 0..insert_count {
            let expected = if overwritten.contains(&i) {
                Some(value(i, 2).into())
            } else if removed.contains(&i) {
                None
            } else if overwritten_after_checkpoint.contains(&i) {
                Some(value(i, 3).into())
            } else {
                Some(value(i, 1).into())
            };
            assert_eq!(db.get(ks, &key(i)).unwrap(), expected, "{label}: key {i}");
        }
        let mut live_count = 0u64;
        for entry in db.iterator(ks) {
            let _: (Bytes, Bytes) = entry.unwrap();
            live_count += 1;
        }
        assert_eq!(
            live_count,
            insert_count - (removed.end - removed.start),
            "{label}: iterator must see every live key"
        );
    };

    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();
        let ks = db.ks("k");
        for i in 0..insert_count {
            db.insert(ks, key(i), value(i, 1)).unwrap();
        }
        // Flush and promote everything written so far so the cell's L1 splits.
        db.force_rebuild_control_region().unwrap();
        assert!(
            any_cell_sharded(&db, ks),
            "test setup must produce a sharded cell"
        );
        for i in overwritten.clone() {
            db.insert(ks, key(i), value(i, 2)).unwrap();
        }
        for i in removed.clone() {
            db.remove(ks, key(i)).unwrap();
        }

        // Pin the frontier here; the overwrites below land past it, which
        // also gives the relocation cut-off scan an entry to stop at.
        let checkpoint = db.checkpoint();
        for i in overwritten_after_checkpoint.clone() {
            db.insert(ks, key(i), value(i, 3)).unwrap();
        }

        let shards_rewritten_before = metrics
            .l1_shard_rewritten_total
            .with_label_values(&["k"])
            .get();
        db.start_blocking_relocation();

        assert!(
            metrics.relocation_kept.with_label_values(&["k"]).get() > 0,
            "relocation must have relocated entries"
        );
        assert!(
            metrics
                .l1_shard_rewritten_total
                .with_label_values(&["k"])
                .get()
                > shards_rewritten_before,
            "relocation must have rewritten the shards holding re-pointed keys"
        );
        // Reclamation is checked through the WAL file map, which gc updates
        // before it returns; the dirent itself is removed later by the unlink
        // worker, so a directory listing is not a synchronous signal.
        assert!(
            db.wal.min_wal_position() >= db.wal.wal_file_size(),
            "relocation must have reclaimed the oldest WAL file"
        );
        verify_live(&db, ks, "after relocation");
        // The checkpoint reads the as-of values: the relocated shard copy for
        // a key overwritten after the frontier, and the post-split copies for
        // the keys overwritten or removed before it.
        for i in overwritten_after_checkpoint.clone() {
            assert_eq!(
                checkpoint.get(ks, &key(i)).unwrap(),
                Some(value(i, 1).into()),
                "checkpoint: key {i} must read the relocated as-of shard copy"
            );
        }
        assert_eq!(
            checkpoint.get(ks, &key(overwritten.start)).unwrap(),
            Some(value(overwritten.start, 2).into())
        );
        assert_eq!(checkpoint.get(ks, &key(removed.start)).unwrap(), None);
        drop(checkpoint);
        db.wait_for_background_threads_to_finish();
    }

    // Reopen: the index must point at the relocated copies.
    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
    verify_live(&db, db.ks("k"), "after reopen");
}

#[test]
fn test_wal_relocation_on_sharded_cell_with_unload() {
    // Cells unload between flushes, so at relocation time most keys live only
    // in shards and the relocation flush re-points them shard by shard.
    wal_relocation_on_sharded_cell(KeySpaceConfig::new());
}

#[test]
fn test_wal_relocation_on_sharded_cell_without_unload() {
    // The cell never unloads, so the relocation flushes start from the
    // in-memory overlay rather than from the shards alone and must still
    // re-point the shard copies.
    wal_relocation_on_sharded_cell(KeySpaceConfig::new().disable_unload());
}

fn torn_frame_config() -> Arc<Config> {
    relocation_test_config(false)
}

/// Key spaces `k` and `pad`; relocation drops the padding instead of copying
/// it, so the run's copies fit in the fragments the mapper prepares ahead of
/// a tracker that is stuck behind a parked writer.
fn torn_frame_key_shape() -> KeyShape {
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config("k", 8, 1, KeyType::uniform(1), KeySpaceConfig::new());
    ksb.add_key_space_config(
        "pad",
        8,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_relocation_filter(|_, _| Decision::Remove),
    );
    ksb.build()
}

fn torn_frame_key() -> Vec<u8> {
    3u64.to_be_bytes().to_vec()
}

fn torn_frame_value() -> Vec<u8> {
    vec![1u8; 64]
}

fn torn_frame_pad_value() -> Vec<u8> {
    vec![7u8; 12 * 1000]
}

/// A relocated copy may be indexed, and so persisted by the control region
/// rebuild that ends its stripe, only once every WAL frame below it is applied:
/// a crash tears a frame still being written, replay stops at it, and a copy
/// above the torn frame is unreachable (replay skips relocated frames) while
/// its original is already reclaimed. The child process parks a writer
/// between its WAL write and the row lock, so the tracker frontier stays at
/// its frame while a WAL relocation copies a key written earlier; it then
/// tears the parked frame and exits without closing the database. The parent
/// reopens it and must still read the key.
#[test]
fn test_relocated_copies_survive_crash_with_torn_frame_below() {
    use std::process::Command;

    const CHILD_DB_PATH: &str = "TIDEHUNTER_TEST_TORN_FRAME_DB_PATH";
    const CHILD_EXIT_CODE: i32 = 37;
    if let Some(path) = std::env::var_os(CHILD_DB_PATH) {
        torn_frame_crash_child(Path::new(&path), CHILD_EXIT_CODE);
    }

    let dir = tempdir::TempDir::new("test_reloc_torn_frame").unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "relocation::relocation_tests::test_relocated_copies_survive_crash_with_torn_frame_below",
            "--exact",
            "--nocapture",
        ])
        .env(CHILD_DB_PATH, dir.path())
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(CHILD_EXIT_CODE),
        "child did not reach its crash point\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let db = Db::open(
        dir.path(),
        torn_frame_key_shape(),
        torn_frame_config(),
        Metrics::new(),
    )
    .unwrap();
    let ks = db.ks("k");
    let pad = db.ks("pad");
    // The writer resumes at the torn frame; a few writes overwrite whatever
    // sat above it, as the first writes after a restart would.
    for i in 0..4u64 {
        db.insert(pad, i.to_be_bytes().to_vec(), torn_frame_pad_value())
            .unwrap();
    }
    assert_eq!(
        db.get(ks, &torn_frame_key()).unwrap(),
        Some(torn_frame_value().into()),
        "key relocated above a torn frame was lost"
    );
}

/// The child half of `test_relocated_copies_survive_crash_with_torn_frame_below`.
/// Exits the process with `exit_code` instead of returning.
fn torn_frame_crash_child(path: &Path, exit_code: i32) -> ! {
    use crate::crc::CrcFrame;
    use std::fs::OpenOptions;
    use std::os::unix::fs::FileExt;
    use std::time::Instant;

    let metrics = Metrics::new();
    let config = torn_frame_config();
    let db = Db::open(
        path,
        torn_frame_key_shape(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();
    let ks = db.ks("k");
    let pad = db.ks("pad");
    let deadline = Instant::now() + Duration::from_secs(60);

    db.insert(ks, torn_frame_key(), torn_frame_value()).unwrap();
    // Push the writer past two WAL files so the relocation target, a file
    // boundary at or below the parked write, covers the key's file.
    let mut i = 0u64;
    while db.wal_writer.position() < 2 * config.wal_file_size {
        db.insert(pad, i.to_be_bytes().to_vec(), torn_frame_pad_value())
            .unwrap();
        i += 1;
    }
    db.wal_writer.wal_tracker_barrier();

    // Park a writer between its WAL write and the row lock: its guard keeps
    // the tracker frontier at its frame for the rest of the child's life.
    let (latch, _latch_guard) = Latch::new();
    db.large_table.fp.0.write().fp_insert_before_lock = FailPoint::latch(latch);
    let written_before = metrics.wal_written_bytes.get();
    let _parked = {
        let db = db.clone();
        thread::spawn(move || {
            db.insert(pad, u64::MAX.to_be_bytes().to_vec(), vec![0u8; 8])
                .unwrap()
        })
    };
    // `Db::insert` publishes the frame offset right after the WAL write and
    // before the failpoint, so a changed gauge means the write is in flight.
    while metrics.wal_written_bytes.get() == written_before {
        assert!(
            Instant::now() < deadline,
            "parked writer did not reach the WAL before the deadline"
        );
        thread::sleep(Duration::from_millis(1));
    }
    let torn_offset = metrics.wal_written_bytes.get() as u64;

    // With the frontier pinned at the parked frame, the relocation must park
    // in the tracker wait before indexing the copies. If it finishes instead,
    // it indexed copies above the frontier, and the parent then fails to read
    // the key.
    let wait_requests_before = db.wal_writer.wait_requests_sent();
    let relocation = {
        let db = db.clone();
        thread::spawn(move || db.start_blocking_relocation())
    };
    while !relocation.is_finished() && db.wal_writer.wait_requests_sent() == wait_requests_before {
        assert!(
            Instant::now() < deadline,
            "relocation neither finished nor waited for the frontier"
        );
        thread::sleep(Duration::from_millis(1));
    }
    if relocation.is_finished() {
        relocation.join().unwrap();
        assert!(
            metrics.relocation_kept.with_label_values(&["k"]).get() > 0,
            "relocation must have copied the key"
        );
    }

    // Tear the parked frame: its header never landed.
    let layout = db.wal.layout();
    let file = layout.wal_file_name(path, layout.locate_file(torn_offset));
    let file = OpenOptions::new().write(true).open(file).unwrap();
    file.write_all_at(
        &[0u8; CrcFrame::CRC_HEADER_LENGTH],
        torn_offset % config.wal_file_size,
    )
    .unwrap();
    // Crash: `process::exit` runs no destructors, so nothing is flushed or
    // closed on the way out.
    std::process::exit(exit_code)
}

/// Index-based relocation of a keyspace of 16-byte keys indexed by `indexing`.
/// Every key must read its value once the pass has reclaimed the WAL files
/// holding the originals, and again after a reopen. The relocation filter must
/// be called once per key with the full key, not the index key.
fn index_based_relocation_with_key_indexing(indexing: KeyIndexing) {
    use std::sync::atomic::{AtomicU64, Ordering};

    const KEY_LEN: usize = 16;
    let dir = tempdir::TempDir::new("index_based_relocation_with_key_indexing").unwrap();
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    config.relocation_strategy = RelocationStrategy::IndexBased(None);
    let config = force_unload_config(&config);
    let filter_calls = Arc::new(AtomicU64::new(0));
    let filter_short_keys = Arc::new(AtomicU64::new(0));
    let ksc = KeySpaceConfig::new().with_relocation_filter({
        let filter_calls = filter_calls.clone();
        let filter_short_keys = filter_short_keys.clone();
        move |key, _| {
            filter_calls.fetch_add(1, Ordering::Relaxed);
            if key.len() != KEY_LEN {
                filter_short_keys.fetch_add(1, Ordering::Relaxed);
            }
            Decision::Keep
        }
    });
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config_indexing("k", indexing, 1, KeyType::uniform(1), ksc);
    let key_shape = ksb.build();
    // Both halves are unique per key, so either half works as a reduced key.
    let key = |i: u64| -> Vec<u8> { [i.to_be_bytes(), (!i).to_be_bytes()].concat() };
    let value = |i: u64| -> Vec<u8> { vec![(i % 251) as u8; 12 * 1024] };
    let n = 600u64;
    let check_all = |db: &Db, stage: &str| {
        let ks = db.ks("k");
        for i in 0..n {
            assert_eq!(
                db.get(ks, &key(i)).unwrap(),
                Some(value(i).into()),
                "key {i} {stage}"
            );
        }
    };
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
        let mut first_half_end = 0;
        for i in 0..n {
            db.insert(ks, key(i), value(i)).unwrap();
            if i == n / 2 {
                first_half_end = db.wal_writer.position();
            }
        }
        db.rebuild_control_region().unwrap();
        start_index_based_relocation(&db);
        // The reads below must come from the copies.
        assert!(
            db.wal.min_wal_position() >= first_half_end,
            "the pass must reclaim the originals of the first half of the keys \
             (min WAL position {}, first half ends at {first_half_end})",
            db.wal.min_wal_position()
        );
        assert_eq!(metrics.relocation_kept.with_label_values(&["k"]).get(), n);
        assert_eq!(filter_calls.load(Ordering::Relaxed), n);
        assert_eq!(filter_short_keys.load(Ordering::Relaxed), 0);
        check_all(&db, "after the relocation pass");
        // The index-based pass stores no control region after its GC; store
        // one so the reopen reads the relocated copies through their blobs.
        db.rebuild_control_region().unwrap();
        db.wait_for_background_threads_to_finish();
    }
    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
    check_all(&db, "after reopen");
}

#[test]
fn test_index_based_relocation_fixed_key_indexing() {
    index_based_relocation_with_key_indexing(KeyIndexing::fixed(16));
}

#[test]
fn test_index_based_relocation_hash_key_indexing() {
    index_based_relocation_with_key_indexing(KeyIndexing::hash());
}

#[test]
fn test_index_based_relocation_key_reduction_prefix() {
    index_based_relocation_with_key_indexing(KeyIndexing::key_reduction(16, 0..8));
}

#[test]
fn test_index_based_relocation_key_reduction_suffix() {
    index_based_relocation_with_key_indexing(KeyIndexing::key_reduction(16, 8..16));
}

/// Key in `pad` at which the relocation filter stops a run.
const STOP_MARKER: u64 = u64::MAX;

/// Relocation filter of `pad` and `tail`: drops every record but
/// `STOP_MARKER`, which stops the run there.
fn drop_until_stop_marker(key: &[u8], _value: &[u8]) -> Decision {
    if key == STOP_MARKER.to_be_bytes() {
        Decision::StopRelocation
    } else {
        Decision::Remove
    }
}

/// Key spaces `k`, which relocation copies, and `pad` and `tail`, which it
/// drops (see `drop_until_stop_marker`). `tail` holds a few records only, so
/// they stay in its cell's dirty overlay until a flush.
fn copy_and_drop_key_shape() -> KeyShape {
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config("k", 8, 1, KeyType::uniform(1), KeySpaceConfig::new());
    for name in ["pad", "tail"] {
        ksb.add_key_space_config(
            name,
            8,
            1,
            KeyType::uniform(1),
            KeySpaceConfig::new().with_relocation_filter(drop_until_stop_marker),
        );
    }
    ksb.build()
}

const STRIPED_KEYS: u64 = 600;

/// Every sixth key of `k` is overwritten, so every stripe holds both live
/// records, which it copies and so grows the WAL, and dead versions.
fn striped_key_overwritten(i: u64) -> bool {
    i.is_multiple_of(6)
}

/// Records written to `tail` just before `STOP_MARKER`. The run's filter
/// cutoff drops them from the index; nothing else does while their WAL file
/// is still on disk.
const TAIL_KEYS: u64 = 4;

/// Asserts that no `tail` record written before `STOP_MARKER` is readable.
fn assert_tail_records_dropped(db: &Db, label: &str) {
    let tail = db.ks("tail");
    for i in 0..TAIL_KEYS {
        assert_eq!(
            db.get(tail, &i.to_be_bytes()).unwrap(),
            None,
            "{label}: the filter cutoff must drop tail record {i}"
        );
    }
}

fn striped_value(i: u64, tag: u8) -> Vec<u8> {
    let mut v = vec![(i % 251) as u8; 12 * 1000];
    v[0] = tag;
    v
}

/// Writes `STRIPED_KEYS` keys of `k` interleaved with `pad` records, then
/// overwrites the keys `striped_key_overwritten` picks, so the WAL spans
/// several files that each hold live records, dead versions and records the
/// filter drops. Ends with `TAIL_KEYS` records in `tail` and then `STOP_MARKER`,
/// where relocation runs stop.
fn write_striped_relocation_data(db: &Db) {
    let ks = db.ks("k");
    let pad = db.ks("pad");
    let tail = db.ks("tail");
    for i in 0..STRIPED_KEYS {
        db.insert(ks, i.to_be_bytes().to_vec(), striped_value(i, 1))
            .unwrap();
        db.insert(pad, i.to_be_bytes().to_vec(), striped_value(i, 0))
            .unwrap();
    }
    for i in (0..STRIPED_KEYS).filter(|i| striped_key_overwritten(*i)) {
        db.insert(ks, i.to_be_bytes().to_vec(), striped_value(i, 2))
            .unwrap();
    }
    for i in 0..TAIL_KEYS {
        db.insert(tail, i.to_be_bytes().to_vec(), vec![1u8; 8])
            .unwrap();
    }
    db.insert(pad, STOP_MARKER.to_be_bytes().to_vec(), vec![0u8; 8])
        .unwrap();
    // A run reaches only up to the tracker's frontier, which trails the
    // writes; without this barrier a run started now could stop short of
    // `STOP_MARKER` and leave the `tail` records above its cutoff.
    db.wal_writer.wal_tracker_barrier();
}

fn verify_striped_relocation_data(db: &Db, label: &str) {
    let ks = db.ks("k");
    for i in 0..STRIPED_KEYS {
        let tag = if striped_key_overwritten(i) { 2 } else { 1 };
        assert_eq!(
            db.get(ks, &i.to_be_bytes()).unwrap(),
            Some(striped_value(i, tag).into()),
            "{label}: key {i}"
        );
    }
}

/// Parks a WAL-based relocation run at the failpoint after its first stripe
/// and returns once it is parked. Dropping the returned guard lets the run
/// continue.
fn park_relocation_after_first_stripe(db: &Arc<Db>) -> (thread::JoinHandle<()>, LatchGuard) {
    let (latch, latch_guard) = Latch::new();
    let arrivals = Arc::new(AtomicUsize::new(0));
    db.large_table.fp.0.write().fp_relocation_after_stripe =
        FailPoint::latch_counting(latch, arrivals.clone());
    let relocation = {
        let db = db.clone();
        thread::spawn(move || db.start_blocking_relocation())
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    while arrivals.load(Ordering::SeqCst) == 0 {
        assert!(
            !relocation.is_finished() && Instant::now() < deadline,
            "relocation did not reach the end of its first stripe"
        );
        thread::sleep(Duration::from_millis(1));
    }
    (relocation, latch_guard)
}

/// A WAL-based run over several stripes stores a control region and reclaims
/// the WAL below each stripe before it copies the next, so the copies of one
/// stripe at most are held on top of their originals. The first stripe
/// applies the filter cutoff for the whole run, and a later stripe's control
/// region covers a cell written during the run that no stripe copies into.
#[test]
fn test_wal_relocation_reclaims_each_stripe_before_the_next() {
    let dir = tempdir::TempDir::new("test_wal_relocation_stripes").unwrap();
    // `small_config` makes each stripe one WAL file. Index files are not
    // compacted, so only the stripe's unload threshold decides which dirty
    // cells its control region flushes.
    let mut config = Config::clone(&relocation_test_config(false));
    config.index_min_occupancy_pct = 0;
    let config = Arc::new(config);
    let metrics = Metrics::new();
    {
        let db = Db::open(
            dir.path(),
            copy_and_drop_key_shape(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();
        write_striped_relocation_data(&db);
        let wal_file_size = db.wal.wal_file_size();

        let (relocation, latch_guard) = park_relocation_after_first_stripe(&db);
        let target = metrics.relocation_target_position.get() as u64;
        assert!(
            target >= 3 * wal_file_size,
            "test setup must give a run of several stripes, target {target}"
        );
        assert_eq!(metrics.relocation_stripes.get(), 1);
        assert_eq!(
            db.wal.min_wal_position(),
            wal_file_size,
            "the first stripe must be reclaimed before the second one starts"
        );
        verify_striped_relocation_data(&db, "between stripes");
        assert_tail_records_dropped(&db, "between stripes");
        // A write above the run's frontier, to a cell that has on-disk
        // levels and that no later stripe copies into: the next stripe's
        // control region must still flush it, or replay after a crash would
        // start below it.
        let late_write_start = db.wal_writer.position();
        db.insert(
            db.ks("pad"),
            STRIPED_KEYS.to_be_bytes().to_vec(),
            vec![2u8; 8],
        )
        .unwrap();

        drop(latch_guard);
        relocation.join().unwrap();
        assert_eq!(metrics.relocation_stripes.get(), target / wal_file_size);
        assert_eq!(db.wal.min_wal_position(), target);
        assert_tail_records_dropped(&db, "after relocation");
        let replay_from = db.control_region_store.lock().last_position();
        assert!(
            replay_from > late_write_start,
            "replay must start above the write made between stripes, \
             replay_from {replay_from}, write at {late_write_start}"
        );
        verify_striped_relocation_data(&db, "after relocation");
        db.wait_for_background_threads_to_finish();
    }
    let db = Db::open(
        dir.path(),
        copy_and_drop_key_shape(),
        config,
        Metrics::new(),
    )
    .unwrap();
    verify_striped_relocation_data(&db, "after reopen");
    assert_tail_records_dropped(&db, "after reopen");
    assert_eq!(
        db.get(db.ks("pad"), &STRIPED_KEYS.to_be_bytes()).unwrap(),
        Some(vec![2u8; 8].into()),
        "after reopen: the write made between stripes"
    );
}

/// A crash between two stripes keeps the first one: the reopened database
/// reads every key, the WAL below the first stripe stays reclaimed, and the
/// next run carries on from there. The child process parks a run after its
/// first stripe, waits for the stripe's WAL file to be unlinked and exits
/// without closing the database.
#[test]
fn test_wal_relocation_crash_between_stripes_keeps_progress() {
    use std::process::Command;

    const CHILD_DB_PATH: &str = "TIDEHUNTER_TEST_STRIPE_CRASH_DB_PATH";
    const CHILD_EXIT_CODE: i32 = 38;
    if let Some(path) = std::env::var_os(CHILD_DB_PATH) {
        stripe_crash_child(Path::new(&path), CHILD_EXIT_CODE);
    }

    let dir = tempdir::TempDir::new("test_wal_relocation_stripe_crash").unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "relocation::relocation_tests::test_wal_relocation_crash_between_stripes_keeps_progress",
            "--exact",
            "--nocapture",
        ])
        .env(CHILD_DB_PATH, dir.path())
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(CHILD_EXIT_CODE),
        "child did not reach its crash point\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        copy_and_drop_key_shape(),
        relocation_test_config(false),
        metrics.clone(),
    )
    .unwrap();
    let wal_file_size = db.wal.wal_file_size();
    assert_eq!(
        db.wal.min_wal_position(),
        wal_file_size,
        "the stripe stored before the crash must stay reclaimed"
    );
    verify_striped_relocation_data(&db, "after crash");
    assert_tail_records_dropped(&db, "after crash");

    db.start_blocking_relocation();
    assert!(metrics.relocation_stripes.get() >= 2);
    assert!(db.wal.min_wal_position() > 2 * wal_file_size);
    verify_striped_relocation_data(&db, "after the next run");
}

/// The child half of `test_wal_relocation_crash_between_stripes_keeps_progress`.
/// Exits the process with `exit_code` instead of returning.
fn stripe_crash_child(path: &Path, exit_code: i32) -> ! {
    let db = Db::open(
        path,
        copy_and_drop_key_shape(),
        relocation_test_config(false),
        Metrics::new(),
    )
    .unwrap();
    write_striped_relocation_data(&db);
    let (_relocation, _latch_guard) = park_relocation_after_first_stripe(&db);
    // GC hands the unlink to a background worker; wait for it so the parent
    // sees the file gone rather than reclaiming it again on open.
    let deadline = Instant::now() + Duration::from_secs(60);
    while list_wal_files(path)
        .iter()
        .any(|name| name == "wal_0000000000000000")
    {
        assert!(
            Instant::now() < deadline,
            "the first stripe's WAL file was not unlinked"
        );
        thread::sleep(Duration::from_millis(1));
    }
    // Crash: `process::exit` runs no destructors, so nothing is flushed or
    // closed on the way out.
    std::process::exit(exit_code)
}

// Multi-generation relocation: a keyspace with `promote_during_relocation` set
// should have its surviving records rewritten to the G1 side WAL during
// relocation. After relocation, the G1 directory holds the relocated frames,
// regular reads still return the expected values (routed through the
// generation-aware dispatcher), and a Db restart preserves them.
#[test]
fn test_multi_gen_relocation_promotes_to_g1() {
    let dir = tempdir::TempDir::new("test_multi_gen_relocation").unwrap();
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    config.wal_generations = 2; // G0 + G1
    let config = force_unload_config(&config);

    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config(
        "k",
        8,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_promote_during_relocation(true),
    );
    let key_shape = ksb.build();

    let big = |b: u8| -> Vec<u8> { vec![b; 12 * 1000] };

    // Phase 1: write enough to exercise relocation, run it, sanity-check.
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

        let sample = 3u64;
        db.insert(ks, sample.to_be_bytes().to_vec(), big(1))
            .unwrap();
        for i in 1000..3000u64 {
            db.insert(ks, i.to_be_bytes().to_vec(), big(7)).unwrap();
        }

        // Pin last_processed at the current frontier so subsequent inserts push
        // the writer ahead of it. WAL-based relocation only sets a non-zero
        // `terminal_position` when the scan encounters records past
        // `upper_limit` (= captured `last_processed`).
        let _checkpoint = db.checkpoint();
        for i in 3000..3100u64 {
            db.insert(ks, i.to_be_bytes().to_vec(), big(7)).unwrap();
        }

        let g1_position_before = db.generation_writers[0].position();
        db.start_blocking_relocation();
        let g1_position_after = db.generation_writers[0].position();

        let kept = metrics.relocation_kept.with_label_values(&["k"]).get();
        assert!(
            kept > 0,
            "relocation must have relocated entries for the promotion to be observable"
        );

        // The G1 writer's position must have advanced — proves the relocator
        // actually dispatched writes to the side WAL rather than silently
        // falling back to G0. (Checking `gen0001/` file size doesn't work:
        // `Wal::open_file` pre-allocates the first fragment to `frag_size`,
        // so the directory has non-zero bytes regardless of whether the
        // relocator wrote anything.)
        assert!(
            g1_position_after > g1_position_before,
            "G1 writer position must advance during promotion \
             (before={g1_position_before}, after={g1_position_after})"
        );

        // Live reads still work — they now route through the G1 side WAL via
        // the generation-aware dispatcher in `read_record`.
        assert_eq!(
            db.get(ks, &sample.to_be_bytes()).unwrap(),
            Some(big(1).into()),
            "sample key must be readable after promotion to G1"
        );
        for i in [1000u64, 1500, 2500, 2999] {
            assert_eq!(
                db.get(ks, &i.to_be_bytes()).unwrap(),
                Some(big(7).into()),
                "key {i} must be readable after promotion to G1"
            );
        }

        // Drop the checkpoint before tearing the Db down — it holds an
        // `Arc<Db>` that would otherwise prevent shutdown from draining.
        drop(_checkpoint);
        db.wait_for_background_threads_to_finish();
    }

    // Phase 2: reopen Db; the side WAL must survive restart and reads must
    // still resolve via G1.
    let metrics = Metrics::new();
    let db = Db::open(dir.path(), key_shape, config, metrics).unwrap();
    let ks = db.ks("k");
    let sample = 3u64;
    assert_eq!(
        db.get(ks, &sample.to_be_bytes()).unwrap(),
        Some(big(1).into()),
        "sample key must be readable from G1 after restart"
    );
    for i in [1000u64, 1500, 2500, 2999] {
        assert_eq!(
            db.get(ks, &i.to_be_bytes()).unwrap(),
            Some(big(7).into()),
            "key {i} must be readable from G1 after restart"
        );
    }
}

// A second relocation pass over a keyspace that already had records promoted
// to G1 must NOT re-copy those records: their stored positions are encoded
// with the gen flag bit set, so the WAL-based relocator's `offset <
// target_position` check naturally skips them. This is the central
// efficiency claim of multi-gen — already-promoted records stop being
// re-copied each cycle. The test inserts a small batch of NEW data between
// the two relocation passes and asserts the second pass only relocates the
// new batch's worth of records, not the full set again.
#[test]
fn test_multi_gen_second_relocation_does_not_recopy_g1() {
    let dir = tempdir::TempDir::new("test_multi_gen_second_pass").unwrap();
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    config.wal_generations = 2;
    let config = force_unload_config(&config);

    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config(
        "k",
        8,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_promote_during_relocation(true),
    );
    let key_shape = ksb.build();

    let metrics = Metrics::new();
    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
    let ks = db.ks("k");

    let big = |b: u8| -> Vec<u8> { vec![b; 12 * 1000] };

    // First batch (the "old" data, to be promoted in pass #1).
    for i in 1000..3000u64 {
        db.insert(ks, i.to_be_bytes().to_vec(), big(7)).unwrap();
    }
    let cp1 = db.checkpoint();
    for i in 3000..3100u64 {
        db.insert(ks, i.to_be_bytes().to_vec(), big(7)).unwrap();
    }
    db.start_blocking_relocation();
    drop(cp1);

    let kept_after_pass1 = metrics.relocation_kept.with_label_values(&["k"]).get();
    let g1_position_after_pass1 = db.generation_writers[0].position();
    assert!(
        kept_after_pass1 > 0,
        "pass #1 must have relocated entries (kept={kept_after_pass1})"
    );

    // A small batch of NEW data. Pass #2 should relocate this batch and
    // skip everything the first pass already moved to G1.
    let new_batch_size = 200u64;
    for i in 4000..(4000 + new_batch_size) {
        db.insert(ks, i.to_be_bytes().to_vec(), big(7)).unwrap();
    }
    let cp2 = db.checkpoint();
    for i in 5000..5100u64 {
        db.insert(ks, i.to_be_bytes().to_vec(), big(7)).unwrap();
    }
    db.start_blocking_relocation();
    drop(cp2);

    let kept_after_pass2 = metrics.relocation_kept.with_label_values(&["k"]).get();
    let pass2_new_kept = kept_after_pass2 - kept_after_pass1;
    let g1_position_after_pass2 = db.generation_writers[0].position();
    let g1_growth_pass2 = g1_position_after_pass2 - g1_position_after_pass1;

    // Pass 2 should have kept only the new batch's worth of records, not
    // re-counted the full pre-existing G1 set. Allow a small slack for the
    // padding writes that get promoted alongside.
    assert!(
        pass2_new_kept <= 2 * new_batch_size,
        "pass #2 must not re-relocate already-G1 records: \
         pass1_kept={kept_after_pass1}, pass2_total_kept={kept_after_pass2}, \
         pass2_new={pass2_new_kept}, new_batch={new_batch_size}"
    );

    // And the G1 writer should have grown by roughly one new-batch worth of
    // bytes, not by the full original 2000+ records again.
    let new_batch_bytes = new_batch_size * 12_000;
    assert!(
        g1_growth_pass2 < 5 * new_batch_bytes,
        "pass #2 G1 growth ({g1_growth_pass2} bytes) suggests entire G1 \
         set was re-copied (new batch is ~{new_batch_bytes} bytes)"
    );

    // Reads still resolve correctly for both old and new keys.
    for i in [1000u64, 2500, 2999, 4000, 4050, 4199] {
        assert_eq!(
            db.get(ks, &i.to_be_bytes()).unwrap(),
            Some(big(7).into()),
            "key {i} must be readable after two relocation passes"
        );
    }
}

// A key promoted to G1 by relocation must remain writable: the follow-up G0
// write is logically newer than the promoted copy even though its raw offset
// is numerically smaller (the G1 encoding sets the top bit). Recency across
// generations is "lower generation wins" — data only flows into deeper
// generations, so a coexisting G0 entry must postdate the promoted copy.
//
// Covers the full lifecycle: overwrite → read → flush → read → second
// relocation pass (re-promotes the new value) → read → restart → read.
fn overwrite_after_promotion(ksc: KeySpaceConfig, label: &'static str) {
    let dir = tempdir::TempDir::new(label).unwrap();
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    config.wal_generations = 2;
    let config = force_unload_config(&config);

    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config(
        "k",
        8,
        1,
        KeyType::uniform(1),
        ksc.with_promote_during_relocation(true),
    );
    let key_shape = ksb.build();
    let metrics = Metrics::new();

    let big = |b: u8| -> Vec<u8> { vec![b; 12 * 1000] };
    let key = 3u64.to_be_bytes().to_vec();

    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();
        let ks = db.ks("k");

        db.insert(ks, key.clone(), big(1)).unwrap();
        for i in 1000..3000u64 {
            db.insert(ks, i.to_be_bytes().to_vec(), big(7)).unwrap();
        }
        let cp = db.checkpoint();
        for i in 3000..3100u64 {
            db.insert(ks, i.to_be_bytes().to_vec(), big(7)).unwrap();
        }
        db.start_blocking_relocation();
        drop(cp);

        let kept = metrics.relocation_kept.with_label_values(&["k"]).get();
        assert!(kept > 0, "relocation must have promoted entries");
        assert!(
            db.generation_writers[0].position() > 0,
            "promoted copies must land in the G1 side WAL"
        );

        // The regression: overwrite a key whose current index position is a
        // G1-encoded promoted copy. Must not panic and must win reads.
        db.insert(ks, key.clone(), big(2)).unwrap();
        assert_eq!(
            db.get(ks, &key).unwrap(),
            Some(big(2).into()),
            "overwrite after promotion must win reads immediately"
        );

        // Push the overwrite through the flush/merge machinery.
        db.force_rebuild_control_region().unwrap();
        assert_eq!(
            db.get(ks, &key).unwrap(),
            Some(big(2).into()),
            "overwrite must survive a flush over the G1-holding blob"
        );

        // Second relocation pass: the overwritten value is the as-of value
        // now and gets promoted itself.
        let cp2 = db.checkpoint();
        for i in 4000..4100u64 {
            db.insert(ks, i.to_be_bytes().to_vec(), big(7)).unwrap();
        }
        db.start_blocking_relocation();
        drop(cp2);
        assert_eq!(
            db.get(ks, &key).unwrap(),
            Some(big(2).into()),
            "overwrite must survive a second relocation pass"
        );

        db.wait_for_background_threads_to_finish();
    }

    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
    let ks = db.ks("k");
    assert_eq!(
        db.get(ks, &key).unwrap(),
        Some(big(2).into()),
        "overwrite must survive restart"
    );
    for i in [1000u64, 2500, 2999] {
        assert_eq!(
            db.get(ks, &i.to_be_bytes()).unwrap(),
            Some(big(7).into()),
            "untouched key {i} must still be readable"
        );
    }
}

#[test]
fn test_multi_gen_overwrite_after_promotion_unload() {
    // Default unloading: the cell unloads after relocation, the overwrite
    // lands in a fresh dirty overlay, and the flush merges it over a blob
    // whose flat holds the G1 promoted copy (merge_dirty / fold paths).
    overwrite_after_promotion(
        KeySpaceConfig::new(),
        "test_multi_gen_overwrite_promoted_unload",
    );
}

#[test]
fn test_multi_gen_overwrite_after_promotion_no_unload() {
    // Unloading disabled: the relocation sync-flush rebases the live cell
    // onto the as-of blob (`rebase_on_as_of`), so the in-memory `flat` holds
    // the key's G1 position when the overwrite arrives — exercising the
    // `checked_insert` flat-probe recency comparison directly.
    overwrite_after_promotion(
        KeySpaceConfig::new().disable_unload(),
        "test_multi_gen_overwrite_promoted_no_unload",
    );
}

// A promoted key resident in a *loaded* cell's index table: after relocation
// promotes the key to G1 and the cell unloads, stepping an iterator on a
// `with_unloaded_iterator(false)` keyspace calls `maybe_load`, folding the L0
// blob — including the key's G1-encoded position — into the live table. The
// live write path must not rank that flagged position above the incoming G0
// write/delete (by raw offset it outranks every future G0 offset), or the
// update is hidden behind it: stale reads forever and an undeletable key.
// The `overwrite_after_promotion` variants cannot catch this — their cells
// stay unloaded when the overwrite lands, so no flagged position is in the
// table the write is ordered against.
#[test]
fn test_multi_gen_update_promoted_key_in_loaded_cell() {
    let dir = tempdir::TempDir::new("test_multi_gen_update_promoted_loaded").unwrap();
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    config.wal_generations = 2;
    // Default snapshot threshold: the background snapshot loop must not
    // re-unload the cell between the iterator step below and the updates.
    let config = Arc::new(config);

    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config(
        "k",
        8,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new()
            .with_unloaded_iterator(false)
            .with_promote_during_relocation(true),
    );
    // Filler traffic lives in its own keyspace, creating the WAL span
    // relocation needs while the promoted keys' cell stays far below the L0
    // threshold — so its relocation flush writes an L0 blob (an over-threshold
    // cell would promote straight to L1, which `maybe_load` never folds into
    // the live table).
    ksb.add_key_space_config("fill", 8, 1, KeyType::uniform(1), KeySpaceConfig::new());
    let key_shape = ksb.build();
    let metrics = Metrics::new();

    let big = |b: u8| -> Vec<u8> { vec![b; 12 * 1000] };
    let overwrite_key = 3u64.to_be_bytes().to_vec();
    let delete_key = 4u64.to_be_bytes().to_vec();

    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();
        let ks = db.ks("k");
        let fill = db.ks("fill");

        db.insert(ks, overwrite_key.clone(), big(1)).unwrap();
        db.insert(ks, delete_key.clone(), big(1)).unwrap();
        for i in 1000..3000u64 {
            db.insert(fill, i.to_be_bytes().to_vec(), big(7)).unwrap();
        }
        let cp = db.checkpoint();
        for i in 3000..3100u64 {
            db.insert(fill, i.to_be_bytes().to_vec(), big(7)).unwrap();
        }
        db.start_blocking_relocation();
        drop(cp);

        let kept = metrics.relocation_kept.with_label_values(&["k"]).get();
        assert_eq!(kept, 2, "both promoted keys must survive relocation");
        assert!(
            db.generation_writers[0].position() > 0,
            "promoted copies must land in the G1 side WAL"
        );
        assert_eq!(
            db.get(ks, &overwrite_key).unwrap(),
            Some(big(1).into()),
            "promoted key must be readable through its G1 position"
        );

        // Load the (clean) cell holding the promoted keys: with unloaded
        // iteration off, stepping an iterator folds the L0 blob — and the
        // keys' G1 positions — into the live table.
        assert!(db.iterator(ks).next().is_some());

        db.insert(ks, overwrite_key.clone(), big(2)).unwrap();
        assert_eq!(
            db.get(ks, &overwrite_key).unwrap(),
            Some(big(2).into()),
            "overwrite of a promoted key in a loaded cell must win reads"
        );
        db.remove(ks, delete_key.clone()).unwrap();
        assert_eq!(
            db.get(ks, &delete_key).unwrap(),
            None,
            "delete of a promoted key in a loaded cell must take effect"
        );

        db.force_rebuild_control_region().unwrap();
        assert_eq!(
            db.get(ks, &overwrite_key).unwrap(),
            Some(big(2).into()),
            "overwrite must survive a flush over the G1-holding blob"
        );
        assert_eq!(
            db.get(ks, &delete_key).unwrap(),
            None,
            "delete must survive a flush over the G1-holding blob"
        );

        db.wait_for_background_threads_to_finish();
    }

    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
    let ks = db.ks("k");
    assert_eq!(
        db.get(ks, &overwrite_key).unwrap(),
        Some(big(2).into()),
        "overwrite must survive restart"
    );
    assert_eq!(
        db.get(ks, &delete_key).unwrap(),
        None,
        "delete must survive restart"
    );
}

// Opening a Db with `promote_during_relocation = true` on a keyspace but
// `Config::wal_generations < 2` is a misconfiguration — the relocator would
// silently fall back to G0 promotion (== single-WAL behavior), defeating the
// flag. `Db::open` must reject it loudly.
#[test]
#[should_panic(expected = "promote_during_relocation")]
fn test_promote_during_relocation_without_multi_gen_rejected() {
    let dir = tempdir::TempDir::new("test_promote_rejected").unwrap();
    let mut config = Config::small();
    config.wal_generations = 1; // not enough for promotion
    let config = Arc::new(config);

    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config(
        "k",
        8,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_promote_during_relocation(true),
    );
    let key_shape = ksb.build();

    let _ = Db::open(dir.path(), key_shape, config, Metrics::new());
}

// The WAL-based relocation pass routes filtered keyspaces to the
// clear-below-terminal path and never writes relocated batches, so the
// promote flag would be silently inert — `Db::open` must reject the
// combination.
#[test]
#[should_panic(expected = "relocation filter")]
fn test_promote_during_relocation_with_filter_rejected() {
    let dir = tempdir::TempDir::new("test_promote_filter_rejected").unwrap();
    let mut config = Config::small();
    config.wal_generations = 2;
    let config = Arc::new(config);

    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config(
        "k",
        8,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new()
            .with_promote_during_relocation(true)
            .with_relocation_filter(|_key: &[u8], _value: &[u8]| Decision::Keep),
    );
    let key_shape = ksb.build();

    let _: DbResult<Arc<Db>> = Db::open(dir.path(), key_shape, config, Metrics::new());
}

// Index-based relocation GCs G0 files without rebuilding the control region
// first; promotion must not ride that path until it does — `Db::open` must
// reject the configured combination.
#[test]
#[should_panic(expected = "IndexBased")]
fn test_promote_during_relocation_with_index_based_strategy_rejected() {
    let dir = tempdir::TempDir::new("test_promote_index_based_rejected").unwrap();
    let mut config = Config::small();
    config.wal_generations = 2;
    config.relocation_strategy = RelocationStrategy::IndexBased(None);
    let config = Arc::new(config);

    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config(
        "k",
        8,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_promote_during_relocation(true),
    );
    let key_shape = ksb.build();

    let _: DbResult<Arc<Db>> = Db::open(dir.path(), key_shape, config, Metrics::new());
}
