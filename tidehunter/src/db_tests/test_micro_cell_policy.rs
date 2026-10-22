//! Db-level tests of per-blob micro-cell counts (`index::micro_cells`). The
//! count a blob was written with travels in its `IndexRef`, so blobs written
//! under different policies coexist in one database and each is read the way
//! it was written, across reopens and policy changes, and a database written
//! before counts were recorded reads every blob with the legacy count.
use super::super::*;
use super::{lookup_counts, sixteen_cells};
use crate::cell::CellId;
use crate::config::Config;
use crate::control::ControlRegion;
use crate::index::micro_cells::{LEGACY_MICRO_CELLS, MicroCellPolicy};
use crate::key_shape::{KeyShape, KeySpaceConfig, KeyType};
use crate::metrics::Metrics;
use minibytes::Bytes;
use std::collections::BTreeMap;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// `sixteen_cells` with the key space's micro-cell policy set to `policy`.
fn shape_with(policy: MicroCellPolicy) -> KeyShape {
    KeyShape::new_single_config(
        4,
        16,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_micro_cell_policy(policy),
    )
}

fn small_config() -> Arc<Config> {
    Arc::new(Config::small())
}

/// 64 keys per cell of `sixteen_cells` (a key's cell is its first byte >> 4)
/// for `round`, in the given cells: enough to exceed `Config::small`'s 32
/// dirty keys per cell so every written cell flushes an index blob.
fn entries(round: u8, cells: Range<u8>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    for cell in cells {
        for i in 0..64u8 {
            out.push((vec![cell << 4, round, i, 0], vec![round, i, cell]));
        }
    }
    out
}

/// Inserts `entries`, flushes every dirty cell and persists the resulting
/// levels. `force_rebuild_control_region` dispatches the flushes and applies
/// completed ones to the cells it visits, so it is repeated until no cell is
/// dirty; the last call writes a control region that references every new
/// blob. Without this a cell whose flush is still in flight when the database
/// closes is replayed from the WAL on the next open and flushed again under
/// that open's policy.
fn insert_all(db: &Db, entries: &[(Vec<u8>, Vec<u8>)]) {
    let ks = db.single_ks();
    for (k, v) in entries {
        db.insert(ks, k.clone(), v.clone()).unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        db.force_rebuild_control_region().unwrap();
        if db.large_table.is_all_clean() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "flushes did not complete in time"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

/// Every entry reads back, two probe keys do not, and an ordered scan yields
/// exactly the entries.
fn check_all(db: &Arc<Db>, entries: &[(Vec<u8>, Vec<u8>)], what: &str) {
    let ks = db.single_ks();
    for (k, v) in entries {
        assert_eq!(
            db.get(ks, k).unwrap(),
            Some(Bytes::from(v.clone())),
            "{what}: key {k:?}"
        );
    }
    assert_eq!(db.get(ks, &[0x00, 0xff, 0xff, 0xff]).unwrap(), None);
    assert_eq!(db.get(ks, &[0xf0, 0xff, 0xff, 0xff]).unwrap(), None);
    let mut expected: Vec<(Bytes, Bytes)> = entries
        .iter()
        .map(|(k, v)| (Bytes::from(k.clone()), Bytes::from(v.clone())))
        .collect();
    expected.sort();
    let scanned: Vec<(Bytes, Bytes)> = db.iterator(ks).collect::<DbResult<_>>().unwrap();
    assert_eq!(scanned, expected, "{what}: scan");
}

/// The micro-cell counts of every live index blob per cell, from the control
/// region on disk.
fn counts_on_disk(dir: &Path, key_shape: &KeyShape) -> BTreeMap<u8, Vec<u32>> {
    let cr = ControlRegion::read(&dir.join(CONTROL_REGION_FILE), key_shape).unwrap();
    let mut out = BTreeMap::new();
    for (cell, entry) in &cr.snapshot().data[0] {
        let CellId::Integer(cell) = cell else {
            panic!("uniform cells are integers")
        };
        let counts: Vec<u32> = entry.levels().iter().map(|r| r.micro_cells()).collect();
        if !counts.is_empty() {
            out.insert(*cell as u8, counts);
        }
    }
    out
}

/// Every blob of the cells in `cells` has micro-cell count `count`.
fn assert_counts(counts: &BTreeMap<u8, Vec<u32>>, cells: Range<u8>, count: u32, what: &str) {
    for cell in cells {
        let got = counts
            .get(&cell)
            .unwrap_or_else(|| panic!("{what}: cell {cell} has no blob"));
        assert!(
            got.iter().all(|&c| c == count),
            "{what}: cell {cell} counts {got:?}, expected all {count}"
        );
    }
}

/// The lookups of a reopened database go to disk: at least one unloaded
/// lookup was served from a mapped or file-backed index blob.
fn assert_disk_lookups(metrics: &Metrics, db: &Db) {
    let (mapped, syscall, cached) = lookup_counts(metrics, db, db.single_ks());
    assert!(
        mapped + syscall + cached > 0,
        "expected on-disk index lookups, got mapped {mapped} syscall {syscall} cached {cached}"
    );
}

#[test]
fn test_blobs_of_different_policies_coexist() {
    let dir = tempdir::TempDir::new("test-micro-cell-policy").unwrap();
    let key_shape = sixteen_cells();
    let small = MicroCellPolicy::Scaled {
        section_target_log2: 5,
    };
    // 64 entries of 16 bytes: 1 KiB of sections. Fixed 128 for legacy; the
    // 32-byte target gives 8 (header-balance rule); the default gives 1.
    let small_count = small.micro_cells(64 * 16);
    assert_eq!(small_count, 8);
    assert_eq!(MicroCellPolicy::DEFAULT.micro_cells(64 * 16), 1);

    // Each phase writes its own cells, so blobs of every policy stay live.
    let phases: [(MicroCellPolicy, Range<u8>, u32); 4] = [
        (MicroCellPolicy::LEGACY, 0..4, LEGACY_MICRO_CELLS),
        (small, 4..8, small_count),
        (MicroCellPolicy::DEFAULT, 8..12, 1),
        (MicroCellPolicy::LEGACY, 12..16, LEGACY_MICRO_CELLS),
    ];
    let mut all: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for (round, (policy, cells, _)) in phases.iter().enumerate() {
        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            shape_with(*policy),
            small_config(),
            metrics.clone(),
        )
        .unwrap();
        // Everything written so far reads back from disk under the new policy.
        check_all(&db, &all, &format!("phase {round} before writing"));
        if round > 0 {
            assert_disk_lookups(&metrics, &db);
        }
        let new = entries(round as u8, cells.clone());
        insert_all(&db, &new);
        all.extend(new);
        check_all(&db, &all, &format!("phase {round} after writing"));
        drop(db);

        let counts = counts_on_disk(dir.path(), &key_shape);
        for (prev_policy, prev_cells, prev_count) in phases.iter().take(round + 1) {
            assert_counts(
                &counts,
                prev_cells.clone(),
                *prev_count,
                &format!("phase {round}, blobs written under {prev_policy:?}"),
            );
        }
    }

    // A final reopen reads blobs of all three counts from disk.
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        shape_with(MicroCellPolicy::DEFAULT),
        small_config(),
        metrics.clone(),
    )
    .unwrap();
    check_all(&db, &all, "final reopen");
    assert_disk_lookups(&metrics, &db);
}

/// A database written before refs recorded micro-cell counts has a V3
/// control region with bare positions. Opening it reads every existing blob
/// with the legacy count, and blobs written from then on carry the count of
/// the configured policy.
#[test]
fn test_pre_count_database_opens_under_scaled_policy() {
    let dir = tempdir::TempDir::new("test-pre-count-db").unwrap();
    let key_shape = sixteen_cells();
    let cr_path = dir.path().join(CONTROL_REGION_FILE);

    let old = entries(0, 0..16);
    {
        let db = Db::open(
            dir.path(),
            shape_with(MicroCellPolicy::LEGACY),
            small_config(),
            Metrics::new(),
        )
        .unwrap();
        insert_all(&db, &old);
    }
    ControlRegion::rewrite_as_v3_for_test(&cr_path, &key_shape);
    let counts = counts_on_disk(dir.path(), &key_shape);
    assert_counts(&counts, 0..16, LEGACY_MICRO_CELLS, "migrated V3");

    // Default policy: old blobs read with 128; new blobs, written to half
    // the cells, get the default count while the other half stays legacy.
    let new = entries(1, 8..16);
    let mut all = old.clone();
    all.extend(new.iter().cloned());
    {
        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            shape_with(MicroCellPolicy::DEFAULT),
            small_config(),
            metrics.clone(),
        )
        .unwrap();
        check_all(&db, &old, "pre-count blobs under the default policy");
        assert_disk_lookups(&metrics, &db);
        insert_all(&db, &new);
        check_all(&db, &all, "pre-count and default blobs");
    }
    let counts = counts_on_disk(dir.path(), &key_shape);
    assert_counts(&counts, 0..8, LEGACY_MICRO_CELLS, "untouched cells");
    assert_counts(&counts, 8..16, 1, "rewritten cells");
    {
        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            shape_with(MicroCellPolicy::DEFAULT),
            small_config(),
            metrics.clone(),
        )
        .unwrap();
        check_all(&db, &all, "pre-count and default blobs after reopen");
        assert_disk_lookups(&metrics, &db);
    }
}
