use super::super::*;
use crate::cell::CellId;
use crate::config::Config;
use crate::key_shape::{KeyShape, KeyShapeBuilder, KeySpace, KeyType};
use crate::metrics::Metrics;
use crate::relocation::watermark::RelocationWatermarks;
use crate::relocation::{CellReference, RelocationStrategy};
use std::path::Path;
use std::sync::Arc;

fn shape(names: &[&str]) -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    for name in names {
        builder.add_key_space(*name, 4, 8, KeyType::uniform(4));
    }
    builder.build()
}

fn prefixed_shape(names: &[&str]) -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    for name in names {
        builder.add_key_space(*name, 4, 8, KeyType::prefix_uniform(2, 0));
    }
    builder.build()
}

/// Opens the db with the given declared keyspace order and resolves the
/// canonical handles by name, returned in declared order so callers index
/// `ks[i]` by declaration position.
fn open_shape(dir: &Path, names: &[&str], config: &Arc<Config>) -> (Arc<Db>, Vec<KeySpace>) {
    open(dir, shape(names), config)
}

fn open(dir: &Path, shape: KeyShape, config: &Arc<Config>) -> (Arc<Db>, Vec<KeySpace>) {
    let names: Vec<String> = shape.iter_ks().map(|ks| ks.name().to_string()).collect();
    let db = Db::open(dir, shape, config.clone(), Metrics::new()).unwrap();
    let ks = names.iter().map(|name| db.ks(name)).collect();
    (db, ks)
}

/// Keys with distinct first bytes so writes land in distinct cells.
fn keys() -> Vec<[u8; 4]> {
    (0..16u8).map(|i| [i * 16, i, 1, 2]).collect()
}

fn write_all(db: &Db, ks: KeySpace, value: &[u8]) {
    for key in keys() {
        db.insert(ks, key.to_vec(), value.to_vec()).unwrap();
    }
}

fn assert_all(db: &Db, ks: KeySpace, value: &[u8]) {
    for key in keys() {
        assert_eq!(
            Some(value.to_vec().into()),
            db.get(ks, &key).unwrap(),
            "key {key:?}"
        );
    }
}

fn assert_none(db: &Db, ks: KeySpace) {
    for key in keys() {
        assert_eq!(None, db.get(ks, &key).unwrap(), "key {key:?}");
    }
}

/// Full lifecycle: create → retain → destroy (idempotent) → tombstone
/// persisted → re-declare the name as a fresh keyspace under a new id.
#[test]
fn test_destroy_keyspace_lifecycle() {
    let dir = tempdir::TempDir::new("test-destroy-keyspace").unwrap();
    let config = Arc::new(Config::small());

    // Phase 1: create a/b with data in both, snapshot.
    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        write_all(&db, ks[0], b"va");
        write_all(&db, ks[1], b"vb");
        db.rebuild_control_region().unwrap();
    }

    // Phase 2: open with b retained (undeclared), destroy it. A second
    // destroy is a no-op.
    {
        let (db, _ks) = open_shape(dir.path(), &["a"], &config);
        db.destroy_key_space("b").unwrap();
        db.destroy_key_space("b").unwrap();
    }

    // The registry now carries a tombstone for b: the name is still
    // recorded (the id slot is kept forever) but flagged destroyed.
    {
        let stored = Db::load_key_shape(dir.path()).unwrap();
        let entries: Vec<_> = stored
            .iter_ks()
            .map(|ks| (ks.name().to_string(), ks.destroyed()))
            .collect();
        assert_eq!(
            vec![("a".to_string(), false), ("b".to_string(), true)],
            entries
        );
    }

    // Phase 3: reopen; a intact, destroying the tombstone again is a no-op.
    {
        let (db, ks) = open_shape(dir.path(), &["a"], &config);
        assert_all(&db, ks[0], b"va");
        db.destroy_key_space("b").unwrap();
    }

    // Phase 4: re-declare b — a fresh, empty keyspace under a new id
    // (the tombstone keeps id 1, so the reborn b gets id 2).
    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        assert_eq!(KeySpace::new_test(2), ks[1]);
        assert_all(&db, ks[0], b"va");
        assert_none(&db, ks[1]);
        write_all(&db, ks[1], b"vb2");
    }

    // Phase 5: the reborn b persists across reopen.
    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        assert_all(&db, ks[0], b"va");
        assert_all(&db, ks[1], b"vb2");
    }
}

/// A destroy that "crashes" right after the registry write (no data drop,
/// no control-region rebuild) must converge on reopen alone: WAL replay
/// skips the destroyed keyspace's frames.
#[test]
fn test_destroy_keyspace_crash_after_commit_wal_replay() {
    let dir = tempdir::TempDir::new("test-destroy-keyspace-wal").unwrap();
    let config = Arc::new(Config::small());

    // No snapshot: everything lives in the WAL only.
    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        write_all(&db, ks[0], b"va");
        write_all(&db, ks[1], b"vb");
    }
    {
        let (db, _ks) = open_shape(dir.path(), &["a"], &config);
        db.destroy_key_space_commit_only("b").unwrap();
    }
    // This reopen replays b's frames from the WAL; they must be skipped.
    {
        let (db, ks) = open_shape(dir.path(), &["a"], &config);
        assert_all(&db, ks[0], b"va");
    }
    // The reborn b sees none of the old data.
    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        assert_all(&db, ks[0], b"va");
        assert_none(&db, ks[1]);
    }
}

/// Same crash point, but with the destroyed keyspace's cells present in the
/// control-region snapshot: the stale slot must be ignored on open and
/// written empty by the next snapshot.
#[test]
fn test_destroy_keyspace_crash_after_commit_snapshot() {
    let dir = tempdir::TempDir::new("test-destroy-keyspace-cr").unwrap();
    let config = Arc::new(Config::small());

    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        write_all(&db, ks[0], b"va");
        write_all(&db, ks[1], b"vb");
        db.rebuild_control_region().unwrap();
    }
    {
        let (db, _ks) = open_shape(dir.path(), &["a"], &config);
        db.destroy_key_space_commit_only("b").unwrap();
    }
    // The control region still references b's index blobs; open must ignore
    // the slot. Rebuild afterwards to write it out empty.
    {
        let (db, ks) = open_shape(dir.path(), &["a"], &config);
        assert_all(&db, ks[0], b"va");
        db.rebuild_control_region().unwrap();
    }
    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        assert_all(&db, ks[0], b"va");
        assert_none(&db, ks[1]);
    }
}

/// After a destroy, snapshots and relocation run with the tombstone in the
/// shape: the control region writes the destroyed keyspace's slot empty,
/// relocation skips the tombstone, and a reborn keyspace round-trips
/// through its own snapshot slot, positioned after the tombstone's hole.
/// The final reopen reads through the control region and index blobs (WAL
/// replay is empty after the snapshot), so slot misalignment across the
/// tombstone would surface as misses.
#[test]
fn test_destroy_keyspace_snapshot_and_relocation_roundtrip() {
    let dir = tempdir::TempDir::new("test-destroy-keyspace-roundtrip").unwrap();
    let config = Arc::new(Config::small());

    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        write_all(&db, ks[0], b"va");
        write_all(&db, ks[1], b"vb");
        db.rebuild_control_region().unwrap();
    }
    {
        let (db, ks) = open_shape(dir.path(), &["a"], &config);
        db.destroy_key_space("b").unwrap();
        db.rebuild_control_region().unwrap();
        db.start_blocking_relocation_with_strategy(RelocationStrategy::IndexBased(None));
        assert_all(&db, ks[0], b"va");
    }
    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        assert_all(&db, ks[0], b"va");
        assert_none(&db, ks[1]);
        write_all(&db, ks[1], b"vb2");
        db.rebuild_control_region().unwrap();
    }
    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        assert_all(&db, ks[0], b"va");
        assert_all(&db, ks[1], b"vb2");
    }
}

/// Destroying a reborn keyspace leaves two same-named tombstones; the next
/// rebirth must target the live generation and get a third id.
#[test]
fn test_destroy_and_recreate_same_name_twice() {
    let dir = tempdir::TempDir::new("test-destroy-keyspace-twice").unwrap();
    let config = Arc::new(Config::small());

    // Generation 1: create and destroy.
    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        write_all(&db, ks[1], b"vb1");
    }
    {
        let (db, _ks) = open_shape(dir.path(), &["a"], &config);
        db.destroy_key_space("b").unwrap();
    }
    // Generation 2: reborn under id 2, written to, destroyed in turn.
    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        assert_eq!(KeySpace::new_test(2), ks[1]);
        assert_none(&db, ks[1]);
        write_all(&db, ks[1], b"vb2");
    }
    {
        let (db, _ks) = open_shape(dir.path(), &["a"], &config);
        db.destroy_key_space("b").unwrap();
    }
    // Generation 3: two tombstones named b hold ids 1 and 2; the new b
    // gets id 3 and sees neither generation's data.
    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        assert_eq!(KeySpace::new_test(3), ks[1]);
        assert_none(&db, ks[1]);
        write_all(&db, ks[1], b"vb3");

        let stored = Db::load_key_shape(dir.path()).unwrap();
        let entries: Vec<_> = stored
            .iter_ks()
            .map(|ks| (ks.name().to_string(), ks.destroyed()))
            .collect();
        assert_eq!(
            vec![
                ("a".to_string(), false),
                ("b".to_string(), true),
                ("b".to_string(), true),
                ("b".to_string(), false),
            ],
            entries
        );
    }
    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        assert_all(&db, ks[1], b"vb3");
    }
}

/// A relocation watermark saved before a destroy may point `next_to_process`
/// into the destroyed keyspace. On resume after a restart, relocation must
/// re-anchor to the next live keyspace instead of touching the destroyed
/// stub's (empty) cell array.
#[test]
fn test_relocation_resume_into_destroyed_keyspace() {
    let dir = tempdir::TempDir::new("test-destroy-keyspace-watermark").unwrap();
    let config = Arc::new(Config::small());

    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        write_all(&db, ks[0], b"va");
        write_all(&db, ks[1], b"vb");
    }
    {
        let (db, _ks) = open_shape(dir.path(), &["a"], &config);
        db.destroy_key_space("b").unwrap();
    }
    // Simulate a watermark persisted mid-run before the destroy: the next
    // cell to process is in b (canonical id 1).
    {
        let mut watermarks = RelocationWatermarks::read_or_create(dir.path()).unwrap();
        watermarks.data.next_to_process = Some(CellReference {
            keyspace: KeySpace::new_test(1),
            cell_id: CellId::Integer(0),
        });
        watermarks.save().unwrap();
    }
    {
        let (db, ks) = open_shape(dir.path(), &["a"], &config);
        // Resumes from the saved watermark; must skip the destroyed b.
        db.start_blocking_relocation_with_strategy(RelocationStrategy::IndexBased(None));
        assert_all(&db, ks[0], b"va");
    }
}

/// Destroy on a prefixed (tree-based) keyspace exercises the tree clearing
/// and cell-index cleanup paths.
#[test]
fn test_destroy_keyspace_prefixed() {
    let dir = tempdir::TempDir::new("test-destroy-keyspace-prefixed").unwrap();
    let config = Arc::new(Config::small());

    {
        let (db, ks) = open(dir.path(), prefixed_shape(&["a", "b"]), &config);
        write_all(&db, ks[0], b"va");
        write_all(&db, ks[1], b"vb");
        db.rebuild_control_region().unwrap();
    }
    {
        let (db, _ks) = open(dir.path(), prefixed_shape(&["a"]), &config);
        db.destroy_key_space("b").unwrap();
    }
    {
        let (db, ks) = open(dir.path(), prefixed_shape(&["a", "b"]), &config);
        assert_all(&db, ks[0], b"va");
        assert_none(&db, ks[1]);
        write_all(&db, ks[1], b"vb2");
        assert_all(&db, ks[1], b"vb2");
    }
}

/// A re-declared name is a fresh keyspace: unlike a retained keyspace, it
/// is not layout-checked against the tombstone, so the layout may change.
#[test]
fn test_redeclare_destroyed_name_with_new_layout() {
    let dir = tempdir::TempDir::new("test-destroy-keyspace-layout").unwrap();
    let config = Arc::new(Config::small());

    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        write_all(&db, ks[1], b"vb");
    }
    {
        let (db, _ks) = open_shape(dir.path(), &["a"], &config);
        db.destroy_key_space("b").unwrap();
    }
    // Reborn b: prefixed instead of uniform, different mutex count and key
    // size. A retained keyspace would panic on this layout change.
    {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("a", 4, 8, KeyType::uniform(4));
        builder.add_key_space("b", 8, 16, KeyType::prefix_uniform(2, 0));
        let (db, ks) = open(dir.path(), builder.build(), &config);
        let key = [1u8, 2, 3, 4, 5, 6, 7, 8];
        assert_eq!(None, db.get(ks[1], &key).unwrap());
        db.insert(ks[1], key.to_vec(), b"vb2".to_vec()).unwrap();
        assert_eq!(Some(b"vb2".to_vec().into()), db.get(ks[1], &key).unwrap());
    }
}

#[test]
#[should_panic(expected = "it is declared in the current shape")]
fn test_destroy_declared_keyspace_panics() {
    let dir = tempdir::TempDir::new("test-destroy-keyspace-declared").unwrap();
    let config = Arc::new(Config::small());
    let (db, _ks) = open_shape(dir.path(), &["a", "b"], &config);
    db.destroy_key_space("b").unwrap();
}

#[test]
#[should_panic(expected = "Unknown key space 'nope'")]
fn test_destroy_unknown_keyspace_panics() {
    let dir = tempdir::TempDir::new("test-destroy-keyspace-unknown").unwrap();
    let config = Arc::new(Config::small());
    let (db, _ks) = open_shape(dir.path(), &["a"], &config);
    db.destroy_key_space("nope").unwrap();
}
