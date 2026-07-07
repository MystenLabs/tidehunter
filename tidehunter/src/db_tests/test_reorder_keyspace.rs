use super::super::*;
use crate::config::Config;
use crate::key_shape::{KeyShape, KeyShapeBuilder, KeySpace, KeyType};
use crate::metrics::Metrics;
use std::path::Path;
use std::sync::Arc;

fn shape(names: &[&str]) -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    for name in names {
        builder.add_key_space(*name, 4, 8, KeyType::uniform(4));
    }
    builder.build()
}

/// Opens the db with the given declared keyspace order and resolves the
/// canonical handles by name, returned in declared order so callers index
/// `ks[i]` by declaration position.
fn open_shape(dir: &Path, names: &[&str], config: &Arc<Config>) -> (Arc<Db>, Vec<KeySpace>) {
    let db = Db::open(dir, shape(names), config.clone(), Metrics::new()).unwrap();
    let ks = names.iter().map(|name| db.ks(name)).collect();
    (db, ks)
}

/// The same key is written to every keyspace with a per-keyspace value, so a
/// handle routed to the wrong keyspace returns a wrong value instead of None.
const KEY: [u8; 4] = [1, 2, 3, 4];

#[test]
fn test_reorder_keyspaces() {
    let dir = tempdir::TempDir::new("test-reorder-keyspace").unwrap();
    let config = Arc::new(Config::small());

    // Phase 1: create as a/b/c, snapshot
    {
        let (db, ks) = open_shape(dir.path(), &["a", "b", "c"], &config);
        db.insert(ks[0], KEY.to_vec(), b"va".to_vec()).unwrap();
        db.insert(ks[1], KEY.to_vec(), b"vb".to_vec()).unwrap();
        db.insert(ks[2], KEY.to_vec(), b"vc".to_vec()).unwrap();
        db.rebuild_control_region().unwrap();
    }

    // Phase 2: reopen as a/c/b — handles must resolve by name
    {
        let (db, ks) = open_shape(dir.path(), &["a", "c", "b"], &config);
        assert_eq!(Some(b"va".to_vec().into()), db.get(ks[0], &KEY).unwrap());
        assert_eq!(Some(b"vc".to_vec().into()), db.get(ks[1], &KEY).unwrap());
        assert_eq!(Some(b"vb".to_vec().into()), db.get(ks[2], &KEY).unwrap());
        assert_eq!("a", db.ks_name(ks[0]));
        assert_eq!("c", db.ks_name(ks[1]));
        assert_eq!("b", db.ks_name(ks[2]));
        // New write through a reordered handle lands in the right keyspace
        db.insert(ks[1], vec![5, 6, 7, 8], b"vc2".to_vec()).unwrap();
        db.remove(ks[2], KEY.to_vec()).unwrap();
    }

    // Phase 3: reopen as c/a/b — WAL replay of phase 2 writes resolves correctly
    {
        let (db, ks) = open_shape(dir.path(), &["c", "a", "b"], &config);
        assert_eq!(Some(b"vc".to_vec().into()), db.get(ks[0], &KEY).unwrap());
        assert_eq!(
            Some(b"vc2".to_vec().into()),
            db.get(ks[0], &[5, 6, 7, 8]).unwrap()
        );
        assert_eq!(Some(b"va".to_vec().into()), db.get(ks[1], &KEY).unwrap());
        assert_eq!(None, db.get(ks[2], &KEY).unwrap());
        assert_eq!(None, db.get(ks[1], &[5, 6, 7, 8]).unwrap());
    }
}

#[test]
fn test_reorder_keyspaces_wal_replay_only() {
    let dir = tempdir::TempDir::new("test-reorder-keyspace-wal").unwrap();
    let config = Arc::new(Config::small());

    // No snapshot: the reopen below relies on shape_v2.yaml alone for the
    // canonical order and replays everything from the WAL.
    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        db.insert(ks[0], KEY.to_vec(), b"va".to_vec()).unwrap();
        db.insert(ks[1], KEY.to_vec(), b"vb".to_vec()).unwrap();
    }
    {
        let (db, ks) = open_shape(dir.path(), &["b", "a"], &config);
        assert_eq!(Some(b"vb".to_vec().into()), db.get(ks[0], &KEY).unwrap());
        assert_eq!(Some(b"va".to_vec().into()), db.get(ks[1], &KEY).unwrap());
    }
}

#[test]
fn test_reorder_and_add_keyspace() {
    let dir = tempdir::TempDir::new("test-reorder-add-keyspace").unwrap();
    let config = Arc::new(Config::small());

    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        db.insert(ks[0], KEY.to_vec(), b"va".to_vec()).unwrap();
        db.insert(ks[1], KEY.to_vec(), b"vb".to_vec()).unwrap();
        db.rebuild_control_region().unwrap();
    }

    // Reopen with a new keyspace "c" declared first: canonical order stays
    // a/b with c appended.
    {
        let (db, ks) = open_shape(dir.path(), &["c", "b", "a"], &config);
        assert_eq!(None, db.get(ks[0], &KEY).unwrap());
        assert_eq!(Some(b"vb".to_vec().into()), db.get(ks[1], &KEY).unwrap());
        assert_eq!(Some(b"va".to_vec().into()), db.get(ks[2], &KEY).unwrap());
        db.insert(ks[0], KEY.to_vec(), b"vc".to_vec()).unwrap();
    }

    // The stored shape now ends with "c"; reopening in canonical order works
    // and resolves the same data.
    {
        let stored = Db::load_key_shape(dir.path()).unwrap();
        let stored_names: Vec<_> = stored.iter_ks().map(|k| k.name().to_string()).collect();
        assert_eq!(vec!["a", "b", "c"], stored_names);

        let (db, ks) = open_shape(dir.path(), &["a", "b", "c"], &config);
        assert_eq!(Some(b"va".to_vec().into()), db.get(ks[0], &KEY).unwrap());
        assert_eq!(Some(b"vb".to_vec().into()), db.get(ks[1], &KEY).unwrap());
        assert_eq!(Some(b"vc".to_vec().into()), db.get(ks[2], &KEY).unwrap());
    }
}

#[test]
fn test_reorder_keyspaces_batch_and_iterator() {
    let dir = tempdir::TempDir::new("test-reorder-keyspace-batch").unwrap();
    let config = Arc::new(Config::small());

    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        db.insert(ks[0], vec![1, 0, 0, 0], b"a1".to_vec()).unwrap();
        db.insert(ks[1], vec![2, 0, 0, 0], b"b1".to_vec()).unwrap();
    }
    {
        let (db, ks) = open_shape(dir.path(), &["b", "a"], &config);

        let mut batch = db.write_batch();
        batch.write(ks[1], vec![3, 0, 0, 0], b"a2".to_vec());
        batch.delete(ks[0], vec![2, 0, 0, 0]);
        batch.commit().unwrap();

        let a_entries: DbResult<Vec<_>> = db.iterator(ks[1]).collect();
        let a_entries = a_entries.unwrap();
        assert_eq!(
            vec![
                (vec![1, 0, 0, 0].into(), b"a1".to_vec().into()),
                (vec![3, 0, 0, 0].into(), b"a2".to_vec().into()),
            ],
            a_entries
        );

        let b_entries: DbResult<Vec<_>> = db.iterator(ks[0]).collect();
        assert!(b_entries.unwrap().is_empty());
    }
}

#[test]
fn test_legacy_db_first_open_creates_registry() {
    // Simulates a database created before the shape_v2.yaml registry
    // existed: the first open with the original keyspace order creates the
    // registry, after which reordering works.
    let dir = tempdir::TempDir::new("test-reorder-legacy").unwrap();
    let config = Arc::new(Config::small());

    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        db.insert(ks[0], KEY.to_vec(), b"va".to_vec()).unwrap();
        db.insert(ks[1], KEY.to_vec(), b"vb".to_vec()).unwrap();
    }
    std::fs::remove_file(Db::shape_v2_file_path(dir.path())).unwrap();
    {
        let (_db, _ks) = open_shape(dir.path(), &["a", "b"], &config);
        assert!(Db::shape_v2_file_path(dir.path()).exists());
    }
    {
        let (db, ks) = open_shape(dir.path(), &["b", "a"], &config);
        assert_eq!(Some(b"vb".to_vec().into()), db.get(ks[0], &KEY).unwrap());
        assert_eq!(Some(b"va".to_vec().into()), db.get(ks[1], &KEY).unwrap());
    }
}

#[test]
#[should_panic(expected = "Keyspace mismatch at canonical position")]
fn test_legacy_db_reorder_without_registry_panics() {
    // Without a registry the declared order is taken as canonical, so a
    // reordered declaration disagrees with the control region and must be
    // rejected before any data is written.
    let dir = tempdir::TempDir::new("test-reorder-legacy-panic").unwrap();
    let config = Arc::new(Config::small());

    {
        let (db, _ks) = open_shape(dir.path(), &["a", "b"], &config);
        db.rebuild_control_region().unwrap();
    }
    std::fs::remove_file(Db::shape_v2_file_path(dir.path())).unwrap();
    let _db = Db::open(dir.path(), shape(&["b", "a"]), config, Metrics::new());
}

#[test]
fn test_main_compat_same_shape_upgrade() {
    // A node upgrading from a build that predates shape_v2.yaml: its control
    // region has populated keyspace_names (written on every snapshot). The
    // upgrade contract is "open with the SAME shape works; a changed shape
    // panics" — never silent misroute. This covers the works-unchanged side;
    // the panic-on-reorder side is test_legacy_db_reorder_without_registry_panics.
    let dir = tempdir::TempDir::new("test-main-compat").unwrap();
    let config = Arc::new(Config::small());

    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        db.insert(ks[0], KEY.to_vec(), b"va".to_vec()).unwrap();
        db.insert(ks[1], KEY.to_vec(), b"vb".to_vec()).unwrap();
        // Snapshot writes the control region with keyspace_names populated,
        // as any regularly-running node would have.
        db.rebuild_control_region().unwrap();
    }
    // Simulate a database that never had a shape_v2.yaml registry.
    std::fs::remove_file(Db::shape_v2_file_path(dir.path())).unwrap();
    // Reopen with the unchanged shape: must work, data intact, registry created.
    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        assert_eq!(Some(b"va".to_vec().into()), db.get(ks[0], &KEY).unwrap());
        assert_eq!(Some(b"vb".to_vec().into()), db.get(ks[1], &KEY).unwrap());
        assert!(Db::shape_v2_file_path(dir.path()).exists());
    }
}

#[test]
fn test_remove_keyspace() {
    let dir = tempdir::TempDir::new("test-remove-keyspace").unwrap();
    let config = Arc::new(Config::small());

    {
        let (db, ks) = open_shape(dir.path(), &["a", "b", "c"], &config);
        db.insert(ks[0], KEY.to_vec(), b"va".to_vec()).unwrap();
        db.insert(ks[1], KEY.to_vec(), b"vb".to_vec()).unwrap();
        db.insert(ks[2], KEY.to_vec(), b"vc".to_vec()).unwrap();
        db.rebuild_control_region().unwrap();
    }

    // Omit "b": it is retained (not destroyed), a/c keep their data, and the
    // canonical registry is unchanged — "b" still listed at its original id.
    {
        let (db, ks) = open_shape(dir.path(), &["a", "c"], &config);
        assert_eq!(Some(b"va".to_vec().into()), db.get(ks[0], &KEY).unwrap());
        assert_eq!(Some(b"vc".to_vec().into()), db.get(ks[1], &KEY).unwrap());

        let stored = Db::load_key_shape(dir.path()).unwrap();
        let stored_names: Vec<_> = stored.iter_ks().map(|k| k.name().to_string()).collect();
        assert_eq!(vec!["a", "b", "c"], stored_names);
    }

    // Re-declaring "b" reconnects to the retained keyspace — its data is back.
    {
        let (db, ks) = open_shape(dir.path(), &["a", "c", "b"], &config);
        assert_eq!(Some(b"vb".to_vec().into()), db.get(ks[2], &KEY).unwrap());
        assert_eq!(Some(b"va".to_vec().into()), db.get(ks[0], &KEY).unwrap());
        assert_eq!(Some(b"vc".to_vec().into()), db.get(ks[1], &KEY).unwrap());
        // A write through the reconnected handle lands in the same keyspace.
        db.insert(ks[2], vec![9, 9, 9, 9], b"vb2".to_vec()).unwrap();
    }

    // The reconnected "b" (and its new write) persist; registry still [a,b,c].
    {
        let (db, ks) = open_shape(dir.path(), &["a", "b", "c"], &config);
        assert_eq!(Some(b"vb".to_vec().into()), db.get(ks[1], &KEY).unwrap());
        assert_eq!(
            Some(b"vb2".to_vec().into()),
            db.get(ks[1], &[9, 9, 9, 9]).unwrap()
        );

        let stored = Db::load_key_shape(dir.path()).unwrap();
        let stored_names: Vec<_> = stored.iter_ks().map(|k| k.name().to_string()).collect();
        assert_eq!(vec!["a", "b", "c"], stored_names);
    }
}

#[test]
fn test_remove_keyspace_wal_replay_only() {
    // Removal without a snapshot: a retained keyspace's records (including
    // batch records) must still be replayed so re-declaring it reconnects to
    // intact data.
    let dir = tempdir::TempDir::new("test-remove-keyspace-wal").unwrap();
    let config = Arc::new(Config::small());

    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        db.insert(ks[0], KEY.to_vec(), b"va".to_vec()).unwrap();
        db.insert(ks[1], KEY.to_vec(), b"vb".to_vec()).unwrap();
        let mut batch = db.write_batch();
        batch.write(ks[1], vec![5, 6, 7, 8], b"vb2".to_vec());
        batch.commit().unwrap();
    }
    // Reopen without "b": it is retained and replayed, just not exposed.
    {
        let (db, ks) = open_shape(dir.path(), &["a"], &config);
        assert_eq!(Some(b"va".to_vec().into()), db.get(ks[0], &KEY).unwrap());
    }
    // Re-declare "b": all of its data (direct + batch writes) is intact.
    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        assert_eq!(Some(b"vb".to_vec().into()), db.get(ks[1], &KEY).unwrap());
        assert_eq!(
            Some(b"vb2".to_vec().into()),
            db.get(ks[1], &[5, 6, 7, 8]).unwrap()
        );
        let b_entries: DbResult<Vec<_>> = db.iterator(ks[1]).collect();
        assert_eq!(2, b_entries.unwrap().len());
    }
}

#[test]
fn test_open_with_loaded_shape_roundtrip() {
    // `load_key_shape` returns the full canonical registry — including
    // keyspaces that are currently removed (retained). Opening with that
    // shape is an identity (no registry rewrite) and re-exposes the retained
    // keyspaces with their data intact. This is what tools like tideconsole
    // do.
    let dir = tempdir::TempDir::new("test-loaded-shape").unwrap();
    let config = Arc::new(Config::small());

    {
        let (db, ks) = open_shape(dir.path(), &["a", "b"], &config);
        db.insert(ks[0], KEY.to_vec(), b"va".to_vec()).unwrap();
        db.insert(ks[1], KEY.to_vec(), b"vb".to_vec()).unwrap();
    }
    // Remove "b" (retained, not exposed).
    {
        let (_db, _ks) = open_shape(dir.path(), &["a"], &config);
    }

    let stored = Db::load_key_shape(dir.path()).unwrap();
    let names: Vec<_> = stored.iter_ks().map(|k| k.name().to_string()).collect();
    assert_eq!(vec!["a", "b"], names);

    // Open with the loaded full shape: "b" is re-exposed with its data, and
    // the registry is unchanged.
    {
        let db = Db::open(dir.path(), stored, config.clone(), Metrics::new()).unwrap();
        assert_eq!(
            Some(b"va".to_vec().into()),
            db.get(db.ks("a"), &KEY).unwrap()
        );
        assert_eq!(
            Some(b"vb".to_vec().into()),
            db.get(db.ks("b"), &KEY).unwrap()
        );
    }
    let reopened: Vec<_> = Db::load_key_shape(dir.path())
        .unwrap()
        .iter_ks()
        .map(|k| k.name().to_string())
        .collect();
    assert_eq!(vec!["a", "b"], reopened);
}

#[test]
#[should_panic(expected = "Duplicate key space name")]
fn test_duplicate_keyspace_name_panics() {
    shape(&["a", "a"]);
}

/// Opens (or creates) a single-keyspace "a" database with the given layout.
/// Returns the result so the layout check — which panics inside `Db::open` —
/// surfaces to the caller.
fn open_a(
    dir: &Path,
    config: &Arc<Config>,
    key_size: usize,
    mutexes: usize,
    key_type: KeyType,
) -> DbResult<Arc<Db>> {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("a", key_size, mutexes, key_type);
    Db::open(dir, builder.build(), config.clone(), Metrics::new())
}

#[test]
#[should_panic(expected = "was created with a different layout")]
fn test_change_mutex_count_panics() {
    let dir = tempdir::TempDir::new("test-layout-mutexes").unwrap();
    let config = Arc::new(Config::small());
    open_a(dir.path(), &config, 4, 8, KeyType::uniform(4)).unwrap();
    // Reopen "a" with a different mutex count — misroutes existing data.
    let _ = open_a(dir.path(), &config, 4, 16, KeyType::uniform(4));
}

#[test]
#[should_panic(expected = "was created with a different layout")]
fn test_change_cells_per_mutex_panics() {
    let dir = tempdir::TempDir::new("test-layout-cells").unwrap();
    let config = Arc::new(Config::small());
    open_a(dir.path(), &config, 4, 8, KeyType::uniform(4)).unwrap();
    let _ = open_a(dir.path(), &config, 4, 8, KeyType::uniform(8));
}

#[test]
#[should_panic(expected = "was created with a different layout")]
fn test_change_key_length_panics() {
    let dir = tempdir::TempDir::new("test-layout-keylen").unwrap();
    let config = Arc::new(Config::small());
    open_a(dir.path(), &config, 4, 8, KeyType::uniform(4)).unwrap();
    let _ = open_a(dir.path(), &config, 8, 8, KeyType::uniform(4));
}

#[test]
#[should_panic(expected = "was created with a different layout")]
fn test_change_key_type_panics() {
    let dir = tempdir::TempDir::new("test-layout-keytype").unwrap();
    let config = Arc::new(Config::small());
    open_a(dir.path(), &config, 4, 8, KeyType::uniform(4)).unwrap();
    let _ = open_a(dir.path(), &config, 4, 8, KeyType::prefix_uniform(2, 0));
}

#[test]
fn test_same_layout_reopens_fine() {
    // The layout check must not reject an unchanged reopen.
    let dir = tempdir::TempDir::new("test-layout-ok").unwrap();
    let config = Arc::new(Config::small());
    open_a(dir.path(), &config, 4, 8, KeyType::uniform(4)).unwrap();
    open_a(dir.path(), &config, 4, 8, KeyType::uniform(4)).unwrap();
}
