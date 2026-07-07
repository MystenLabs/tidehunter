use super::super::*;
use crate::config::Config;
use crate::key_shape::{KeyShapeBuilder, KeyType};
use crate::metrics::Metrics;
use std::sync::Arc;

#[test]
fn test_add_keyspace_at_end() {
    let dir = tempdir::TempDir::new("test-add-keyspace").unwrap();
    let config = Arc::new(Config::small());

    // Phase 1: Create DB with one keyspace named "ks1"
    {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("ks1", 4, 16, KeyType::uniform(16));
        let db = Db::open(dir.path(), builder.build(), config.clone(), Metrics::new()).unwrap();
        let ks1 = db.ks("ks1");

        // Write some data to ks1
        db.insert(ks1, vec![1, 2, 3, 4], vec![10, 20]).unwrap();
        db.insert(ks1, vec![5, 6, 7, 8], vec![30, 40]).unwrap();

        // Create snapshot
        db.rebuild_control_region().unwrap();
    }
    // DB is closed here

    // Phase 2: Reopen DB with extended KeyShape that has ks1 + ks2 at the end
    {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("ks1", 4, 16, KeyType::uniform(16));
        builder.add_key_space("ks2", 4, 16, KeyType::uniform(16));
        let db = Db::open(dir.path(), builder.build(), config.clone(), Metrics::new()).unwrap();
        let ks1 = db.ks("ks1");
        let ks2 = db.ks("ks2");

        // Verify old data from ks1 is still accessible
        assert_eq!(
            Some(vec![10, 20].into()),
            db.get(ks1, &[1, 2, 3, 4]).unwrap()
        );
        assert_eq!(
            Some(vec![30, 40].into()),
            db.get(ks1, &[5, 6, 7, 8]).unwrap()
        );

        // Write to the new keyspace ks2
        db.insert(ks2, vec![9, 10, 11, 12], vec![50, 60]).unwrap();
        assert_eq!(
            Some(vec![50, 60].into()),
            db.get(ks2, &[9, 10, 11, 12]).unwrap()
        );

        // Verify ks1 and ks2 are independent
        assert_eq!(None, db.get(ks2, &[1, 2, 3, 4]).unwrap());
        assert_eq!(None, db.get(ks1, &[9, 10, 11, 12]).unwrap());
    }

    // Phase 3: Reopen again to verify persistence
    {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("ks1", 4, 16, KeyType::uniform(16));
        builder.add_key_space("ks2", 4, 16, KeyType::uniform(16));
        let db = Db::open(dir.path(), builder.build(), config.clone(), Metrics::new()).unwrap();
        let ks1 = db.ks("ks1");
        let ks2 = db.ks("ks2");

        // Verify all data persisted
        assert_eq!(
            Some(vec![10, 20].into()),
            db.get(ks1, &[1, 2, 3, 4]).unwrap()
        );
        assert_eq!(
            Some(vec![30, 40].into()),
            db.get(ks1, &[5, 6, 7, 8]).unwrap()
        );
        assert_eq!(
            Some(vec![50, 60].into()),
            db.get(ks2, &[9, 10, 11, 12]).unwrap()
        );
    }
}

#[test]
fn test_add_multiple_keyspaces_at_end() {
    let dir = tempdir::TempDir::new("test-add-multiple-keyspaces").unwrap();
    let config = Arc::new(Config::small());

    // Phase 1: Create DB with two keyspaces
    {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("alpha", 4, 16, KeyType::uniform(16));
        builder.add_key_space("beta", 4, 16, KeyType::uniform(16));
        let db = Db::open(dir.path(), builder.build(), config.clone(), Metrics::new()).unwrap();
        let ks1 = db.ks("alpha");
        let ks2 = db.ks("beta");

        db.insert(ks1, vec![1, 1, 1, 1], vec![11]).unwrap();
        db.insert(ks2, vec![2, 2, 2, 2], vec![22]).unwrap();

        db.rebuild_control_region().unwrap();
    }

    // Phase 2: Add two more keyspaces at the end
    {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("alpha", 4, 16, KeyType::uniform(16));
        builder.add_key_space("beta", 4, 16, KeyType::uniform(16));
        builder.add_key_space("gamma", 4, 16, KeyType::uniform(16));
        builder.add_key_space("delta", 4, 16, KeyType::uniform(16));
        let db = Db::open(dir.path(), builder.build(), config.clone(), Metrics::new()).unwrap();
        let ks1 = db.ks("alpha");
        let ks2 = db.ks("beta");
        let ks3 = db.ks("gamma");
        let ks4 = db.ks("delta");

        // Verify old data
        assert_eq!(Some(vec![11].into()), db.get(ks1, &[1, 1, 1, 1]).unwrap());
        assert_eq!(Some(vec![22].into()), db.get(ks2, &[2, 2, 2, 2]).unwrap());

        // Write to new keyspaces
        db.insert(ks3, vec![3, 3, 3, 3], vec![33]).unwrap();
        db.insert(ks4, vec![4, 4, 4, 4], vec![44]).unwrap();

        assert_eq!(Some(vec![33].into()), db.get(ks3, &[3, 3, 3, 3]).unwrap());
        assert_eq!(Some(vec![44].into()), db.get(ks4, &[4, 4, 4, 4]).unwrap());
    }
}

#[test]
fn test_insert_keyspace_in_middle() {
    let dir = tempdir::TempDir::new("test-insert-middle").unwrap();
    let config = Arc::new(Config::small());

    // Phase 1: Create DB with keyspaces "alpha" and "gamma"
    {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("alpha", 4, 16, KeyType::uniform(16));
        builder.add_key_space("gamma", 4, 16, KeyType::uniform(16));
        let db = Db::open(dir.path(), builder.build(), config.clone(), Metrics::new()).unwrap();
        let ks1 = db.ks("alpha");
        let ks2 = db.ks("gamma");
        db.insert(ks1, vec![1, 1, 1, 1], vec![11]).unwrap();
        db.insert(ks2, vec![2, 2, 2, 2], vec![22]).unwrap();
        db.rebuild_control_region().unwrap();
    }

    // Phase 2: Declare "beta" in the middle. The declared position is
    // cosmetic — "beta" is appended to the canonical order and the existing
    // keyspaces keep their data.
    {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("alpha", 4, 16, KeyType::uniform(16));
        builder.add_key_space("beta", 4, 16, KeyType::uniform(16));
        builder.add_key_space("gamma", 4, 16, KeyType::uniform(16));
        let db = Db::open(dir.path(), builder.build(), config.clone(), Metrics::new()).unwrap();
        let ks1 = db.ks("alpha");
        let ks_new = db.ks("beta");
        let ks2 = db.ks("gamma");
        assert_eq!(Some(vec![11].into()), db.get(ks1, &[1, 1, 1, 1]).unwrap());
        assert_eq!(Some(vec![22].into()), db.get(ks2, &[2, 2, 2, 2]).unwrap());
        db.insert(ks_new, vec![3, 3, 3, 3], vec![33]).unwrap();
        assert_eq!(
            Some(vec![33].into()),
            db.get(ks_new, &[3, 3, 3, 3]).unwrap()
        );
        assert_eq!(None, db.get(ks_new, &[1, 1, 1, 1]).unwrap());

        // Canonically "beta" comes last, after the keyspaces that existed before it
        let stored = Db::load_key_shape(dir.path()).unwrap();
        let stored_names: Vec<_> = stored.iter_ks().map(|k| k.name().to_string()).collect();
        assert_eq!(vec!["alpha", "gamma", "beta"], stored_names);
    }
}
