use super::super::*;
use crate::config::Config;
use crate::key_shape::{KeyShapeBuilder, KeySpace, KeyType};
use crate::metrics::Metrics;
use std::sync::Arc;

#[test]
fn test_add_keyspace_at_end() {
    let dir = tempdir::TempDir::new("test-add-keyspace").unwrap();
    let config = Arc::new(Config::small());

    // Phase 1: Create DB with one keyspace named "ks1"
    let mut ks1: KeySpace;
    {
        let mut builder = KeyShapeBuilder::new();
        ks1 = builder.add_key_space("ks1", 4, 16, KeyType::uniform(16));
        let key_shape = builder.build();

        let db = Db::open(dir.path(), key_shape, config.clone(), Metrics::new()).unwrap();

        // Write some data to ks1
        db.insert(ks1, vec![1, 2, 3, 4], vec![10, 20]).unwrap();
        db.insert(ks1, vec![5, 6, 7, 8], vec![30, 40]).unwrap();

        // Create snapshot
        db.rebuild_control_region().unwrap();
    }
    // DB is closed here

    // Phase 2: Reopen DB with extended KeyShape that has ks1 + ks2 at the end
    let mut ks2: KeySpace;
    {
        let mut builder = KeyShapeBuilder::new();
        ks1 = builder.add_key_space("ks1", 4, 16, KeyType::uniform(16));
        ks2 = builder.add_key_space("ks2", 4, 16, KeyType::uniform(16));
        let key_shape = builder.build();

        let db = Db::open(dir.path(), key_shape, config.clone(), Metrics::new()).unwrap();

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
        ks1 = builder.add_key_space("ks1", 4, 16, KeyType::uniform(16));
        ks2 = builder.add_key_space("ks2", 4, 16, KeyType::uniform(16));
        let key_shape = builder.build();

        let db = Db::open(dir.path(), key_shape, config.clone(), Metrics::new()).unwrap();

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
    let mut ks1: KeySpace;
    let mut ks2: KeySpace;
    {
        let mut builder = KeyShapeBuilder::new();
        ks1 = builder.add_key_space("alpha", 4, 16, KeyType::uniform(16));
        ks2 = builder.add_key_space("beta", 4, 16, KeyType::uniform(16));
        let key_shape = builder.build();

        let db = Db::open(dir.path(), key_shape, config.clone(), Metrics::new()).unwrap();

        db.insert(ks1, vec![1, 1, 1, 1], vec![11]).unwrap();
        db.insert(ks2, vec![2, 2, 2, 2], vec![22]).unwrap();

        db.rebuild_control_region().unwrap();
    }

    // Phase 2: Add two more keyspaces at the end
    let ks3: KeySpace;
    let ks4: KeySpace;
    {
        let mut builder = KeyShapeBuilder::new();
        ks1 = builder.add_key_space("alpha", 4, 16, KeyType::uniform(16));
        ks2 = builder.add_key_space("beta", 4, 16, KeyType::uniform(16));
        ks3 = builder.add_key_space("gamma", 4, 16, KeyType::uniform(16));
        ks4 = builder.add_key_space("delta", 4, 16, KeyType::uniform(16));
        let key_shape = builder.build();

        let db = Db::open(dir.path(), key_shape, config.clone(), Metrics::new()).unwrap();

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
#[should_panic(expected = "Keyspace mismatch at position")]
fn test_cannot_reorder_keyspaces() {
    let dir = tempdir::TempDir::new("test-reorder-keyspaces").unwrap();
    let config = Arc::new(Config::small());

    // Phase 1: Create DB with keyspaces "alpha" and "beta"
    {
        let mut builder = KeyShapeBuilder::new();
        let ks1 = builder.add_key_space("alpha", 4, 16, KeyType::uniform(16));
        builder.add_key_space("beta", 4, 16, KeyType::uniform(16));
        let key_shape = builder.build();

        let db = Db::open(dir.path(), key_shape, config.clone(), Metrics::new()).unwrap();
        db.insert(ks1, vec![1, 1, 1, 1], vec![11]).unwrap();
        db.rebuild_control_region().unwrap();
    }

    // Phase 2: Try to open with reordered keyspaces ("beta", "alpha")
    // This should panic
    {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("beta", 4, 16, KeyType::uniform(16)); // Swapped!
        builder.add_key_space("alpha", 4, 16, KeyType::uniform(16)); // Swapped!
        let key_shape = builder.build();

        // This should panic because keyspaces are reordered
        let _db = Db::open(dir.path(), key_shape, config.clone(), Metrics::new()).unwrap();
    }
}

#[test]
#[should_panic(expected = "Keyspace mismatch at position")]
fn test_cannot_insert_keyspace_in_middle() {
    let dir = tempdir::TempDir::new("test-insert-middle").unwrap();
    let config = Arc::new(Config::small());

    // Phase 1: Create DB with keyspaces "alpha" and "gamma"
    {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("alpha", 4, 16, KeyType::uniform(16));
        builder.add_key_space("gamma", 4, 16, KeyType::uniform(16));
        let key_shape = builder.build();

        let db = Db::open(dir.path(), key_shape, config.clone(), Metrics::new()).unwrap();
        db.rebuild_control_region().unwrap();
    }

    // Phase 2: Try to insert "beta" in the middle
    // This should panic
    {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("alpha", 4, 16, KeyType::uniform(16));
        builder.add_key_space("beta", 4, 16, KeyType::uniform(16)); // Inserted in middle!
        builder.add_key_space("gamma", 4, 16, KeyType::uniform(16));
        let key_shape = builder.build();

        // This should panic because "beta" was inserted in the middle
        let _db = Db::open(dir.path(), key_shape, config.clone(), Metrics::new()).unwrap();
    }
}

#[test]
#[should_panic(expected = "Removing key spaces is not supported")]
fn test_cannot_remove_keyspace() {
    let dir = tempdir::TempDir::new("test-remove-keyspace").unwrap();
    let config = Arc::new(Config::small());

    // Phase 1: Create DB with two keyspaces
    {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("alpha", 4, 16, KeyType::uniform(16));
        builder.add_key_space("beta", 4, 16, KeyType::uniform(16));
        let key_shape = builder.build();

        let db = Db::open(dir.path(), key_shape, config.clone(), Metrics::new()).unwrap();
        db.rebuild_control_region().unwrap();
    }

    // Phase 2: Try to open with only one keyspace
    // This should panic
    {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("alpha", 4, 16, KeyType::uniform(16));
        let key_shape = builder.build();

        // This should panic because we removed "beta"
        let _db = Db::open(dir.path(), key_shape, config.clone(), Metrics::new()).unwrap();
    }
}
