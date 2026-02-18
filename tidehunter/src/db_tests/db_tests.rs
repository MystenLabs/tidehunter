use super::super::*;
use crate::config::Config;
use crate::crc::CrcFrame;
use crate::failpoints::FailPoint;
use crate::index::index_format::IndexFormatType;
use crate::index::uniform_lookup::UniformLookupIndex;
use crate::key_shape::{KeyIndexing, KeyShape, KeyShapeBuilder, KeySpace, KeySpaceConfig, KeyType};
use crate::latch::Latch;
use crate::metrics::Metrics;
use hex_literal::hex;
use minibytes::Bytes;
use rand::rngs::{StdRng, ThreadRng};
use rand::{Rng, SeedableRng};
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::time::Duration;
use std::{thread, usize};

// see generate.py
pub(super) fn db_test((key_shape, ks): (KeyShape, KeySpace)) {
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
        db.insert(ks, vec![3, 4, 5, 6], vec![9]).unwrap();
        assert_eq!(Some(vec![5, 6].into()), db.get(ks, &[1, 2, 3, 4]).unwrap());
        assert_eq!(Some(vec![9].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
    }
}

#[test]
fn test_multi_thread_write() {
    let dir = tempdir::TempDir::new("test-batch").unwrap();
    let config = Config::small();
    let config = Arc::new(config);
    let (key_shape, ks) = KeyShape::new_single(8, 16, KeyType::uniform(16));
    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
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

#[test]
fn test_batch() {
    let dir = tempdir::TempDir::new("test-batch").unwrap();
    let config = Arc::new(Config::small());
    let (key_shape, ks) = KeyShape::new_single(4, 16, KeyType::uniform(16));
    let metrics = Metrics::new();
    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
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

    // Check pending_table_len after commit but before promote_pending
    let pending_len = metrics.pending_table_len.with_label_values(&["root"]).get();
    assert_eq!(
        pending_len, 2,
        "Should still have 2 pending entries after commit"
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
    let ks = ksb.add_key_space_config("ks", 4, 16, KeyType::uniform(16), ksc);
    let key_shape = ksb.build();

    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();

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
    let dir = tempdir::TempDir::new("test_batch_replay").unwrap();
    let config = Arc::new(Config::small());
    let (key_shape, ks) = KeyShape::new_single(4, 16, KeyType::uniform(16));
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        let mut batch = db.write_batch();
        batch.write(ks, vec![5, 6, 7, 8], vec![15]);
        batch.write(ks, vec![6, 7, 8, 9], vec![17]);
        batch.commit().unwrap();
    }
    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
    assert_eq!(Some(vec![15].into()), db.get(ks, &[5, 6, 7, 8]).unwrap());
    assert_eq!(Some(vec![17].into()), db.get(ks, &[6, 7, 8, 9]).unwrap());
}

#[test]
fn test_corrupted_batch_replay() {
    let dir = tempdir::TempDir::new("test_corrupted_batch_replay").unwrap();
    let config = Arc::new(Config::small());
    let (key_a, key_b) = (vec![5, 6, 7, 8], vec![6, 7, 8, 9]);
    let (value_a, value_b) = (vec![15], vec![17]);

    let (key_shape, ks) = KeyShape::new_single(4, 16, KeyType::uniform(16));
    let (position, file) = {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
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
    file.write_all_at(&mut data, position).unwrap();

    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
    assert_eq!(Some(value_a.into()), db.get(ks, &key_a).unwrap());
    assert_eq!(Some(value_b.into()), db.get(ks, &key_b).unwrap());
}

#[test]
fn test_concurrent_batch() {
    let dir = tempdir::TempDir::new("test_concurrent_batch").unwrap();
    let config = Arc::new(Config::small());
    let ksc = KeySpaceConfig::new().with_value_cache_size(10);
    let (key_shape, ks) = KeyShape::new_single_config(1, 16, KeyType::uniform(16), ksc);
    let (key_a, key_b, key_c) = (vec![15], vec![16], vec![17]);
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
pub(super) fn test_remove((key_shape, ks): (KeyShape, KeySpace)) {
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
        assert_eq!(metrics.replayed_wal_records.get(), 1);
        assert_eq!(None, db.get(ks, &[1, 2, 3, 4]).unwrap());
        assert_eq!(Some(vec![7].into()), db.get(ks, &[3, 4, 5, 6]).unwrap());
    }
}

// see generate.py
pub(super) fn test_iterator((key_shape, ks): (KeyShape, KeySpace)) {
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
    let (key_shape, ks) = KeyShape::new_single_config_indexing(
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
        Box::new(slice.into_iter().rev())
    } else {
        Box::new(slice.into_iter())
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
    let (key_shape, ks) = KeyShape::new_single(2, 16, key_type);

    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();

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
    let (key_shape, ks) = KeyShape::new_single(5, 16, KeyType::uniform(16));
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
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
            v.get(0).unwrap(),
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
        let mut it = db.iterator(ks);
        it.set_lower_bound(vec![1, 2, 3, 4, 0]);
        it.set_upper_bound(vec![1, 2, 3, 4, 10]);
        let v: DbResult<Vec<_>> = it.collect();
        let v = v.unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(
            v.get(0).unwrap(),
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
    let (key_shape, ks) = KeyShape::new_single(2, 16, KeyType::uniform(16));
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();
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
    let (key_shape, ks) = KeyShape::new_single(5, 16, KeyType::uniform(16));
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();
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
    let (key_shape, ks) = KeyShape::new_single(4, 16, KeyType::uniform(16));

    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();

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
    let (key_shape, ks) = KeyShape::new_single_config_indexing(
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
    let (key_shape, ks) = KeyShape::new_single(5, 16, KeyType::uniform(16));
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
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
    let ks0 = ksb.add_key_space("a", 0, 16, KeyType::uniform(16));
    let ks1 = ksb.add_key_space("b", 1, 16, KeyType::uniform(16));
    let ks2 = ksb.add_key_space("c", 2, 16, KeyType::uniform(16));
    let _ks3 = ksb.add_key_space("d", 3, 16, KeyType::uniform(16));
    let key_shape = ksb.build();
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
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
    let ks = ksb.add_key_space_config("k", 8, 1, KeyType::uniform(1), ksc);
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();

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
    let ks = ksb.add_key_space_config("k", 8, 1, KeyType::uniform(1), ksc);
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();

    for i in 0..1000u64 {
        db.insert(ks, i.to_be_bytes().to_vec(), vec![]).unwrap();
    }

    for i in 0..1000u64 {
        assert!(db.exists(ks, &i.to_be_bytes()).unwrap());
    }

    for i in 1000..2000u64 {
        assert!(!db.exists(ks, &i.to_be_bytes()).unwrap());
    }
    let found = metrics
        .lookup_result
        .with_label_values(&["k", "found", "cache"])
        .get()
        + metrics
            .lookup_result
            .with_label_values(&["k", "found", "lookup"])
            .get();
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
    let (key_shape, ks) = KeyShape::new_single(5, 2, KeyType::uniform(1024));
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
    let ks = ksb.add_key_space_config("k", 1, 1, KeyType::uniform(1), ksc);
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();
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
    let (key_shape, ks) = KeyShape::new_single_config(4, 1, KeyType::uniform(1), ks_config);
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
    let (key_shape, ks) = KeyShape::new_single_config_indexing(
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
    let (key_shape, ks) =
        KeyShape::new_single_config_indexing(key_indexing, 1, KeyType::uniform(1), ks_config);
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();

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
    let (key_shape, ks) = KeyShape::new_single(32, 16, key_type);
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();
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

pub(super) fn default_key_shape() -> (KeyShape, KeySpace) {
    KeyShape::new_single(4, 16, KeyType::uniform(16))
}

pub(super) fn prefix_key_shape() -> (KeyShape, KeySpace) {
    KeyShape::new_single(4, 16, KeyType::prefix_uniform(2, 0))
}

pub(super) fn hashed_index_key_shape() -> (KeyShape, KeySpace) {
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
    let ks1 = builder.add_key_space("lookup_header", 4, 16, KeyType::uniform(16));

    // Second key space with UniformLookup index format
    let uniform_index = UniformLookupIndex::new();
    let ks2_config =
        KeySpaceConfig::default().with_index_format(IndexFormatType::Uniform(uniform_index));
    let ks2 =
        builder.add_key_space_config("uniform_lookup", 4, 16, KeyType::uniform(16), ks2_config);

    (builder.build(), ks1, ks2)
}

pub(super) fn prefix_two_key_spaces() -> (KeyShape, KeySpace, KeySpace) {
    let mut builder = KeyShapeBuilder::new();
    let ks1 = builder.add_key_space("lookup_header", 4, 16, KeyType::prefix_uniform(2, 0));
    // Second key space with UniformLookup index format
    let uniform_index = UniformLookupIndex::new();
    let ks2_config =
        KeySpaceConfig::default().with_index_format(IndexFormatType::Uniform(uniform_index));
    let ks2 = builder.add_key_space_config(
        "prefix_lookup",
        4,
        16,
        KeyType::prefix_uniform(2, 0),
        ks2_config,
    );
    (builder.build(), ks1, ks2)
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
    let (key_shape, ks) = KeyShape::new_single(2, 16, KeyType::uniform(16));

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
    file.write_all_at(&mut data, position).unwrap();

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
    let (key_shape, ks) = KeyShape::new_single(2, 16, KeyType::uniform(16));

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
    file.write_all_at(&mut data, last_position).unwrap();

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
    let (key_shape, ks) = KeyShape::new_single(2, 16, KeyType::uniform(16));

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
    let (key_shape, ks) = KeyShape::new_single(2, 16, KeyType::uniform(16));

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
    let (key_shape, ks) = KeyShape::new_single(4, 16, KeyType::uniform(16));

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
    let ks = ksb.add_key_space_config("k", 8, 1, KeyType::uniform(1), ksc);
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
            .with_label_values(&["k", "found", "lookup"])
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
    let ks_config = KeySpaceConfig::default()
        // todo unloaded iterator is not supported
        // .with_unloaded_iterator(true)
        .with_max_dirty_keys(1);
    let (key_shape, ks) = KeyShape::new_single_config_indexing(
        KeyIndexing::VariableLength,
        16,
        KeyType::uniform(1),
        ks_config,
    );
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

#[test]
fn test_reverse_iterator_without_bounds() {
    // Test that reverse iterator works without setting any bounds
    let dir = tempdir::TempDir::new("test-reverse-no-bounds").unwrap();
    let config = Arc::new(Config::small());
    let (key_shape, ks) = KeyShape::new_single(8, 16, KeyType::uniform(16));

    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();

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

#[ignore]
#[test]
fn test_force_rebuild_control_region() {
    let dir = tempdir::TempDir::new("test-force-rebuild").unwrap();
    let config = Arc::new(Config::small());
    let (key_shape, ks) = KeyShape::new_single(4, 16, KeyType::uniform(16));

    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();

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
    let ks = builder.add_key_space("test", 8, 16, KeyType::uniform(16));
    // Make sure ks that only was written once does not affect forced snapshot
    let ks2 = builder.add_key_space("small", 8, 16, KeyType::uniform(16));
    // Make sure empty key space does not affect forced snapshot
    let _ks2 = builder.add_key_space("empty", 8, 16, KeyType::uniform(16));
    let key_shape = builder.build();

    let metrics = Metrics::new();

    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .expect("open failed");

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
    let config = Arc::new(config);
    let (key_shape, ks) = KeyShape::new_single(2, 1, KeyType::uniform(1));

    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();

    const SLEEP: u64 = 200;

    db.insert(ks, vec![1, 2], vec![5, 6]).unwrap();
    db.wal_writer.wal_tracker_barrier();
    db.force_rebuild_control_region().unwrap();
    assert!(db.large_table.is_all_clean());
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
    let (key_shape, ks) = hashed_index_key_shape();
    let metrics = Metrics::new();

    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();

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
}

#[test]
fn test_batch_after_incomplete_batch() {
    let dir = tempdir::TempDir::new("test_wal_failpoint_panic_during_batch").unwrap();
    let config = Arc::new(Config::small());
    let (key_shape, ks) = KeyShape::new_single(4, 16, KeyType::uniform(16));

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
    let (key_shape, ks) = KeyShape::new_single(4, 16, KeyType::uniform(16));

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
    let (key_shape, ks) = KeyShape::new_single(10, 2, KeyType::uniform(2));
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
fn test_drop_cells_in_range_prefixed_uniform() {
    let dir = tempdir::TempDir::new("test-drop-cells-prefixed").unwrap();
    let (key_shape, ks) = KeyShape::new_single(10, 16, KeyType::from_prefix_bits(16));
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
