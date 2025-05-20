use super::super::*;
use crate::batch::WriteBatch;
use crate::config::Config;
use crate::crc::CrcFrame;
use crate::index::index_format::IndexFormatType;
use crate::index::uniform_lookup::UniformLookupIndex;
use crate::key_shape::{
    KeyShape, KeyShapeBuilder, KeySpace, KeySpaceConfig, KeyTranslation, KeyType,
};
use crate::metrics::Metrics;
use minibytes::Bytes;
use rand::rngs::{StdRng, ThreadRng};
use rand::{Rng, SeedableRng};
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::thread;

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
    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
    let mut batch = WriteBatch::new();
    batch.write(ks, vec![5, 6, 7, 8], vec![15]);
    batch.write(ks, vec![6, 7, 8, 9], vec![17]);
    db.write_batch(batch).unwrap();
    assert_eq!(Some(vec![15].into()), db.get(ks, &[5, 6, 7, 8]).unwrap());
    assert_eq!(Some(vec![17].into()), db.get(ks, &[6, 7, 8, 9]).unwrap());
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
        db.rebuild_control_region().unwrap();
        db.remove(ks, vec![1, 2, 3, 4]).unwrap();
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
        let key_translation = if reduced {
            KeyTranslation::key_reduction(16, 0..16)
        } else {
            KeyTranslation::none(16)
        };
        println!("Starting sequential test, reduced={reduced}");
        test_iterator_run(sequential.clone(), key_translation.clone());
        println!("Starting random test, reduced={reduced}");
        test_iterator_run(random.clone(), key_translation);
    }
}

fn test_iterator_run(data: Vec<u128>, key_translation: KeyTranslation) {
    let dir = tempdir::TempDir::new("test-iterator").unwrap();
    let config = Arc::new(Config::small());
    let (key_shape, ks) = KeyShape::new_single_config_translation(
        key_translation,
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
    for k in &data {
        db.insert(ks, ku128(*k), vu128(*k)).unwrap();
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
    let key_translation = KeyTranslation::key_reduction(4, 0..2);
    let (key_shape, ks) = KeyShape::new_single_config_translation(
        key_translation,
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
fn test_last_in_range() {
    let dir = tempdir::TempDir::new("test-last-in-range").unwrap();
    let config = Arc::new(Config::small());
    let (key_shape, ks) = KeyShape::new_single(5, 16, KeyType::uniform(16));
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();
    db.insert(ks, vec![1, 2, 3, 4, 6], vec![1]).unwrap();
    db.insert(ks, vec![1, 2, 3, 4, 5], vec![2]).unwrap();
    db.insert(ks, vec![1, 2, 3, 4, 10], vec![3]).unwrap();
    assert_eq!(
        db.last_in_range(ks, &vec![1, 2, 3, 4, 5].into(), &vec![1, 2, 3, 4, 8].into())
            .unwrap(),
        Some((vec![1, 2, 3, 4, 6].into(), vec![1].into()))
    );
    assert_eq!(
        db.last_in_range(ks, &vec![1, 2, 3, 4, 5].into(), &vec![1, 2, 3, 4, 6].into())
            .unwrap(),
        Some((vec![1, 2, 3, 4, 6].into(), vec![1].into()))
    );
    assert_eq!(
        db.last_in_range(ks, &vec![1, 2, 3, 4, 5].into(), &vec![1, 2, 3, 4, 5].into())
            .unwrap(),
        Some((vec![1, 2, 3, 4, 5].into(), vec![2].into()))
    );
    assert_eq!(
        db.last_in_range(ks, &vec![1, 2, 3, 4, 4].into(), &vec![1, 2, 3, 4, 4].into())
            .unwrap(),
        None
    );
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

#[test]
fn test_dirty_unloading() {
    let dir = tempdir::TempDir::new("test-dirty-unloading").unwrap();
    let mut config = Config::small();
    config.max_dirty_keys = 2;
    let config = Arc::new(config);
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

// This test is disabled as it takes a long time, but it should be run if logic around
// IndexTable::insert is changed.
// todo we can also rewrite this test more efficiently if we introduce
// random sleep between wal write and large_table insert during the test.
#[ignore]
#[test]
// This test verifies that the last value written into the large table
// cache matches the last value written to wal.
// Because wal write and write into large table are not done under single mutex,
// there can be race condition unless special measures are taken.
fn test_concurrent_single_value_update() {
    let num_threads = 8;
    let mut threads = Vec::with_capacity(num_threads);
    for i in 0..num_threads {
        let jh = thread::spawn(move || {
            for _ in 0..256 {
                test_concurrent_single_value_update_iteration(i)
            }
        });
        threads.push(jh);
    }
    for jh in threads {
        jh.join().unwrap();
    }
}

fn test_concurrent_single_value_update_iteration(i: usize) {
    let dir = tempdir::TempDir::new(&format!("test-concurrent-single-value-update-{i}")).unwrap();
    let config = Arc::new(Config::small());
    let (key_shape, ks) = KeyShape::new_single(4, 1, KeyType::uniform(1));
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
        let num_threads = 16;
        let mut threads = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            let db = db.clone();
            let jh = thread::spawn(move || {
                let mut rng = ThreadRng::default();
                for _ in 0..1024 {
                    let key = Bytes::from(15u32.to_be_bytes().to_vec());
                    let value: u32 = rng.gen();
                    db.insert(ks, key.clone(), value.to_be_bytes().to_vec())
                        .unwrap();
                }
            });
            threads.push(jh);
        }
        for jh in threads {
            jh.join().unwrap();
        }
        cached_value = db.get(ks, &key).unwrap().unwrap();
    }
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            Metrics::new(),
        )
        .unwrap();
        let replay_value = db.get(ks, &key).unwrap().unwrap();
        assert_eq!(replay_value, cached_value);
    }
}

#[test]
fn test_key_reduction() {
    let dir = tempdir::TempDir::new("test_key_reduction").unwrap();
    let config = Arc::new(Config::small());
    let key_translation = KeyTranslation::key_reduction(4, 0..2);
    let (key_shape, ks) = KeyShape::new_single_config_translation(
        key_translation,
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

    // Last in range tests
    let (k, v) = db
        .last_in_range(
            ks,
            &Bytes::from(vec![1, 3, 3, 4]),
            &Bytes::from(vec![1, 3, 3, 4]),
        )
        .unwrap()
        .unwrap();
    assert_eq!(k.as_ref(), &[1, 3, 3, 4]);
    assert_eq!(v.as_ref(), &[2]);

    let r = db
        .last_in_range(
            ks,
            &Bytes::from(vec![1, 3, 6, 7]),
            &Bytes::from(vec![1, 3, 6, 7]),
        )
        .unwrap();
    assert_eq!(None, r);

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
    let key_translation = KeyTranslation::key_reduction(4, 0..2);
    let ks_config = KeySpaceConfig::new().with_value_cache_size(2);
    let (key_shape, ks) =
        KeyShape::new_single_config_translation(key_translation, 1, KeyType::uniform(1), ks_config);
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
