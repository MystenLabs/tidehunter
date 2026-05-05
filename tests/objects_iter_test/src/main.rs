//! Stress reproducer for the dirty-overlay-loss bug observed in Sui's
//! `perpetual.objects` table.
//!
//! ## Background
//! Sui keys the `objects` table with `(ObjectID, VersionNumber)` — a 40-byte
//! key (`32` byte id + `8` byte big-endian version) — and reads the latest
//! version of an object with
//! `reversed_safe_iter_with_bounds(min_for_id, max_for_id).next()`.
//!
//! In production this iterator was observed returning a stale on-disk
//! `RECORD` instead of a newer `RECORD` (or a newer Sui-level
//! `StoreObject::Deleted` tombstone) that was supposed to be in the
//! in-memory dirty overlay. The cell's overlay was empty at read time
//! because `retain_unprocessed` cleared `flat` unconditionally while a
//! still-pending write had been promoted into `flat` between the flush
//! snapshot capture and `update_flushed_index` (see commit
//! `[tidehunter] Preserve unprocessed flat entries in retain_unprocessed`).
//!
//! ## What this binary does
//! - Mirrors prod key shape: `KeyIndexing::fixed(40)`, `KeyType::uniform(1)`,
//!   small mutex count to concentrate cells, and the same
//!   "keep latest version per 32-byte id" compactor as
//!   `authority_store_tables.rs`.
//! - Hot pool of object ids (default 64) with multiple versions per id, so
//!   one cell sees the `(id, vN)`/`(id, vN+1)`/`REMOVE` interleavings the
//!   bug needs.
//! - Workload mix per op: write live record, write tombstone-shaped record,
//!   prune older versions with `db.remove`, and reverse-range read of
//!   "latest version of id" verified against an in-memory shadow.
//! - Aggressive triggers: tiny `max_dirty_keys`, frequent
//!   `test_promote_flat_force`, occasional `start_relocation`, occasional
//!   db restart.
//!
//! ## Failure mode
//! Any read where the reverse iterator's first result for an id disagrees
//! with the shadow's highest live version is the bug.

use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use parking_lot::{Mutex, RwLock};
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use tidehunter::Decision;
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::key_shape::{KeyShape, KeyShapeBuilder, KeySpace, KeySpaceConfig, KeyType};
use tidehunter::metrics::Metrics;
use tidehunter::minibytes::Bytes;

/// Number of distinct 32-byte object ids in the hot pool.
const NUM_OBJECTS: usize = 64;
/// Worker thread count.
const NUM_THREADS: usize = 8;
/// Operations per thread.
const OPS_PER_THREAD: usize = 200_000;
/// Object id length, matches `ObjectID` size.
const OID_SIZE: usize = 32;
/// Version length (big-endian u64), matches `VersionNumber` serialization.
const VERSION_SIZE: usize = 8;
/// Full key length: 32 + 8.
const KEY_SIZE: usize = OID_SIZE + VERSION_SIZE;

/// First byte of a "live" record value. Matches a hypothetical
/// `StoreObject::Live` discriminant.
const TAG_LIVE: u8 = 0x01;
/// First byte of a "tombstone-record" value (Sui's `StoreObject::Deleted`).
/// Sui writes a *new RECORD* at a higher version with this payload rather
/// than issuing a REMOVE; tidehunter has no value-level tombstone concept,
/// so this is just a 2-byte record from the engine's perspective.
const TAG_TOMBSTONE: u8 = 0x02;

type ShadowMap = Arc<Mutex<HashMap<[u8; OID_SIZE], BTreeMap<u64, Vec<u8>>>>>;
type IdLockMap = Arc<Mutex<HashMap<[u8; OID_SIZE], Arc<Mutex<()>>>>>;

/// Per-id mutex so concurrent ops on different ids run in parallel but
/// ops on the same id (write/read/prune) are atomic w.r.t. the shadow.
#[derive(Clone)]
struct IdLockManager {
    locks: IdLockMap,
}

impl IdLockManager {
    fn new() -> Self {
        Self {
            locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn get(&self, id: &[u8; OID_SIZE]) -> Arc<Mutex<()>> {
        self.locks
            .lock()
            .entry(*id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

/// Build the test object id pool. Spread first 4 bytes so cell hashing
/// distributes across cells, but keep total small so each cell gets
/// multiple ids and many versions per id.
fn build_object_ids() -> Vec<[u8; OID_SIZE]> {
    (0..NUM_OBJECTS)
        .map(|i| {
            let mut id = [0u8; OID_SIZE];
            // Big-endian u32 in first 4 bytes drives `cell_id` (see
            // `key_shape.rs::cell_id` for `KeyType::Uniform`).
            id[..4].copy_from_slice(&(i as u32 * 0x9E3779B1).to_be_bytes());
            // Salt the rest to avoid accidental prefix collisions.
            id[4..8].copy_from_slice(&(i as u32).to_be_bytes());
            id
        })
        .collect()
}

fn make_key(id: &[u8; OID_SIZE], version: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(KEY_SIZE);
    k.extend_from_slice(id);
    k.extend_from_slice(&version.to_be_bytes());
    k
}

fn id_min_key(id: &[u8; OID_SIZE]) -> Vec<u8> {
    make_key(id, 0)
}

/// Exclusive upper bound for a single-id range scan: next id with version 0.
/// If the id is `0xff..ff`, returns the saturated all-ones bound (still
/// strictly greater than every (id, v) since the version part is appended).
fn id_upper_exclusive(id: &[u8; OID_SIZE]) -> Vec<u8> {
    let mut next = *id;
    let mut carry = true;
    for b in next.iter_mut().rev() {
        if !carry {
            break;
        }
        let (nb, c) = b.overflowing_add(1);
        *b = nb;
        carry = c;
    }
    if carry {
        // Saturated: use max id || max version. Iterator `set_upper_bound`
        // is exclusive, but no key with this id can equal a 40-byte
        // all-ones with version 0xff..ff used as exclusive bound — this
        // is fine for our test ids which never have `0xff..ff` as id.
        let mut k = Vec::with_capacity(KEY_SIZE);
        k.extend_from_slice(&[0xff; OID_SIZE]);
        k.extend_from_slice(&[0xff; VERSION_SIZE]);
        return k;
    }
    let mut k = Vec::with_capacity(KEY_SIZE);
    k.extend_from_slice(&next);
    k.extend_from_slice(&[0u8; VERSION_SIZE]);
    k
}

fn live_value(thread_id: usize, op_num: usize, version: u64) -> Vec<u8> {
    let mut v = vec![0u8; 32];
    v[0] = TAG_LIVE;
    v[8..16].copy_from_slice(&(thread_id as u64).to_be_bytes());
    v[16..24].copy_from_slice(&(op_num as u64).to_be_bytes());
    v[24..32].copy_from_slice(&version.to_be_bytes());
    v
}

fn tombstone_value() -> Vec<u8> {
    // Sui's StoreObject::Deleted serializes very small. Two bytes here
    // mirrors the prod payload size we observed (2 B in the bug report).
    vec![TAG_TOMBSTONE, 0]
}

fn open_db(
    db_path: &Path,
    key_shape: KeyShape,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
) -> Arc<Db> {
    let db = Db::open(db_path, key_shape, config, metrics).unwrap();
    db.start_periodic_snapshot();
    db
}

fn build_key_shape() -> (KeyShape, KeySpace) {
    let mut b = KeyShapeBuilder::new();

    // "keep latest version per object id" — same shape as
    // `authority_store_tables.rs::open` (tidehunter branch). The
    // compactor walks keys in reverse and retains only the first
    // (highest-version) entry per id prefix.
    let compactor: tidehunter::key_shape::Compactor = Box::new(|iter| {
        let mut retain = HashSet::new();
        let mut previous: Option<Bytes> = None;
        for key in iter.rev() {
            if let Some(prev) = &previous
                && prev[..OID_SIZE] == key[..OID_SIZE]
            {
                continue;
            }
            previous = Some(key.clone());
            retain.insert(key.clone());
        }
        retain
    });

    // Drop tombstone-shaped records during relocation, mirroring the
    // `apply_relocation_filter` step Sui applies to objects/effects.
    let relocation_filter = |_k: &[u8], v: &[u8]| -> Decision {
        if v.first().copied() == Some(TAG_TOMBSTONE) {
            Decision::Remove
        } else {
            Decision::Keep
        }
    };

    let cfg = KeySpaceConfig::new()
        .with_max_dirty_keys(8)
        .with_compactor(compactor)
        .with_relocation_filter(relocation_filter);

    // mutexes power-of-two; with KeyType::uniform(1) the cell count
    // equals the mutex count, so 32 cells total over 64 ids → ~2 ids
    // per cell, hot enough for the race.
    let ks = b.add_key_space_config("objects", KEY_SIZE, 32, KeyType::uniform(1), cfg);
    (b.build(), ks)
}

fn main() {
    let temp_dir = tempdir::TempDir::new("objects_iter_test").unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let mut config = Config::small();
    config.max_dirty_keys = 4;
    config.l0_max_entries = Some(6);
    config.snapshot_unload_threshold = 1024;
    config.snapshot_written_bytes = 4 * 1024 * 1024;
    let config = Arc::new(config);

    let (key_shape, ks) = build_key_shape();

    let shared_metrics = Metrics::new();

    let db = Arc::new(RwLock::new(Some(open_db(
        &db_path,
        key_shape.clone(),
        config.clone(),
        shared_metrics.clone(),
    ))));

    let object_ids = Arc::new(build_object_ids());
    let next_version: Arc<Vec<AtomicU64>> = Arc::new(
        (0..NUM_OBJECTS)
            .map(|_| AtomicU64::new(1))
            .collect::<Vec<_>>(),
    );

    let shadow: ShadowMap = Arc::new(Mutex::new(HashMap::new()));
    let id_locks = IdLockManager::new();

    let restart_count = Arc::new(AtomicU64::new(0));
    let mismatch_count = Arc::new(AtomicU64::new(0));

    let no_progress = std::env::var("NO_PROGRESS").is_ok();
    let multi = if no_progress {
        MultiProgress::with_draw_target(ProgressDrawTarget::hidden())
    } else {
        MultiProgress::new()
    };

    let total = (NUM_THREADS * OPS_PER_THREAD) as u64;
    let overall = Arc::new(multi.add(ProgressBar::new(total)));
    if !no_progress {
        overall.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
                .unwrap()
                .progress_chars("##-"),
        );
        overall.set_message("ops");
    }

    let mut handles = vec![];
    for thread_id in 0..NUM_THREADS {
        let db = db.clone();
        let ids = object_ids.clone();
        let next_version = next_version.clone();
        let shadow = shadow.clone();
        let id_locks = id_locks.clone();
        let restart_count = restart_count.clone();
        let mismatch_count = mismatch_count.clone();
        let db_path = db_path.clone();
        let key_shape = key_shape.clone();
        let config = config.clone();
        let shared_metrics = shared_metrics.clone();
        let overall = overall.clone();

        let thread_pb = multi.add(ProgressBar::new(OPS_PER_THREAD as u64));
        if !no_progress {
            thread_pb.set_style(
                ProgressStyle::default_bar()
                    .template(&format!(
                        "[T{thread_id}] {{bar:30.green/white}} {{pos:>6}}/{{len:6}} {{msg}}"
                    ))
                    .unwrap()
                    .progress_chars("=>-"),
            );
            thread_pb.set_message("running");
        }

        handles.push(thread::spawn(move || {
            let mut rng = rand::rngs::StdRng::seed_from_u64(thread_id as u64 * 0xC2B2AE35 + 1);

            for op_num in 0..OPS_PER_THREAD {
                thread_pb.inc(1);
                overall.inc(1);

                // 1% chance: restart db (whole-process unload window).
                if rng.gen_range(0..100) < 1 {
                    let mut w = db.write();
                    if let Some(old) = w.take() {
                        old.wait_for_background_threads_to_finish();
                        *w = Some(open_db(
                            &db_path,
                            key_shape.clone(),
                            config.clone(),
                            shared_metrics.clone(),
                        ));
                        restart_count.fetch_add(1, Ordering::Relaxed);
                    }
                    continue;
                }

                // 1% chance: explicit relocation pass.
                if rng.gen_range(0..100) < 1 {
                    let r = db.read();
                    let _: Result<(), _> = r.as_ref().unwrap().start_relocation();
                }

                // 5% chance: force the BTreeMap → flat promotion. This
                // is the trigger that widens the
                // `insert → promote → flush-completion → retain_unprocessed`
                // race window — production hits it on the 10s timer; we
                // hammer it.
                if rng.gen_range(0..100) < 5 {
                    let r = db.read();
                    r.as_ref().unwrap().test_promote_flat_force();
                }

                // Pick a hot id.
                let idx = rng.gen_range(0..ids.len());
                let id = ids[idx];

                let id_lock = id_locks.get(&id);
                let _g = id_lock.lock();

                // Op selection:
                //   55% write live record at next version
                //   15% write tombstone-record at next version
                //   15% prune (remove all but latest)
                //   15% read latest via reverse-range iter
                let roll = rng.gen_range(0..100u32);

                let r = db.read();
                let db_ref = r.as_ref().unwrap();

                if roll < 55 {
                    let v = next_version[idx].fetch_add(1, Ordering::Relaxed);
                    let key = make_key(&id, v);
                    let value = live_value(thread_id, op_num, v);
                    if rng.r#gen() {
                        db_ref.insert(ks, key.clone(), value.clone()).unwrap();
                    } else {
                        let mut wb = db_ref.write_batch();
                        wb.write(ks, key.clone(), value.clone());
                        wb.commit().unwrap();
                    }
                    shadow.lock().entry(id).or_default().insert(v, value);
                } else if roll < 70 {
                    let v = next_version[idx].fetch_add(1, Ordering::Relaxed);
                    let key = make_key(&id, v);
                    let value = tombstone_value();
                    db_ref.insert(ks, key.clone(), value.clone()).unwrap();
                    shadow.lock().entry(id).or_default().insert(v, value);
                } else if roll < 85 {
                    // Prune: remove every version except the highest. Mirrors
                    // `authority_store_pruner` walking (id, v) and issuing
                    // REMOVEs below the watermark.
                    let mut s = shadow.lock();
                    if let Some(versions) = s.get_mut(&id)
                        && versions.len() > 1
                    {
                        let highest = *versions.keys().next_back().unwrap();
                        let to_remove: Vec<u64> = versions
                            .keys()
                            .copied()
                            .filter(|v| *v < highest)
                            .collect();
                        for v in to_remove {
                            let key = make_key(&id, v);
                            db_ref.remove(ks, key).unwrap();
                            versions.remove(&v);
                        }
                    }
                } else {
                    // Read latest via reverse-range iter — the exact code
                    // path Sui's `get_latest_object_or_tombstone` runs.
                    let mut it = db_ref.iterator(ks);
                    it.set_lower_bound(id_min_key(&id));
                    it.set_upper_bound(id_upper_exclusive(&id));
                    it.reverse();
                    let got = it.next();
                    let s = shadow.lock();
                    let expected = s.get(&id).and_then(|m| m.iter().next_back());
                    match (got, expected) {
                        (None, None) => {}
                        (Some(Ok((k, v))), Some((exp_v, exp_val))) => {
                            let exp_key = make_key(&id, *exp_v);
                            if k.as_ref() != exp_key.as_slice()
                                || v.as_ref() != exp_val.as_slice()
                            {
                                mismatch_count.fetch_add(1, Ordering::Relaxed);
                                eprintln!(
                                    "MISMATCH id={} got=({:02x?}, {:02x?}) expected=({:02x?}, {:02x?})",
                                    hex32(&id),
                                    k.as_ref(),
                                    v.as_ref(),
                                    exp_key,
                                    exp_val
                                );
                                eprintln!(
                                    "  shadow versions for id: {:?}",
                                    s.get(&id).map(|m| m.keys().collect::<Vec<_>>())
                                );
                                std::process::exit(1);
                            }
                        }
                        (Some(Ok((k, v))), None) => {
                            mismatch_count.fetch_add(1, Ordering::Relaxed);
                            eprintln!(
                                "MISMATCH id={} got=({:02x?}, {:02x?}) expected=None",
                                hex32(&id),
                                k.as_ref(),
                                v.as_ref()
                            );
                            std::process::exit(1);
                        }
                        (None, Some((exp_v, exp_val))) => {
                            mismatch_count.fetch_add(1, Ordering::Relaxed);
                            eprintln!(
                                "MISMATCH id={} got=None expected=(v={}, {:02x?})",
                                hex32(&id),
                                exp_v,
                                exp_val
                            );
                            eprintln!(
                                "  shadow versions for id: {:?}",
                                s.get(&id).map(|m| m.keys().collect::<Vec<_>>())
                            );
                            std::process::exit(1);
                        }
                        (Some(Err(e)), _) => {
                            eprintln!("iterator error for id={}: {e:?}", hex32(&id));
                            std::process::exit(1);
                        }
                    }
                }
            }
            thread_pb.finish_with_message("done");
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
    overall.finish_with_message("workers done");
    drop(multi);

    println!("\nFinal verification...");
    final_verify(&db, ks, &object_ids, &shadow);
    println!("✓ all per-id reverse scans match shadow");

    println!(
        "  restarts: {}, mismatches caught mid-run: {}",
        restart_count.load(Ordering::Relaxed),
        mismatch_count.load(Ordering::Relaxed)
    );
    println!(
        "  wal_written: {}",
        human_bytes(shared_metrics.wal_written_bytes.get() as u64)
    );

    println!("\nTest passed.");
}

fn final_verify(
    db: &Arc<RwLock<Option<Arc<Db>>>>,
    ks: KeySpace,
    ids: &[[u8; OID_SIZE]],
    shadow: &ShadowMap,
) {
    let r = db.read();
    let db_ref = r.as_ref().unwrap();
    let s = shadow.lock();
    for id in ids {
        let mut it = db_ref.iterator(ks);
        it.set_lower_bound(id_min_key(id));
        it.set_upper_bound(id_upper_exclusive(id));
        let mut db_versions: Vec<(u64, Vec<u8>)> = Vec::new();
        for r in it {
            let (k, v) = r.unwrap();
            assert_eq!(k.len(), KEY_SIZE);
            assert_eq!(&k[..OID_SIZE], id);
            let mut vbytes = [0u8; 8];
            vbytes.copy_from_slice(&k[OID_SIZE..]);
            db_versions.push((u64::from_be_bytes(vbytes), v.as_ref().to_vec()));
        }
        let expected: Vec<(u64, Vec<u8>)> = s
            .get(id)
            .map(|m| m.iter().map(|(v, val)| (*v, val.clone())).collect())
            .unwrap_or_default();
        if db_versions != expected {
            eprintln!(
                "FINAL MISMATCH id={} db={:?} expected={:?}",
                hex32(id),
                db_versions.iter().map(|(v, _)| v).collect::<Vec<_>>(),
                expected.iter().map(|(v, _)| v).collect::<Vec<_>>()
            );
            std::process::exit(1);
        }
    }
}

fn hex32(id: &[u8; OID_SIZE]) -> String {
    let mut s = String::with_capacity(8);
    for b in &id[..4] {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

fn human_bytes(b: u64) -> String {
    if b < 1024 {
        format!("{b} B")
    } else if b < 1024 * 1024 {
        format!("{:.1} KB", b as f64 / 1024.0)
    } else if b < 1024 * 1024 * 1024 {
        format!("{:.1} MB", b as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", b as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}
