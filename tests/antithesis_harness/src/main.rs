use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::key_shape::{KeyShape, KeyShapeBuilder, KeyType};
use tidehunter::metrics::Metrics;
use tidehunter::{RelocationStrategy, compute_target_position_from_ratio};

const VALUE_MAGIC: &[u8; 8] = b"THANT001";
const CHECKPOINT_MAGIC: &[u8; 8] = b"THHWM001";
const SNAPSHOT_WAL_POSITION_FILE: &str = "ptr";
const RANDOM_SPACE_COUNT: usize = 2;
const DURABLE_SPACE_INDEX: usize = 2;

macro_rules! th_assert_always {
    ($condition:expr, $id:literal, $message:literal $(, $arg:expr)* $(,)?) => {{
        let ok = $condition;
        #[cfg(feature = "sdk")]
        {
            let details = if ok {
                antithesis_sdk::serde_json::json!({
                    "condition": stringify!($condition),
                })
            } else {
                antithesis_sdk::serde_json::json!({
                    "condition": stringify!($condition),
                    "failure": format!($message $(, $arg)*),
                })
            };
            antithesis_sdk::assert_always_or_unreachable!(ok, $id, &details);
        }
        if !ok {
            panic!($message $(, $arg)*);
        }
    }};
}

#[derive(Clone)]
struct Space {
    name: &'static str,
    key_len: usize,
    salt: u64,
}

#[derive(Clone)]
struct SavedSnapshot {
    path: PathBuf,
    model: Vec<BTreeMap<Vec<u8>, Vec<u8>>>,
    wal_position: u64,
    durable_version: u64,
    checkpoint_version: u64,
}

struct Settings {
    root: PathBuf,
    cleanup_root: bool,
    in_antithesis: bool,
    ops: u64,
    seed: u64,
    key_domain: u64,
    verify_every: u64,
    max_snapshots: usize,
    keep_db: bool,
}

struct Harness {
    settings: Settings,
    db_path: PathBuf,
    snapshot_root: PathBuf,
    checkpoint_path: PathBuf,
    key_shape: KeyShape,
    spaces: Vec<Space>,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
    db: Option<Arc<Db>>,
    model: Vec<BTreeMap<Vec<u8>, Vec<u8>>>,
    snapshots: Vec<SavedSnapshot>,
    next_snapshot_id: u64,
    next_value_version: u64,
    next_durable_version: u64,
    checkpoint_version: u64,
    recovered_existing_db: bool,
}

#[derive(Clone)]
enum BatchOp {
    Insert {
        space: usize,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Remove {
        space: usize,
        key: Vec<u8>,
    },
}

#[derive(Debug, Clone, Copy)]
struct DecodedValue {
    space_idx: usize,
    version: u64,
}

enum HarnessRng {
    Local(Box<StdRng>),
    #[cfg(feature = "sdk")]
    Antithesis(antithesis_sdk::random::AntithesisRng),
}

fn main() {
    sdk::init();

    let settings = Settings::from_env();
    if settings.in_antithesis {
        assert!(
            cfg!(feature = "sdk"),
            "Antithesis runs require building antithesis_harness with --features sdk"
        );
    }

    let mut rng = HarnessRng::new(&settings);

    println!(
        "antithesis_harness start: root={} ops={} seed={} key_domain={} verify_every={} antithesis={}",
        settings.root.display(),
        settings.ops,
        settings.seed,
        settings.key_domain,
        settings.verify_every,
        settings.in_antithesis
    );

    let mut harness = Harness::new(settings);
    harness.run(&mut rng);
}

impl Settings {
    fn from_env() -> Self {
        let in_antithesis = env::var_os("ANTITHESIS_OUTPUT_DIR").is_some();
        let seed = env_u64("TIDEHUNTER_ANTITHESIS_SEED", 0x5449_4445_4855_4e54);
        let configured_root = env::var_os("TIDEHUNTER_ANTITHESIS_ROOT").map(PathBuf::from);
        let cleanup_root = configured_root.is_none() && !in_antithesis;
        let root = configured_root.unwrap_or_else(|| env::temp_dir().join("tidehunter-antithesis"));

        Self {
            root,
            cleanup_root,
            in_antithesis,
            ops: env_u64("TIDEHUNTER_ANTITHESIS_OPS", 20_000),
            seed,
            key_domain: env_u64("TIDEHUNTER_ANTITHESIS_KEYS", 192).max(1),
            verify_every: env_u64("TIDEHUNTER_ANTITHESIS_VERIFY_EVERY", 250),
            max_snapshots: env_usize("TIDEHUNTER_ANTITHESIS_MAX_SNAPSHOTS", 8),
            keep_db: env_bool("TIDEHUNTER_ANTITHESIS_KEEP_DB"),
        }
    }
}

impl Harness {
    fn new(settings: Settings) -> Self {
        let db_path = settings.root.join("db");
        let snapshot_root = settings.root.join("snapshots");
        let checkpoint_path = settings.root.join("checkpoint.hwm");
        let recovered_existing_db = db_path.join("shape.yaml").exists();

        fs::create_dir_all(&settings.root).expect("create harness root directory");
        fs::create_dir_all(&db_path).expect("create harness db directory");
        fs::create_dir_all(&snapshot_root).expect("create harness snapshot directory");

        let mut key_shape_builder = KeyShapeBuilder::new();
        key_shape_builder
            .add_key_space("main", 8, 8, KeyType::uniform(16))
            .add_key_space("secondary", 4, 8, KeyType::uniform(16))
            .add_key_space("durable", 8, 8, KeyType::uniform(16));
        let key_shape = key_shape_builder.build();

        let spaces = vec![
            Space {
                name: "main",
                key_len: 8,
                salt: 0xa4a4_0000_0000_0001,
            },
            Space {
                name: "secondary",
                key_len: 4,
                salt: 0xb5b5_0000_0000_0002,
            },
            Space {
                name: "durable",
                key_len: 8,
                salt: 0xc6c6_0000_0000_0003,
            },
        ];

        let config = Arc::new(harness_config());
        let metrics = Metrics::new();
        let model = (0..spaces.len()).map(|_| BTreeMap::new()).collect();

        let mut this = Self {
            settings,
            db_path,
            snapshot_root,
            checkpoint_path,
            key_shape,
            spaces,
            config,
            metrics,
            db: None,
            model,
            snapshots: Vec::new(),
            next_snapshot_id: 0,
            next_value_version: 1,
            next_durable_version: 1,
            checkpoint_version: 0,
            recovered_existing_db,
        };
        this.open_db();
        this.recover_oracle_from_db();
        sdk::setup_complete();
        this
    }

    fn run(&mut self, rng: &mut HarnessRng) {
        let trace = env::var_os("TIDEHUNTER_ANTITHESIS_TRACE").is_some();
        // Classification aid: when set, disable the local-only restart ops (reopen +
        // restore_snapshot) so we can tell whether a mismatch lives in the core
        // CRUD/flush/relocation path or only in the reopen/snapshot path.
        let local_restart_ops = !self.settings.in_antithesis
            && env::var_os("TIDEHUNTER_ANTITHESIS_NO_RESTART").is_none();
        let relocation_ops = env::var_os("TIDEHUNTER_ANTITHESIS_NO_RELOCATION").is_none();

        for op_idx in 0..self.settings.ops {
            let roll = rng.range_u32(100);
            if trace {
                let kind = match roll {
                    0..=14 => "durable_insert",
                    15..=33 => "insert",
                    34..=45 => "remove",
                    46..=60 => "get",
                    61..=70 => "exists",
                    71..=80 => "iterator",
                    81..=88 => "batch",
                    89..=91 => "rebuild",
                    92..=93 if local_restart_ops => "reopen",
                    92..=93 => "get",
                    94..=95 if relocation_ops => "relocation",
                    94..=95 => "get",
                    96..=97 => "create_snapshot",
                    98 if local_restart_ops => "restore_snapshot",
                    98 => "rebuild",
                    _ => "verify_all",
                };
                eprintln!("TRACE op={op_idx} kind={kind}");
            }
            match roll {
                0..=14 => self.op_durable_insert(op_idx, rng),
                15..=33 => self.op_insert(op_idx, rng),
                34..=45 => self.op_remove(op_idx, rng),
                46..=60 => self.op_get(op_idx, rng),
                61..=70 => self.op_exists(op_idx, rng),
                71..=80 => self.op_iterator(op_idx, rng),
                81..=88 => self.op_batch(op_idx, rng),
                89..=91 => self.op_rebuild(op_idx, rng),
                92..=93 if local_restart_ops => self.op_reopen(op_idx),
                92..=93 => self.op_get(op_idx, rng),
                94..=95 if relocation_ops => self.op_relocation(op_idx, rng),
                94..=95 => self.op_get(op_idx, rng),
                96..=97 => self.op_create_snapshot(op_idx),
                98 if local_restart_ops => self.op_restore_snapshot(op_idx, rng),
                98 => self.op_rebuild(op_idx, rng),
                _ => self.verify_all(op_idx),
            }

            if self.settings.verify_every > 0 && op_idx % self.settings.verify_every == 0 {
                self.verify_all(op_idx);
                println!("antithesis_harness progress: completed_op={op_idx}");
            }
        }

        self.verify_all(self.settings.ops);
        println!(
            "antithesis_harness complete: ops={} wal_written={} snapshots={} checkpoint_version={}",
            self.settings.ops,
            self.metrics.wal_written_bytes.get(),
            self.snapshots.len(),
            self.checkpoint_version
        );
    }

    fn open_db(&mut self) {
        let db = Db::open(
            &self.db_path,
            self.key_shape.clone(),
            self.config.clone(),
            self.metrics.clone(),
        )
        .expect("open Tidehunter database");
        self.db = Some(db);
    }

    fn close_db(&mut self) {
        if let Some(db) = self.db.take() {
            if self.settings.in_antithesis {
                drop(db);
            } else {
                db.wait_for_background_threads_to_finish();
            }
        }
    }

    fn db(&self) -> Arc<Db> {
        self.db.as_ref().expect("database is open").clone()
    }

    fn recover_oracle_from_db(&mut self) {
        self.checkpoint_version = read_checkpoint(&self.checkpoint_path)
            .unwrap_or_else(|err| panic!("read checkpoint failed: {err}"));

        let db = self.db();
        let mut max_value_version = 0;
        self.model = (0..self.spaces.len()).map(|_| BTreeMap::new()).collect();

        for (space_idx, space) in self.spaces.iter().enumerate() {
            let entries = collect_iterator(db.iterator(db.ks(space.name)), 0, space.name);
            for (key, value) in entries {
                let decoded = validate_value(space_idx, &key, &value).unwrap_or_else(|err| {
                    sdk::unreachable(&format!(
                        "invalid value during recovery ks={} key={} err={err}",
                        space.name,
                        hex(&key)
                    ))
                });
                max_value_version = max_value_version.max(decoded.version);
                self.model[space_idx].insert(key, value);
            }
        }

        self.verify_durable_high_water_mark(0);
        self.next_value_version = max_value_version.saturating_add(1).max(1);
        self.next_durable_version = self.max_durable_version().saturating_add(1).max(1);

        sdk::sometimes(
            self.recovered_existing_db && self.checkpoint_version > 0,
            sdk::CoverageEvent::RecoveryScanAfterProcessRestart,
        );
    }

    fn op_durable_insert(&mut self, op_idx: u64, rng: &mut HarnessRng) {
        let version = self.next_durable_version;
        self.next_durable_version += 1;
        let key = durable_key(version);
        let value = self.make_value(DURABLE_SPACE_INDEX, &key, version, rng);
        let space = self.spaces[DURABLE_SPACE_INDEX].clone();

        let db = self.db();
        db.insert(db.ks(space.name), key.clone(), value.clone())
            .unwrap_or_else(|err| panic!("durable insert failed at op {op_idx}: {err:?}"));
        self.model[DURABLE_SPACE_INDEX].insert(key.clone(), value);
        self.verify_key(op_idx, DURABLE_SPACE_INDEX, &key);
    }

    fn op_insert(&mut self, op_idx: u64, rng: &mut HarnessRng) {
        let space_idx = self.random_model_space(rng);
        let key = self.random_key(space_idx, rng);
        let version = self.next_value_version;
        self.next_value_version += 1;
        let value = self.make_value(space_idx, &key, version, rng);
        let space = self.spaces[space_idx].clone();

        let db = self.db();
        db.insert(db.ks(space.name), key.clone(), value.clone())
            .unwrap_or_else(|err| panic!("insert failed at op {op_idx}: {err:?}"));
        self.model[space_idx].insert(key.clone(), value);
        self.verify_key(op_idx, space_idx, &key);
    }

    fn op_remove(&mut self, op_idx: u64, rng: &mut HarnessRng) {
        let space_idx = self.random_model_space(rng);
        let key = self.random_key(space_idx, rng);
        let space = self.spaces[space_idx].clone();

        let db = self.db();
        db.remove(db.ks(space.name), key.clone())
            .unwrap_or_else(|err| panic!("remove failed at op {op_idx}: {err:?}"));
        self.model[space_idx].remove(&key);
        self.verify_key(op_idx, space_idx, &key);
    }

    fn op_get(&self, op_idx: u64, rng: &mut HarnessRng) {
        let space_idx = self.random_space(rng);
        let key = if space_idx == DURABLE_SPACE_INDEX {
            let upper = self.next_durable_version.max(2);
            durable_key(rng.range_u64(1, upper))
        } else {
            self.random_key(space_idx, rng)
        };
        self.verify_key(op_idx, space_idx, &key);
    }

    fn op_exists(&self, op_idx: u64, rng: &mut HarnessRng) {
        let space_idx = self.random_space(rng);
        let key = if space_idx == DURABLE_SPACE_INDEX {
            let upper = self.next_durable_version.max(2);
            durable_key(rng.range_u64(1, upper))
        } else {
            self.random_key(space_idx, rng)
        };
        let db = self.db();
        let space = &self.spaces[space_idx];
        let expected = self.model[space_idx].contains_key(&key);
        let actual = db
            .exists(db.ks(space.name), &key)
            .unwrap_or_else(|err| panic!("exists failed at op {op_idx}: {err:?}"));
        th_assert_always!(
            actual == expected,
            "exists_matches_model",
            "exists mismatch at op {op_idx} ks={} key={} actual={actual} expected={expected}",
            space.name,
            hex(&key)
        );
    }

    fn op_iterator(&self, op_idx: u64, rng: &mut HarnessRng) {
        let space_idx = self.random_space(rng);
        if rng.chance(35, 100) || space_idx == DURABLE_SPACE_INDEX {
            self.verify_iterator_full(op_idx, space_idx);
        } else {
            self.verify_iterator_range(op_idx, space_idx, rng);
        }
    }

    fn op_batch(&mut self, op_idx: u64, rng: &mut HarnessRng) {
        let db = self.db();
        let mut batch = db.write_batch();
        batch.set_tag("antithesis".to_string());

        let len = rng.range_usize_inclusive(1, 8);
        let mut ops = Vec::with_capacity(len);
        for _ in 0..len {
            let space_idx = self.random_model_space(rng);
            let key = self.random_key(space_idx, rng);
            let space = &self.spaces[space_idx];
            if rng.chance(65, 100) {
                let version = self.next_value_version;
                self.next_value_version += 1;
                let value = self.make_value(space_idx, &key, version, rng);
                batch.write(db.ks(space.name), key.clone(), value.clone());
                ops.push(BatchOp::Insert {
                    space: space_idx,
                    key,
                    value,
                });
            } else {
                batch.delete(db.ks(space.name), key.clone());
                ops.push(BatchOp::Remove {
                    space: space_idx,
                    key,
                });
            }
        }

        batch
            .commit()
            .unwrap_or_else(|err| panic!("batch commit failed at op {op_idx}: {err:?}"));

        for op in &ops {
            match op {
                BatchOp::Insert { space, key, value } => {
                    self.model[*space].insert(key.clone(), value.clone());
                }
                BatchOp::Remove { space, key } => {
                    self.model[*space].remove(key);
                }
            }
        }
        for op in ops {
            match op {
                BatchOp::Insert { space, key, .. } | BatchOp::Remove { space, key } => {
                    self.verify_key(op_idx, space, &key);
                }
            }
        }
    }

    fn op_rebuild(&mut self, op_idx: u64, rng: &mut HarnessRng) {
        if rng.chance(20, 100) {
            self.db()
                .force_rebuild_control_region()
                .unwrap_or_else(|err| panic!("force rebuild failed at op {op_idx}: {err:?}"));
        } else {
            self.db()
                .rebuild_control_region()
                .unwrap_or_else(|err| panic!("rebuild failed at op {op_idx}: {err:?}"));
        }
        self.persist_checkpoint(self.max_durable_version(), op_idx);
        sdk::sometimes(true, sdk::CoverageEvent::CheckpointCompleted);
        sdk::sometimes(
            self.recovered_existing_db && self.checkpoint_version > 0,
            sdk::CoverageEvent::CheckpointAfterProcessRestart,
        );
    }

    fn op_reopen(&mut self, op_idx: u64) {
        self.close_db();
        self.open_db();
        self.recover_oracle_from_db();
        self.verify_all(op_idx);
    }

    fn op_relocation(&self, op_idx: u64, rng: &mut HarnessRng) {
        let db = self.db();
        match rng.range_u32(4) {
            0 => db
                .start_relocation()
                .unwrap_or_else(|err| panic!("start relocation failed at op {op_idx}: {err:?}")),
            1 => db
                .start_relocation_with_strategy(RelocationStrategy::WalBased)
                .unwrap_or_else(|err| {
                    panic!("start WAL relocation failed at op {op_idx}: {err:?}")
                }),
            2 => {
                let target = compute_target_position_from_ratio(&db, rng.range_f64(0.1, 1.0));
                db.start_relocation_with_strategy(RelocationStrategy::IndexBased(target))
                    .unwrap_or_else(|err| {
                        panic!("start index relocation failed at op {op_idx}: {err:?}")
                    });
            }
            _ => {
                let strategy = if rng.chance(50, 100) {
                    RelocationStrategy::WalBased
                } else {
                    let target = compute_target_position_from_ratio(&db, rng.range_f64(0.1, 1.0));
                    RelocationStrategy::IndexBased(target)
                };
                db.start_blocking_relocation_with_strategy(strategy);
                sdk::sometimes(true, sdk::CoverageEvent::RelocationCompleted);
                self.verify_all(op_idx);
            }
        }
    }

    fn op_create_snapshot(&mut self, op_idx: u64) {
        let path = self
            .snapshot_root
            .join(format!("snapshot-{}", self.next_snapshot_id));
        self.next_snapshot_id += 1;
        fs::create_dir_all(&path).expect("create state snapshot directory");

        let db = self.db();
        db.rebuild_control_region()
            .unwrap_or_else(|err| panic!("snapshot rebuild failed at op {op_idx}: {err:?}"));
        db.create_state_snapshot(path.clone())
            .unwrap_or_else(|err| panic!("create state snapshot failed at op {op_idx}: {err:?}"));

        let checkpoint_version = self.max_durable_version();
        self.persist_checkpoint(checkpoint_version, op_idx);
        let wal_position = read_snapshot_wal_position(&path).unwrap_or_else(|err| {
            panic!(
                "read state snapshot WAL position failed at op {op_idx} path={}: {err}",
                path.display()
            )
        });
        self.snapshots.push(SavedSnapshot {
            path,
            model: self.model.clone(),
            wal_position,
            durable_version: self.next_durable_version,
            checkpoint_version,
        });

        while self.snapshots.len() > self.settings.max_snapshots {
            let old = self.snapshots.remove(0);
            let _ = fs::remove_dir_all(old.path);
        }
    }

    fn op_restore_snapshot(&mut self, op_idx: u64, rng: &mut HarnessRng) {
        let candidates: Vec<usize> = self
            .snapshots
            .iter()
            .enumerate()
            .filter_map(|(idx, snapshot)| {
                (snapshot.wal_position < self.config.wal_file_size).then_some(idx)
            })
            .collect();
        if candidates.is_empty() {
            self.op_create_snapshot(op_idx);
            return;
        }

        let snapshot_idx = candidates[rng.range_usize(candidates.len())];
        let snapshot = self.snapshots[snapshot_idx].clone();

        self.persist_checkpoint(snapshot.checkpoint_version, op_idx);
        self.close_db();
        let db = Db::restore_state_snapshot(
            snapshot.path.clone(),
            self.db_path.clone(),
            self.key_shape.clone(),
            self.config.clone(),
            self.metrics.clone(),
        )
        .unwrap_or_else(|err| panic!("restore state snapshot failed at op {op_idx}: {err:?}"));
        self.db = Some(db);
        self.model = snapshot.model;
        self.next_durable_version = snapshot.durable_version;

        let stale = self.snapshots.split_off(snapshot_idx + 1);
        for old in stale {
            let _ = fs::remove_dir_all(old.path);
        }

        self.verify_all(op_idx);
    }

    fn verify_all(&self, op_idx: u64) {
        for space_idx in 0..self.spaces.len() {
            if space_idx == DURABLE_SPACE_INDEX {
                self.verify_durable_high_water_mark(op_idx);
            } else {
                for key_id in 0..self.settings.key_domain {
                    let key = self.key_for(space_idx, key_id);
                    self.verify_key(op_idx, space_idx, &key);
                }
            }
            self.verify_iterator_full(op_idx, space_idx);
        }
    }

    fn verify_durable_high_water_mark(&self, op_idx: u64) {
        let db = self.db();
        let space = &self.spaces[DURABLE_SPACE_INDEX];
        for version in 1..=self.checkpoint_version {
            let key = durable_key(version);
            let value = db
                .get(db.ks(space.name), &key)
                .unwrap_or_else(|err| panic!("durable get failed at op {op_idx}: {err:?}"));
            th_assert_always!(
                value.is_some(),
                "durable_committed_key_present",
                "durable committed key missing at op {op_idx} key={} checkpoint_version={}",
                hex(&key),
                self.checkpoint_version
            );
            let value = value.expect("checked above");
            let decoded =
                validate_value(DURABLE_SPACE_INDEX, &key, value.as_ref()).unwrap_or_else(|err| {
                    sdk::unreachable(&format!(
                        "invalid durable value at op {op_idx} key={} err={err}",
                        hex(&key)
                    ))
                });
            th_assert_always!(
                decoded.version == version,
                "durable_version_matches_checkpoint",
                "durable version mismatch at op {op_idx} key={} actual={} expected={version}",
                hex(&key),
                decoded.version
            );
        }
    }

    fn verify_key(&self, op_idx: u64, space_idx: usize, key: &[u8]) {
        let db = self.db();
        let space = &self.spaces[space_idx];
        let expected = self.model[space_idx].get(key).cloned();
        let actual = db
            .get(db.ks(space.name), key)
            .unwrap_or_else(|err| panic!("get failed at op {op_idx}: {err:?}"))
            .map(|value| value.as_ref().to_vec());

        if let Some(actual_value) = &actual {
            let decoded = validate_value(space_idx, key, actual_value).unwrap_or_else(|err| {
                sdk::unreachable(&format!(
                    "invalid get value at op {op_idx} ks={} key={} err={err}",
                    space.name,
                    hex(key)
                ))
            });
            th_assert_always!(
                decoded.space_idx == space_idx,
                "decoded_value_space_matches_keyspace",
                "decoded space mismatch at op {op_idx} ks={} key={}",
                space.name,
                hex(key)
            );
        }

        th_assert_always!(
            actual == expected,
            "get_matches_model",
            "get mismatch at op {op_idx} ks={} key={} actual={} expected={}",
            space.name,
            hex(key),
            short_value(actual.as_deref()),
            short_value(expected.as_deref())
        );

        let exists = db
            .exists(db.ks(space.name), key)
            .unwrap_or_else(|err| panic!("exists failed at op {op_idx}: {err:?}"));
        th_assert_always!(
            exists == expected.is_some(),
            "exists_matches_get",
            "exists/get mismatch at op {op_idx} ks={} key={}",
            space.name,
            hex(key)
        );
    }

    fn verify_iterator_full(&self, op_idx: u64, space_idx: usize) {
        let db = self.db();
        let space = &self.spaces[space_idx];
        let actual = collect_iterator(db.iterator(db.ks(space.name)), op_idx, space.name);
        self.validate_iterator_values(op_idx, space_idx, &actual, false);
        let expected: Vec<(Vec<u8>, Vec<u8>)> = self.model[space_idx]
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        self.assert_entries_equal(op_idx, space_idx, "full iterator", &actual, &expected);
    }

    fn verify_iterator_range(&self, op_idx: u64, space_idx: usize, rng: &mut HarnessRng) {
        let db = self.db();
        let space = &self.spaces[space_idx];

        let mut lower = self.random_key(space_idx, rng);
        let mut upper = self.random_key(space_idx, rng);
        if lower > upper {
            std::mem::swap(&mut lower, &mut upper);
        }

        let mut iterator = db.iterator(db.ks(space.name));
        iterator.set_lower_bound(lower.clone());
        iterator.set_upper_bound(upper.clone());

        let mut expected: Vec<(Vec<u8>, Vec<u8>)> = self.model[space_idx]
            .range(lower.clone()..upper.clone())
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();

        let reverse = rng.chance(50, 100);
        if reverse {
            iterator.reverse();
            expected.reverse();
        }

        let actual = collect_iterator(iterator, op_idx, space.name);
        self.validate_iterator_values(op_idx, space_idx, &actual, reverse);
        self.assert_entries_equal(
            op_idx,
            space_idx,
            if reverse {
                "reverse range iterator"
            } else {
                "range iterator"
            },
            &actual,
            &expected,
        );
    }

    fn validate_iterator_values(
        &self,
        op_idx: u64,
        space_idx: usize,
        entries: &[(Vec<u8>, Vec<u8>)],
        reverse: bool,
    ) {
        let space = &self.spaces[space_idx];
        for (key, value) in entries {
            validate_value(space_idx, key, value).unwrap_or_else(|err| {
                sdk::unreachable(&format!(
                    "invalid iterator value at op {op_idx} ks={} key={} err={err}",
                    space.name,
                    hex(key)
                ))
            });
        }
        let ordered = if reverse {
            entries.windows(2).all(|pair| pair[0].0 > pair[1].0)
        } else {
            entries.windows(2).all(|pair| pair[0].0 < pair[1].0)
        };
        th_assert_always!(
            ordered,
            "iterator_ordered",
            "iterator order violation at op {op_idx} ks={}",
            space.name
        );
    }

    fn assert_entries_equal(
        &self,
        op_idx: u64,
        space_idx: usize,
        label: &str,
        actual: &[(Vec<u8>, Vec<u8>)],
        expected: &[(Vec<u8>, Vec<u8>)],
    ) {
        if actual == expected {
            return;
        }
        let space = &self.spaces[space_idx];
        let mismatch = first_mismatch(actual, expected);
        th_assert_always!(
            false,
            "iterator_entries_match_model",
            "{label} mismatch at op {op_idx} ks={} actual_len={} expected_len={} first_mismatch={}",
            space.name,
            actual.len(),
            expected.len(),
            mismatch
        );
    }

    fn persist_checkpoint(&mut self, version: u64, op_idx: u64) {
        write_checkpoint(&self.checkpoint_path, version)
            .unwrap_or_else(|err| panic!("write checkpoint failed at op {op_idx}: {err}"));
        self.checkpoint_version = version;
        self.verify_durable_high_water_mark(op_idx);
    }

    fn random_space(&self, rng: &mut HarnessRng) -> usize {
        rng.range_usize(self.spaces.len())
    }

    fn random_model_space(&self, rng: &mut HarnessRng) -> usize {
        rng.range_usize(RANDOM_SPACE_COUNT)
    }

    fn random_key(&self, space_idx: usize, rng: &mut HarnessRng) -> Vec<u8> {
        let key_id = rng.range_u64(0, self.settings.key_domain);
        self.key_for(space_idx, key_id)
    }

    fn key_for(&self, space_idx: usize, key_id: u64) -> Vec<u8> {
        let space = &self.spaces[space_idx];
        let mut key = Vec::with_capacity(space.key_len);
        let mut state = key_id ^ space.salt;
        while key.len() < space.key_len {
            state = splitmix64(state);
            key.extend_from_slice(&state.to_be_bytes());
        }
        key.truncate(space.key_len);
        key
    }

    fn make_value(
        &self,
        space_idx: usize,
        key: &[u8],
        version: u64,
        rng: &mut HarnessRng,
    ) -> Vec<u8> {
        let payload_len = rng.range_usize_inclusive(0, 96);
        let mut payload = vec![0; payload_len];
        rng.fill_bytes(&mut payload);
        encode_value(space_idx, key, version, &payload)
    }

    fn max_durable_version(&self) -> u64 {
        self.model[DURABLE_SPACE_INDEX]
            .values()
            .filter_map(|value| decode_value(value).ok())
            .map(|decoded| decoded.version)
            .max()
            .unwrap_or(0)
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.close_db();
        if !self.settings.keep_db {
            if self.settings.cleanup_root {
                let _ = fs::remove_dir_all(&self.settings.root);
            } else if !self.settings.in_antithesis {
                let _ = fs::remove_dir_all(&self.snapshot_root);
            }
        }
    }
}

impl HarnessRng {
    fn new(settings: &Settings) -> Self {
        if settings.in_antithesis {
            #[cfg(feature = "sdk")]
            {
                return Self::Antithesis(antithesis_sdk::random::AntithesisRng);
            }
        }
        Self::Local(Box::new(StdRng::seed_from_u64(settings.seed)))
    }

    fn next_u64(&mut self) -> u64 {
        match self {
            Self::Local(rng) => rng.next_u64(),
            #[cfg(feature = "sdk")]
            Self::Antithesis(rng) => rng.next_u64(),
        }
    }

    fn fill_bytes(&mut self, bytes: &mut [u8]) {
        match self {
            Self::Local(rng) => rng.fill_bytes(bytes),
            #[cfg(feature = "sdk")]
            Self::Antithesis(rng) => rng.fill_bytes(bytes),
        }
    }

    fn range_u32(&mut self, upper_exclusive: u32) -> u32 {
        (self.next_u64() % upper_exclusive as u64) as u32
    }

    fn range_usize(&mut self, upper_exclusive: usize) -> usize {
        if upper_exclusive == 0 {
            sdk::unreachable("empty random range");
        }
        (self.next_u64() as usize) % upper_exclusive
    }

    fn range_usize_inclusive(&mut self, lower: usize, upper: usize) -> usize {
        lower + self.range_usize(upper - lower + 1)
    }

    fn range_u64(&mut self, lower: u64, upper_exclusive: u64) -> u64 {
        if lower >= upper_exclusive {
            sdk::unreachable("empty u64 random range");
        }
        lower + (self.next_u64() % (upper_exclusive - lower))
    }

    fn range_f64(&mut self, lower: f64, upper: f64) -> f64 {
        let unit = (self.next_u64() as f64) / (u64::MAX as f64);
        lower + (upper - lower) * unit
    }

    fn chance(&mut self, numerator: u32, denominator: u32) -> bool {
        self.range_u32(denominator) < numerator
    }
}

fn harness_config() -> Config {
    let mut config = Config::small();
    config.frag_size = env_u64("TIDEHUNTER_ANTITHESIS_FRAG_SIZE", 1024 * 1024);
    config.wal_file_size = env_u64("TIDEHUNTER_ANTITHESIS_WAL_FILE_SIZE", 64 * 1024 * 1024);
    config.max_maps = env_usize("TIDEHUNTER_ANTITHESIS_MAX_MAPS", 256);
    config.max_index_maps = Some(env_usize("TIDEHUNTER_ANTITHESIS_MAX_INDEX_MAPS", 256));
    config.max_dirty_keys = env_usize("TIDEHUNTER_ANTITHESIS_MAX_DIRTY_KEYS", 4);
    config.l0_max_entries = Some(env_usize("TIDEHUNTER_ANTITHESIS_L0_MAX_ENTRIES", 8));
    config.snapshot_written_bytes = env_u64("TIDEHUNTER_ANTITHESIS_SNAPSHOT_BYTES", 16 * 1024);
    config.snapshot_unload_threshold =
        env_u64("TIDEHUNTER_ANTITHESIS_SNAPSHOT_UNLOAD_THRESHOLD", 8 * 1024);
    config.unload_jitter_pct = env_usize("TIDEHUNTER_ANTITHESIS_UNLOAD_JITTER_PCT", 0);
    config.num_flusher_threads = env_usize("TIDEHUNTER_ANTITHESIS_FLUSHER_THREADS", 2);
    config.relocation_max_reclaim_pct =
        env_u8("TIDEHUNTER_ANTITHESIS_RELOCATION_MAX_RECLAIM_PCT", 100);
    config.relocation_batch_max_bytes = Some(env_usize(
        "TIDEHUNTER_ANTITHESIS_RELOCATION_BATCH_BYTES",
        16 * 1024,
    ));
    config.index_min_occupancy_pct = env_u8("TIDEHUNTER_ANTITHESIS_INDEX_MIN_OCCUPANCY_PCT", 1);
    config.commit_pool_size = env_usize("TIDEHUNTER_ANTITHESIS_COMMIT_POOL_SIZE", 2);
    config.num_pending_promotion_threads =
        env_usize("TIDEHUNTER_ANTITHESIS_PENDING_PROMOTION_THREADS", 2);
    config.open_lock_retry_timeout = Duration::from_secs(5);
    config
}

fn collect_iterator(
    iterator: tidehunter::iterators::db_iterator::DbIterator,
    op_idx: u64,
    space_name: &str,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    iterator
        .map(|result| {
            let (key, value) = result.unwrap_or_else(|err| {
                panic!("iterator failed at op {op_idx} ks={space_name}: {err:?}")
            });
            (key.as_ref().to_vec(), value.as_ref().to_vec())
        })
        .collect()
}

fn encode_value(space_idx: usize, key: &[u8], version: u64, payload: &[u8]) -> Vec<u8> {
    let mut value = Vec::with_capacity(32 + key.len() + payload.len());
    value.extend_from_slice(VALUE_MAGIC);
    value.push(space_idx as u8);
    value.extend_from_slice(&version.to_be_bytes());
    value.extend_from_slice(&(key.len() as u16).to_be_bytes());
    value.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    value.extend_from_slice(&0u64.to_be_bytes());
    value.extend_from_slice(key);
    value.extend_from_slice(payload);
    let checksum = value_checksum(&value);
    value[21..29].copy_from_slice(&checksum.to_be_bytes());
    value
}

fn validate_value(space_idx: usize, key: &[u8], value: &[u8]) -> Result<DecodedValue, String> {
    let decoded = decode_value(value)?;
    if decoded.space_idx != space_idx {
        return Err(format!(
            "space mismatch actual={} expected={space_idx}",
            decoded.space_idx
        ));
    }
    let value_key = value_key(value)?;
    if value_key != key {
        return Err(format!(
            "key mismatch actual={} expected={}",
            hex(value_key),
            hex(key)
        ));
    }
    Ok(decoded)
}

fn decode_value(value: &[u8]) -> Result<DecodedValue, String> {
    if value.len() < 29 {
        return Err(format!("value too short len={}", value.len()));
    }
    if &value[0..8] != VALUE_MAGIC {
        return Err("bad magic".to_string());
    }
    let space_idx = value[8] as usize;
    let version = u64::from_be_bytes(value[9..17].try_into().unwrap());
    let key_len = u16::from_be_bytes(value[17..19].try_into().unwrap()) as usize;
    let payload_len = u16::from_be_bytes(value[19..21].try_into().unwrap()) as usize;
    let expected_len = 29 + key_len + payload_len;
    if value.len() != expected_len {
        return Err(format!(
            "length mismatch actual={} expected={expected_len}",
            value.len()
        ));
    }
    let actual_checksum = u64::from_be_bytes(value[21..29].try_into().unwrap());
    let expected_checksum = value_checksum(value);
    if actual_checksum != expected_checksum {
        return Err(format!(
            "checksum mismatch actual={actual_checksum:016x} expected={expected_checksum:016x}"
        ));
    }
    Ok(DecodedValue { space_idx, version })
}

fn value_key(value: &[u8]) -> Result<&[u8], String> {
    decode_value(value)?;
    let key_len = u16::from_be_bytes(value[17..19].try_into().unwrap()) as usize;
    Ok(&value[29..29 + key_len])
}

fn value_checksum(value: &[u8]) -> u64 {
    let mut hash = 0x1234_5678_9abc_def0;
    for (idx, byte) in value.iter().enumerate() {
        let b = if (21..29).contains(&idx) { 0 } else { *byte };
        hash = splitmix64(hash ^ ((b as u64) << ((idx % 8) * 8)) ^ idx as u64);
    }
    hash
}

fn durable_key(version: u64) -> Vec<u8> {
    version.to_be_bytes().to_vec()
}

fn read_checkpoint(path: &Path) -> io::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let bytes = fs::read(path)?;
    if bytes.len() != 24 || &bytes[0..8] != CHECKPOINT_MAGIC {
        sdk::unreachable("invalid checkpoint file");
    }
    let version = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
    let checksum = u64::from_be_bytes(bytes[16..24].try_into().unwrap());
    let expected = checkpoint_checksum(version);
    if checksum != expected {
        sdk::unreachable("checkpoint checksum mismatch");
    }
    Ok(version)
}

fn write_checkpoint(path: &Path, version: u64) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    let mut bytes = Vec::with_capacity(24);
    bytes.extend_from_slice(CHECKPOINT_MAGIC);
    bytes.extend_from_slice(&version.to_be_bytes());
    bytes.extend_from_slice(&checkpoint_checksum(version).to_be_bytes());

    {
        let mut file = File::create(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent()
        && let Ok(dir) = OpenOptions::new().read(true).open(parent)
    {
        let _ = dir.sync_all();
    }
    Ok(())
}

fn read_snapshot_wal_position(snapshot_path: &Path) -> io::Result<u64> {
    let bytes = fs::read(snapshot_path.join(SNAPSHOT_WAL_POSITION_FILE))?;
    bincode::deserialize(&bytes)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
}

fn checkpoint_checksum(version: u64) -> u64 {
    splitmix64(version ^ 0xd00d_f00d_cafe_beef)
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = value;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn first_mismatch(actual: &[(Vec<u8>, Vec<u8>)], expected: &[(Vec<u8>, Vec<u8>)]) -> String {
    let len = actual.len().min(expected.len());
    for idx in 0..len {
        if actual[idx] != expected[idx] {
            return format!(
                "idx={idx} actual_key={} expected_key={} actual_value={} expected_value={}",
                hex(&actual[idx].0),
                hex(&expected[idx].0),
                short_value(Some(&actual[idx].1)),
                short_value(Some(&expected[idx].1))
            );
        }
    }
    if actual.len() > expected.len() {
        format!(
            "extra_actual idx={len} key={} value={}",
            hex(&actual[len].0),
            short_value(Some(&actual[len].1))
        )
    } else {
        format!(
            "missing_actual idx={len} key={} value={}",
            hex(&expected[len].0),
            short_value(Some(&expected[len].1))
        )
    }
}

fn short_value(value: Option<&[u8]>) -> String {
    match value {
        Some(value) => {
            let prefix_len = value.len().min(16);
            format!("len={} prefix={}", value.len(), hex(&value[..prefix_len]))
        }
        None => "None".to_string(),
    }
}

fn env_bool(name: &str) -> bool {
    env::var(name)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u8(name: &str, default: u8) -> u8 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

mod sdk {
    #[derive(Clone, Copy)]
    pub enum CoverageEvent {
        RecoveryScanAfterProcessRestart,
        CheckpointCompleted,
        CheckpointAfterProcessRestart,
        RelocationCompleted,
    }

    impl CoverageEvent {
        #[cfg(feature = "sdk")]
        fn name(self) -> &'static str {
            match self {
                Self::RecoveryScanAfterProcessRestart => "recovery_scan_after_process_restart",
                Self::CheckpointCompleted => "checkpoint_completed",
                Self::CheckpointAfterProcessRestart => "checkpoint_after_process_restart",
                Self::RelocationCompleted => "relocation_completed",
            }
        }
    }

    pub fn init() {
        #[cfg(feature = "sdk")]
        antithesis_sdk::antithesis_init();
    }

    pub fn setup_complete() {
        #[cfg(feature = "sdk")]
        {
            let details = antithesis_sdk::serde_json::json!({
                "component": "tidehunter_antithesis_harness",
            });
            antithesis_sdk::lifecycle::setup_complete(&details);
        }
    }

    pub fn sometimes(condition: bool, event: CoverageEvent) {
        #[cfg(feature = "sdk")]
        {
            let details = antithesis_sdk::serde_json::json!({ "event": event.name() });
            match event {
                CoverageEvent::RecoveryScanAfterProcessRestart => {
                    antithesis_sdk::assert_sometimes!(
                        condition,
                        "recovery_scan_after_process_restart",
                        &details
                    )
                }
                CoverageEvent::CheckpointCompleted => {
                    antithesis_sdk::assert_sometimes!(condition, "checkpoint_completed", &details)
                }
                CoverageEvent::CheckpointAfterProcessRestart => antithesis_sdk::assert_sometimes!(
                    condition,
                    "checkpoint_after_process_restart",
                    &details
                ),
                CoverageEvent::RelocationCompleted => {
                    antithesis_sdk::assert_sometimes!(condition, "relocation_completed", &details)
                }
            }
        }
        let _ = condition;
        let _ = event;
    }

    pub fn unreachable(message: &str) -> ! {
        #[cfg(feature = "sdk")]
        {
            let details = antithesis_sdk::serde_json::json!({ "message": message });
            antithesis_sdk::assert_unreachable!("harness unreachable state", &details);
        }
        panic!("{message}");
    }
}
