use rand::Rng;
#[cfg(not(feature = "antithesis_sdk"))]
use rand::SeedableRng;
#[cfg(not(feature = "antithesis_sdk"))]
use rand::rngs::StdRng;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tempdir::TempDir;
use tidehunter::config::Config;
use tidehunter::db::{Db, DbResult};
use tidehunter::key_shape::{KeyShape, KeyShapeBuilder, KeySpace, KeySpaceConfig, KeyType};
use tidehunter::metrics::Metrics;
use tidehunter::{Decision, RelocationStrategy};

const VALUE_MAGIC: &[u8; 8] = b"THSEM001";
const WRITE_SPACE_COUNT: usize = 2;
const ITER_MODEL: usize = 2;

macro_rules! th_assert_always {
    ($condition:expr, $id:literal, $message:literal $(, $arg:expr)* $(,)?) => {{
        let ok = $condition;
        let failure = if ok {
            None
        } else {
            Some(format!($message $(, $arg)*))
        };
        #[cfg(feature = "antithesis_sdk")]
        {
            let details = if ok {
                antithesis_sdk::serde_json::json!({
                    "condition": stringify!($condition),
                })
            } else {
                antithesis_sdk::serde_json::json!({
                    "condition": stringify!($condition),
                    "failure": failure.as_ref().expect("failure is set when assertion fails"),
                })
            };
            antithesis_sdk::assert_always_or_unreachable!(ok, $id, &details);
        }
        if let Some(failure) = failure {
            panic!("{failure}");
        }
    }};
}

#[derive(Clone, Copy)]
struct WriteSpace {
    name: &'static str,
    ks: KeySpace,
}

struct Settings {
    root: PathBuf,
    _temp_dir: Option<TempDir>,
    in_antithesis: bool,
    ops: u64,
    seed: u64,
    key_domain: u64,
    verify_every: u64,
    relocation_epochs: u64,
    relocation_keys_per_epoch: usize,
    relocation_value_bytes: usize,
}

struct Harness {
    settings: Settings,
    db: Arc<Db>,
    write_spaces: [WriteSpace; WRITE_SPACE_COUNT],
    iter_space: WriteSpace,
    models: Vec<BTreeMap<Vec<u8>, Vec<u8>>>,
}

#[derive(Clone)]
enum BatchMutation {
    Write {
        space: usize,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        space: usize,
        key: Vec<u8>,
    },
}

#[cfg(not(feature = "antithesis_sdk"))]
type SemanticsRng = StdRng;
#[cfg(feature = "antithesis_sdk")]
type SemanticsRng = antithesis_sdk::random::AntithesisRng;

#[cfg(not(feature = "antithesis_sdk"))]
fn semantics_rng(settings: &Settings) -> SemanticsRng {
    StdRng::seed_from_u64(settings.seed)
}

// The sdk build always draws randomness through the SDK so Antithesis can steer
// exploration; outside an Antithesis runtime the SDK falls back to its own
// source, so seeded determinism only applies to non-sdk builds.
#[cfg(feature = "antithesis_sdk")]
fn semantics_rng(_settings: &Settings) -> SemanticsRng {
    antithesis_sdk::random::AntithesisRng
}

fn main() {
    sdk::init();

    let settings = Settings::from_env();
    if settings.in_antithesis {
        assert!(
            cfg!(feature = "antithesis_sdk"),
            "Antithesis runs require building antithesis_semantics with --features antithesis_sdk"
        );
    }

    println!(
        "antithesis_semantics start: root={} ops={} seed={} key_domain={} verify_every={} relocation_epochs={} relocation_keys_per_epoch={} antithesis={}",
        settings.root.display(),
        settings.ops,
        settings.seed,
        settings.key_domain,
        settings.verify_every,
        settings.relocation_epochs,
        settings.relocation_keys_per_epoch,
        settings.in_antithesis
    );

    let mut rng = semantics_rng(&settings);
    let mut harness = Harness::new(settings);
    sdk::setup_complete();
    harness.run(&mut rng);
    harness.finish();
}

impl Settings {
    fn from_env() -> Self {
        let in_antithesis = env::var_os("ANTITHESIS_OUTPUT_DIR").is_some();
        let (root, temp_dir) = match env::var_os("SEMANTICS_TEST_ROOT") {
            Some(path) => (PathBuf::from(path), None),
            None => {
                let dir =
                    TempDir::new("tidehunter-antithesis-semantics").expect("create temp root");
                (dir.path().to_path_buf(), Some(dir))
            }
        };

        Self {
            root,
            _temp_dir: temp_dir,
            in_antithesis,
            ops: env_u64("SEMANTICS_TEST_OPS", 20_000),
            seed: env_u64("SEMANTICS_TEST_SEED", 0x5448_5345_4d41_4e54),
            key_domain: env_u64("SEMANTICS_TEST_KEYS", 384).max(2),
            verify_every: env_u64("SEMANTICS_TEST_VERIFY_EVERY", 250).max(1),
            relocation_epochs: env_u64("SEMANTICS_TEST_RELOCATION_EPOCHS", 8).max(4),
            relocation_keys_per_epoch: env_usize("SEMANTICS_TEST_RELOCATION_KEYS_PER_EPOCH", 256)
                .max(1),
            relocation_value_bytes: env_usize("SEMANTICS_TEST_RELOCATION_VALUE_BYTES", 2048)
                .max(64),
        }
    }
}

impl Harness {
    fn new(settings: Settings) -> Self {
        assert!(
            settings.root != PathBuf::from("/"),
            "refusing to clean filesystem root"
        );
        if settings.root.exists() {
            fs::remove_dir_all(&settings.root).expect("clean semantics root");
        }
        fs::create_dir_all(&settings.root).expect("create semantics root");

        let db_path = settings.root.join("db");
        fs::create_dir_all(&db_path).expect("create db root");

        let mut builder = KeyShapeBuilder::new();
        builder
            .add_key_space_config(
                "batch_main",
                8,
                8,
                KeyType::uniform(16),
                KeySpaceConfig::new(),
            )
            .add_key_space_config(
                "batch_aux",
                8,
                8,
                KeyType::uniform(16),
                KeySpaceConfig::new(),
            )
            .add_key_space_config(
                "iter",
                8,
                8,
                KeyType::uniform(16),
                KeySpaceConfig::new().with_bloom_filter(0.01, 4096),
            );
        let key_shape = builder.build();
        let config = semantics_config();
        let metrics = Metrics::new();
        let db = Db::open(&db_path, key_shape, config, metrics).expect("open semantics db");

        let write_spaces = [
            WriteSpace {
                name: "batch_main",
                ks: db.ks("batch_main"),
            },
            WriteSpace {
                name: "batch_aux",
                ks: db.ks("batch_aux"),
            },
        ];
        let iter_space = WriteSpace {
            name: "iter",
            ks: db.ks("iter"),
        };

        Self {
            settings,
            db,
            write_spaces,
            iter_space,
            models: vec![BTreeMap::new(), BTreeMap::new(), BTreeMap::new()],
        }
    }

    fn run(&mut self, rng: &mut SemanticsRng) {
        for step in 0..self.settings.ops {
            match rng.gen_range(0..100) {
                0..=44 => self.run_batch(rng, step),
                45..=69 => self.mutate_iterator_space(rng, step),
                70..=85 => self.verify_iterator_random(rng),
                86..=94 => self.verify_checkpoint_iterator(rng, step),
                _ => {
                    expect_db(self.db.rebuild_control_region(), "rebuild control region");
                }
            }

            if step % self.settings.verify_every == 0 {
                self.verify_batch_spaces();
                self.verify_iterator_all(false, None, None);
                expect_db(
                    self.db.rebuild_control_region(),
                    "periodic rebuild control region",
                );
            }
        }

        self.verify_batch_spaces();
        self.verify_iterator_all(false, None, None);
        self.verify_iterator_all(true, None, None);
        self.run_relocation_pruning();
    }

    fn finish(self) {
        let Harness { settings, db, .. } = self;
        db.wait_for_background_threads_to_finish();
        drop(settings);
    }

    fn run_batch(&mut self, rng: &mut SemanticsRng, step: u64) {
        let mut batch = self.db.write_batch();
        batch.set_tag(format!("semantics-batch-{step}"));
        let mut pending = Vec::new();
        let mut touched = BTreeSet::new();

        if rng.gen_bool(0.25) {
            let space = rng.gen_range(0..WRITE_SPACE_COUNT);
            let key_id = rng.gen_range(0..self.settings.key_domain);
            let key = key_from_id(key_id);
            let first = value_for(space as u8, step, key_id, 0);
            let second = value_for(space as u8, step, key_id, 1);
            batch.write(self.write_spaces[space].ks, key.clone(), first.clone());
            batch.delete(self.write_spaces[space].ks, key.clone());
            batch.write(self.write_spaces[space].ks, key.clone(), second.clone());
            pending.push(BatchMutation::Write {
                space,
                key: key.clone(),
                value: first,
            });
            pending.push(BatchMutation::Delete {
                space,
                key: key.clone(),
            });
            pending.push(BatchMutation::Write {
                space,
                key: key.clone(),
                value: second,
            });
            touched.insert((space, key));
            sdk::sometimes(true, sdk::CoverageEvent::BatchRepeatedKey);
        }

        let op_count = rng.gen_range(1..=16);
        for idx in 0..op_count {
            let space = rng.gen_range(0..WRITE_SPACE_COUNT);
            let key_id = rng.gen_range(0..self.settings.key_domain);
            let key = key_from_id(key_id);
            if rng.gen_bool(0.30) {
                batch.delete(self.write_spaces[space].ks, key.clone());
                pending.push(BatchMutation::Delete {
                    space,
                    key: key.clone(),
                });
            } else {
                let value = value_for(space as u8, step, key_id, idx);
                batch.write(self.write_spaces[space].ks, key.clone(), value.clone());
                pending.push(BatchMutation::Write {
                    space,
                    key: key.clone(),
                    value,
                });
            }
            touched.insert((space, key));
        }

        expect_db(batch.commit(), "commit write batch");
        for mutation in pending {
            self.apply_batch_mutation(mutation);
        }
        sdk::sometimes(true, sdk::CoverageEvent::BatchCommitted);

        for (space, key) in touched {
            self.assert_batch_key_matches(space, &key);
        }
    }

    fn apply_batch_mutation(&mut self, mutation: BatchMutation) {
        match mutation {
            BatchMutation::Write { space, key, value } => {
                self.models[space].insert(key, value);
            }
            BatchMutation::Delete { space, key } => {
                self.models[space].remove(&key);
            }
        }
    }

    fn assert_batch_key_matches(&self, space: usize, key: &[u8]) {
        let actual = expect_db(
            self.db.get(self.write_spaces[space].ks, key),
            "read batch key",
        )
        .map(|value| value.to_vec());
        let expected = self.models[space].get(key).cloned();
        th_assert_always!(
            actual == expected,
            "semantics_batch_key_matches_model",
            "batch key mismatch: space={} key={} expected_len={:?} actual_len={:?}",
            self.write_spaces[space].name,
            hex_key(key),
            expected.as_ref().map(Vec::len),
            actual.as_ref().map(Vec::len)
        );
    }

    fn verify_batch_spaces(&self) {
        for space in 0..WRITE_SPACE_COUNT {
            for key in self.models[space].keys() {
                self.assert_batch_key_matches(space, key);
            }
            let actual = self.collect_db_iterator(self.write_spaces[space].ks, false, None, None);
            let expected = entries_from_model(&self.models[space], false, None, None);
            th_assert_always!(
                actual == expected,
                "semantics_batch_key_matches_model",
                "batch iterator mismatch: space={} expected_len={} actual_len={}",
                self.write_spaces[space].name,
                expected.len(),
                actual.len()
            );
        }
    }

    fn mutate_iterator_space(&mut self, rng: &mut SemanticsRng, step: u64) {
        let count = rng.gen_range(1..=8);
        for idx in 0..count {
            let key_id = rng.gen_range(0..self.settings.key_domain);
            let key = key_from_id(key_id);
            if rng.gen_bool(0.25) {
                expect_db(
                    self.db.remove(self.iter_space.ks, key.clone()),
                    "remove iter key",
                );
                self.models[ITER_MODEL].remove(&key);
            } else {
                let value = value_for(0x80, step, key_id, idx);
                expect_db(
                    self.db
                        .insert(self.iter_space.ks, key.clone(), value.clone()),
                    "insert iter key",
                );
                self.models[ITER_MODEL].insert(key, value);
            }
        }
    }

    fn verify_iterator_random(&self, rng: &mut SemanticsRng) {
        match rng.gen_range(0..4) {
            0 => self.verify_iterator_all(false, None, None),
            1 => self.verify_iterator_all(true, None, None),
            _ => {
                let (lower, upper) = self.random_bounds(rng);
                let reverse = rng.gen_bool(0.5);
                self.verify_iterator_all(reverse, Some(lower), Some(upper));
            }
        }
    }

    fn verify_iterator_all(&self, reverse: bool, lower: Option<Vec<u8>>, upper: Option<Vec<u8>>) {
        let actual =
            self.collect_db_iterator(self.iter_space.ks, reverse, lower.as_ref(), upper.as_ref());
        let expected = entries_from_model(
            &self.models[ITER_MODEL],
            reverse,
            lower.as_ref(),
            upper.as_ref(),
        );
        th_assert_always!(
            actual == expected,
            "semantics_iterator_matches_model",
            "iterator mismatch: reverse={} bounded={} expected_len={} actual_len={}",
            reverse,
            lower.is_some() || upper.is_some(),
            expected.len(),
            actual.len()
        );

        match (reverse, lower.is_some() || upper.is_some()) {
            (false, false) => sdk::sometimes(true, sdk::CoverageEvent::IteratorForward),
            (true, false) => sdk::sometimes(true, sdk::CoverageEvent::IteratorReverse),
            (_, true) => sdk::sometimes(true, sdk::CoverageEvent::IteratorBounded),
        }
    }

    fn verify_checkpoint_iterator(&mut self, rng: &mut SemanticsRng, step: u64) {
        let checkpoint = self.db.checkpoint();
        let snapshot = self.models[ITER_MODEL].clone();
        let (lower, upper) = self.random_bounds(rng);
        let reverse = rng.gen_bool(0.5);

        self.mutate_iterator_space(rng, step);

        let actual = collect_checkpoint_iterator(
            &checkpoint,
            self.iter_space.ks,
            reverse,
            Some(&lower),
            Some(&upper),
        );
        let expected = entries_from_model(&snapshot, reverse, Some(&lower), Some(&upper));
        th_assert_always!(
            actual == expected,
            "semantics_checkpoint_iterator_stable",
            "checkpoint iterator mismatch: reverse={} expected_len={} actual_len={}",
            reverse,
            expected.len(),
            actual.len()
        );
        sdk::sometimes(true, sdk::CoverageEvent::CheckpointIterator);
    }

    fn random_bounds(&self, rng: &mut SemanticsRng) -> (Vec<u8>, Vec<u8>) {
        let a = rng.gen_range(0..self.settings.key_domain);
        let b = rng.gen_range(0..self.settings.key_domain);
        let lower = a.min(b);
        let upper = a.max(b).saturating_add(1).min(self.settings.key_domain);
        if lower == upper {
            (
                key_from_id(lower),
                key_from_id((lower + 1).min(self.settings.key_domain)),
            )
        } else {
            (key_from_id(lower), key_from_id(upper))
        }
    }

    fn run_relocation_pruning(&mut self) {
        let db_path = self.settings.root.join("relocation_pruning_db");
        fs::create_dir_all(&db_path).expect("create relocation pruning db root");
        let relocation_watermark = Arc::new(AtomicU64::new(0));
        let key_shape = relocation_key_shape(relocation_watermark.clone());
        let config = semantics_config();
        let metrics = Metrics::new();
        let mut prune_model = BTreeMap::new();
        let watermark = (self.settings.relocation_epochs / 2).max(1);
        let strict_prune_watermark = watermark.saturating_sub(1).max(1);

        if !self.relocation_payload_spans_reclaimable_prefix(&config, strict_prune_watermark) {
            println!(
                "skipping relocation pruning checks: payload is too small to span a reclaimable WAL prefix"
            );
            return;
        }

        {
            let db = Db::open(&db_path, key_shape.clone(), config.clone(), metrics.clone())
                .expect("open relocation pruning db for writes");
            let prune_ks = db.ks("prune");
            for epoch in 0..self.settings.relocation_epochs {
                for id in 0..self.settings.relocation_keys_per_epoch {
                    let key = relocation_key(epoch, id);
                    let value = relocation_value(epoch, id, self.settings.relocation_value_bytes);
                    expect_db(
                        db.insert(prune_ks, key.clone(), value.clone()),
                        "insert relocation key",
                    );
                    prune_model.insert(key, value);
                }
            }
            expect_db(
                db.rebuild_control_region(),
                "rebuild control region before relocation close",
            );
            db.wait_for_background_threads_to_finish();
        }

        relocation_watermark.store(watermark, Ordering::Relaxed);
        {
            let db = Db::open(&db_path, key_shape.clone(), config.clone(), metrics.clone())
                .expect("open relocation pruning db for relocation");
            expect_db(
                db.rebuild_control_region(),
                "rebuild control region before relocation",
            );
            db.start_blocking_relocation_with_strategy(RelocationStrategy::WalBased);
            // The relocation-filter contract is eventual: a Remove-decided key
            // must eventually disappear, but may stay readable after this call
            // returns (index cleanup and WAL file deletion are deferred). The
            // gauges are logged to make Antithesis reports easier to debug;
            // nothing is asserted on them.
            println!(
                "relocation positions: terminal={} target={}",
                metrics.relocation_terminal_position.get(),
                metrics.relocation_target_position.get(),
            );
            sdk::sometimes(true, sdk::CoverageEvent::RelocationRan);
            expect_db(
                db.rebuild_control_region(),
                "rebuild control region after relocation",
            );
            db.wait_for_background_threads_to_finish();
        }

        let mut pruned_seen = 0_u64;
        let mut old_readable = 0_u64;
        let mut live_checked = 0_u64;
        {
            let db = Db::open(&db_path, key_shape, config, metrics)
                .expect("open relocation pruning db for verification");
            let prune_ks = db.ks("prune");
            expect_db(
                db.rebuild_control_region(),
                "rebuild control region before relocation verification",
            );
            for epoch in 0..self.settings.relocation_epochs {
                for id in 0..self.settings.relocation_keys_per_epoch {
                    let key = relocation_key(epoch, id);
                    let actual = expect_db(db.get(prune_ks, &key), "read relocation key")
                        .map(|value| value.to_vec());
                    if epoch < watermark {
                        // Old key: gone is fine, still readable is fine, but a
                        // readable old key must hold its exact old value.
                        match actual {
                            None => pruned_seen += 1,
                            Some(value) => {
                                let expected = prune_model.get(&key);
                                th_assert_always!(
                                    expected == Some(&value),
                                    "semantics_relocation_old_key_value_intact",
                                    "old key readable with wrong value: epoch={} id={} expected_len={:?} actual_len={}",
                                    epoch,
                                    id,
                                    expected.map(Vec::len),
                                    value.len()
                                );
                                old_readable += 1;
                            }
                        }
                    } else {
                        let expected = prune_model.get(&key).cloned();
                        th_assert_always!(
                            actual == expected,
                            "semantics_relocation_keeps_live_keys",
                            "relocation changed live key: epoch={} id={} expected_len={:?} actual_len={:?}",
                            epoch,
                            id,
                            expected.as_ref().map(Vec::len),
                            actual.as_ref().map(Vec::len)
                        );
                        live_checked += 1;
                    }
                }
            }

            self.verify_relocation_iterators(&db, prune_ks, &prune_model, watermark);

            db.wait_for_background_threads_to_finish();
        }
        println!(
            "relocation pruning: pruned_seen={pruned_seen} old_readable={old_readable} live_checked={live_checked}"
        );
        sdk::sometimes(
            pruned_seen > 0 && live_checked > 0,
            sdk::CoverageEvent::PruningVerified,
        );
    }

    // After a filtered relocation, stale index entries for removed keys can
    // legitimately exist (eventual cleanup) and sort in front of the live
    // keys. Iteration must skip entries whose WAL position was already
    // reclaimed and still expose every live key; ending the iteration early
    // instead would silently hide live data.
    fn verify_relocation_iterators(
        &self,
        db: &Arc<Db>,
        prune_ks: KeySpace,
        prune_model: &BTreeMap<Vec<u8>, Vec<u8>>,
        watermark: u64,
    ) {
        let live_floor = relocation_key(watermark, 0);

        let forward = collect_db_iterator_entries(db, prune_ks, false, None, None);
        Self::check_iterated_entries(&forward, prune_model, "forward");
        Self::check_iteration_order(&forward, false, "forward");
        let forward_keys: BTreeSet<&[u8]> = forward.iter().map(|(key, _)| key.as_slice()).collect();
        for key in prune_model.range(live_floor.clone()..).map(|(key, _)| key) {
            th_assert_always!(
                forward_keys.contains(key.as_slice()),
                "semantics_relocation_iter_forward_covers_live",
                "forward iteration missed live key: epoch={} id={}",
                relocation_epoch_from_key(key),
                relocation_id_from_key(key)
            );
        }

        let reverse = collect_db_iterator_entries(db, prune_ks, true, None, None);
        Self::check_iterated_entries(&reverse, prune_model, "reverse");
        Self::check_iteration_order(&reverse, true, "reverse");
        let reverse_keys: BTreeSet<&[u8]> = reverse.iter().map(|(key, _)| key.as_slice()).collect();
        for key in prune_model.range(live_floor.clone()..).map(|(key, _)| key) {
            th_assert_always!(
                reverse_keys.contains(key.as_slice()),
                "semantics_relocation_iter_reverse_covers_live",
                "reverse iteration missed live key: epoch={} id={}",
                relocation_epoch_from_key(key),
                relocation_id_from_key(key)
            );
        }

        // Start inside the removed key range so the iterator has to begin
        // directly on potentially-stale entries.
        let bounded_lower = relocation_key(1, 0);
        let bounded = collect_db_iterator_entries(db, prune_ks, false, Some(&bounded_lower), None);
        Self::check_iterated_entries(&bounded, prune_model, "bounded");
        Self::check_iteration_order(&bounded, false, "bounded");
        for (key, _) in &bounded {
            th_assert_always!(
                key.as_slice() >= bounded_lower.as_slice(),
                "semantics_relocation_iter_respects_lower_bound",
                "bounded iteration returned key below lower bound: epoch={} id={}",
                relocation_epoch_from_key(key),
                relocation_id_from_key(key)
            );
        }
        let bounded_keys: BTreeSet<&[u8]> = bounded.iter().map(|(key, _)| key.as_slice()).collect();
        for key in prune_model.range(live_floor.clone()..).map(|(key, _)| key) {
            th_assert_always!(
                bounded_keys.contains(key.as_slice()),
                "semantics_relocation_iter_bounded_covers_live",
                "bounded iteration missed live key: epoch={} id={}",
                relocation_epoch_from_key(key),
                relocation_id_from_key(key)
            );
        }
    }

    fn check_iterated_entries(
        entries: &[(Vec<u8>, Vec<u8>)],
        prune_model: &BTreeMap<Vec<u8>, Vec<u8>>,
        direction: &str,
    ) {
        for (key, value) in entries {
            let expected = prune_model.get(key);
            th_assert_always!(
                expected == Some(value),
                "semantics_relocation_iter_entry_matches_model",
                "{} iteration returned unexpected entry: epoch={} id={} known_key={} value_len={}",
                direction,
                relocation_epoch_from_key(key),
                relocation_id_from_key(key),
                expected.is_some(),
                value.len()
            );
        }
    }

    fn check_iteration_order(entries: &[(Vec<u8>, Vec<u8>)], reverse: bool, direction: &str) {
        for pair in entries.windows(2) {
            let ordered = if reverse {
                pair[0].0 > pair[1].0
            } else {
                pair[0].0 < pair[1].0
            };
            th_assert_always!(
                ordered,
                "semantics_relocation_iter_ordered",
                "{} iteration returned keys out of order: epoch={} id={} then epoch={} id={}",
                direction,
                relocation_epoch_from_key(&pair[0].0),
                relocation_id_from_key(&pair[0].0),
                relocation_epoch_from_key(&pair[1].0),
                relocation_id_from_key(&pair[1].0)
            );
        }
    }

    fn relocation_payload_spans_reclaimable_prefix(
        &self,
        config: &Config,
        strict_prune_watermark: u64,
    ) -> bool {
        let estimated_strict_prefix_bytes = strict_prune_watermark as usize
            * self.settings.relocation_keys_per_epoch
            * self.settings.relocation_value_bytes;
        estimated_strict_prefix_bytes >= (config.wal_file_size as usize * 2)
    }

    fn collect_db_iterator(
        &self,
        ks: KeySpace,
        reverse: bool,
        lower: Option<&Vec<u8>>,
        upper: Option<&Vec<u8>>,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        collect_db_iterator_entries(&self.db, ks, reverse, lower, upper)
    }
}

fn semantics_config() -> Arc<Config> {
    let mut config = Config::small();
    config.frag_size = 256 * 1024;
    config.wal_file_size = 2 * config.frag_size;
    config.snapshot_unload_threshold = 0;
    config.snapshot_written_bytes = 2 * config.frag_size;
    config.relocation_max_reclaim_pct = 100;
    config.relocation_batch_max_bytes = Some((config.frag_size / 2) as usize);
    Arc::new(config)
}

fn relocation_key_shape(relocation_watermark: Arc<AtomicU64>) -> KeyShape {
    let filter_watermark = relocation_watermark.clone();
    let prune_config = KeySpaceConfig::new().with_relocation_filter(move |key, _value| {
        let epoch = relocation_epoch_from_key(key);
        if epoch < filter_watermark.load(Ordering::Relaxed) {
            Decision::Remove
        } else {
            Decision::StopRelocation
        }
    });

    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space_config("prune", 8, 1, KeyType::uniform(1), prune_config);
    builder.build()
}

fn collect_db_iterator_entries(
    db: &Arc<Db>,
    ks: KeySpace,
    reverse: bool,
    lower: Option<&Vec<u8>>,
    upper: Option<&Vec<u8>>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut iterator = db.iterator(ks);
    if let Some(lower) = lower {
        iterator.set_lower_bound(lower.clone());
    }
    if let Some(upper) = upper {
        iterator.set_upper_bound(upper.clone());
    }
    if reverse {
        iterator.reverse();
    }
    iterator
        .map(|entry| {
            let (key, value) = expect_db(entry, "advance db iterator");
            (key.to_vec(), value.to_vec())
        })
        .collect()
}

fn collect_checkpoint_iterator(
    checkpoint: &Arc<tidehunter::checkpoint::DbCheckpoint>,
    ks: KeySpace,
    reverse: bool,
    lower: Option<&Vec<u8>>,
    upper: Option<&Vec<u8>>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut iterator = checkpoint.iterator(ks);
    if let Some(lower) = lower {
        iterator.set_lower_bound(lower.clone());
    }
    if let Some(upper) = upper {
        iterator.set_upper_bound(upper.clone());
    }
    if reverse {
        iterator.reverse();
    }
    iterator
        .map(|entry| {
            let (key, value) = expect_db(entry, "advance checkpoint iterator");
            (key.to_vec(), value.to_vec())
        })
        .collect()
}

fn entries_from_model(
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
    reverse: bool,
    lower: Option<&Vec<u8>>,
    upper: Option<&Vec<u8>>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut entries = model
        .iter()
        .filter(|(key, _value)| {
            let lower_ok = lower.is_none_or(|bound| key.as_slice() >= bound.as_slice());
            let upper_ok = upper.is_none_or(|bound| key.as_slice() < bound.as_slice());
            lower_ok && upper_ok
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    if reverse {
        entries.reverse();
    }
    entries
}

fn key_from_id(id: u64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

fn value_for(tag: u8, step: u64, key_id: u64, variant: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(48);
    value.extend_from_slice(VALUE_MAGIC);
    value.push(tag);
    value.extend_from_slice(&step.to_be_bytes());
    value.extend_from_slice(&key_id.to_be_bytes());
    value.extend_from_slice(&(variant as u64).to_be_bytes());
    let target_len = 32 + ((step ^ key_id ^ variant as u64) % 31) as usize;
    value.resize(target_len, tag ^ 0x5a);
    value
}

fn relocation_key(epoch: u64, id: usize) -> Vec<u8> {
    let packed = (epoch << 32) | ((id as u64) & 0xffff_ffff);
    packed.to_be_bytes().to_vec()
}

fn relocation_epoch_from_key(key: &[u8]) -> u64 {
    let bytes: [u8; 8] = key.try_into().expect("relocation keys are fixed 8 bytes");
    u64::from_be_bytes(bytes) >> 32
}

fn relocation_id_from_key(key: &[u8]) -> u64 {
    let bytes: [u8; 8] = key.try_into().expect("relocation keys are fixed 8 bytes");
    u64::from_be_bytes(bytes) & 0xffff_ffff
}

fn relocation_value(epoch: u64, id: usize, value_bytes: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(value_bytes);
    value.extend_from_slice(VALUE_MAGIC);
    value.extend_from_slice(&epoch.to_be_bytes());
    value.extend_from_slice(&(id as u64).to_be_bytes());
    value.resize(value_bytes, (epoch as u8).wrapping_add(id as u8));
    value
}

fn hex_key(key: &[u8]) -> String {
    key.iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

fn expect_db<T>(result: DbResult<T>, context: &str) -> T {
    match result {
        Ok(value) => value,
        Err(error) => {
            let message = format!("{context}: {error:?}");
            sdk::unreachable(&message);
        }
    }
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

mod sdk {
    #[derive(Clone, Copy)]
    pub enum CoverageEvent {
        BatchCommitted,
        BatchRepeatedKey,
        IteratorForward,
        IteratorReverse,
        IteratorBounded,
        CheckpointIterator,
        RelocationRan,
        PruningVerified,
    }

    impl CoverageEvent {
        #[cfg(feature = "antithesis_sdk")]
        fn name(self) -> &'static str {
            match self {
                Self::BatchCommitted => "semantics_batch_committed",
                Self::BatchRepeatedKey => "semantics_batch_repeated_key",
                Self::IteratorForward => "semantics_iterator_forward",
                Self::IteratorReverse => "semantics_iterator_reverse",
                Self::IteratorBounded => "semantics_iterator_bounded",
                Self::CheckpointIterator => "semantics_checkpoint_iterator",
                Self::RelocationRan => "semantics_relocation_ran",
                Self::PruningVerified => "semantics_pruning_verified",
            }
        }
    }

    pub fn init() {
        #[cfg(feature = "antithesis_sdk")]
        antithesis_sdk::antithesis_init();
    }

    pub fn setup_complete() {
        #[cfg(feature = "antithesis_sdk")]
        {
            let details = antithesis_sdk::serde_json::json!({
                "component": "tidehunter_antithesis_semantics",
            });
            antithesis_sdk::lifecycle::setup_complete(&details);
        }
    }

    pub fn sometimes(condition: bool, event: CoverageEvent) {
        #[cfg(feature = "antithesis_sdk")]
        {
            let details = antithesis_sdk::serde_json::json!({ "event": event.name() });
            match event {
                CoverageEvent::BatchCommitted => antithesis_sdk::assert_sometimes!(
                    condition,
                    "semantics_batch_committed",
                    &details
                ),
                CoverageEvent::BatchRepeatedKey => antithesis_sdk::assert_sometimes!(
                    condition,
                    "semantics_batch_repeated_key",
                    &details
                ),
                CoverageEvent::IteratorForward => antithesis_sdk::assert_sometimes!(
                    condition,
                    "semantics_iterator_forward",
                    &details
                ),
                CoverageEvent::IteratorReverse => antithesis_sdk::assert_sometimes!(
                    condition,
                    "semantics_iterator_reverse",
                    &details
                ),
                CoverageEvent::IteratorBounded => antithesis_sdk::assert_sometimes!(
                    condition,
                    "semantics_iterator_bounded",
                    &details
                ),
                CoverageEvent::CheckpointIterator => antithesis_sdk::assert_sometimes!(
                    condition,
                    "semantics_checkpoint_iterator",
                    &details
                ),
                CoverageEvent::RelocationRan => antithesis_sdk::assert_sometimes!(
                    condition,
                    "semantics_relocation_ran",
                    &details
                ),
                CoverageEvent::PruningVerified => antithesis_sdk::assert_sometimes!(
                    condition,
                    "semantics_pruning_verified",
                    &details
                ),
            }
        }
        let _ = condition;
        let _ = event;
    }

    pub fn unreachable(message: &str) -> ! {
        #[cfg(feature = "antithesis_sdk")]
        {
            let details = antithesis_sdk::serde_json::json!({ "message": message });
            antithesis_sdk::assert_unreachable!("semantics workload unreachable state", &details);
        }
        panic!("{message}");
    }
}
