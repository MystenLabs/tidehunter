use crate::configs::METRICS_PORT;
use crate::metrics::BenchmarkMetrics;
use crate::storage::Storage;
use crate::storage::rocks::RocksStorage;
use ::prometheus::Registry;
use bytes::BufMut;
use clap::Parser;
use configs::{
    Backend, EpochFilterMode, KeyLayout, ReadMode, RelocationConfig, StressArgs,
    StressClientParameters, StressTestConfigs,
};
use histogram::AtomicHistogram;
use parking_lot::RwLock;
use rand::rngs::{StdRng, ThreadRng};
use rand::{Rng, RngCore, SeedableRng};
use rand_distr::{Distribution, Zipf};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};
use std::{fs, thread};
use tidehunter::key_shape::{KeyShape, KeySpaceConfig, KeyType};
use tidehunter::{Decision, RelocationStrategy, compute_target_position_from_ratio};

mod configs;
mod metrics;

/// Maximum value power for the latency histogram (max recordable value is 2^26 microseconds ≈ 67s)
const LATENCY_HISTOGRAM_MAX_VALUE_POWER: u8 = 26;
#[allow(dead_code)]
mod prometheus;
mod storage;

macro_rules! report {
    ($report: expr, $($arg:tt)*) => {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let line = format!("[{}] {}", timestamp, format!($($arg)*));
        println!("{line}");
        $report.lines.push('\n');
        $report.lines.push_str(&line);
    };
}

pub fn main() {
    let start_time = SystemTime::now();
    let mut report = Report::default();

    report!(report, "BENCHMARK_START");

    let args: StressArgs = StressArgs::parse();
    let default_config = if let Some(parameters_path) = &args.parameters_path {
        report!(report, "Loading default configs from {}", parameters_path);
        StressTestConfigs::from_yml(parameters_path).unwrap()
    } else {
        StressTestConfigs::default()
    };
    let config = configs::override_default_args(args, default_config);

    report!(report, "DB parameters: {:#?}", &config.db_parameters);
    println!(
        "Stress client parameters: {:#?}",
        &config.stress_client_parameters
    );

    if config.stress_client_parameters.reuse.is_some()
        && config.stress_client_parameters.db_path.is_some()
    {
        panic!("--reuse and --db-path are mutually exclusive");
    }
    let temp_dir = if let Some(path) = &config.stress_client_parameters.path {
        tempdir::TempDir::new_in(path, "stress").unwrap()
    } else {
        tempdir::TempDir::new("stress").unwrap()
    };

    let path = if let Some(reuse) = &config.stress_client_parameters.reuse {
        reuse.parse().unwrap()
    } else if let Some(db_path) = &config.stress_client_parameters.db_path {
        let p: std::path::PathBuf = db_path.parse().unwrap();
        fs::create_dir_all(&p).expect("failed to create db_path");
        p
    } else {
        temp_dir.path().to_path_buf()
    };

    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "<unknown>".to_string());
    report!(report, "Hostname: {}", hostname);
    report!(report, "Path to storage: {}", path.display());
    report!(
        report,
        "Using {:?} key layout",
        config.stress_client_parameters.key_layout
    );
    report!(
        report,
        "Using {:?} read mode",
        config.stress_client_parameters.read_mode
    );
    let print_report = config.stress_client_parameters.report;
    let measure_open = config.stress_client_parameters.measure_open;
    if measure_open && !matches!(config.stress_client_parameters.backend, Backend::Tidehunter) {
        panic!("--measure-open is only supported for the Tidehunter backend");
    }
    if measure_open && config.stress_client_parameters.crash_after_secs.is_some() {
        panic!("--measure-open and --crash-after-secs are mutually exclusive");
    }
    if let Some(secs) = config.stress_client_parameters.crash_after_secs {
        report!(
            report,
            "CRASH: scheduled simulated crash via process::exit(137) at +{secs}s"
        );
        thread::spawn(move || {
            thread::sleep(Duration::from_secs(secs));
            // process::exit bypasses Drop and stack unwinding (running only
            // atexit hooks + stdio flush), simulating SIGKILL for recovery
            // tests. The Db is never drop()-ped, so no in-flight WAL fsync
            // or relocation cleanup runs.
            eprintln!("[crash-after-{secs}s] exiting via process::exit(137)");
            std::process::exit(137);
        });
    }
    let mut recovery_timings: Option<tidehunter::db::RecoveryTimings> = None;
    let registry = Registry::new();
    let benchmark_metrics = BenchmarkMetrics::new_in(&registry, &config);
    prometheus::start_prometheus_server(
        format!("0.0.0.0:{METRICS_PORT}").parse().unwrap(),
        &registry,
    );

    // Cumulative count of foreground (app-side) bytes written. Incremented
    // by writer threads on each insert; consumed by the relocation driver
    // (when `epoch_budget_bytes` is set) to decide when to trigger the next
    // pass, and reported at the end as the denominator for foreground WA.
    let app_bytes_written: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    // Bytes the relocation filter has seen during the current pass. Reset
    // to 0 by the relocation driver at the start of each pass; incremented
    // by the filter closure on each WAL entry visited in Phase A.
    let bytes_seen_in_pass: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

    let storage: Arc<dyn Storage> = match config.stress_client_parameters.backend {
        Backend::Tidehunter => {
            if config.db_parameters.direct_io {
                report!(report, "Using **direct IO**");
            }
            use crate::storage::tidehunter::TidehunterStorage;
            let mutexes = config
                .stress_client_parameters
                .num_mutexes
                .unwrap_or(4096 * 32);
            report!(report, "Num mutexes: **{mutexes}**");
            let key_len = config.stress_client_parameters.key_len;
            let bloom = match (
                config.stress_client_parameters.bloom_filter_rate,
                config.stress_client_parameters.bloom_filter_count,
            ) {
                (Some(rate), Some(count)) => {
                    report!(
                        report,
                        "Bloom filter **enabled** (rate={rate}, count={count})"
                    );
                    Some((rate, count))
                }
                (None, None) => None,
                _ => panic!(
                    "bloom_filter_rate and bloom_filter_count must both be set or both be unset"
                ),
            };
            let cells_per_mutex = config.stress_client_parameters.cells_per_mutex.unwrap_or(1);
            if config.stress_client_parameters.cells_per_mutex.is_some()
                && !matches!(
                    config.stress_client_parameters.key_layout,
                    KeyLayout::Uniform
                )
            {
                panic!("cells_per_mutex only applies to key_layout: Uniform");
            }
            report!(report, "Cells per mutex: **{cells_per_mutex}**");
            let epoch_filter = config
                .stress_client_parameters
                .epoch_budget_bytes
                .map(|budget| {
                    report!(
                        report,
                        "Epoch filter **enabled** (budget={} bytes, mode={:?})",
                        budget,
                        config.stress_client_parameters.epoch_filter_mode
                    );
                    EpochFilterArgs {
                        bytes_seen: bytes_seen_in_pass.clone(),
                        budget,
                        mode: config.stress_client_parameters.epoch_filter_mode,
                    }
                });
            let (key_shape, ks) = match config.stress_client_parameters.key_layout {
                KeyLayout::Uniform => KeyShape::new_single_config(
                    key_len,
                    mutexes,
                    KeyType::uniform(cells_per_mutex),
                    key_space_config(bloom, epoch_filter.clone()),
                ),
                KeyLayout::SequenceChoice => {
                    let key_type = KeyType::prefix_uniform(8, 2);
                    KeyShape::new_single_config(
                        key_len,
                        mutexes,
                        key_type,
                        key_space_config(bloom, epoch_filter.clone()),
                    )
                }
                KeyLayout::ChoiceSequence => {
                    let key_type = KeyType::prefix_uniform(15, 5);
                    KeyShape::new_single_config(
                        key_len,
                        mutexes,
                        key_type,
                        key_space_config(bloom, epoch_filter.clone()),
                    )
                }
            };
            let (storage, timings) = TidehunterStorage::open_with_timings(
                &registry,
                config.db_parameters,
                &path,
                (key_shape, ks),
            );
            recovery_timings = Some(timings);
            if !measure_open {
                if !config.stress_client_parameters.no_snapshot {
                    report!(report, "Periodic snapshot **enabled**");
                    storage.db.start_periodic_snapshot();
                } else {
                    report!(report, "Periodic snapshot **disabled**");
                }

                // Start continuous relocation if enabled
                if let Some(ref relocation_config) = config.stress_client_parameters.relocation {
                    let epoch_budget = config.stress_client_parameters.epoch_budget_bytes;
                    report!(
                        report,
                        "Starting continuous {:?} relocation (trigger: {})",
                        relocation_config,
                        match epoch_budget {
                            Some(b) => format!("every {b} app bytes written"),
                            None => "every 30s".to_string(),
                        }
                    );
                    let db_clone = storage.db.clone();
                    let relocation_config = relocation_config.clone();
                    let app_bytes_clone = app_bytes_written.clone();
                    let bytes_seen_clone = bytes_seen_in_pass.clone();
                    thread::spawn(move || {
                        let mut last_trigger_bytes = 0u64;
                        loop {
                            // Wait for the next trigger.
                            match epoch_budget {
                                Some(budget) => {
                                    // Bytes-based: poll until the writer has
                                    // produced `budget` more bytes since the
                                    // last pass, then reset the filter's
                                    // pass-local counter.
                                    loop {
                                        let cur = app_bytes_clone.load(Ordering::Relaxed);
                                        if cur.saturating_sub(last_trigger_bytes) >= budget {
                                            last_trigger_bytes = cur;
                                            break;
                                        }
                                        thread::sleep(Duration::from_secs(5));
                                    }
                                    bytes_seen_clone.store(0, Ordering::Relaxed);
                                }
                                None => {
                                    // Time-based: existing behavior. The
                                    // sleep happens *after* the pass below so
                                    // the first pass starts immediately.
                                }
                            }

                            // Convert RelocationConfig to RelocationStrategy for this iteration
                            let strategy = match &relocation_config {
                                RelocationConfig::Wal => RelocationStrategy::WalBased,
                                RelocationConfig::Index { ratio: None } => {
                                    RelocationStrategy::IndexBased(None)
                                }
                                RelocationConfig::Index { ratio: Some(r) } => {
                                    // Compute fresh target position from ratio each iteration
                                    let target_position =
                                        compute_target_position_from_ratio(&db_clone, *r);
                                    RelocationStrategy::IndexBased(target_position)
                                }
                            };

                            // Start relocation and let it run to completion
                            db_clone.start_blocking_relocation_with_strategy(strategy);

                            if epoch_budget.is_none() {
                                // Take a 30 second break between relocations
                                thread::sleep(Duration::from_secs(30));
                            }
                        }
                    });
                }
            }

            Arc::new(storage)
        }
        Backend::Rocksdb => {
            let storage = RocksStorage::open(&path, false, config.db_parameters.metrics_enabled);
            Arc::new(storage)
        }
        Backend::Blobdb => {
            let storage = RocksStorage::open(&path, true, config.db_parameters.metrics_enabled);
            Arc::new(storage)
        }
    };
    if measure_open {
        run_measure_open(
            &storage,
            recovery_timings.expect("Tidehunter open should have produced timings"),
            &config.stress_client_parameters,
            &mut report,
        );
        let clean_path = if config.stress_client_parameters.clean_after_measure {
            config.stress_client_parameters.reuse.clone()
        } else {
            None
        };
        // Drop the storage (and the underlying Db) so the lock is released and
        // mmaps unmapped before we delete files.
        drop(storage);
        if let Some(p) = clean_path {
            report!(report, "CLEAN: removing {p}");
            if let Err(e) = fs::remove_dir_all(&p) {
                eprintln!("clean_after_measure: failed to remove {p}: {e}");
            }
        }
        if print_report {
            report!(report, "Writing report file");
            fs::write("report.txt", &report.lines).unwrap();
        }
        if config.stress_client_parameters.preserve {
            temp_dir.into_path();
        }
        return;
    }
    let stress = Stress {
        storage,
        parameters: Arc::new(config.stress_client_parameters),
        benchmark_metrics,
        app_bytes_written: app_bytes_written.clone(),
    };
    report!(report, "Starting write test");
    let write_sec;
    if stress.parameters.reuse.is_none() {
        let (elapsed, _) = stress.measure(
            stress.parameters.write_threads,
            StressThread::run_writes,
            &mut report,
        );
        let written = stress.parameters.writes * stress.parameters.write_threads;
        let written_bytes = written * stress.parameters.write_size;
        let msecs = elapsed.as_millis() as usize;
        write_sec = dec_div(written / msecs * 1000);
        report!(
            report,
            "Write test done in {elapsed:?}: {} writes/s, {}/sec",
            write_sec,
            byte_div(written_bytes / msecs * 1000)
        );
    } else {
        write_sec = "".to_string();
        report!(report, "Skipping writes because reuse is specified");
    }
    {
        let storage_len = fs_extra::dir::get_size(&path).unwrap();
        report!(
            report,
            "Storage used {:.1} Gb",
            storage_len as f64 / 1024. / 1024. / 1024.
        );
    }
    let ops_sec = if stress.parameters.mixed_duration_secs == 0 {
        // Skip the mixed phase entirely. Running it with duration 0 produces
        // an empty latency histogram, and `Stress::measure`'s percentile
        // extraction unwraps a `None` and panics. Used by R6 fill-only entries
        // and any other "fill, then exit" workflow.
        report!(report, "Mixed phase skipped (mixed_duration_secs = 0)");
        String::new()
    } else {
        if stress.parameters.pause_between_phases_secs > 0 {
            report!(
                report,
                "Pausing for {} seconds between phases",
                stress.parameters.pause_between_phases_secs
            );
            thread::sleep(Duration::from_secs(
                stress.parameters.pause_between_phases_secs,
            ));
        }
        report!(
            report,
            "Starting mixed read/write test for {} seconds ({}% reads, {}% writes)",
            stress.parameters.mixed_duration_secs,
            stress.parameters.read_percentage,
            100 - stress.parameters.read_percentage
        );
        let manual_stop = if stress.parameters.background_writes > 0 {
            stress.background(
                stress.parameters.write_threads,
                StressThread::run_background_writes,
            )
        } else {
            Default::default()
        };
        let (elapsed, total_ops) = stress.measure(
            stress.parameters.mixed_threads,
            StressThread::run_mixed_operations,
            &mut report,
        );
        manual_stop.store(true, Ordering::Relaxed);
        let total_bytes = total_ops * stress.parameters.write_size;
        let msecs = elapsed.as_millis() as usize;
        let ops_sec = dec_div(total_ops / msecs * 1000);
        report!(
            report,
            "Mixed test done in {elapsed:?}: {} ops/s, {}/sec",
            ops_sec,
            byte_div(total_bytes / msecs * 1000),
        );
        ops_sec
    };
    {
        let total_app_bytes = stress.app_bytes_written.load(Ordering::Relaxed);
        report!(
            report,
            "App bytes written total: {} ({})",
            total_app_bytes,
            byte_div(total_app_bytes as usize)
        );
    }
    if print_report {
        report!(report, "Writing report file");
        fs::write("report.txt", &report.lines).unwrap();
    }
    if !stress.parameters.tldr.is_empty() {
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open("tldr.txt")
            .unwrap();
        let start_time = start_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let end_time = SystemTime::now();
        let end_time = end_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        writeln!(
            file,
            "{: <15}|{: <15}|{: <24}|{: <8}|{: <8}",
            start_time, end_time, stress.parameters.tldr, write_sec, ops_sec
        )
        .unwrap();
    }
    report!(report, "BENCHMARK_END");

    if stress.parameters.preserve {
        temp_dir.into_path();
    }
}

fn pos_to_key_value(
    pos: u64,
    key_len: usize,
    value_len: usize,
    layout: &KeyLayout,
) -> (Vec<u8>, Vec<u8>) {
    // Mirror StressThread::key_value exactly: a single pos-seeded RNG fills
    // the key bytes first, then the value bytes. The key prefix is rewritten
    // by the layout *after* RNG consumption, so the value bytes are
    // determined by the RNG state after `key_len` bytes have been drawn.
    let mut seed = <StdRng as SeedableRng>::Seed::default();
    {
        let mut writer = &mut seed[..];
        writer.put_u64(pos);
    }
    let mut rng = StdRng::from_seed(seed);
    let mut key = vec![0u8; key_len];
    rng.fill_bytes(&mut key);
    match layout {
        KeyLayout::Uniform => {}
        KeyLayout::SequenceChoice => {
            key[..8].copy_from_slice(&u64::to_be_bytes(pos / 256));
            key[8..16].copy_from_slice(&u64::to_be_bytes(pos % 256));
        }
        KeyLayout::ChoiceSequence => {
            key[..8].copy_from_slice(&u64::to_be_bytes(pos % 256));
            key[8..16].copy_from_slice(&u64::to_be_bytes(pos / 256));
        }
    }
    let mut value = vec![0u8; value_len];
    rng.fill_bytes(&mut value);
    (key, value)
}

fn run_measure_open(
    storage: &Arc<dyn Storage>,
    timings: tidehunter::db::RecoveryTimings,
    params: &StressClientParameters,
    report: &mut Report,
) {
    report!(
        report,
        "RECOVERY: total_ms={} lock_acquire_ms={} read_control_region_ms={} wal_open_ms={} large_table_init_ms={} wal_replay_ms={} index_writer_open_ms={} background_start_ms={} other_ms={} bytes_replayed={} entries_replayed={}",
        timings.total.as_millis(),
        timings.lock_acquire.as_millis(),
        timings.read_control_region.as_millis(),
        timings.wal_open.as_millis(),
        timings.large_table_init.as_millis(),
        timings.wal_replay.as_millis(),
        timings.index_writer_open.as_millis(),
        timings.background_start.as_millis(),
        timings.other.as_millis(),
        timings.bytes_replayed,
        timings.entries_replayed,
    );

    let n = params.first_read_samples;
    if n == 0 {
        return;
    }
    let total_keys = (params.writes as u64).saturating_mul(params.write_threads as u64);
    if total_keys == 0 {
        report!(
            report,
            "FIRST_READ: skipped (writes * write_threads = 0; set --writes and --write-threads to match the original fill)"
        );
        return;
    }
    let mut rng = StdRng::seed_from_u64(0x5236_4649_5253_5400u64.wrapping_add(n as u64));
    let mut latencies_us: Vec<u64> = Vec::with_capacity(n);
    let mut hits: usize = 0;
    let mut mismatches: usize = 0;
    for _ in 0..n {
        let pos = rng.gen_range(0..total_keys);
        let (key, expected) =
            pos_to_key_value(pos, params.key_len, params.write_size, &params.key_layout);
        let t = Instant::now();
        let found = storage.get(&key);
        latencies_us.push(t.elapsed().as_micros() as u64);
        if let Some(actual) = found {
            hits += 1;
            // After a clean reopen every key we check should round-trip exactly;
            // a mismatch points to recovery corruption (the bar that R6's
            // crash-during-relocation experiment is meant to clear).
            if actual.as_ref() != expected.as_slice() {
                mismatches += 1;
            }
        }
    }
    latencies_us.sort_unstable();
    let pct = |q: f64| -> u64 {
        let idx = ((latencies_us.len() as f64 * q) as usize).min(latencies_us.len() - 1);
        latencies_us[idx]
    };
    let avg_us = latencies_us.iter().copied().sum::<u64>() / latencies_us.len() as u64;
    report!(
        report,
        "FIRST_READ: samples={} hits={} mismatches={} avg_us={} p50_us={} p99_us={} p999_us={}",
        n,
        hits,
        mismatches,
        avg_us,
        pct(0.50),
        pct(0.99),
        pct(0.999),
    );
    if mismatches > 0 {
        eprintln!(
            "RECOVERY CORRUPTION: {mismatches}/{hits} sampled values disagree with the writer's deterministic value"
        );
    }
}

/// Args for the byte-counting relocation filter (R2-D6).
#[derive(Clone)]
struct EpochFilterArgs {
    /// Shared with the relocation driver so it can reset to 0 before each pass.
    bytes_seen: Arc<AtomicU64>,
    /// Per-pass byte budget. Once `bytes_seen >= budget` and `mode == Stop`,
    /// the filter returns `Decision::StopRelocation`.
    budget: u64,
    /// `Stop` is the production-canonical short-circuit. `Keep` is the
    /// ablation that disables the short-circuit.
    mode: EpochFilterMode,
}

fn key_space_config(
    bloom: Option<(f32, u32)>,
    epoch_filter: Option<EpochFilterArgs>,
) -> KeySpaceConfig {
    use tidehunter::index::index_format::IndexFormatType;
    use tidehunter::index::uniform_lookup::UniformLookupIndex;
    let mut cfg = KeySpaceConfig::new()
        .with_index_format(IndexFormatType::Uniform(
            UniformLookupIndex::new_with_window_size(744),
        ))
        .with_unloaded_iterator(true);
    if let Some((rate, count)) = bloom {
        cfg = cfg.with_bloom_filter(rate, count);
    }
    if let Some(args) = epoch_filter {
        let EpochFilterArgs {
            bytes_seen,
            budget,
            mode,
        } = args;
        cfg = cfg.with_relocation_filter(move |key: &[u8], value: &[u8]| {
            let prev = bytes_seen.fetch_add((key.len() + value.len()) as u64, Ordering::Relaxed);
            if prev >= budget && mode == EpochFilterMode::Stop {
                Decision::StopRelocation
            } else {
                // Keep is fine here even though entries are about to be
                // bulk-dropped: WAL-based relocation only consults the filter
                // for StopRelocation in Phase A; Keep/Remove are silently
                // ignored. See tidehunter/src/relocation/mod.rs:333-338.
                Decision::Keep
            }
        });
    }
    cfg
}

struct Stress {
    storage: Arc<dyn Storage>,
    parameters: Arc<StressClientParameters>,
    benchmark_metrics: Arc<BenchmarkMetrics>,
    /// Cumulative app-side bytes written across all writer threads.
    /// Read by the relocation driver (bytes-based trigger) and reported
    /// at end-of-run as the foreground-WA denominator.
    app_bytes_written: Arc<AtomicU64>,
}

#[derive(Default)]
struct Report {
    lines: String,
}

impl Stress {
    pub fn background<F: FnOnce(StressThread) + Clone + Send + 'static>(
        &self,
        n: usize,
        f: F,
    ) -> Arc<AtomicBool> {
        let (_, manual_stop, _, _, _) = self.start_threads(n, f);
        manual_stop
    }

    pub fn measure<F: FnOnce(StressThread) + Clone + Send + 'static>(
        &self,
        n: usize,
        f: F,
        report: &mut Report,
    ) -> (Duration, usize) {
        let (threads, _, latency, latency_errors, operations_counter) = self.start_threads(n, f);
        let start = Instant::now();
        for t in threads {
            t.join().unwrap();
        }
        let total_ops = operations_counter.load(Ordering::Relaxed);
        let latency = latency.drain();
        let percentiles = latency
            .percentiles(&[50., 90., 99., 99.9, 99.99, 99.999])
            .unwrap()
            .unwrap();
        let p = move |i: usize| percentiles.get(i).unwrap().1.range();
        let latency_errors = latency_errors.load(Ordering::Relaxed);
        let latency_errors = if latency_errors > 0 {
            format!(", {latency_errors} out of bound")
        } else {
            "".to_string()
        };
        report!(
            report,
            "Latency(mcs): p50: {:?}, p90: {:?}, p99: {:?}, p99.9: {:?}, p99.99: {:?}, p99.999: {:?}{latency_errors}",
            p(0),
            p(1),
            p(2),
            p(3),
            p(4),
            p(5)
        );
        (start.elapsed(), total_ops)
    }

    #[allow(clippy::type_complexity)]
    fn start_threads<F: FnOnce(StressThread) + Clone + Send + 'static>(
        &self,
        n: usize,
        f: F,
    ) -> (
        Vec<JoinHandle<()>>,
        Arc<AtomicBool>,
        Arc<AtomicHistogram>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let mut threads = Vec::with_capacity(n);
        let start_lock = Arc::new(RwLock::new(()));
        let start_w = start_lock.write();
        let manual_stop = Arc::new(AtomicBool::new(false));
        let latency = AtomicHistogram::new(12, LATENCY_HISTOGRAM_MAX_VALUE_POWER).unwrap();
        let latency = Arc::new(latency);
        let latency_errors = Arc::new(AtomicUsize::default());
        let operations_counter = Arc::new(AtomicUsize::new(0));
        for index in 0..n {
            let thread = StressThread {
                db: self.storage.clone(),
                start_lock: start_lock.clone(),
                parameters: self.parameters.clone(),
                index: index as u64,
                manual_stop: manual_stop.clone(),

                latency: latency.clone(),
                latency_errors: latency_errors.clone(),
                benchmark_metrics: self.benchmark_metrics.clone(),
                operations_counter: operations_counter.clone(),
                app_bytes_written: self.app_bytes_written.clone(),
            };
            let f = f.clone();
            let thread = thread::spawn(move || f(thread));
            threads.push(thread);
        }
        drop(start_w);
        (
            threads,
            manual_stop,
            latency,
            latency_errors,
            operations_counter,
        )
    }
}

fn dec_div(n: usize) -> String {
    const M: usize = 1_000_000;
    const K: usize = 1_000;
    if n > M {
        format!("{:.2}M", n as f64 / M as f64)
    } else if n > K {
        format!("{:.2}K", n as f64 / K as f64)
    } else {
        format!("{n}")
    }
}

fn byte_div(n: usize) -> String {
    const K: usize = 1_024;
    const M: usize = K * K;
    if n > M {
        format!("{}Mb", n / M)
    } else if n > K {
        format!("{}Kb", n / K)
    } else {
        format!("{n}")
    }
}

struct StressThread {
    db: Arc<dyn Storage>,
    start_lock: Arc<RwLock<()>>,
    parameters: Arc<StressClientParameters>,
    index: u64,
    manual_stop: Arc<AtomicBool>,

    latency: Arc<AtomicHistogram>,
    latency_errors: Arc<AtomicUsize>,
    benchmark_metrics: Arc<BenchmarkMetrics>,
    operations_counter: Arc<AtomicUsize>,
    app_bytes_written: Arc<AtomicU64>,
}

impl StressThread {
    /// Select an existing key position using either uniform or zipf distribution
    fn select_existing_key<R: Rng>(&self, rng: &mut R, highest_local_pos: usize) -> u64 {
        let upper_bound = self.global_pos(highest_local_pos) + 1;

        if self.parameters.zipf_exponent != 0.0 && upper_bound > 1 {
            // Use zipf distribution (hot keys)
            let zipf = Zipf::new(upper_bound, self.parameters.zipf_exponent).unwrap();
            let sample = zipf.sample(rng) as u64;
            // The Zipf distribution generates number from 1 to N, where lower numbers are
            // more likely. We want to read higher positions more often, so we subtract
            // the sample from the upper bound.
            upper_bound - sample
        } else if upper_bound > 0 {
            // Uniform distribution
            rng.gen_range(0..upper_bound)
        } else {
            0
        }
    }

    pub fn run_writes(self) {
        #[allow(clippy::let_underscore_lock)] // RWLock here acts as a barrier
        let _ = self.start_lock.read();
        let bytes_per_op = (self.parameters.key_len + self.parameters.write_size) as u64;
        for pos in 0..self.parameters.writes {
            let pos = self.global_pos(pos);
            let (key, value) = self.key_value(pos);
            let timer = Instant::now();
            self.db.insert(key.into(), value.into());
            // Clamp to the histogram's max recordable value (2^LATENCY_HISTOGRAM_MAX_VALUE_POWER)
            let latency = timer
                .elapsed()
                .as_micros()
                .min((1u128 << LATENCY_HISTOGRAM_MAX_VALUE_POWER) - 1);
            self.benchmark_metrics
                .bench_writes
                .with_label_values(&[self.db.name()])
                .observe(latency as f64);
            if self.latency.increment(latency as u64).is_err() {
                self.latency_errors.fetch_add(1, Ordering::Relaxed);
            }
            self.operations_counter.fetch_add(1, Ordering::Relaxed);
            self.app_bytes_written
                .fetch_add(bytes_per_op, Ordering::Relaxed);
        }
    }

    pub fn run_background_writes(self) {
        let writes_per_thread = self.parameters.background_writes / self.parameters.write_threads;
        let delay = Duration::from_micros(1_000_000 / writes_per_thread as u64);
        let mut deadline = Instant::now();
        let mut pos = u32::MAX;
        let bytes_per_op = (self.parameters.key_len + self.parameters.write_size) as u64;
        while !self.manual_stop.load(Ordering::Relaxed) {
            deadline += delay;
            pos -= 1;
            let pos = self.global_pos(pos as usize);
            let (key, value) = self.key_value(pos);
            self.db.insert(key.into(), value.into());
            self.app_bytes_written
                .fetch_add(bytes_per_op, Ordering::Relaxed);
            thread::sleep(
                deadline
                    .checked_duration_since(Instant::now())
                    .unwrap_or_default(),
            )
        }
    }

    pub fn run_mixed_operations(self) {
        #[allow(clippy::let_underscore_lock)] // RWLock here acts as a barrier
        let _ = self.start_lock.read();
        let mut thread_rng = ThreadRng::default();
        let read_percentage = self.parameters.read_percentage;

        // Start writing new keys just after the ones written in the initial phase.
        let mut local_write_pos_counter = self.parameters.writes;

        let deadline = Instant::now() + Duration::from_secs(self.parameters.mixed_duration_secs);

        loop {
            if Instant::now() >= deadline {
                break;
            }

            // Randomly decide whether to read or write based on percentage
            let do_read = thread_rng.gen_range(0..100) < read_percentage;

            if do_read {
                // Perform a read operation.
                // Read from the whole keyspace, which expands as writes are made. The highest
                // key position this thread can read is determined by the latest key it has written.
                let highest_local_pos = local_write_pos_counter.saturating_sub(1);
                let pos = self.select_existing_key(&mut thread_rng, highest_local_pos);

                let timer;
                match self.parameters.read_mode {
                    ReadMode::Get => {
                        let (key, value) = self.key_value(pos);
                        timer = Instant::now();
                        if let Some(found_value) = self.db.get(&key) {
                            assert_eq!(
                                &value[..],
                                &found_value[..],
                                "Found value does not match expected value"
                            );
                        }
                        // If the key is not found, we do nothing as it may not have been written yet.
                        // This can happen because we select pos between 0 and global_pos(highest_local_pos)
                        // This range includes global positions that are owned by other threads,
                        // who may not have used those positions yet.
                    }
                    ReadMode::Lt(iterations) => {
                        let mut key = vec![0u8; self.parameters.key_len];
                        thread_rng.fill(&mut key[..]);
                        timer = Instant::now();
                        let result = self.db.get_lt(&key, iterations);
                        let result = if result.len() == iterations {
                            "found"
                        } else if result.is_empty() {
                            "not_found"
                        } else {
                            "partial"
                        };
                        self.benchmark_metrics
                            .get_lt_result
                            .with_label_values(&[result])
                            .inc();
                    }
                    ReadMode::Exists => {
                        let (key, _) = self.key_value(pos);
                        timer = Instant::now();
                        let exists = self.db.exists(&key);
                        // For exists mode, we expect the key to exist if pos < self.parameters.writes
                        // since those were written in the initial write phase
                        if pos < self.global_pos(self.parameters.writes - 1) {
                            assert!(exists, "Key should exist but was not found");
                        }
                        // Keys beyond initial writes may or may not exist depending on
                        // whether they were written during the mixed phase. For more details,
                        // see comment above for get mode.
                    }
                }
                // Clamp to the histogram's max recordable value (2^LATENCY_HISTOGRAM_MAX_VALUE_POWER)
                let latency = timer
                    .elapsed()
                    .as_micros()
                    .min((1u128 << LATENCY_HISTOGRAM_MAX_VALUE_POWER) - 1);
                self.benchmark_metrics
                    .bench_reads
                    .with_label_values(&[self.db.name()])
                    .observe(latency as f64);
                if self.latency.increment(latency as u64).is_err() {
                    self.latency_errors.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                // Pick one of: delete an existing key, overwrite an existing key, or
                // insert a fresh key. The two ratios share a single random draw so
                // their sum is the probability of touching an existing key.
                let r: f64 = thread_rng.r#gen::<f64>();
                let has_existing_keys = local_write_pos_counter > 0;
                let do_delete = has_existing_keys && r < self.parameters.delete_ratio;
                let do_overwrite = has_existing_keys
                    && !do_delete
                    && r < self.parameters.delete_ratio + self.parameters.overwrite_ratio;

                let (histogram, timer, bytes_this_op) = if do_delete {
                    let highest_local_pos = local_write_pos_counter.saturating_sub(1);
                    let pos = self.select_existing_key(&mut thread_rng, highest_local_pos);
                    let key = self.key(pos);
                    let bytes_this_op = key.len() as u64;
                    let timer = Instant::now();
                    self.db.delete(key.into());
                    (&self.benchmark_metrics.bench_deletes, timer, bytes_this_op)
                } else {
                    let pos = if do_overwrite {
                        let highest_local_pos = local_write_pos_counter.saturating_sub(1);
                        self.select_existing_key(&mut thread_rng, highest_local_pos)
                    } else {
                        let pos = self.global_pos(local_write_pos_counter);
                        local_write_pos_counter += 1;
                        pos
                    };
                    let (key, value) = self.key_value(pos);
                    let bytes_this_op = (key.len() + value.len()) as u64;
                    let timer = Instant::now();
                    self.db.insert(key.into(), value.into());
                    (&self.benchmark_metrics.bench_writes, timer, bytes_this_op)
                };

                // Clamp to the histogram's max recordable value (2^LATENCY_HISTOGRAM_MAX_VALUE_POWER)
                let latency = timer
                    .elapsed()
                    .as_micros()
                    .min((1u128 << LATENCY_HISTOGRAM_MAX_VALUE_POWER) - 1);
                histogram
                    .with_label_values(&[self.db.name()])
                    .observe(latency as f64);
                if self.latency.increment(latency as u64).is_err() {
                    self.latency_errors.fetch_add(1, Ordering::Relaxed);
                }
                self.app_bytes_written
                    .fetch_add(bytes_this_op, Ordering::Relaxed);
            }

            // Track operations for reporting
            self.operations_counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[allow(dead_code)]
    fn key(&self, pos: u64) -> Vec<u8> {
        let (key, _) = self.key_and_rng(pos);
        key
    }

    fn key_and_rng(&self, pos: u64) -> (Vec<u8>, StdRng) {
        let mut rng = Self::rng_at(pos);
        let mut key = vec![0u8; self.parameters.key_len];
        rng.fill_bytes(&mut key);
        match self.parameters.key_layout {
            KeyLayout::Uniform => {}
            KeyLayout::SequenceChoice => {
                // the first 16 bytes of a key are not random anymore
                // First 8 bytes are a sequentially growing value (like consensus round)
                key[..8].copy_from_slice(&u64::to_be_bytes(pos / 256));
                // Next 8 bytes are choice of value in range 0..255 (like consensus validator index)
                key[8..16].copy_from_slice(&u64::to_be_bytes(pos % 256));
            }
            KeyLayout::ChoiceSequence => {
                // Doing the same as above in different order
                key[..8].copy_from_slice(&u64::to_be_bytes(pos % 256));
                key[8..16].copy_from_slice(&u64::to_be_bytes(pos / 256));
            }
        }
        (key, rng)
    }

    fn key_value(&self, pos: u64) -> (Vec<u8>, Vec<u8>) {
        let (key, mut rng) = self.key_and_rng(pos);
        let mut value = vec![0u8; self.parameters.write_size];
        rng.fill_bytes(&mut value);
        (key, value)
    }

    /// Maps local index into continuous global space
    fn global_pos(&self, pos: usize) -> u64 {
        (pos * self.parameters.write_threads) as u64 + self.index
    }

    fn rng_at(pos: u64) -> StdRng {
        let mut seed = <StdRng as SeedableRng>::Seed::default();
        let mut writer = &mut seed[..];
        writer.put_u64(pos);
        StdRng::from_seed(seed)
    }
}
