use anyhow::{Context, Result, bail};
use benchmark::configs::{
    Backend, DEFAULT_DB_DIR, EpochFilterMode, KeyLayout, ReadMode, RelocationConfig,
    StressTestConfigs,
};
use clap::{Parser, Subcommand};
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use std::sync::OnceLock;

#[derive(Parser, Debug)]
#[command(
    name = "generate_target_configs",
    about = "Generate orchestrator/assets/target_configs.yml for one experiment mode.",
    long_about = "Generate orchestrator/assets/target_configs.yml for one experiment mode.\n\
\n\
Modes map one-to-one onto the figures and tables of the Tidehunter paper;\n\
the per-mode docs below give the exact cell grid each mode emits. Every\n\
mode overwrites the same output file: orchestrator/assets/target_configs.yml."
)]
struct Args {
    #[command(subcommand)]
    mode: Mode,
}

#[derive(Subcommand, Debug)]
enum Mode {
    /// Tidehunter cells of the main benchmark figures (Figures 5 and 6):
    /// 42 cells = value size {1 KB, 64 B, 128 B} x Zipf {0, 2} x workload
    /// {write-only, 50/50 Get/Exists/Lt, 100% Get/Exists/Lt}; 1 TiB
    /// pre-fill, 30-min measured phase, max_maps=64. The six 50/50 Get
    /// cells duplicate value-scaling points; skip them if that mode
    /// already ran.
    #[command(alias = "paper-redo64gb")]
    MainBenchmark,
    /// RocksDB and BlobDB cells of the main benchmark figures (Figures 5
    /// and 6): 84 cells = 2 backends x 3 value sizes x 2 skews x the same
    /// 7 workloads; 1 TiB pre-fill, 10-min measured phase. db_parameters
    /// are ignored by these backends at runtime.
    MainBenchmarkBaselines,
    /// Tidehunter curves of the value-size scaling figure (Figure 1):
    /// 20 cells = value size {64, 128, 256, 512, 1024} B x 2 skews x
    /// 2 replicates; 1 TiB pre-fill, 50/50 Get, 30-min measured phase,
    /// max_maps=64.
    ValueScaling,
    /// RocksDB and BlobDB curves of Figure 1: 20 cells = 2 backends x
    /// 5 value sizes x 2 skews, 50/50 Get, single replicate; 1 TiB
    /// pre-fill, 10-min measured phase. db_parameters are ignored by
    /// these backends at runtime.
    ValueScalingBaselines,
    /// Stability table (Table 1): 6 cells = read percentage {0, 50, 100}
    /// x 2 skews on 1 KB values; 1 TiB pre-fill, 30-min measured phase,
    /// max_maps=128. metrics_enabled=true: the lock-overhead column needs
    /// the large_table_contention metric, so these runs need a Prometheus
    /// scraping the client.
    Stability,
    /// Application-workload regimes figure (Figure 7): 30 cells =
    /// (key, value) sizes {24/10, 48/43, 20/44, 38/38, 76/50} B x 2 skews
    /// x 3 backends, 50/50 Get, single replicate; 500 GiB of raw
    /// key+value bytes pre-fill, 30-min measured phase.
    AppWorkloads,
    /// Relocation on/off figure (Figure 8): 4 cells = relocation
    /// {on, off} x 2 skews; 1 TiB pre-fill of 1 KB values, then a 10-min
    /// delete-only mixed phase. On-cells use Index-based relocation
    /// (ratio 1.0) with relocation_max_reclaim_pct=20; max_maps=128,
    /// metrics_enabled=true.
    Relocation,
    /// Churn tables, Tidehunter rows (Tables 2 and 3): 13 cells =
    /// strategy {None, WalBased, IndexBased} x mix {100% overwrite, 50/50
    /// overwrite+delete, 100% delete}, plus relocation_max_reclaim_pct
    /// {1, 10, 25, 50} on the WalBased 50/50 cell (the 5% point is that
    /// cell's default); 500 GiB pre-fill, 60-min pure-write measured
    /// phase.
    #[command(alias = "r4-churn-full")]
    Churn,
    /// BlobDB rows of the churn tables: 3 cells = {100% overwrite, 50/50
    /// overwrite+delete, 100% delete} with the same 500 GiB pre-fill and
    /// 60-min pure-write phase. relocation: None (RocksDB compaction
    /// drives blob GC); the BlobDB tuning is hard-coded in
    /// benchmarks/benchmark/src/storage/rocks.rs.
    #[command(alias = "r4-churn-blobdb")]
    ChurnBlobdb,
    /// Epoch-based GC table; commented out of the published paper.
    /// 8 cells = {budget 25/50/100 GiB with the Stop filter, budget
    /// 50 GiB with the Keep ablation} x 2 replicates; 50 GiB pre-fill,
    /// 60-min pure-write phase with continuous WAL-based relocation.
    #[command(alias = "r2d6-epoch-gc")]
    EpochGc,
    /// Recovery table (Table 4): 24 cells = fill/measure pairs in batches
    /// of exactly 4, so on a 4-machine testbed each measure lands on the
    /// machine holding its fill. Series A: cold start at {100 GiB,
    /// 500 GiB, 1 TiB (x2 replicates)}. Series B: snapshot_written_bytes
    /// {16, 64, 256 GiB, unlimited} at 1 TiB. Series C: crash during
    /// relocation x 4 (200 GiB fill, crash at +600 s of a 1200 s mixed
    /// phase). Measures use measure_open with first_read_samples=1000.
    #[command(alias = "r6-recovery")]
    Recovery,
    /// Extra replicates for Table 4: 32 cells = Series B at {16, 64, 128,
    /// 256} GiB x 2 rounds plus Series C crash replicates 5-12, as
    /// fill/measure pairs in batches of 4; measures use
    /// first_read_samples=1000.
    #[command(alias = "r6-recovery-supplemental")]
    RecoveryReplicates,
    /// Runtime memory table (Table 5): 4 identical cells of the headline
    /// 50/50 Get / 1 KB / uniform / 1 TiB-fill config with
    /// metrics_enabled=true; the per-keyspace gauges feed the table and
    /// need a Prometheus scraping the client.
    #[command(alias = "r3-instrumented-replicates")]
    MemoryInstrumented,
    /// Memory-sensitivity table (Table 6), Bloom-FPR rows: 8 cells = FPR
    /// {0.001, 0.01, 0.05, 0.10} x 2 replicates; 100% Get on 1 KB values,
    /// uniform, 1 TiB pre-fill, bloom_filter_count=8192.
    #[command(alias = "bloom-fpr-sweep")]
    SweepBloomFpr,
    /// Table 6, mmap-window rows: 16 cells = max_maps {16, 32, 64, 128} x
    /// 4 replicates on the headline 50/50 Get cell. The budget applies
    /// per WAL kind, so the sweep spans 32-256 GiB total mapped at 1 GiB
    /// fragments.
    SweepMmapWindow,
    /// Table 6, cell-count rows: 5 cells = num_mutexes {2^14, 2^16, 2^17,
    /// 2^19, 2^20} (cells_per_mutex defaults to 1, so total cells =
    /// num_mutexes) on the headline 50/50 Get cell at max_maps=128.
    SweepCellCount,
    /// Table 6, dirty-key rows: 5 cells = max_dirty_keys {64, 256, 1024,
    /// 4096, 16384} on the headline 50/50 Get cell at max_maps=128.
    SweepDirtyKeys,
    /// Diagnostic, no paper element: Table 4 Series C with
    /// num_replay_threads=1 to force single-threaded WAL replay.
    /// 8 fill/measure pairs; measures sample 1,000,000 first reads.
    #[command(alias = "r6-recovery-crash-single-thread-replay")]
    DiagnosticCrashSingleThreadReplay,
    /// Diagnostic, no paper element: Table 4 Series C against the
    /// relocation-guard + silent-skip tidehunter patches (the patches
    /// must be present in the built tree). 4 fill/measure pairs; measures
    /// sample 1,000,000 first reads.
    #[command(alias = "r6-recovery-crash-relocation-guard-diagnostic")]
    DiagnosticCrashRelocationGuard,
}

const KEY_LEN: usize = 32;
const FRAG_SIZE: u64 = 1024 * 1024 * 1024; // 1 GB
const MAX_MAPS: usize = 64;
const DEFAULT_MAX_DIRTY_KEYS: usize = 1024;

/// The orchestrator settings file; its `working_dir` (the database directory
/// on the benchmark machines) is baked into the emitted `path` / `db_path` /
/// `reuse` fields.
const SETTINGS_PATH: &str = "orchestrator/assets/settings.yml";

static DB_DIR: OnceLock<String> = OnceLock::new();

/// Database directory used in emitted configs. Set by `init_db_dir`.
fn db_dir() -> &'static str {
    DB_DIR.get().expect("init_db_dir runs first in main")
}

/// Resolve the database directory: `working_dir` from SETTINGS_PATH when the
/// file exists, else DEFAULT_DB_DIR. A settings file that exists but cannot
/// be parsed is an error (the orchestrator would reject it too). Error
/// messages must never echo file contents: the file holds access tokens.
fn init_db_dir() -> Result<()> {
    let dir = match fs::read_to_string(SETTINGS_PATH) {
        Ok(data) => match working_dir_from_settings(&data) {
            Ok(Some(dir)) => {
                eprintln!("Database directory for configs: {dir} (working_dir of {SETTINGS_PATH})");
                dir
            }
            Ok(None) => {
                eprintln!(
                    "Database directory for configs: {DEFAULT_DB_DIR} (default; {SETTINGS_PATH} sets no working_dir)"
                );
                DEFAULT_DB_DIR.to_string()
            }
            Err(e) => bail!("failed to read working_dir from {SETTINGS_PATH}: {e}"),
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            eprintln!(
                "Database directory for configs: {DEFAULT_DB_DIR} (default; no {SETTINGS_PATH})"
            );
            DEFAULT_DB_DIR.to_string()
        }
        Err(e) => return Err(e).context(format!("failed to read {SETTINGS_PATH}")),
    };
    DB_DIR.set(dir).expect("init_db_dir called once");
    Ok(())
}

/// Extract `working_dir` from settings YAML, resolving `${ENV}` references the
/// same way the orchestrator's `Settings::load` does. Returns Ok(None) when
/// the field is absent. The error never includes the YAML content.
fn working_dir_from_settings(data: &str) -> Result<Option<String>> {
    let mut data = data.to_string();
    for (name, value) in std::env::vars() {
        data = data.replace(&format!("${{{name}}}"), &value);
    }
    #[derive(Deserialize)]
    struct PartialSettings {
        working_dir: Option<String>,
    }
    let settings: PartialSettings =
        serde_yaml::from_str(&data).map_err(|e| anyhow::anyhow!("invalid YAML: {e}"))?;
    match settings.working_dir {
        Some(dir) if dir.contains("${") => bail!("unresolved ${{ENV}} variable in working_dir"),
        Some(dir) => Ok(Some(dir.trim_end_matches('/').to_string())),
        None => Ok(None),
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    init_db_dir()?;
    match args.mode {
        Mode::MainBenchmark => generate_main_benchmark(),
        Mode::MainBenchmarkBaselines => generate_main_benchmark_baselines(),
        Mode::ValueScaling => generate_value_scaling(),
        Mode::ValueScalingBaselines => generate_value_scaling_baselines(),
        Mode::Stability => generate_stability(),
        Mode::AppWorkloads => generate_app_workloads(),
        Mode::Relocation => generate_relocation(),
        Mode::Churn => generate_churn(),
        Mode::ChurnBlobdb => generate_churn_blobdb(),
        Mode::EpochGc => generate_epoch_gc(),
        Mode::Recovery => generate_recovery(),
        Mode::RecoveryReplicates => generate_recovery_replicates(),
        Mode::MemoryInstrumented => generate_memory_instrumented(),
        Mode::SweepBloomFpr => generate_sweep_bloom_fpr(),
        Mode::SweepMmapWindow => generate_sweep_mmap_window(),
        Mode::SweepCellCount => generate_sweep_cell_count(),
        Mode::SweepDirtyKeys => generate_sweep_dirty_keys(),
        Mode::DiagnosticCrashSingleThreadReplay => generate_diagnostic_crash_single_thread_replay(),
        Mode::DiagnosticCrashRelocationGuard => generate_diagnostic_crash_relocation_guard(),
    }
}

fn write_configs(items: &[StressTestConfigs], out_path: &str) -> Result<()> {
    for item in items {
        let yaml = serde_yaml::to_string(item)?;
        println!("{yaml}");
    }
    let out_path = PathBuf::from(out_path);
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let yaml_list = serde_yaml::to_string(items)?;
    fs::write(&out_path, yaml_list)?;
    println!(
        "Generated {} configurations in: {}",
        items.len(),
        out_path.display()
    );
    Ok(())
}

fn writes_for_size_with_threads(size_gb: u64, write_threads: u64, write_size: usize) -> usize {
    let bytes_per_write = (KEY_LEN + write_size) as u64;
    ((size_gb * 1024 * 1024 * 1024) / (bytes_per_write * write_threads)) as usize
}

fn generate_value_scaling() -> Result<()> {
    const DB_SIZE_BYTES: usize = 1024 * 1024 * 1024 * 1024; // 1 TB
    const REPLICATES: usize = 2;

    let mut base_item = StressTestConfigs::default();

    base_item.stress_client_parameters.backend = Backend::Tidehunter;
    base_item.stress_client_parameters.mixed_threads = 36;
    base_item.stress_client_parameters.write_threads = 36;
    base_item.stress_client_parameters.mixed_duration_secs = 1800;
    base_item.stress_client_parameters.background_writes = 0;
    base_item.stress_client_parameters.no_snapshot = false;
    base_item.stress_client_parameters.report = true;
    base_item.stress_client_parameters.key_layout = KeyLayout::Uniform;
    base_item.stress_client_parameters.tldr = String::new();
    base_item.stress_client_parameters.preserve = false;
    base_item.stress_client_parameters.path = Some(format!("{}/", db_dir()));
    base_item.stress_client_parameters.key_len = KEY_LEN;
    base_item.stress_client_parameters.read_mode = ReadMode::Get;
    base_item.stress_client_parameters.read_percentage = 50;
    base_item.stress_client_parameters.relocation = None;
    base_item.stress_client_parameters.bloom_filter_rate = None;
    base_item.stress_client_parameters.bloom_filter_count = None;

    base_item.db_parameters.frag_size = FRAG_SIZE;
    base_item.db_parameters.max_maps = MAX_MAPS;
    base_item.db_parameters.max_dirty_keys = DEFAULT_MAX_DIRTY_KEYS;
    base_item.db_parameters.num_flusher_threads = 12;
    base_item.db_parameters.metrics_enabled = false;
    base_item.db_parameters.direct_io = false;

    let value_sizes: [usize; 5] = [64, 128, 256, 512, 1024];
    let zipf_exponents: [f64; 2] = [0.0, 2.0];

    let mut items: Vec<StressTestConfigs> = Vec::new();

    for replicate in 1..=REPLICATES {
        for &value_size in &value_sizes {
            for &zipf_exponent in &zipf_exponents {
                let mut item = base_item.clone();
                item.stress_client_parameters.write_size = value_size;
                item.stress_client_parameters.zipf_exponent = zipf_exponent;
                item.stress_client_parameters.writes = DB_SIZE_BYTES
                    / (item.stress_client_parameters.write_threads
                        * (item.stress_client_parameters.key_len
                            + item.stress_client_parameters.write_size));
                item.stress_client_parameters.tldr = format!(
                    "value-scaling-v{value_size}-z{}-r{replicate}",
                    zipf_exponent as u32
                );
                items.push(item);
            }
        }
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_value_scaling_baselines() -> Result<()> {
    const DB_SIZE_BYTES: usize = 1024 * 1024 * 1024 * 1024; // 1 TB

    let backends: [(Backend, &str); 2] =
        [(Backend::Rocksdb, "rocksdb"), (Backend::Blobdb, "blobdb")];
    let value_sizes: [usize; 5] = [64, 128, 256, 512, 1024];
    let zipf_exponents: [f64; 2] = [0.0, 2.0];

    let mut base = StressTestConfigs::default();
    base.stress_client_parameters.mixed_threads = 36;
    base.stress_client_parameters.write_threads = 36;
    base.stress_client_parameters.mixed_duration_secs = 600;
    base.stress_client_parameters.pause_between_phases_secs = 600;
    base.stress_client_parameters.background_writes = 0;
    base.stress_client_parameters.no_snapshot = false;
    base.stress_client_parameters.report = true;
    base.stress_client_parameters.key_layout = KeyLayout::Uniform;
    base.stress_client_parameters.tldr = String::new();
    base.stress_client_parameters.preserve = false;
    base.stress_client_parameters.path = Some(format!("{}/", db_dir()));
    base.stress_client_parameters.key_len = KEY_LEN;
    base.stress_client_parameters.read_mode = ReadMode::Get;
    base.stress_client_parameters.read_percentage = 50;
    base.stress_client_parameters.relocation = None;
    base.stress_client_parameters.bloom_filter_rate = None;
    base.stress_client_parameters.bloom_filter_count = None;

    base.db_parameters.frag_size = FRAG_SIZE;
    base.db_parameters.max_maps = 128; // ignored by RocksStorage at runtime
    base.db_parameters.max_dirty_keys = DEFAULT_MAX_DIRTY_KEYS;
    base.db_parameters.num_flusher_threads = 12;
    base.db_parameters.metrics_enabled = false;
    base.db_parameters.direct_io = false;

    let mut items: Vec<StressTestConfigs> = Vec::new();
    for (backend, backend_name) in &backends {
        for &value_size in &value_sizes {
            for &zipf_exponent in &zipf_exponents {
                let mut item = base.clone();
                item.stress_client_parameters.backend = backend.clone();
                item.stress_client_parameters.write_size = value_size;
                item.stress_client_parameters.zipf_exponent = zipf_exponent;
                item.stress_client_parameters.writes = DB_SIZE_BYTES
                    / (item.stress_client_parameters.write_threads
                        * (item.stress_client_parameters.key_len
                            + item.stress_client_parameters.write_size));
                item.stress_client_parameters.tldr = format!(
                    "value-scaling-baseline-{backend_name}-v{value_size}-z{}",
                    zipf_exponent as u32
                );
                items.push(item);
            }
        }
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_main_benchmark_baselines() -> Result<()> {
    // RocksDB/BlobDB tuning (LZ4/ZSTD, 16 KB blocks, Bloom filters,
    // increase_parallelism(12), BlobDB blob settings) is hard-coded in
    // benchmarks/benchmark/src/storage/rocks.rs, not YAML-settable.

    const DB_SIZE_BYTES: usize = 1024 * 1024 * 1024 * 1024; // 1 TB

    let backends: [(Backend, &str); 2] =
        [(Backend::Rocksdb, "rocksdb"), (Backend::Blobdb, "blobdb")];
    let value_sizes: [usize; 3] = [1024, 64, 128];
    let zipf_exponents: [f64; 2] = [0.0, 2.0];

    // (label, read_percentage, read_mode) — the 7 workload bars of the
    // figures. read_mode is ignored for the write-only cell.
    let configs: [(&str, u8, ReadMode); 7] = [
        ("write", 0, ReadMode::Get),
        ("mix50-get", 50, ReadMode::Get),
        ("mix50-exists", 50, ReadMode::Exists),
        ("mix50-lt", 50, ReadMode::Lt(1)),
        ("read-get", 100, ReadMode::Get),
        ("read-exists", 100, ReadMode::Exists),
        ("read-lt", 100, ReadMode::Lt(1)),
    ];

    let mut base = StressTestConfigs::default();
    base.stress_client_parameters.mixed_threads = 36;
    base.stress_client_parameters.write_threads = 36;
    base.stress_client_parameters.mixed_duration_secs = 600;
    base.stress_client_parameters.pause_between_phases_secs = 600;
    base.stress_client_parameters.background_writes = 0;
    base.stress_client_parameters.no_snapshot = false;
    base.stress_client_parameters.report = true;
    base.stress_client_parameters.key_layout = KeyLayout::Uniform;
    base.stress_client_parameters.tldr = String::new();
    base.stress_client_parameters.preserve = false;
    base.stress_client_parameters.path = Some(format!("{}/", db_dir()));
    base.stress_client_parameters.key_len = KEY_LEN;
    base.stress_client_parameters.relocation = None;
    base.stress_client_parameters.bloom_filter_rate = None;
    base.stress_client_parameters.bloom_filter_count = None;

    base.db_parameters.frag_size = FRAG_SIZE;
    base.db_parameters.max_maps = 128; // ignored by RocksStorage at runtime
    base.db_parameters.max_dirty_keys = DEFAULT_MAX_DIRTY_KEYS;
    base.db_parameters.num_flusher_threads = 12;
    base.db_parameters.metrics_enabled = false;
    base.db_parameters.direct_io = false;

    let mut items: Vec<StressTestConfigs> = Vec::new();
    for (backend, backend_name) in &backends {
        for &value_size in &value_sizes {
            for &zipf_exponent in &zipf_exponents {
                for (label, read_pct, read_mode) in &configs {
                    let mut item = base.clone();
                    item.stress_client_parameters.backend = backend.clone();
                    item.stress_client_parameters.write_size = value_size;
                    item.stress_client_parameters.zipf_exponent = zipf_exponent;
                    item.stress_client_parameters.read_percentage = *read_pct;
                    item.stress_client_parameters.read_mode = read_mode.clone();
                    item.stress_client_parameters.writes = DB_SIZE_BYTES
                        / (item.stress_client_parameters.write_threads
                            * (item.stress_client_parameters.key_len
                                + item.stress_client_parameters.write_size));
                    item.stress_client_parameters.tldr = format!(
                        "main-baseline-{backend_name}-v{value_size}-z{}-{label}",
                        zipf_exponent as u32
                    );
                    items.push(item);
                }
            }
        }
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_stability() -> Result<()> {
    const DB_SIZE_BYTES: usize = 1024 * 1024 * 1024 * 1024; // 1 TB
    const VALUE_SIZE: usize = 1024;

    let read_percentages: [u8; 3] = [0, 50, 100];
    let zipf_exponents: [f64; 2] = [0.0, 2.0];

    let mut base = StressTestConfigs::default();
    base.stress_client_parameters.backend = Backend::Tidehunter;
    base.stress_client_parameters.mixed_threads = 36;
    base.stress_client_parameters.write_threads = 36;
    base.stress_client_parameters.write_size = VALUE_SIZE;
    base.stress_client_parameters.key_len = KEY_LEN;
    base.stress_client_parameters.mixed_duration_secs = 1800;
    base.stress_client_parameters.pause_between_phases_secs = 600;
    base.stress_client_parameters.background_writes = 0;
    base.stress_client_parameters.no_snapshot = false;
    base.stress_client_parameters.report = true;
    base.stress_client_parameters.key_layout = KeyLayout::Uniform;
    base.stress_client_parameters.tldr = String::new();
    base.stress_client_parameters.preserve = false;
    base.stress_client_parameters.path = Some(format!("{}/", db_dir()));
    base.stress_client_parameters.read_mode = ReadMode::Get;
    base.stress_client_parameters.relocation = None;
    base.stress_client_parameters.bloom_filter_rate = None;
    base.stress_client_parameters.bloom_filter_count = None;
    base.stress_client_parameters.writes = DB_SIZE_BYTES
        / (base.stress_client_parameters.write_threads
            * (base.stress_client_parameters.key_len + base.stress_client_parameters.write_size));

    base.db_parameters.frag_size = FRAG_SIZE;
    base.db_parameters.max_maps = 128; // as reported in the paper's table
    base.db_parameters.max_dirty_keys = DEFAULT_MAX_DIRTY_KEYS;
    base.db_parameters.num_flusher_threads = 12;
    base.db_parameters.metrics_enabled = true; // lock-overhead column needs large_table_contention
    base.db_parameters.direct_io = false;

    let mut items: Vec<StressTestConfigs> = Vec::new();
    for &read_percentage in &read_percentages {
        for &zipf_exponent in &zipf_exponents {
            let mut item = base.clone();
            item.stress_client_parameters.read_percentage = read_percentage;
            item.stress_client_parameters.zipf_exponent = zipf_exponent;
            item.stress_client_parameters.tldr =
                format!("stability-r{read_percentage}-z{}", zipf_exponent as u32);
            items.push(item);
        }
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_app_workloads() -> Result<()> {
    // (key_len, value_size, writes-per-thread). 24/10 and 48/43 match the
    // RTDATA and ZippyDB workload signatures; writes give 500 GiB of raw
    // key+value bytes per cell.
    let combos: [(usize, usize, usize); 5] = [
        (24, 10, 438_620_026),
        (48, 43, 163_880_009),
        (20, 44, 233_016_888),
        (38, 38, 196_224_748),
        (76, 50, 118_357_784),
    ];
    let backends: [(Backend, &str); 3] = [
        (Backend::Tidehunter, "tidehunter"),
        (Backend::Rocksdb, "rocksdb"),
        (Backend::Blobdb, "blobdb"),
    ];
    let zipf_exponents: [f64; 2] = [0.0, 2.0];

    let mut base = StressTestConfigs::default();
    base.stress_client_parameters.mixed_threads = 36;
    base.stress_client_parameters.write_threads = 36;
    base.stress_client_parameters.mixed_duration_secs = 1800;
    base.stress_client_parameters.pause_between_phases_secs = 600;
    base.stress_client_parameters.background_writes = 0;
    base.stress_client_parameters.no_snapshot = false;
    base.stress_client_parameters.report = true;
    base.stress_client_parameters.key_layout = KeyLayout::Uniform;
    base.stress_client_parameters.tldr = String::new();
    base.stress_client_parameters.preserve = false;
    base.stress_client_parameters.path = Some(format!("{}/", db_dir()));
    base.stress_client_parameters.read_mode = ReadMode::Get;
    base.stress_client_parameters.read_percentage = 50;
    base.stress_client_parameters.relocation = None;
    base.stress_client_parameters.bloom_filter_rate = None;
    base.stress_client_parameters.bloom_filter_count = None;

    base.db_parameters.frag_size = FRAG_SIZE;
    base.db_parameters.max_maps = 128;
    base.db_parameters.max_dirty_keys = DEFAULT_MAX_DIRTY_KEYS;
    base.db_parameters.num_flusher_threads = 12;
    base.db_parameters.metrics_enabled = false;
    base.db_parameters.direct_io = false;

    let mut items: Vec<StressTestConfigs> = Vec::new();
    for (backend, backend_name) in &backends {
        for &(key_len, value_size, writes) in &combos {
            for &zipf_exponent in &zipf_exponents {
                let mut item = base.clone();
                item.stress_client_parameters.backend = backend.clone();
                item.stress_client_parameters.key_len = key_len;
                item.stress_client_parameters.write_size = value_size;
                item.stress_client_parameters.writes = writes;
                item.stress_client_parameters.zipf_exponent = zipf_exponent;
                item.stress_client_parameters.tldr = format!(
                    "app-k{key_len}v{value_size}-{backend_name}-z{}",
                    zipf_exponent as u32
                );
                items.push(item);
            }
        }
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_relocation() -> Result<()> {
    // Figure 8: relocation on vs off under a 100%-delete mixed phase.

    const VALUE_SIZE: usize = 1024;
    const FILL_GB: u64 = 1024; // 1 TiB

    let zipf_exponents: [f64; 2] = [0.0, 2.0];
    // (label, relocation)
    let relocations: [(&str, Option<RelocationConfig>); 2] = [
        ("on", Some(RelocationConfig::Index { ratio: Some(1.0) })),
        ("off", None),
    ];

    let mut base = StressTestConfigs::default();
    base.stress_client_parameters.backend = Backend::Tidehunter;
    base.stress_client_parameters.mixed_threads = 36;
    base.stress_client_parameters.write_threads = 36;
    base.stress_client_parameters.write_size = VALUE_SIZE;
    base.stress_client_parameters.key_len = KEY_LEN;
    base.stress_client_parameters.mixed_duration_secs = 600;
    base.stress_client_parameters.pause_between_phases_secs = 600;
    base.stress_client_parameters.background_writes = 0;
    base.stress_client_parameters.no_snapshot = false;
    base.stress_client_parameters.report = true;
    base.stress_client_parameters.key_layout = KeyLayout::Uniform;
    base.stress_client_parameters.tldr = String::new();
    base.stress_client_parameters.preserve = false;
    base.stress_client_parameters.path = Some(format!("{}/", db_dir()));
    base.stress_client_parameters.read_mode = ReadMode::Get;
    base.stress_client_parameters.read_percentage = 0;
    base.stress_client_parameters.overwrite_ratio = 0.0;
    base.stress_client_parameters.delete_ratio = 1.0;
    base.stress_client_parameters.bloom_filter_rate = None;
    base.stress_client_parameters.bloom_filter_count = None;
    base.stress_client_parameters.writes = writes_for_size_with_threads(
        FILL_GB,
        base.stress_client_parameters.write_threads as u64,
        base.stress_client_parameters.write_size,
    );

    base.db_parameters.frag_size = FRAG_SIZE;
    base.db_parameters.max_maps = 128;
    base.db_parameters.max_dirty_keys = DEFAULT_MAX_DIRTY_KEYS;
    base.db_parameters.num_flusher_threads = 12;
    base.db_parameters.metrics_enabled = true;
    base.db_parameters.direct_io = false;
    base.db_parameters.relocation_max_reclaim_pct = 20;

    let mut items: Vec<StressTestConfigs> = Vec::new();
    for &zipf_exponent in &zipf_exponents {
        for (label, relocation) in &relocations {
            let mut item = base.clone();
            item.stress_client_parameters.zipf_exponent = zipf_exponent;
            item.stress_client_parameters.relocation = relocation.clone();
            item.stress_client_parameters.tldr =
                format!("relocation-{label}-z{}", zipf_exponent as u32);
            items.push(item);
        }
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_recovery() -> Result<()> {
    // Table 4. Two kinds of cells:
    //
    //   Fill: write the target DB at a deterministic path under
    //     <db_dir>/r6/<name>/, outside the orchestrator's `stress.*`
    //     cleanup pattern so it survives into later batches.
    //
    //   Measure: reopen the filled DB with measure_open, emitting the
    //     RECOVERY: breakdown and a 1,000-key first-read sample
    //     (FIRST_READ:); mismatches indicate recovery corruption.

    const VALUE_SIZE: usize = 1024;
    const SNAPSHOT_DEFAULT: u64 = 128 * 1024 * 1024 * 1024; // 128 GB
    const FIRST_READ_SAMPLES: usize = 1000;
    // For "no snapshot" runs, sized larger than the largest pre-fill so it
    // never triggers (treat as ∞).
    const SNAPSHOT_INFINITE: u64 = 16 * 1024 * 1024 * 1024 * 1024; // 16 TB

    let mut base = StressTestConfigs::default();
    base.stress_client_parameters.backend = Backend::Tidehunter;
    base.stress_client_parameters.mixed_threads = 36;
    base.stress_client_parameters.write_threads = 36;
    base.stress_client_parameters.write_size = VALUE_SIZE;
    base.stress_client_parameters.key_len = KEY_LEN;
    base.stress_client_parameters.key_layout = KeyLayout::Uniform;
    base.stress_client_parameters.read_mode = ReadMode::Get;
    base.stress_client_parameters.read_percentage = 50;
    base.stress_client_parameters.no_snapshot = false;
    base.stress_client_parameters.report = true;
    base.stress_client_parameters.tldr = String::new();
    base.stress_client_parameters.preserve = true;
    base.stress_client_parameters.relocation = None;
    base.stress_client_parameters.bloom_filter_rate = None;
    base.stress_client_parameters.bloom_filter_count = None;
    base.db_parameters.frag_size = FRAG_SIZE;
    base.db_parameters.max_maps = MAX_MAPS;
    base.db_parameters.max_dirty_keys = DEFAULT_MAX_DIRTY_KEYS;
    base.db_parameters.num_flusher_threads = 12;
    base.db_parameters.metrics_enabled = false;
    base.db_parameters.direct_io = false;

    let bytes_per_write = (KEY_LEN + VALUE_SIZE) as u64;
    let writes_for_size = |size_gb: u64| -> usize {
        ((size_gb * 1024 * 1024 * 1024)
            / (bytes_per_write * base.stress_client_parameters.write_threads as u64))
            as usize
    };

    // Each series emits its fill batch immediately followed by its
    // measure batch, so per-machine peak disk = the largest single fill
    // in the series.
    fn emit_series(
        items: &mut Vec<StressTestConfigs>,
        base: &StressTestConfigs,
        runs: &[(String, u64, u64)],
        first_read_samples: usize,
    ) {
        for (name, size_gb, snap) in runs {
            let mut fill = base.clone();
            fill.db_parameters.snapshot_written_bytes = *snap;
            fill.stress_client_parameters.writes = writes_for_size_with_threads(
                *size_gb,
                base.stress_client_parameters.write_threads as u64,
                base.stress_client_parameters.write_size,
            );
            fill.stress_client_parameters.mixed_duration_secs = 0;
            fill.stress_client_parameters.pause_between_phases_secs = 0;
            fill.stress_client_parameters.db_path = Some(format!("{}/r6/{name}", db_dir()));
            fill.stress_client_parameters.tldr = format!("r6-fill-{name}");
            items.push(fill);
        }
        for (name, size_gb, snap) in runs {
            let mut measure = base.clone();
            measure.db_parameters.snapshot_written_bytes = *snap;
            measure.stress_client_parameters.writes = writes_for_size_with_threads(
                *size_gb,
                base.stress_client_parameters.write_threads as u64,
                base.stress_client_parameters.write_size,
            );
            measure.stress_client_parameters.measure_open = true;
            measure.stress_client_parameters.first_read_samples = first_read_samples;
            measure.stress_client_parameters.clean_after_measure = true;
            measure.stress_client_parameters.mixed_duration_secs = 0;
            measure.stress_client_parameters.pause_between_phases_secs = 0;
            measure.stress_client_parameters.reuse = Some(format!("{}/r6/{name}", db_dir()));
            measure.stress_client_parameters.tldr = format!("r6-measure-{name}");
            items.push(measure);
        }
    }

    let mut items: Vec<StressTestConfigs> = Vec::new();

    // Series A: cold-start vs DB size. The second 1 TiB replicate pads
    // the series to exactly one 4-machine batch.
    let series_a: Vec<(String, u64, u64)> = vec![
        ("cold-100gb".into(), 100, SNAPSHOT_DEFAULT),
        ("cold-500gb".into(), 500, SNAPSHOT_DEFAULT),
        ("cold-1tb".into(), 1024, SNAPSHOT_DEFAULT),
        ("cold-1tb-r2".into(), 1024, SNAPSHOT_DEFAULT),
    ];
    emit_series(&mut items, &base, &series_a, FIRST_READ_SAMPLES);

    // Series B: 1 TiB DB at varying snapshot intervals (the default
    // cadence is covered by series A).
    let series_b: Vec<(String, u64, u64)> = vec![
        ("snap-16gb".into(), 1024, 16 * 1024 * 1024 * 1024),
        ("snap-64gb".into(), 1024, 64 * 1024 * 1024 * 1024),
        ("snap-256gb".into(), 1024, 256 * 1024 * 1024 * 1024),
        ("snap-inf".into(), 1024, SNAPSHOT_INFINITE),
    ];
    emit_series(&mut items, &base, &series_b, FIRST_READ_SAMPLES);

    // Series C: crash during relocation. The mixed phase keeps the
    // workload running until --crash-after-secs fires; without it, writes
    // finish before the crash deadline and the process exits cleanly.
    // 4 replicates so the fills exactly fill one batch: with a partial
    // batch the orchestrator would round-robin a measure onto a machine
    // that does not hold its fill.
    const CRASH_FILL_GB: u64 = 200;
    const CRASH_AFTER_SECS: u64 = 600; // 10 min
    const CRASH_MIXED_SECS: u64 = 1200; // 20 min — comfortably > CRASH_AFTER_SECS
    const CRASH_REPLICATES: usize = 4;

    for replicate in 1..=CRASH_REPLICATES {
        let name = format!("crash-relo-{replicate}");
        let mut fill = base.clone();
        fill.db_parameters.snapshot_written_bytes = SNAPSHOT_DEFAULT;
        fill.stress_client_parameters.writes = writes_for_size(CRASH_FILL_GB);
        fill.stress_client_parameters.mixed_duration_secs = CRASH_MIXED_SECS;
        fill.stress_client_parameters.pause_between_phases_secs = 0;
        fill.stress_client_parameters.relocation = Some(RelocationConfig::Wal);
        fill.stress_client_parameters.crash_after_secs = Some(CRASH_AFTER_SECS);
        fill.stress_client_parameters.db_path = Some(format!("{}/r6/{name}", db_dir()));
        fill.stress_client_parameters.tldr = format!("r6-crash-{replicate}");
        items.push(fill);
    }
    for replicate in 1..=CRASH_REPLICATES {
        let name = format!("crash-relo-{replicate}");
        let mut measure = base.clone();
        measure.db_parameters.snapshot_written_bytes = SNAPSHOT_DEFAULT;
        measure.stress_client_parameters.writes = writes_for_size(CRASH_FILL_GB);
        measure.stress_client_parameters.measure_open = true;
        measure.stress_client_parameters.first_read_samples = FIRST_READ_SAMPLES;
        measure.stress_client_parameters.clean_after_measure = true;
        measure.stress_client_parameters.mixed_duration_secs = 0;
        measure.stress_client_parameters.pause_between_phases_secs = 0;
        measure.stress_client_parameters.reuse = Some(format!("{}/r6/{name}", db_dir()));
        measure.stress_client_parameters.tldr = format!("r6-measure-crash-{replicate}");
        items.push(measure);
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_recovery_replicates() -> Result<()> {
    // Extra rounds for Table 4, same workload shape as `recovery`. The
    // Series B paths are reused across rounds: measures set
    // clean_after_measure=true, so each fill starts on a clean slate.
    // The round suffix (-r1/-r2) exists only in `tldr`.

    const VALUE_SIZE: usize = 1024;
    const SNAPSHOT_DEFAULT: u64 = 128 * 1024 * 1024 * 1024;
    const FIRST_READ_SAMPLES: usize = 1000;

    let mut base = StressTestConfigs::default();
    base.stress_client_parameters.backend = Backend::Tidehunter;
    base.stress_client_parameters.mixed_threads = 36;
    base.stress_client_parameters.write_threads = 36;
    base.stress_client_parameters.write_size = VALUE_SIZE;
    base.stress_client_parameters.key_len = KEY_LEN;
    base.stress_client_parameters.key_layout = KeyLayout::Uniform;
    base.stress_client_parameters.read_mode = ReadMode::Get;
    base.stress_client_parameters.read_percentage = 50;
    base.stress_client_parameters.no_snapshot = false;
    base.stress_client_parameters.report = true;
    base.stress_client_parameters.tldr = String::new();
    base.stress_client_parameters.preserve = true;
    base.stress_client_parameters.relocation = None;
    base.stress_client_parameters.bloom_filter_rate = None;
    base.stress_client_parameters.bloom_filter_count = None;
    base.db_parameters.frag_size = FRAG_SIZE;
    base.db_parameters.max_maps = MAX_MAPS;
    base.db_parameters.max_dirty_keys = DEFAULT_MAX_DIRTY_KEYS;
    base.db_parameters.num_flusher_threads = 12;
    base.db_parameters.metrics_enabled = false;
    base.db_parameters.direct_io = false;

    // Same as generate_recovery's emit_series, plus a round suffix on tldr.
    fn emit_series_round(
        items: &mut Vec<StressTestConfigs>,
        base: &StressTestConfigs,
        runs: &[(String, u64, u64)],
        round: usize,
        first_read_samples: usize,
    ) {
        for (name, size_gb, snap) in runs {
            let mut fill = base.clone();
            fill.db_parameters.snapshot_written_bytes = *snap;
            fill.stress_client_parameters.writes = writes_for_size_with_threads(
                *size_gb,
                base.stress_client_parameters.write_threads as u64,
                base.stress_client_parameters.write_size,
            );
            fill.stress_client_parameters.mixed_duration_secs = 0;
            fill.stress_client_parameters.pause_between_phases_secs = 0;
            fill.stress_client_parameters.db_path = Some(format!("{}/r6/{name}", db_dir()));
            fill.stress_client_parameters.tldr = format!("r6-fill-{name}-r{round}");
            items.push(fill);
        }
        for (name, size_gb, snap) in runs {
            let mut measure = base.clone();
            measure.db_parameters.snapshot_written_bytes = *snap;
            measure.stress_client_parameters.writes = writes_for_size_with_threads(
                *size_gb,
                base.stress_client_parameters.write_threads as u64,
                base.stress_client_parameters.write_size,
            );
            measure.stress_client_parameters.measure_open = true;
            measure.stress_client_parameters.first_read_samples = first_read_samples;
            measure.stress_client_parameters.clean_after_measure = true;
            measure.stress_client_parameters.mixed_duration_secs = 0;
            measure.stress_client_parameters.pause_between_phases_secs = 0;
            measure.stress_client_parameters.reuse = Some(format!("{}/r6/{name}", db_dir()));
            measure.stress_client_parameters.tldr = format!("r6-measure-{name}-r{round}");
            items.push(measure);
        }
    }

    let mut items: Vec<StressTestConfigs> = Vec::new();

    // Series B x 2 rounds. snap-128 fills the 4th slot so each batch is
    // exactly 4 (see the batching note in generate_recovery's Series C).
    let series_b: Vec<(String, u64, u64)> = vec![
        ("snap-16gb".into(), 1024, 16 * 1024 * 1024 * 1024),
        ("snap-64gb".into(), 1024, 64 * 1024 * 1024 * 1024),
        ("snap-128gb".into(), 1024, SNAPSHOT_DEFAULT),
        ("snap-256gb".into(), 1024, 256 * 1024 * 1024 * 1024),
    ];
    emit_series_round(&mut items, &base, &series_b, 1, FIRST_READ_SAMPLES);
    emit_series_round(&mut items, &base, &series_b, 2, FIRST_READ_SAMPLES);

    // Series C replicates 5..=12, numbered to extend crash-relo-{1..4}
    // from `recovery`; same shape as that mode's Series C.
    const CRASH_FILL_GB: u64 = 200;
    const CRASH_AFTER_SECS: u64 = 600;
    const CRASH_MIXED_SECS: u64 = 1200;
    const CRASH_NEW_REPLICATES: std::ops::RangeInclusive<usize> = 5..=12;

    let writes_for_crash = writes_for_size_with_threads(
        CRASH_FILL_GB,
        base.stress_client_parameters.write_threads as u64,
        base.stress_client_parameters.write_size,
    );

    // Emit fills and measures in groups of 4 so each batch is full.
    let crash_reps: Vec<usize> = CRASH_NEW_REPLICATES.collect();
    for chunk in crash_reps.chunks(4) {
        for replicate in chunk {
            let name = format!("crash-relo-{replicate}");
            let mut fill = base.clone();
            fill.db_parameters.snapshot_written_bytes = SNAPSHOT_DEFAULT;
            fill.stress_client_parameters.writes = writes_for_crash;
            fill.stress_client_parameters.mixed_duration_secs = CRASH_MIXED_SECS;
            fill.stress_client_parameters.pause_between_phases_secs = 0;
            fill.stress_client_parameters.relocation = Some(RelocationConfig::Wal);
            fill.stress_client_parameters.crash_after_secs = Some(CRASH_AFTER_SECS);
            fill.stress_client_parameters.db_path = Some(format!("{}/r6/{name}", db_dir()));
            fill.stress_client_parameters.tldr = format!("r6-crash-{replicate}");
            items.push(fill);
        }
        for replicate in chunk {
            let name = format!("crash-relo-{replicate}");
            let mut measure = base.clone();
            measure.db_parameters.snapshot_written_bytes = SNAPSHOT_DEFAULT;
            measure.stress_client_parameters.writes = writes_for_crash;
            measure.stress_client_parameters.measure_open = true;
            measure.stress_client_parameters.first_read_samples = FIRST_READ_SAMPLES;
            measure.stress_client_parameters.clean_after_measure = true;
            measure.stress_client_parameters.mixed_duration_secs = 0;
            measure.stress_client_parameters.pause_between_phases_secs = 0;
            measure.stress_client_parameters.reuse = Some(format!("{}/r6/{name}", db_dir()));
            measure.stress_client_parameters.tldr = format!("r6-measure-crash-{replicate}");
            items.push(measure);
        }
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_diagnostic_crash_single_thread_replay() -> Result<()> {
    // Table 4 Series C shape with single-threaded WAL replay;
    // 8 fill/measure pairs at distinct crash-str1-N paths.

    const VALUE_SIZE: usize = 1024;
    const SNAPSHOT_DEFAULT: u64 = 128 * 1024 * 1024 * 1024;
    const FIRST_READ_SAMPLES: usize = 1_000_000;

    let mut base = StressTestConfigs::default();
    base.stress_client_parameters.backend = Backend::Tidehunter;
    base.stress_client_parameters.mixed_threads = 36;
    base.stress_client_parameters.write_threads = 36;
    base.stress_client_parameters.write_size = VALUE_SIZE;
    base.stress_client_parameters.key_len = KEY_LEN;
    base.stress_client_parameters.key_layout = KeyLayout::Uniform;
    base.stress_client_parameters.read_mode = ReadMode::Get;
    base.stress_client_parameters.read_percentage = 50;
    base.stress_client_parameters.no_snapshot = false;
    base.stress_client_parameters.report = true;
    base.stress_client_parameters.tldr = String::new();
    base.stress_client_parameters.preserve = true;
    base.stress_client_parameters.relocation = None;
    base.stress_client_parameters.bloom_filter_rate = None;
    base.stress_client_parameters.bloom_filter_count = None;
    base.db_parameters.frag_size = FRAG_SIZE;
    base.db_parameters.max_maps = MAX_MAPS;
    base.db_parameters.max_dirty_keys = DEFAULT_MAX_DIRTY_KEYS;
    base.db_parameters.num_flusher_threads = 12;
    base.db_parameters.metrics_enabled = false;
    base.db_parameters.direct_io = false;
    // The point of this mode: single-threaded WAL replay on the measure
    // path.
    base.db_parameters.num_replay_threads = 1;

    const CRASH_FILL_GB: u64 = 200;
    const CRASH_AFTER_SECS: u64 = 600;
    const CRASH_MIXED_SECS: u64 = 1200;
    const CRASH_REPLICATES: std::ops::RangeInclusive<usize> = 1..=8;

    let writes_for_crash = writes_for_size_with_threads(
        CRASH_FILL_GB,
        base.stress_client_parameters.write_threads as u64,
        base.stress_client_parameters.write_size,
    );

    let mut items: Vec<StressTestConfigs> = Vec::new();

    // Groups of 4 so each fill batch is full (see generate_recovery).
    let reps: Vec<usize> = CRASH_REPLICATES.collect();
    for chunk in reps.chunks(4) {
        for replicate in chunk {
            let name = format!("crash-str1-{replicate}");
            let mut fill = base.clone();
            fill.db_parameters.snapshot_written_bytes = SNAPSHOT_DEFAULT;
            fill.stress_client_parameters.writes = writes_for_crash;
            fill.stress_client_parameters.mixed_duration_secs = CRASH_MIXED_SECS;
            fill.stress_client_parameters.pause_between_phases_secs = 0;
            fill.stress_client_parameters.relocation = Some(RelocationConfig::Wal);
            fill.stress_client_parameters.crash_after_secs = Some(CRASH_AFTER_SECS);
            fill.stress_client_parameters.db_path = Some(format!("{}/r6/{name}", db_dir()));
            fill.stress_client_parameters.tldr = format!("r6-fill-{name}");
            items.push(fill);
        }
        for replicate in chunk {
            let name = format!("crash-str1-{replicate}");
            let mut measure = base.clone();
            measure.db_parameters.snapshot_written_bytes = SNAPSHOT_DEFAULT;
            measure.stress_client_parameters.writes = writes_for_crash;
            measure.stress_client_parameters.measure_open = true;
            measure.stress_client_parameters.first_read_samples = FIRST_READ_SAMPLES;
            measure.stress_client_parameters.clean_after_measure = true;
            measure.stress_client_parameters.mixed_duration_secs = 0;
            measure.stress_client_parameters.pause_between_phases_secs = 0;
            measure.stress_client_parameters.reuse = Some(format!("{}/r6/{name}", db_dir()));
            measure.stress_client_parameters.tldr = format!("r6-measure-{name}");
            items.push(measure);
        }
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_diagnostic_crash_relocation_guard() -> Result<()> {
    // Table 4 Series C shape for the relocation-guard + silent-skip
    // tidehunter patches; the patches must be present in the built tree.
    // Distinct crash-guard-N paths.

    const VALUE_SIZE: usize = 1024;
    const SNAPSHOT_DEFAULT: u64 = 128 * 1024 * 1024 * 1024;
    const FIRST_READ_SAMPLES: usize = 1_000_000;

    let mut base = StressTestConfigs::default();
    base.stress_client_parameters.backend = Backend::Tidehunter;
    base.stress_client_parameters.mixed_threads = 36;
    base.stress_client_parameters.write_threads = 36;
    base.stress_client_parameters.write_size = VALUE_SIZE;
    base.stress_client_parameters.key_len = KEY_LEN;
    base.stress_client_parameters.key_layout = KeyLayout::Uniform;
    base.stress_client_parameters.read_mode = ReadMode::Get;
    base.stress_client_parameters.read_percentage = 50;
    base.stress_client_parameters.no_snapshot = false;
    base.stress_client_parameters.report = true;
    base.stress_client_parameters.tldr = String::new();
    base.stress_client_parameters.preserve = true;
    base.stress_client_parameters.relocation = None;
    base.stress_client_parameters.bloom_filter_rate = None;
    base.stress_client_parameters.bloom_filter_count = None;
    base.db_parameters.frag_size = FRAG_SIZE;
    base.db_parameters.max_maps = MAX_MAPS;
    base.db_parameters.max_dirty_keys = DEFAULT_MAX_DIRTY_KEYS;
    base.db_parameters.num_flusher_threads = 12;
    base.db_parameters.metrics_enabled = false;
    base.db_parameters.direct_io = false;

    const CRASH_FILL_GB: u64 = 200;
    const CRASH_AFTER_SECS: u64 = 600;
    const CRASH_MIXED_SECS: u64 = 1200;

    let writes_for_crash = writes_for_size_with_threads(
        CRASH_FILL_GB,
        base.stress_client_parameters.write_threads as u64,
        base.stress_client_parameters.write_size,
    );

    let mut items: Vec<StressTestConfigs> = Vec::new();

    // 4 replicates: one fill batch followed by one measure batch.
    for replicate in 1..=4 {
        let name = format!("crash-guard-{replicate}");
        let mut fill = base.clone();
        fill.db_parameters.snapshot_written_bytes = SNAPSHOT_DEFAULT;
        fill.stress_client_parameters.writes = writes_for_crash;
        fill.stress_client_parameters.mixed_duration_secs = CRASH_MIXED_SECS;
        fill.stress_client_parameters.pause_between_phases_secs = 0;
        fill.stress_client_parameters.relocation = Some(RelocationConfig::Wal);
        fill.stress_client_parameters.crash_after_secs = Some(CRASH_AFTER_SECS);
        fill.stress_client_parameters.db_path = Some(format!("{}/r6/{name}", db_dir()));
        fill.stress_client_parameters.tldr = format!("r6-fill-{name}");
        items.push(fill);
    }
    for replicate in 1..=4 {
        let name = format!("crash-guard-{replicate}");
        let mut measure = base.clone();
        measure.db_parameters.snapshot_written_bytes = SNAPSHOT_DEFAULT;
        measure.stress_client_parameters.writes = writes_for_crash;
        measure.stress_client_parameters.measure_open = true;
        measure.stress_client_parameters.first_read_samples = FIRST_READ_SAMPLES;
        measure.stress_client_parameters.clean_after_measure = true;
        measure.stress_client_parameters.mixed_duration_secs = 0;
        measure.stress_client_parameters.pause_between_phases_secs = 0;
        measure.stress_client_parameters.reuse = Some(format!("{}/r6/{name}", db_dir()));
        measure.stress_client_parameters.tldr = format!("r6-measure-{name}");
        items.push(measure);
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_churn() -> Result<()> {
    // Tables 2 and 3. Each mixed-phase op is an overwrite, a delete, or a
    // fresh insert per (overwrite_ratio, delete_ratio); where the ratios
    // sum to 1.0 there are no fresh inserts, keeping the working set
    // bounded. tldr strings are kept identical to the original runs.
    // Cells are ordered in batches of 4 for a 4-machine testbed.

    const VALUE_SIZE: usize = 1024;
    const FILL_GB: u64 = 500;
    const MIXED_DURATION_SECS: u64 = 3600; // 60 minutes
    const SNAPSHOT_DEFAULT: u64 = 128 * 1024 * 1024 * 1024; // 128 GB

    struct Cell {
        tldr: &'static str,
        relocation: Option<RelocationConfig>,
        overwrite_ratio: f64,
        delete_ratio: f64,
        reclaim_pct: Option<u8>,
    }
    let wal = || Some(RelocationConfig::Wal);
    let index = || Some(RelocationConfig::Index { ratio: None });
    let runs: [Cell; 13] = [
        // Batch 1: WalBased row + None+mixed.
        Cell {
            tldr: "r4-smoke-walbased-overwrite",
            relocation: wal(),
            overwrite_ratio: 1.0,
            delete_ratio: 0.0,
            reclaim_pct: None,
        },
        Cell {
            tldr: "r4-smoke-walbased-mixed",
            relocation: wal(),
            overwrite_ratio: 0.5,
            delete_ratio: 0.5,
            reclaim_pct: None,
        },
        Cell {
            tldr: "r4-smoke-walbased-delete",
            relocation: wal(),
            overwrite_ratio: 0.0,
            delete_ratio: 1.0,
            reclaim_pct: None,
        },
        Cell {
            tldr: "r4-smoke-none-mixed",
            relocation: None,
            overwrite_ratio: 0.5,
            delete_ratio: 0.5,
            reclaim_pct: None,
        },
        // Batch 2: complete the None row + threshold extremes.
        Cell {
            tldr: "r4-full-none-overwrite",
            relocation: None,
            overwrite_ratio: 1.0,
            delete_ratio: 0.0,
            reclaim_pct: None,
        },
        Cell {
            tldr: "r4-full-none-delete",
            relocation: None,
            overwrite_ratio: 0.0,
            delete_ratio: 1.0,
            reclaim_pct: None,
        },
        Cell {
            tldr: "r4-full-e2-walbased-reclaim1",
            relocation: wal(),
            overwrite_ratio: 0.5,
            delete_ratio: 0.5,
            reclaim_pct: Some(1),
        },
        Cell {
            tldr: "r4-full-e2-walbased-reclaim50",
            relocation: wal(),
            overwrite_ratio: 0.5,
            delete_ratio: 0.5,
            reclaim_pct: Some(50),
        },
        // Batch 3: complete the IndexBased row + one more threshold point.
        Cell {
            tldr: "r4-full-indexbased-overwrite",
            relocation: index(),
            overwrite_ratio: 1.0,
            delete_ratio: 0.0,
            reclaim_pct: None,
        },
        Cell {
            tldr: "r4-full-indexbased-mixed",
            relocation: index(),
            overwrite_ratio: 0.5,
            delete_ratio: 0.5,
            reclaim_pct: None,
        },
        Cell {
            tldr: "r4-full-indexbased-delete",
            relocation: index(),
            overwrite_ratio: 0.0,
            delete_ratio: 1.0,
            reclaim_pct: None,
        },
        Cell {
            tldr: "r4-full-e2-walbased-reclaim10",
            relocation: wal(),
            overwrite_ratio: 0.5,
            delete_ratio: 0.5,
            reclaim_pct: Some(10),
        },
        // Batch 4: last threshold point.
        Cell {
            tldr: "r4-full-e2-walbased-reclaim25",
            relocation: wal(),
            overwrite_ratio: 0.5,
            delete_ratio: 0.5,
            reclaim_pct: Some(25),
        },
    ];

    let mut base = StressTestConfigs::default();
    base.stress_client_parameters.backend = Backend::Tidehunter;
    base.stress_client_parameters.mixed_threads = 36;
    base.stress_client_parameters.write_threads = 36;
    base.stress_client_parameters.write_size = VALUE_SIZE;
    base.stress_client_parameters.key_len = KEY_LEN;
    base.stress_client_parameters.key_layout = KeyLayout::Uniform;
    base.stress_client_parameters.read_mode = ReadMode::Get;
    base.stress_client_parameters.read_percentage = 0;
    base.stress_client_parameters.no_snapshot = false;
    base.stress_client_parameters.report = true;
    base.stress_client_parameters.tldr = String::new();
    base.stress_client_parameters.preserve = false;
    base.stress_client_parameters.path = Some(format!("{}/", db_dir()));
    base.stress_client_parameters.bloom_filter_rate = None;
    base.stress_client_parameters.bloom_filter_count = None;
    base.stress_client_parameters.mixed_duration_secs = MIXED_DURATION_SECS;
    base.stress_client_parameters.pause_between_phases_secs = 0;
    base.stress_client_parameters.writes = writes_for_size_with_threads(
        FILL_GB,
        base.stress_client_parameters.write_threads as u64,
        base.stress_client_parameters.write_size,
    );
    base.db_parameters.frag_size = FRAG_SIZE;
    base.db_parameters.max_maps = MAX_MAPS;
    base.db_parameters.max_dirty_keys = DEFAULT_MAX_DIRTY_KEYS;
    base.db_parameters.num_flusher_threads = 12;
    base.db_parameters.snapshot_written_bytes = SNAPSHOT_DEFAULT;
    base.db_parameters.metrics_enabled = false;
    base.db_parameters.direct_io = false;

    let mut items: Vec<StressTestConfigs> = Vec::new();
    for cell in &runs {
        let mut item = base.clone();
        item.stress_client_parameters.relocation = cell.relocation.clone();
        item.stress_client_parameters.overwrite_ratio = cell.overwrite_ratio;
        item.stress_client_parameters.delete_ratio = cell.delete_ratio;
        if let Some(pct) = cell.reclaim_pct {
            item.db_parameters.relocation_max_reclaim_pct = pct;
        }
        item.stress_client_parameters.tldr = cell.tldr.to_string();
        items.push(item);
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_churn_blobdb() -> Result<()> {
    // BlobDB rows of the churn tables; workload matches `churn`'s three
    // corners. Backend::Blobdb switches RocksStorage::open into
    // integrated-BlobDB mode; the BlobDB tunables are hard-coded in
    // rocks.rs and db_parameters are ignored at runtime.

    const VALUE_SIZE: usize = 1024;
    const FILL_GB: u64 = 500;
    const MIXED_DURATION_SECS: u64 = 3600; // 60 minutes
    const SNAPSHOT_DEFAULT: u64 = 128 * 1024 * 1024 * 1024; // 128 GB (Tidehunter-only; ignored here)

    // (label, overwrite_ratio, delete_ratio)
    let runs: [(&str, f64, f64); 3] = [
        ("overwrite", 1.0, 0.0),
        ("mixed", 0.5, 0.5),
        ("delete", 0.0, 1.0),
    ];

    let mut base = StressTestConfigs::default();
    base.stress_client_parameters.backend = Backend::Blobdb;
    base.stress_client_parameters.mixed_threads = 36;
    base.stress_client_parameters.write_threads = 36;
    base.stress_client_parameters.write_size = VALUE_SIZE;
    base.stress_client_parameters.key_len = KEY_LEN;
    base.stress_client_parameters.key_layout = KeyLayout::Uniform;
    base.stress_client_parameters.read_mode = ReadMode::Get;
    base.stress_client_parameters.read_percentage = 0;
    base.stress_client_parameters.no_snapshot = false;
    base.stress_client_parameters.report = true;
    base.stress_client_parameters.tldr = String::new();
    base.stress_client_parameters.preserve = false;
    base.stress_client_parameters.path = Some(format!("{}/", db_dir()));
    base.stress_client_parameters.bloom_filter_rate = None;
    base.stress_client_parameters.bloom_filter_count = None;
    base.stress_client_parameters.relocation = None;
    base.stress_client_parameters.mixed_duration_secs = MIXED_DURATION_SECS;
    base.stress_client_parameters.pause_between_phases_secs = 0;
    base.stress_client_parameters.writes = writes_for_size_with_threads(
        FILL_GB,
        base.stress_client_parameters.write_threads as u64,
        base.stress_client_parameters.write_size,
    );
    base.db_parameters.frag_size = FRAG_SIZE;
    base.db_parameters.max_maps = MAX_MAPS;
    base.db_parameters.max_dirty_keys = DEFAULT_MAX_DIRTY_KEYS;
    base.db_parameters.num_flusher_threads = 12;
    base.db_parameters.snapshot_written_bytes = SNAPSHOT_DEFAULT;
    base.db_parameters.metrics_enabled = false;
    base.db_parameters.direct_io = false;

    let mut items: Vec<StressTestConfigs> = Vec::new();
    for (label, overwrite_ratio, delete_ratio) in &runs {
        let mut item = base.clone();
        item.stress_client_parameters.overwrite_ratio = *overwrite_ratio;
        item.stress_client_parameters.delete_ratio = *delete_ratio;
        item.stress_client_parameters.tldr = format!("r4-blobdb-{label}");
        items.push(item);
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_epoch_gc() -> Result<()> {
    // Epoch-GC table (not in the published paper): 50 GiB pre-fill, then
    // a 60-min pure-write phase with continuous WAL-based relocation.
    // The byte-counting filter registered via --epoch-budget-bytes
    // returns StopRelocation after seeing the budget (mode=Stop) or keeps
    // scanning (mode=Keep ablation). budget50-stop doubles as the budget
    // sweep's middle point.

    const VALUE_SIZE: usize = 1024;
    const FILL_GB: u64 = 50;
    const MIXED_DURATION_SECS: u64 = 3600; // 60 minutes
    const REPLICATES: usize = 2;
    const GB: u64 = 1024 * 1024 * 1024;

    // (label, budget_bytes, mode)
    let runs: [(&str, u64, EpochFilterMode); 4] = [
        ("budget25-stop", 25 * GB, EpochFilterMode::Stop),
        ("budget50-stop", 50 * GB, EpochFilterMode::Stop),
        ("budget100-stop", 100 * GB, EpochFilterMode::Stop),
        ("budget50-keep", 50 * GB, EpochFilterMode::Keep),
    ];

    let mut base = StressTestConfigs::default();
    base.stress_client_parameters.backend = Backend::Tidehunter;
    base.stress_client_parameters.mixed_threads = 36;
    base.stress_client_parameters.write_threads = 36;
    base.stress_client_parameters.write_size = VALUE_SIZE;
    base.stress_client_parameters.key_len = KEY_LEN;
    base.stress_client_parameters.key_layout = KeyLayout::Uniform;
    base.stress_client_parameters.read_mode = ReadMode::Get;
    base.stress_client_parameters.read_percentage = 0;
    base.stress_client_parameters.overwrite_ratio = 0.0;
    base.stress_client_parameters.no_snapshot = false;
    base.stress_client_parameters.report = true;
    base.stress_client_parameters.tldr = String::new();
    base.stress_client_parameters.preserve = false;
    base.stress_client_parameters.path = Some(format!("{}/", db_dir()));
    base.stress_client_parameters.relocation = Some(RelocationConfig::Wal);
    base.stress_client_parameters.bloom_filter_rate = None;
    base.stress_client_parameters.bloom_filter_count = None;
    base.stress_client_parameters.mixed_duration_secs = MIXED_DURATION_SECS;
    base.stress_client_parameters.pause_between_phases_secs = 0;
    base.stress_client_parameters.writes = writes_for_size_with_threads(
        FILL_GB,
        base.stress_client_parameters.write_threads as u64,
        base.stress_client_parameters.write_size,
    );
    base.db_parameters.frag_size = FRAG_SIZE;
    base.db_parameters.max_maps = MAX_MAPS;
    base.db_parameters.max_dirty_keys = DEFAULT_MAX_DIRTY_KEYS;
    base.db_parameters.num_flusher_threads = 12;
    base.db_parameters.metrics_enabled = false;
    base.db_parameters.direct_io = false;

    let mut items: Vec<StressTestConfigs> = Vec::new();
    for replicate in 1..=REPLICATES {
        for (label, budget, mode) in &runs {
            let mut item = base.clone();
            item.stress_client_parameters.epoch_budget_bytes = Some(*budget);
            item.stress_client_parameters.epoch_filter_mode = *mode;
            item.stress_client_parameters.tldr = format!("r2d6-{label}-r{replicate}");
            items.push(item);
        }
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_sweep_bloom_fpr() -> Result<()> {
    // Table 6, Bloom-FPR rows. bloom_filter_count=8192 sizes the filter
    // for the ~7.2K keys/cell of a 1 TiB fill at the default 131072
    // cells.

    const DB_SIZE_BYTES: usize = 1024 * 1024 * 1024 * 1024; // 1 TB
    const VALUE_SIZE: usize = 1024;
    const BLOOM_FILTER_COUNT: u32 = 8192;
    const REPLICATES: usize = 2;

    let fprs: [f32; 4] = [0.001, 0.01, 0.05, 0.10];

    let mut base = StressTestConfigs::default();
    base.stress_client_parameters.backend = Backend::Tidehunter;
    base.stress_client_parameters.mixed_threads = 36;
    base.stress_client_parameters.write_threads = 36;
    base.stress_client_parameters.mixed_duration_secs = 1800;
    base.stress_client_parameters.background_writes = 0;
    base.stress_client_parameters.no_snapshot = false;
    base.stress_client_parameters.report = true;
    base.stress_client_parameters.key_layout = KeyLayout::Uniform;
    base.stress_client_parameters.tldr = String::new();
    base.stress_client_parameters.preserve = false;
    base.stress_client_parameters.path = Some(format!("{}/", db_dir()));
    base.stress_client_parameters.key_len = KEY_LEN;
    base.stress_client_parameters.write_size = VALUE_SIZE;
    base.stress_client_parameters.read_mode = ReadMode::Get;
    base.stress_client_parameters.read_percentage = 100;
    base.stress_client_parameters.zipf_exponent = 0.0;
    base.stress_client_parameters.writes = DB_SIZE_BYTES
        / (base.stress_client_parameters.write_threads
            * (base.stress_client_parameters.key_len + base.stress_client_parameters.write_size));
    base.stress_client_parameters.relocation = None;
    base.stress_client_parameters.bloom_filter_count = Some(BLOOM_FILTER_COUNT);

    base.db_parameters.frag_size = FRAG_SIZE;
    base.db_parameters.max_maps = MAX_MAPS;
    base.db_parameters.max_dirty_keys = DEFAULT_MAX_DIRTY_KEYS;
    base.db_parameters.num_flusher_threads = 12;
    base.db_parameters.metrics_enabled = false;
    base.db_parameters.direct_io = false;

    let mut items: Vec<StressTestConfigs> = Vec::new();
    for replicate in 1..=REPLICATES {
        for &fpr in &fprs {
            let mut item = base.clone();
            item.stress_client_parameters.bloom_filter_rate = Some(fpr);
            item.stress_client_parameters.tldr =
                format!("r3-bloom-fpr{}-r{replicate}", (fpr * 1000.0) as u32);
            items.push(item);
        }
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

/// Base config shared by the Table 6 sweeps: the headline 50/50 Get cell
/// (1 KB values, 32 B keys, uniform, 1 TiB pre-fill, 30-min measured
/// phase).
fn r3_sweep_base() -> StressTestConfigs {
    const DB_SIZE_BYTES: usize = 1024 * 1024 * 1024 * 1024; // 1 TB
    const VALUE_SIZE: usize = 1024;

    let mut base = StressTestConfigs::default();
    base.stress_client_parameters.backend = Backend::Tidehunter;
    base.stress_client_parameters.mixed_threads = 36;
    base.stress_client_parameters.write_threads = 36;
    base.stress_client_parameters.mixed_duration_secs = 1800;
    base.stress_client_parameters.background_writes = 0;
    base.stress_client_parameters.no_snapshot = false;
    base.stress_client_parameters.report = true;
    base.stress_client_parameters.key_layout = KeyLayout::Uniform;
    base.stress_client_parameters.tldr = String::new();
    base.stress_client_parameters.preserve = false;
    base.stress_client_parameters.path = Some(format!("{}/", db_dir()));
    base.stress_client_parameters.key_len = KEY_LEN;
    base.stress_client_parameters.write_size = VALUE_SIZE;
    base.stress_client_parameters.read_mode = ReadMode::Get;
    base.stress_client_parameters.read_percentage = 50;
    base.stress_client_parameters.zipf_exponent = 0.0;
    base.stress_client_parameters.writes = DB_SIZE_BYTES
        / (base.stress_client_parameters.write_threads
            * (base.stress_client_parameters.key_len + base.stress_client_parameters.write_size));
    base.stress_client_parameters.relocation = None;
    base.stress_client_parameters.bloom_filter_rate = None;
    base.stress_client_parameters.bloom_filter_count = None;

    base.db_parameters.frag_size = FRAG_SIZE;
    base.db_parameters.max_maps = MAX_MAPS;
    base.db_parameters.max_dirty_keys = DEFAULT_MAX_DIRTY_KEYS;
    base.db_parameters.num_flusher_threads = 12;
    base.db_parameters.metrics_enabled = false;
    base.db_parameters.direct_io = false;

    base
}

fn generate_sweep_mmap_window() -> Result<()> {
    // Table 6, mmap-window rows. Emitted replicate-major so each
    // 4-machine batch runs one replicate of all 4 points.

    const REPLICATES: usize = 4;
    let max_maps_values: [usize; 4] = [16, 32, 64, 128];

    let base = r3_sweep_base();

    let mut items: Vec<StressTestConfigs> = Vec::new();
    for replicate in 1..=REPLICATES {
        for &max_maps in &max_maps_values {
            let mut item = base.clone();
            item.db_parameters.max_maps = max_maps;
            item.stress_client_parameters.tldr = format!("sweep-mmap-{max_maps}-r{replicate}");
            items.push(item);
        }
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_sweep_cell_count() -> Result<()> {
    // Table 6, cell-count rows; num_mutexes with cells_per_mutex unset
    // (defaults to 1).

    let cell_counts: [usize; 5] = [
        1 << 14, // 16384
        1 << 16, // 65536
        1 << 17, // 131072 (default)
        1 << 19, // 524288
        1 << 20, // 1048576
    ];

    let mut base = r3_sweep_base();
    base.db_parameters.max_maps = 128;

    let mut items: Vec<StressTestConfigs> = Vec::new();
    for &cells in &cell_counts {
        let mut item = base.clone();
        item.stress_client_parameters.num_mutexes = Some(cells);
        item.stress_client_parameters.cells_per_mutex = None;
        item.stress_client_parameters.tldr = format!("sweep-cells-{cells}");
        items.push(item);
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_sweep_dirty_keys() -> Result<()> {
    // Table 6, dirty-key rows.

    let dirty_key_caps: [usize; 5] = [64, 256, 1024, 4096, 16384];

    let mut base = r3_sweep_base();
    base.db_parameters.max_maps = 128;

    let mut items: Vec<StressTestConfigs> = Vec::new();
    for &cap in &dirty_key_caps {
        let mut item = base.clone();
        item.db_parameters.max_dirty_keys = cap;
        item.stress_client_parameters.tldr = format!("sweep-dirty-{cap}");
        items.push(item);
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_memory_instrumented() -> Result<()> {
    // Table 5: metrics_enabled=true exports the per-keyspace runtime
    // gauges (lookup_result by source, flush/unload counters, dirty_keys,
    // loaded_key_bytes, flat_index_bytes) that feed the table.

    const DB_SIZE_BYTES: usize = 1024 * 1024 * 1024 * 1024; // 1 TB
    const VALUE_SIZE: usize = 1024;
    const REPLICATES: usize = 4;

    let mut base = StressTestConfigs::default();
    base.stress_client_parameters.backend = Backend::Tidehunter;
    base.stress_client_parameters.mixed_threads = 36;
    base.stress_client_parameters.write_threads = 36;
    base.stress_client_parameters.mixed_duration_secs = 1800;
    base.stress_client_parameters.background_writes = 0;
    base.stress_client_parameters.no_snapshot = false;
    base.stress_client_parameters.report = true;
    base.stress_client_parameters.key_layout = KeyLayout::Uniform;
    base.stress_client_parameters.tldr = String::new();
    base.stress_client_parameters.preserve = false;
    base.stress_client_parameters.path = Some(format!("{}/", db_dir()));
    base.stress_client_parameters.key_len = KEY_LEN;
    base.stress_client_parameters.write_size = VALUE_SIZE;
    base.stress_client_parameters.read_mode = ReadMode::Get;
    base.stress_client_parameters.read_percentage = 50;
    base.stress_client_parameters.zipf_exponent = 0.0;
    base.stress_client_parameters.writes = DB_SIZE_BYTES
        / (base.stress_client_parameters.write_threads
            * (base.stress_client_parameters.key_len + base.stress_client_parameters.write_size));
    base.stress_client_parameters.relocation = None;
    base.stress_client_parameters.bloom_filter_rate = None;
    base.stress_client_parameters.bloom_filter_count = None;

    base.db_parameters.frag_size = FRAG_SIZE;
    base.db_parameters.max_maps = MAX_MAPS;
    base.db_parameters.max_dirty_keys = DEFAULT_MAX_DIRTY_KEYS;
    base.db_parameters.num_flusher_threads = 12;
    base.db_parameters.metrics_enabled = true;
    base.db_parameters.direct_io = false;

    let mut items: Vec<StressTestConfigs> = Vec::new();
    for replicate in 1..=REPLICATES {
        let mut item = base.clone();
        item.stress_client_parameters.tldr = format!("r3-instrumented-r{replicate}");
        items.push(item);
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_main_benchmark() -> Result<()> {
    // Figures 5 and 6: 7 workloads x 3 value sizes x 2 skews, single
    // replicate; read_mode is ignored for the write-only cell.

    const DB_SIZE_BYTES: usize = 1024 * 1024 * 1024 * 1024; // 1 TB

    let value_sizes: [usize; 3] = [1024, 64, 128];
    let zipf_exponents: [f64; 2] = [0.0, 2.0];

    // (label, read_percentage, read_mode)
    let configs: [(&str, u8, ReadMode); 7] = [
        ("write", 0, ReadMode::Get),
        ("mix50-get", 50, ReadMode::Get),
        ("mix50-exists", 50, ReadMode::Exists),
        ("mix50-lt", 50, ReadMode::Lt(1)),
        ("read-get", 100, ReadMode::Get),
        ("read-exists", 100, ReadMode::Exists),
        ("read-lt", 100, ReadMode::Lt(1)),
    ];

    let mut base = StressTestConfigs::default();
    base.stress_client_parameters.backend = Backend::Tidehunter;
    base.stress_client_parameters.mixed_threads = 36;
    base.stress_client_parameters.write_threads = 36;
    base.stress_client_parameters.mixed_duration_secs = 1800;
    base.stress_client_parameters.background_writes = 0;
    base.stress_client_parameters.no_snapshot = false;
    base.stress_client_parameters.report = true;
    base.stress_client_parameters.key_layout = KeyLayout::Uniform;
    base.stress_client_parameters.tldr = String::new();
    base.stress_client_parameters.preserve = false;
    base.stress_client_parameters.path = Some(format!("{}/", db_dir()));
    base.stress_client_parameters.key_len = KEY_LEN;
    base.stress_client_parameters.relocation = None;
    base.stress_client_parameters.bloom_filter_rate = None;
    base.stress_client_parameters.bloom_filter_count = None;

    base.db_parameters.frag_size = FRAG_SIZE;
    base.db_parameters.max_maps = MAX_MAPS;
    base.db_parameters.max_dirty_keys = DEFAULT_MAX_DIRTY_KEYS;
    base.db_parameters.num_flusher_threads = 12;
    base.db_parameters.metrics_enabled = false;
    base.db_parameters.direct_io = false;

    let mut items: Vec<StressTestConfigs> = Vec::new();
    for &value_size in &value_sizes {
        for &zipf_exponent in &zipf_exponents {
            for (label, read_pct, read_mode) in &configs {
                let mut item = base.clone();
                item.stress_client_parameters.write_size = value_size;
                item.stress_client_parameters.zipf_exponent = zipf_exponent;
                item.stress_client_parameters.read_percentage = *read_pct;
                item.stress_client_parameters.read_mode = read_mode.clone();
                item.stress_client_parameters.writes = DB_SIZE_BYTES
                    / (item.stress_client_parameters.write_threads
                        * (item.stress_client_parameters.key_len
                            + item.stress_client_parameters.write_size));
                item.stress_client_parameters.tldr = format!(
                    "paper-redo-64gb-v{value_size}-z{}-{label}",
                    zipf_exponent as u32
                );
                items.push(item);
            }
        }
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn working_dir_parsed_from_settings() {
        let yaml = "testbed_id: x\nworking_dir: /data/db\nmonitoring: false\n";
        assert_eq!(
            working_dir_from_settings(yaml).unwrap().as_deref(),
            Some("/data/db")
        );
        // Trailing slash is normalized away.
        let yaml = "working_dir: /data/db/\n";
        assert_eq!(
            working_dir_from_settings(yaml).unwrap().as_deref(),
            Some("/data/db")
        );
    }

    #[test]
    fn working_dir_absent_yields_none() {
        assert_eq!(working_dir_from_settings("testbed_id: x\n").unwrap(), None);
    }

    #[test]
    fn working_dir_env_vars_resolved() {
        // SAFETY: test-only; no other thread reads the environment here.
        unsafe { std::env::set_var("GTC_TEST_DB_ROOT", "/mnt/fast") };
        let yaml = "working_dir: ${GTC_TEST_DB_ROOT}/db\n";
        assert_eq!(
            working_dir_from_settings(yaml).unwrap().as_deref(),
            Some("/mnt/fast/db")
        );
    }

    #[test]
    fn working_dir_unresolved_env_is_terse_error() {
        let yaml = "working_dir: ${GTC_TEST_UNSET_VAR}/db\nsecret: token-abc\n";
        let err = working_dir_from_settings(yaml).unwrap_err().to_string();
        assert!(err.contains("unresolved"));
        // Never echo file contents (the real file holds access tokens).
        assert!(!err.contains("token-abc"));
    }
}
