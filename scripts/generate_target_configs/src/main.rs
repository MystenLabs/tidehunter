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
Naming convention: modes map one-to-one onto the figures and tables of the\n\
Tidehunter paper. `main-benchmark` / `main-benchmark-baselines` cover the main\n\
throughput figures (fig:benchmark-results-1k/-64b/-128b), `value-scaling` /\n\
`value-scaling-baselines` cover fig:value-scaling, `stability` covers\n\
tab:stability, `app-workloads` covers fig:benchmark-results-app-workloads,\n\
`relocation` covers fig:relocation-results, `churn` / `churn-blobdb` cover\n\
tab:churn-strategy and tab:churn-threshold, `recovery` / `recovery-replicates`\n\
cover tab:recovery, `memory-instrumented` covers tab:memory-runtime, and the\n\
`sweep-*` modes cover the rows of tab:r3-sweeps. `epoch-gc` reproduces the\n\
epoch-GC table that is commented out of the published paper, and the\n\
`diagnostic-*` modes are bug-hunt experiments with no paper element.\n\
\n\
Historical revision-item names (paper-redo64gb, bloom-fpr-sweep, r3-*, r4-*,\n\
r6-*, r2d6-*) still work as aliases. Run `--help` after a mode name (or read\n\
the per-mode doc below) for the exact workload each mode emits. Every mode\n\
overwrites the same output file: orchestrator/assets/target_configs.yml."
)]
struct Args {
    #[command(subcommand)]
    mode: Mode,
}

#[derive(Subcommand, Debug)]
enum Mode {
    /// Tidehunter cells of the main benchmark figures
    /// (fig:benchmark-results-1k/-64b/-128b): 42 cells = 3 value sizes
    /// {1 KB, 64 B, 128 B} × 2 skews × 7 workloads (write-only, 50/50
    /// Get/Exists/Lt, 100% Get/Exists/Lt), single replicate; 1 TiB
    /// pre-fill, 30-min measured phase, max_maps=64. The 50/50 Get cells
    /// overlap with `value-scaling` (same parameters); they are emitted here
    /// too so the mode is self-contained. (Historical: `paper-redo64gb`, the
    /// redo of the figures at the 64 × 1 GB mmap budget after the r3 cache
    /// sweep, which excluded the 50/50 Get cells as already covered;
    /// relocation, app_workloads and the stability table were excluded from
    /// that pass by design.)
    #[command(alias = "paper-redo64gb")]
    MainBenchmark,
    /// RocksDB and BlobDB cells of the main benchmark figures
    /// (fig:benchmark-results-1k/-64b/-128b): 84 cells = 2 backends ×
    /// 3 value sizes {1 KB, 64 B, 128 B} × 2 skews × 7 workloads (write-only,
    /// 50/50 Get/Exists/Lt, 100% Get/Exists/Lt), single replicate; 1 TiB
    /// pre-fill, 10-min measured phase (mixed_duration_secs=600). Parameters
    /// extracted from the archived runs in
    /// logs/logs-revision-experiments/rockdb-blobdb-12bgthreads (84 logs).
    /// The Tidehunter-only db_parameters (max_maps=128 etc.) match those
    /// logs byte-for-byte but are ignored by RocksStorage at runtime.
    MainBenchmarkBaselines,
    /// Tidehunter curves of fig:value-scaling: 20 cells
    /// = 5 value sizes {64..1024 B} × 2 skews × 2 replicates; 1 TiB pre-fill,
    /// 30-min measured phase, max_maps=64. The {64, 128, 1024} B points use
    /// the same parameters as `main-benchmark`'s 50/50 Get cells (the two
    /// modes overlap but are each self-contained). (Historical: the
    /// value-scaling redo at the 64 × 1 GB mmap budget; earlier emissions had
    /// empty tldr fields — cells now carry value-scaling-v{size}-z{zipf}-r{rep}.)
    ValueScaling,
    /// RocksDB and BlobDB curves of fig:value-scaling: 20 cells = 2 backends
    /// × 5 value sizes {64, 128, 256, 512, 1024} × 2 skews, 50/50 Get,
    /// single replicate; 1 TiB pre-fill, 10-min measured phase
    /// (mixed_duration_secs=600). Parameters extracted from the archived runs
    /// in logs/logs-revision-experiments/rocksdb-blobdb-12t-valuescaling
    /// (20 logs). db_parameters match those logs (max_maps=128) but are
    /// ignored by RocksStorage at runtime.
    ValueScalingBaselines,
    /// tab:stability: 6 cells = read percentage {0, 50, 100} × 2 skews on
    /// 1 KB values; 1 TiB pre-fill, 30-min measured phase
    /// (mixed_duration_secs=1800). Parameters extracted from the archived
    /// runs in logs/logs-revision-experiments/r7 (6 logs): max_maps=128 (what
    /// the paper's table reports) and metrics_enabled=true — the table's
    /// Large-Table lock-overhead column needs the large_table_contention
    /// metric.
    Stability,
    /// fig:benchmark-results-app-workloads: 30 cells = 5 (key,value) combos
    /// {24/10 (RTDATA), 48/43 (ZippyDB), 20/44, 38/38, 76/50} × 2 skews ×
    /// 3 backends (Tidehunter, RocksDB, BlobDB), 50/50 Get, single replicate;
    /// 500 GiB raw key+value pre-fill (writes = 500 GiB / ((key+value)×36),
    /// exact per-combo counts taken from the logs), 30-min measured phase
    /// (mixed_duration_secs=1800). Parameters extracted from the archived
    /// runs in logs/logs-revision-experiments/r9-larger-keys.
    AppWorkloads,
    /// fig:relocation-results: 4 cells = relocation {on, off} × 2 skews under
    /// a 100%-delete mixed phase; 1 TiB pre-fill of 1 KB values, 10-min
    /// measured phase (mixed_duration_secs=600), max_maps=128 and
    /// metrics_enabled=true as in the archived figure runs
    /// (logs/logs-add-cooldown-and-gc-metric/opts-1-2-sort). NOTE: those
    /// archived runs used Index-based relocation (ratio 1.0, reclaim_pct 20)
    /// on the pre-revision code; this mode uses the WalBased strategy that is
    /// now the paper's default (relocation: Some(Wal), default reclaim_pct).
    Relocation,
    /// tab:churn-strategy (Tidehunter rows) and tab:churn-threshold: 13 cells
    /// = 3 strategies {None, WalBased, IndexBased} × 3 mixes {100% overwrite,
    /// 50/50 overwrite+delete, 100% delete} (9 cells) plus reclaim_pct
    /// {1, 10, 25, 50} on WalBased 50/50 (4 cells; the 5% point of the
    /// threshold sweep is the WalBased 50/50 cell of the 3×3 at the default
    /// reclaim_pct=5). 500 GB pre-fill, 60-min pure-write measured phase
    /// (mixed_duration_secs=3600, read_percentage=0). Cells keep their
    /// historical r4-smoke-* / r4-full-* tldr strings byte-identical so new
    /// runs remain comparable with the archived logs. (Merges the former
    /// r4-churn-smoke and r4-churn-full modes.)
    #[command(alias = "r4-churn-full")]
    Churn,
    /// BlobDB rows of tab:churn-strategy: 3 cells matching the three workload
    /// corners of the Tidehunter churn matrix (100% overwrite, 50/50
    /// overwrite+delete, 100% delete) with the same 500 GB pre-fill and
    /// 60-min pure-write measured phase, so the rows append directly to the
    /// table. `Backend::Blobdb` switches `RocksStorage::open` into
    /// integrated-BlobDB mode (`enable_blob_files`, 256 B min blob size,
    /// 128 MB blob files, ZSTD); those tunables are baked into rocks.rs, and
    /// the Tidehunter-specific db_parameters are ignored at runtime.
    /// `relocation: None` because BlobDB owns its own GC (RocksDB compaction
    /// drives blob-file cleanup). (Historical: r4-churn-blobdb, the R1-D3
    /// cross-system comparison.)
    #[command(alias = "r4-churn-blobdb")]
    ChurnBlobdb,
    /// Epoch-based GC evaluation (tab:epoch-gc) — NOT in the published paper:
    /// the table and its subsection are commented out of both the VLDB and
    /// arXiv versions. 8 cells = {budget 25/50/100 GiB with Stop filter,
    /// budget 50 GiB with Keep ablation} × 2 replicates; 50 GB pre-fill,
    /// 60-min pure-write measured phase of fresh inserts with continuous
    /// WAL-based relocation. (Historical: R2-D6, a best-effort revision item;
    /// E2.a Stop@50 GiB is identical to E1's middle point, so only 4 unique
    /// configs exist.)
    #[command(alias = "r2d6-epoch-gc")]
    EpochGc,
    /// tab:recovery: 24 cells = fill/measure pairs for (A) cold-start vs DB
    /// size {100 GB, 500 GB, 1 TiB, 1 TiB-r2}, (B) snapshot-interval sweep at
    /// 1 TiB {16, 64, 256 GiB, ∞}, and (C) crash-during-relocation × 4
    /// replicates (200 GB fill, crash at +600 s of a 1200 s mixed phase).
    /// Fills come in batches of exactly 4 followed by their measure batch so
    /// measure i+4 round-robins onto the machine holding fill i. Measures use
    /// measure_open with first_read_samples=1000, matching the published runs
    /// (logs/logs-revision-experiments/r6/paper show first_read_samples:
    /// 1000). (Historical: r6-recovery; that mode's 2 TiB cold-start pair —
    /// not in the paper's table — is replaced by a second 1 TiB replicate to
    /// keep Series A a full 4-machine batch.)
    #[command(alias = "r6-recovery")]
    Recovery,
    /// Extra replicates for tab:recovery: 32 cells = Series B (snapshot
    /// sweep, now {16, 64, 128, 256} GiB) × 2 rounds and Series C
    /// (crash-during-relocation) replicates 5–12, all as fill/measure pairs
    /// in batches of 4. Measures use first_read_samples=1000 as in the
    /// published runs. (Historical: r6-recovery-supplemental, added when the
    /// May 14 re-runs showed the Series B trend and Series C variance needed
    /// more data.)
    #[command(alias = "r6-recovery-supplemental")]
    RecoveryReplicates,
    /// tab:memory-runtime: 4 identical replicates of the headline 50/50 Get /
    /// 1 KB / θ=0 / 1 TiB-fill cell with metrics_enabled=true, 30-min
    /// measured phase — the per-keyspace runtime gauges (lookup_result by
    /// source, flush/unload counters, dirty_keys, loaded_key_bytes,
    /// flat_index_bytes) feed the table's lookup-source split, eviction
    /// totals, and memory gauges. (Historical: r3-instrumented-replicates;
    /// all earlier r3 runs had metrics disabled.)
    #[command(alias = "r3-instrumented-replicates")]
    MemoryInstrumented,
    /// tab:r3-sweeps, Bloom-FPR rows: 8 cells = FPR {0.001, 0.01, 0.05, 0.10}
    /// × 2 replicates at the fixed 100%-Get workload (1 KB values, θ=0, 1 TiB
    /// pre-fill), 30-min measured phase; bloom_filter_count=8192 sized for
    /// ~7.2 K keys/cell at the default 131072 cells. (Historical:
    /// bloom-fpr-sweep, revision-plan §3 Experiment A.)
    #[command(alias = "bloom-fpr-sweep")]
    SweepBloomFpr,
    /// tab:r3-sweeps, mmap-window rows: 16 cells = max_maps {16, 32, 64, 128}
    /// (= 32–256 GiB total mapped across the Value WAL and Index Store, at
    /// 1 GiB fragments × 2 WALs) × 4 replicates on the headline 50/50 Get /
    /// 1 KB / θ=0 / 1 TiB-fill cell, 30-min measured phase. Replicate count
    /// (4) copied from the archived sweep in
    /// logs/logs-revision-experiments/r3.
    SweepMmapWindow,
    /// tab:r3-sweeps, cell-count rows: 5 cells = num_mutexes {2^14, 2^16,
    /// 2^17, 2^19, 2^20} with cells_per_mutex unset (defaults to 1, so total
    /// cells = num_mutexes — exactly how the archived r3 runs varied it) on
    /// the headline 50/50 Get / 1 KB / θ=0 / 1 TiB-fill cell at max_maps=128,
    /// 30-min measured phase, single replicate as in
    /// logs/logs-revision-experiments/r3.
    SweepCellCount,
    /// tab:r3-sweeps, dirty-key rows: 5 cells = max_dirty_keys {64, 256,
    /// 1024, 4096, 16384} on the headline 50/50 Get / 1 KB / θ=0 /
    /// 1 TiB-fill cell at max_maps=128, 30-min measured phase, single
    /// replicate as in the archived sweep (logs/logs-revision-experiments/r3;
    /// the 1024 point is the default also covered by the mmap-window sweep's
    /// max_maps=128 cells).
    SweepDirtyKeys,
    /// Diagnostic — no paper element. Crash-during-relocation (tab:recovery
    /// Series C shape) with single-threaded WAL replay, for the `hits=999`
    /// misses observed in 2/12 May 14/15 Series C runs. Hypothesis: the
    /// parallel WAL replay added in commit `2fcb226` (which fans entries
    /// across `num_replay_threads` workers keyed by cell) has a race that
    /// occasionally drops an entry. The original paper Series C runs
    /// (pre-parallel-replay) had hits=1000 in 12/12. This mode forces
    /// `num_replay_threads = 1` to fall back on the single-threaded replay
    /// path while holding everything else fixed — same workload, same crash
    /// timing, same crash sampler seed; measures keep the diagnostic's
    /// 1,000,000 first-read samples. 8 replicates (2 batches of 4) is enough
    /// for a first signal: if 0/8 show misses, parallel replay is the likely
    /// culprit; if any do, the misses come from somewhere else.
    #[command(alias = "r6-recovery-crash-single-thread-replay")]
    DiagnosticCrashSingleThreadReplay,
    /// Diagnostic — no paper element. Crash-during-relocation (tab:recovery
    /// Series C shape) with the relocation-guard + silent-skip diagnostic
    /// patches applied to tidehunter (see investigation notes from May 15).
    /// Tests two candidate explanations for the missing-key bug at once:
    ///   1. Patch in `db.rs::write_relocated_batch` holds the relocated WAL
    ///      guards through `sync_flush_for_relocation`, restoring the
    ///      WalTracker invariant (guard lives until in-memory index is
    ///      updated).
    ///   2. Patch in `relocation/mod.rs::wal_based_relocation` logs
    ///      `RELOCATION_SILENT_SKIP` when `read_record` returns None for an
    ///      entry below `target_position`.
    ///
    /// Measures keep the diagnostic's 1,000,000 first-read samples.
    /// 4 replicates (one batch): the existing data shows ~40% miss rate in
    /// long-cluster runs, so 0/4 misses is decisive (P=0.6^2 ≈ 0.36 under
    /// null).
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

    // Value-scaling redo at the 64 × 1 GB setup.
    //
    // r3 found that 64 maps of 1 GB outperforms 128 maps of 1 GB on the θ=0
    // max_maps sweep at 1 KB values. This run set re-measures the value-scaling
    // figure (paper/plots/value_scaling.tex) under that setup so the figure can
    // be refreshed with the new baseline.
    //
    // Workload shape matches the figure: 50/50 Get mixed phase, 1 TB pre-fill,
    // 30-min measurement, both homogeneous (θ=0) and skewed (θ=2) access.
    // Five value sizes spanning 64 B to 1 KB. Each (size, skew) point is
    // replicated 2× to bound run-to-run noise.

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
    // RocksDB/BlobDB cells of fig:value-scaling, regenerated from the archived
    // runs in logs/logs-revision-experiments/rocksdb-blobdb-12t-valuescaling
    // (20 logs = 2 backends × 5 value sizes × 2 skews, single replicate).
    //
    // Log-derived parameters: 50/50 Get mixed phase, 1 TiB pre-fill,
    // 10-min measurement (mixed_duration_secs=600) with the default 600 s
    // pause between phases, 36+36 threads, 32 B keys. db_parameters in those
    // logs show max_maps=128 / max_dirty_keys=1024 / 12 flusher threads —
    // all ignored by RocksStorage at runtime, but reproduced here so the
    // emitted YAML matches the archived runs field-for-field.

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
    base.db_parameters.max_maps = 128; // as in the archived logs; ignored by RocksStorage
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
    // RocksDB and BlobDB cells of the main benchmark figures, regenerated
    // from the archived runs in
    // logs/logs-revision-experiments/rockdb-blobdb-12bgthreads (84 logs =
    // 2 backends × 3 value sizes × 2 skews × 7 workloads, single replicate).
    //
    // Log-derived parameters: 1 TiB pre-fill, 10-min measurement
    // (mixed_duration_secs=600) with the default 600 s pause between phases,
    // 36+36 threads, 32 B keys. The baseline 50/50 Get numbers for the
    // figures come from this grid.
    // db_parameters in those logs show max_maps=128 etc. — ignored by
    // RocksStorage at runtime, but reproduced so the YAML matches the
    // archived runs field-for-field. RocksDB/BlobDB tuning (LZ4/ZSTD, 16 KB
    // blocks, Bloom filters, increase_parallelism(12), BlobDB blob settings)
    // is hard-coded in benchmarks/benchmark/src/storage/rocks.rs, not
    // YAML-settable.

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
    base.db_parameters.max_maps = 128; // as in the archived logs; ignored by RocksStorage
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
    // tab:stability — 6 cells regenerated from the archived runs in
    // logs/logs-revision-experiments/r7 (6 logs).
    //
    // Log-derived parameters: 1 KB values, 32 B keys, 1 TiB pre-fill,
    // 30-min measurement (mixed_duration_secs=1800) with the default 600 s
    // pause, read percentage {0, 50, 100} × zipf {0, 2}, max_maps=128 (what
    // the paper's table reports), metrics_enabled=true — the table's
    // Large-Table row-mutex overhead column is computed from the
    // large_table_contention metric, so metrics must stay on.

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
    base.db_parameters.max_maps = 128; // as in the archived r7 runs and the paper's table
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
    // fig:benchmark-results-app-workloads — 30 cells regenerated from the
    // archived runs in logs/logs-revision-experiments/r9-larger-keys
    // (33 logs = the 30-cell grid plus 3 Tidehunter re-runs).
    //
    // Log-derived parameters: five (key_len, value_size) combos — (24, 10)
    // and (48, 43) match the RTDATA and ZippyDB workload signatures — with
    // 500 GiB of raw key+value bytes pre-filled per cell (writes =
    // 500 GiB / ((key+value)×36); the exact per-combo counts below are taken
    // verbatim from the logs), 50/50 Get mixed phase, 30-min measurement
    // (mixed_duration_secs=1800) with the default 600 s pause, zipf {0, 2},
    // all three backends, single replicate, max_maps=128 as in the logs.

    // (key_len, value_size, writes-per-thread from the archived logs)
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
    base.db_parameters.max_maps = 128; // as in the archived r9 runs
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
    // fig:relocation-results — relocation on vs off under a 100%-delete
    // mixed phase. 4 cells = one 4-machine batch.
    //
    // The archived figure runs live in
    // logs/logs-add-cooldown-and-gc-metric/opts-1-2-sort (their storage and
    // throughput numbers match the plotted values). Log-derived parameters:
    // 1 TiB pre-fill of 1 KB values (writes 28922338/thread), 32 B keys,
    // 36+36 threads, read_percentage=0 with delete_ratio=1.0 (every mixed-
    // phase op deletes an existing key), 10-min measurement
    // (mixed_duration_secs=600) with the default 600 s pause, zipf {0, 2},
    // max_maps=128, metrics_enabled=true (the runs tracked the GC metric).
    //
    // The relocation-on cells use Some(Index { ratio: 1.0 }) with
    // relocation_max_reclaim_pct=20, exactly as the archived figure runs did
    // (not the WalBased strategy the churn experiments later compared).

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
    base.db_parameters.max_maps = 128; // as in the archived figure runs
    base.db_parameters.max_dirty_keys = DEFAULT_MAX_DIRTY_KEYS;
    base.db_parameters.num_flusher_threads = 12;
    base.db_parameters.metrics_enabled = true;
    base.db_parameters.direct_io = false;
    base.db_parameters.relocation_max_reclaim_pct = 20; // as in the archived figure runs

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
    // tab:recovery — recovery evaluation. Generates two phases:
    //
    //   Phase 1 (fill): write each target DB at a deterministic path under
    //     <DEFAULT_DB_DIR>/r6/<name>/. The path lives outside the orchestrator's
    //     `stress.*` cleanup pattern (orchestrator/src/protocol/target.rs),
    //     so the fills survive into Phase 2 batches. Each fill runs at the
    //     designated snapshot_written_bytes; mixed phase is skipped.
    //
    //   Phase 2 (measure): reopen each filled DB with `--measure-open`,
    //     emitting the recovery breakdown plus a 1,000-key read sample for
    //     time-to-first-read (first_read_samples=1000, matching the published
    //     runs — see logs/logs-revision-experiments/r6/paper).
    //
    // Three experiments are interleaved:
    //   (A) cold-start vs DB size: 100 GB / 500 GB / 1 TB at default snapshot
    //       cadence (128 GB), plus a second 1 TB replicate (cold-1tb-r2) so
    //       the series still fills exactly one 4-machine batch. (The
    //       historical mode had a 2 TB pair here; the paper's table has no
    //       2 TB row, so it was dropped in favor of the 1 TB replicate.)
    //   (B) recovery vs un-replayed WAL: 1 TB DB, snapshot_written_bytes ∈
    //       {16 GB, 64 GB, 256 GB, ∞}. The 1 TB at 128 GB snapshot is already
    //       covered by experiment (A) so it isn't repeated here.
    //   (C) crash during relocation: smaller DB (200 GB) with continuous
    //       WAL-based relocation; the fill process is killed mid-stream via
    //       --crash-after-secs (process::exit(137), bypassing Db::drop). The
    //       paired measure entry reopens, prints the recovery breakdown, and
    //       sample-verifies that surviving keys round-trip with the correct
    //       value (mismatches indicate recovery corruption). 4 replicates.

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

    // Each "series" emits its fill batch followed immediately by its
    // measure+clean batch. With N machines, this guarantees per-machine peak
    // disk = max single fill in the series — no two fills ever live on the
    // same machine simultaneously. (Without this ordering the orchestrator
    // would interleave a second fill batch onto each machine before any
    // cleanup ran, doubling the disk requirement.)
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

    // Series A: cold-start vs DB size (default snapshot cadence). The second
    // 1 TB replicate pads the series to exactly one 4-machine batch.
    let series_a: Vec<(String, u64, u64)> = vec![
        ("cold-100gb".into(), 100, SNAPSHOT_DEFAULT),
        ("cold-500gb".into(), 500, SNAPSHOT_DEFAULT),
        ("cold-1tb".into(), 1024, SNAPSHOT_DEFAULT),
        ("cold-1tb-r2".into(), 1024, SNAPSHOT_DEFAULT),
    ];
    emit_series(&mut items, &base, &series_a, FIRST_READ_SAMPLES);

    // Series B: 1 TB DB at varying snapshot intervals (the 1 TB at default
    // cadence is already covered by series A).
    let series_b: Vec<(String, u64, u64)> = vec![
        ("snap-16gb".into(), 1024, 16 * 1024 * 1024 * 1024),
        ("snap-64gb".into(), 1024, 64 * 1024 * 1024 * 1024),
        ("snap-256gb".into(), 1024, 256 * 1024 * 1024 * 1024),
        ("snap-inf".into(), 1024, SNAPSHOT_INFINITE),
    ];
    emit_series(&mut items, &base, &series_b, FIRST_READ_SAMPLES);

    // Series C: crash during relocation. The fill writes 200 GB (~4 min at
    // current throughput) and then the mixed phase keeps the workload running
    // alongside the relocation thread until --crash-after-secs fires. Without
    // a non-zero mixed phase, writes finish before the crash deadline and the
    // process exits cleanly — defeating the experiment.
    //
    // 4 replicates (not 3) so the crash fills exactly fill one batch on a
    // 4-machine testbed. With 3 replicates, the orchestrator packed a
    // measure entry into the same batch as the fills (round-robin assignment),
    // sending it to a machine that didn't hold the corresponding fill DB —
    // hard panic at open with NotFound.
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
    // Supplemental replicates for tab:recovery — extra rounds layered on top
    // of the May 14 re-runs. See the Mode docstring for the motivation. This
    // emits 32 entries total (8 batches of 4 on a 4-machine testbed), runtime
    // ~110 min:
    //
    //   Series B round 1 fill   (4)  ~20 min  — bound by 1 TB write
    //   Series B round 1 measure(4)  ~11 min  — bound by snap-256 measure
    //   Series B round 2 fill   (4)  ~20 min
    //   Series B round 2 measure(4)  ~11 min
    //   Series C batch 1 fill   (4)  ~14 min  — 200 GB write + crash@600s
    //   Series C batch 1 measure(4)  ~10 min
    //   Series C batch 2 fill   (4)  ~14 min
    //   Series C batch 2 measure(4)  ~10 min
    //
    // Same workload shape as `recovery`, including the 1,000-key first-read
    // sample on measures. The Series B paths
    // (<DEFAULT_DB_DIR>/r6/snap-{16,64,128,256}gb) are reused across rounds — the
    // measure entries set clean_after_measure=true, so each fill starts on a
    // clean slate. The round suffix (-r1/-r2) is only in `tldr`, which is
    // what we grep in the logs.

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

    // Mirrors `emit_series` in `generate_recovery` but appends a round
    // suffix to `tldr` so successive rounds against the same db_path are
    // separable in the orchestrator logs.
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

    // Series B supplemental: 2 rounds × 4 snapshot cadences. snap-128 fills
    // the 4th slot so each batch is exactly 4 (the orchestrator round-robins
    // into batches of 4 across the 4 testbed machines, and a partial batch
    // would pack measure entries onto machines that don't hold the fill —
    // see comment in generate_recovery's Series C). snap-128 was already
    // covered by Series A's cold-1tb runs, but the extra Series-B-labeled
    // replicates are cheap (~11 min measure) and make the data table tidier.
    let series_b: Vec<(String, u64, u64)> = vec![
        ("snap-16gb".into(), 1024, 16 * 1024 * 1024 * 1024),
        ("snap-64gb".into(), 1024, 64 * 1024 * 1024 * 1024),
        ("snap-128gb".into(), 1024, SNAPSHOT_DEFAULT),
        ("snap-256gb".into(), 1024, 256 * 1024 * 1024 * 1024),
    ];
    emit_series_round(&mut items, &base, &series_b, 1, FIRST_READ_SAMPLES);
    emit_series_round(&mut items, &base, &series_b, 2, FIRST_READ_SAMPLES);

    // Series C supplemental: 8 additional crash-during-relocation replicates,
    // numbered 5..=12 to extend (not collide with) the existing crash-relo-1
    // ... crash-relo-4 on disk. Matches generate_recovery's Series C shape
    // exactly: 200 GB write, continuous WAL relocation, 1200 s mixed phase,
    // process::exit(137) at +600 s. The numbering is contiguous so a single
    // pivot on `crash-relo-{1..=12}` covers all replicates downstream.
    const CRASH_FILL_GB: u64 = 200;
    const CRASH_AFTER_SECS: u64 = 600;
    const CRASH_MIXED_SECS: u64 = 1200;
    const CRASH_NEW_REPLICATES: std::ops::RangeInclusive<usize> = 5..=12;

    let writes_for_crash = writes_for_size_with_threads(
        CRASH_FILL_GB,
        base.stress_client_parameters.write_threads as u64,
        base.stress_client_parameters.write_size,
    );

    // Emit fills and measures in groups of 4 so each pair of batches
    // (fill batch → measure batch) is fully populated.
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
    // Series C with num_replay_threads = 1 — diagnostic. See the Mode
    // docstring for the hypothesis. This emits 8 fills + 8 measures in
    // 4 batches of 4 (~48 min wall clock on a 4-machine testbed). All
    // workload parameters match generate_recovery's Series C exactly
    // (200 GB write + 1200 s mixed phase + crash at +600 s); the only
    // intentional difference is db_parameters.num_replay_threads = 1.
    // Measures keep the diagnostic's 1,000,000 first-read samples (the
    // paper-mode `recovery` uses 1,000).
    //
    // The replicate names use a `crash-str1-N` prefix so the new fills land
    // at distinct on-disk paths from the existing crash-relo-{1..12} and
    // can coexist on the cluster if needed.

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
    // The whole point of this mode: force single-threaded WAL replay so we
    // can see whether the `hits=999` misses still occur. Affects only the
    // measure (recovery) path; the fills don't perform WAL replay.
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

    // Emit fills and measures in groups of 4 so each fill batch is exactly 4
    // and is immediately followed by its measure batch (same convention as
    // the other recovery modes — see comment in generate_recovery).
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
    // Series C relocation-guard diagnostic. See the Mode docstring for the
    // hypothesis. This emits 4 fills + 4 measures = 8 entries in 2 batches
    // (~24 min wall clock on a 4-machine testbed). The workload is identical
    // to generate_recovery_replicates' Series C: 200 GB write + 1200 s
    // mixed phase + crash at +600 s. Measures keep the diagnostic's
    // 1,000,000 first-read samples (the paper-mode `recovery` uses 1,000).
    //
    // Distinct on-disk path prefix `crash-guard-N` so this can coexist with
    // the existing crash-relo-* and crash-str1-* fills.
    //
    // The runtime difference comes from the tidehunter source patches:
    //   * db.rs:write_relocated_batch holds WalGuards through sync_flush.
    //   * relocation/mod.rs logs RELOCATION_SILENT_SKIP on read_record=None.
    // Both are committed in the workspace alongside this mode. To run this
    // diagnostic against an unpatched binary, the patches must be reverted
    // first.

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
    // tab:churn-strategy (Tidehunter rows) + tab:churn-threshold — foreground
    // WA and tail latency under sustained churn. Union of the former
    // r4-churn-smoke (4 cells) and r4-churn-full (9 cells) modes; tldr
    // strings are kept byte-identical to what those modes emitted so new
    // runs remain comparable with the archived logs.
    //
    // Each run does a 500 GB pre-fill (32 B keys, 1 KB values) followed by a
    // 60-minute pure-write mixed phase where every operation is either an
    // overwrite of an existing key, a delete of an existing key, or a fresh
    // insert. The `(overwrite_ratio, delete_ratio)` pair controls the mix;
    // wherever both ratios sum to 1.0 there are no fresh inserts, keeping
    // the working set bounded so relocation has something steady to chew on.
    //
    // E1 (3×3 strategy × workload matrix): strategies {None, WalBased,
    // IndexBased} × mixes {100% overwrite, 50/50 overwrite+delete,
    // 100% delete}.
    // E2 (threshold sweep on WalBased + 50/50): reclaim_pct {1, 10, 25, 50};
    // the 5% point is E1's WalBased+mixed cell (default reclaim_pct = 5).
    //
    // Pure-write mixed phase (`read_percentage = 0`) keeps the latency signal
    // focused on write tail under churn. Cells are ordered in batches of 4
    // for the 4-machine testbed, preserving the historical priority order:
    // the smoke batch first (WalBased row + None+mixed), then the None
    // corners + threshold extremes, then the IndexBased row + one threshold
    // point, then the last threshold point.

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
        // Batch 1 (the historical smoke batch): WalBased row + None+mixed.
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
    // BlobDB rows of tab:churn-strategy — cross-system churn comparison
    // against integrated BlobDB (closes the R1-D3 gap: "contrast these
    // results with garbage collection behavior in BlobDB / WiscKey-style
    // value-log designs"). Three cells matching the three workload corners
    // of the Tidehunter strategy×workload matrix (`churn`): 100% overwrite,
    // 50/50 overwrite+delete, 100% delete. Same workload shape — 500 GB
    // pre-fill, 60-min pure-write mixed phase, 32 B keys / 1 KB values,
    // 36+36 threads — so the new rows can be appended directly to
    // `tab:churn-strategy` in §6.2.5 for an apples-to-apples contrast.
    // `Backend::Blobdb` switches `RocksStorage::open` into integrated-BlobDB
    // mode (`enable_blob_files`, 256 B min blob size, 128 MB blob files,
    // ZSTD-compressed blobs); the BlobDB tunables are baked into rocks.rs
    // and the Tidehunter-specific `db_parameters` are ignored at runtime.
    // `relocation: None` because BlobDB owns its own GC (RocksDB compaction
    // drives blob-file cleanup). 3 cells fit one orchestrator batch on a
    // 4-machine testbed; estimated ~75 min wallclock end-to-end.

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
    // Epoch-based GC evaluation (tab:epoch-gc — commented out of the
    // published paper).
    //
    // Each run does a 50 GB pre-fill followed by a 60-minute pure-write mixed
    // phase, with continuous WAL-based relocation triggered every
    // `epoch_budget_bytes` of foreground writes. The byte-counting filter
    // (registered via `--epoch-budget-bytes`) returns `StopRelocation` once it
    // has seen the budget, modeling Sui's epoch-driven `apply_relocation_filter`
    // (sui/crates/sui-core/src/authority/authority_store_pruner.rs:968-993)
    // without an epoch-id-in-key encoding.
    //
    // The sweep covers two experiments simultaneously:
    //
    //   E1 — Per-pass budget sweep: budget ∈ {25 GB, 50 GB, 100 GB}, mode=Stop.
    //   E2 — `StopRelocation` ablation: budget=50 GB, mode={Stop, Keep}.
    //        E2.a (Stop) is the same as E1's 50 GB run, so the ablation only
    //        adds the Keep variant. With Keep, Phase A scans the entire WAL
    //        each pass (no short-circuit), isolating the read-I/O saving the
    //        StopRelocation mechanism provides.
    //
    // Pure-write mixed phase (`read_percentage = 0`) avoids tangling
    // foreground latency measurements with read-of-deleted-key semantics:
    // the byte-budgeted filter is, by design, dropping older keys from the
    // index as their WAL bytes are bulk-reclaimed.

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
    // Pure-write mixed phase (see comment above).
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
    // tab:r3-sweeps, Bloom-FPR rows.
    //
    // Workload (held fixed across the sweep):
    //   * 1 KB values, 32 B keys, 1 TB pre-fill, 100% reads, uniform (θ=0).
    //   * 30-min mixed phase to match the value-scaling / cache-sweep cadence.
    //
    // The Bloom filter is sized for the actual fill: with default num_mutexes
    // (131072 cells) and a 1 TB pre-fill at (32+1024)-byte entries the keys-
    // per-cell figure is ~7.2K, so bloom_filter_count = 8192 (next power of
    // two up) yields close-to-target FPR at every rate. This matches the
    // count used in the existing r3 FPR=0.01 runs, so the new sweep is
    // directly comparable to that prior data point.
    //
    // FPR axis: 0.1%, 1%, 5%, 10% (4 points × 2 replicates = 8 runs).
    // Why 2 replicates: matches recent practice (cache sweep, value-scaling
    // redo) and gives a noise bound at each FPR without doubling wall clock.

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

/// Base config for the tab:r3-sweeps headline cell: 50/50 mixed Get, 1 KB
/// values, 32 B keys, θ=0, 1 TiB pre-fill, 30-min measured phase, default
/// 600 s pause between phases — exactly the shape of the archived r3 sweep
/// runs (logs/logs-revision-experiments/r3).
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
    // tab:r3-sweeps, mmap-window rows: max_maps {16, 32, 64, 128} on the
    // headline 50/50 cell. With 1 GiB fragments and the budget applied to
    // both the Value WAL and the Index Store, max_maps=N maps 2N GiB total,
    // so the sweep spans the table's 32–256 GiB window axis (128 GiB total =
    // max_maps 64 is the paper default). 4 replicates per point, copied from
    // the archived sweep (logs/logs-revision-experiments/r3 has 4 runs each
    // at max_maps 16/32/64 and 4+ at 128). Cells are emitted replicate-major
    // so each 4-machine batch runs one replicate of all 4 points.

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
    // tab:r3-sweeps, cell-count rows: total Large Table cells {2^14, 2^16,
    // 2^17, 2^19, 2^20} on the headline 50/50 cell. The archived r3 runs
    // varied this via `num_mutexes: Some(total)` with `cells_per_mutex`
    // unset (the benchmark defaults cells_per_mutex to 1, so total cells =
    // num_mutexes), at max_maps=128 — replicated exactly here, single
    // replicate per point as in the logs.

    let cell_counts: [usize; 5] = [
        1 << 14, // 16384
        1 << 16, // 65536
        1 << 17, // 131072 (default)
        1 << 19, // 524288
        1 << 20, // 1048576
    ];

    let mut base = r3_sweep_base();
    base.db_parameters.max_maps = 128; // as in the archived cell-count sweep runs

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
    // tab:r3-sweeps, dirty-key rows: max_dirty_keys {64, 256, 1024, 4096,
    // 16384} on the headline 50/50 cell at max_maps=128, matching the
    // archived r3 sweep (single replicate per point; the 1024 default point
    // also exists there as the plain max_maps=128 runs).

    let dirty_key_caps: [usize; 5] = [64, 256, 1024, 4096, 16384];

    let mut base = r3_sweep_base();
    base.db_parameters.max_maps = 128; // as in the archived dirty-key sweep runs

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
    // tab:memory-runtime — 4 replicates with `metrics_enabled: true` so the
    // per-keyspace runtime gauges (lookup_result by source, flush/unload
    // counters, dirty_keys, loaded_key_bytes, flat_index_bytes) are exported
    // to Prometheus and survive into Grafana. Everything else matches the
    // value-scaling redo baseline so the resulting hit-ratio + eviction-rate
    // numbers can be quoted alongside the existing throughput data.
    //
    // Workload (identical across all 4 replicates):
    //   * 1 KB values, 32 B keys, 1 TB pre-fill.
    //   * 50/50 mixed Get/Put, uniform (θ=0), 30-min mixed phase.
    //   * No bloom filter, no value LRU — characterizes the configuration
    //     the paper actually evaluates rather than a hypothetical tuned one.

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
    // Tidehunter cells of the main benchmark figures
    // (fig:benchmark-results-1k/-64b/-128b) at the 64 × 1 GB mmap budget.
    //
    // Generated here (42 cells = 3 value sizes × 2 skews × 7 configs):
    //   1. write-only          (read_percentage=0,   read_mode=Get [ignored])
    //   2. 50/50 Get           (read_percentage=50,  read_mode=Get)
    //   3. 50/50 Exists        (read_percentage=50,  read_mode=Exists)
    //   4. 50/50 ReverseIter   (read_percentage=50,  read_mode=Lt(1))
    //   5. 100% Get            (read_percentage=100, read_mode=Get)
    //   6. 100% Exists         (read_percentage=100, read_mode=Exists)
    //   7. 100% ReverseIter    (read_percentage=100, read_mode=Lt(1))
    //
    // Single replicate. The 50/50 Get cells use the same parameters as
    // `value-scaling`'s {64, 128, 1024} B points; the mode is self-contained
    // despite the overlap. The relocation figure, app_workloads, and
    // stability table have their own modes (`relocation`, `app-workloads`,
    // `stability`).
    //
    // Workload shape matches the paper's main benchmark figures: 1 TB pre-fill,
    // 32 B keys, 30-min measurement phase, max_maps=64, frag_size=1 GB,
    // no bloom.

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
