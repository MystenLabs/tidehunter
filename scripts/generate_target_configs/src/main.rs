use anyhow::Result;
use benchmark::configs::{
    Backend, EpochFilterMode, KeyLayout, ReadMode, RelocationConfig, StressTestConfigs,
};
use clap::{Parser, Subcommand};
use std::fs;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "generate_target_configs")]
struct Args {
    #[command(subcommand)]
    mode: Mode,
}

#[derive(Subcommand, Debug)]
enum Mode {
    /// Original value-scaling sweep (default behavior).
    ValueScaling,
    /// R6 revision experiments: recovery cold-start + snapshot interval sweep.
    R6Recovery,
    /// R6 series A + B only — re-run after the `mixed_duration_secs=0` fix
    /// landed in `81bd854`. Series A (cold-start vs DB size) and Series B
    /// (snapshot interval sweep) originally ran on May 6 against the
    /// pre-fix code and the fills panicked at the mixed-phase boundary;
    /// this mode regenerates just those 16 cells so the measure runs see
    /// a cleanly-shutdown post-fill state. Series C is excluded because
    /// it already ran post-fix.
    R6RecoveryAb,
    /// R2-D6 revision experiment: epoch-based GC evaluation. Generates a
    /// budget sweep (E1) plus the StopRelocation ablation (E2). E2.a is
    /// identical to E1's middle run, so this emits 4 unique configs.
    R2D6EpochGc,
    /// R4 smoke run: foreground write amplification and tail latency under
    /// sustained churn. 4 configs sized to fill one batch on a 4-machine
    /// testbed; the full 9-run matrix follows once the smoke numbers confirm
    /// the presentation guardrails hold.
    R4ChurnSmoke,
    /// R4 full matrix (post-smoke): 5 remaining E1 cells (None + IndexBased
    /// rows) plus the 4 new E2 threshold-sweep cells on WalBased+50/50.
    /// The 4 smoke cells (3 WalBased corners + None+mixed) and the smoke's
    /// implicit WalBased+50/50+reclaim=5% E2 point are not re-emitted here.
    R4ChurnFull,
    /// R6 supplemental replicates — added after the May 14 re-runs showed
    /// (a) the Series B trend now looks nearly monotonic (was "not strictly
    /// monotonic" in the paper), and (b) Series C variability blew up from
    /// σ<4% in the paper to ~33% across 4 replicates. Both observations need
    /// more data before the paper/letter narrative can be rewritten with
    /// confidence. This mode emits:
    ///   * Series B: 2 additional rounds at snap ∈ {16, 64, 128, 256} GB
    ///     (skip ∞: already have 1 rep and the trend at that point is clear;
    ///     a second ∞ run alone would add ~36 min to a batch). snap-128 is
    ///     in the sweep so each batch has exactly 4 cells — it's also useful
    ///     to have Series-B-labeled replicates of 128 alongside Series A's
    ///     cold-1tb runs.
    ///   * Series C: 8 additional replicates (crash-relo-5 ... crash-relo-12)
    ///     in two batches of 4, bringing the total to 12.
    ///
    /// `-r1`/`-r2` tldr suffixes keep round-1 and round-2 fills separable in
    /// the logs without colliding with the existing R6 names on disk.
    R6RecoverySupplemental,
    /// R6 Series C with single-threaded WAL replay — diagnostic experiment
    /// for the `hits=999` misses observed in 2/12 May 14/15 Series C runs.
    /// Hypothesis: the parallel WAL replay added in commit `2fcb226` (which
    /// fans entries across `num_replay_threads` workers keyed by cell) has a
    /// race that occasionally drops an entry. The original paper Series C
    /// runs (pre-parallel-replay) had hits=1000 in 12/12. This mode forces
    /// `num_replay_threads = 1` to fall back on the single-threaded replay
    /// path while holding everything else fixed — same workload, same crash
    /// timing, same crash sampler seed. 8 replicates (2 batches of 4) is
    /// enough for a first signal: if 0/8 show misses, parallel replay is the
    /// likely culprit (and a real bug, not just a paper issue); if any do,
    /// the misses are coming from somewhere else (unload, relocation, or the
    /// durability model itself) and we follow up.
    R6RecoveryCrashSingleThreadReplay,
    /// R6 Series C with the relocation-guard + silent-skip diagnostic patches
    /// applied to tidehunter (see investigation notes from May 15). Tests two
    /// candidate explanations for the missing-key bug at once:
    ///   1. Patch in `db.rs::write_relocated_batch` holds the relocated WAL
    ///      guards through `sync_flush_for_relocation`, restoring the
    ///      WalTracker invariant (guard lives until in-memory index is
    ///      updated). The audit identified this as the most plausible
    ///      structural cause. If misses disappear, the invariant break is
    ///      causal.
    ///   2. Patch in `relocation/mod.rs::wal_based_relocation` logs
    ///      `RELOCATION_SILENT_SKIP` when `read_record` returns None for an
    ///      entry below `target_position`. This catches the silent path
    ///      where relocation drops a key whose WAL bytes were GC'd between
    ///      `get_index_for_cell` and the read.
    ///
    /// 4 replicates (one batch) is enough: the existing data shows ~40%
    /// miss rate in long-cluster runs, so 4 reps yields ~1 long-cluster
    /// run with misses expected. If the guard-holding patch is correct,
    /// 0/4 misses is decisive (P=0.6^2 ≈ 0.36 under null).
    R6RecoveryCrashRelocationGuardDiagnostic,
}

const KEY_LEN: usize = 32;
const FRAG_SIZE: u64 = 1024 * 1024 * 1024; // 1 GB
const MAX_MAPS: usize = 64;
const DEFAULT_MAX_DIRTY_KEYS: usize = 1024;

fn main() -> Result<()> {
    let args = Args::parse();
    match args.mode {
        Mode::ValueScaling => generate_value_scaling(),
        Mode::R6Recovery => generate_r6_recovery(),
        Mode::R6RecoveryAb => generate_r6_recovery_ab(),
        Mode::R2D6EpochGc => generate_r2d6_epoch_gc(),
        Mode::R4ChurnSmoke => generate_r4_churn_smoke(),
        Mode::R4ChurnFull => generate_r4_churn_full(),
        Mode::R6RecoverySupplemental => generate_r6_recovery_supplemental(),
        Mode::R6RecoveryCrashSingleThreadReplay => {
            generate_r6_recovery_crash_single_thread_replay()
        }
        Mode::R6RecoveryCrashRelocationGuardDiagnostic => {
            generate_r6_recovery_crash_relocation_guard_diagnostic()
        }
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
    base_item.stress_client_parameters.path = Some("/opt/sui/db/".to_string());
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

    for _replicate in 0..REPLICATES {
        for &value_size in &value_sizes {
            for &zipf_exponent in &zipf_exponents {
                let mut item = base_item.clone();
                item.stress_client_parameters.write_size = value_size;
                item.stress_client_parameters.zipf_exponent = zipf_exponent;
                item.stress_client_parameters.writes = DB_SIZE_BYTES
                    / (item.stress_client_parameters.write_threads
                        * (item.stress_client_parameters.key_len
                            + item.stress_client_parameters.write_size));
                items.push(item);
            }
        }
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_r6_recovery() -> Result<()> {
    // R6 — recovery evaluation. Generates two phases:
    //
    //   Phase 1 (fill): write each target DB at a deterministic path under
    //     /opt/sui/db/r6/<name>/. The path lives outside the orchestrator's
    //     `stress.*` cleanup pattern (orchestrator/src/protocol/target.rs),
    //     so the fills survive into Phase 2 batches. Each fill runs at the
    //     designated snapshot_written_bytes; mixed phase is skipped.
    //
    //   Phase 2 (measure): reopen each filled DB with `--measure-open`,
    //     emitting the recovery breakdown plus a small read-sample for
    //     time-to-first-read.
    //
    // Three experiments are interleaved:
    //   (A) cold-start vs DB size: 100 GB / 500 GB / 1 TB / 2 TB at default
    //       snapshot cadence (128 GB).
    //   (B) recovery vs un-replayed WAL: 1 TB DB, snapshot_written_bytes ∈
    //       {16 GB, 64 GB, 256 GB, ∞}. The 1 TB at 128 GB snapshot is already
    //       covered by experiment (A) so it isn't repeated here.
    //   (C) crash during relocation: smaller DB (200 GB) with continuous
    //       WAL-based relocation; the fill process is killed mid-stream via
    //       --crash-after-secs (process::exit(137), bypassing Db::drop). The
    //       paired measure entry reopens, prints the recovery breakdown, and
    //       sample-verifies that surviving keys round-trip with the correct
    //       value (mismatches indicate recovery corruption). 3 replicates.

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
            fill.stress_client_parameters.db_path = Some(format!("/opt/sui/db/r6/{name}"));
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
            measure.stress_client_parameters.first_read_samples = FIRST_READ_SAMPLES;
            measure.stress_client_parameters.clean_after_measure = true;
            measure.stress_client_parameters.mixed_duration_secs = 0;
            measure.stress_client_parameters.pause_between_phases_secs = 0;
            measure.stress_client_parameters.reuse = Some(format!("/opt/sui/db/r6/{name}"));
            measure.stress_client_parameters.tldr = format!("r6-measure-{name}");
            items.push(measure);
        }
    }

    let mut items: Vec<StressTestConfigs> = Vec::new();

    // Series A: cold-start vs DB size (default snapshot cadence).
    let series_a: Vec<(String, u64, u64)> = vec![
        ("cold-100gb".into(), 100, SNAPSHOT_DEFAULT),
        ("cold-500gb".into(), 500, SNAPSHOT_DEFAULT),
        ("cold-1tb".into(), 1024, SNAPSHOT_DEFAULT),
        ("cold-2tb".into(), 2048, SNAPSHOT_DEFAULT),
    ];
    emit_series(&mut items, &base, &series_a);

    // Series B: 1 TB DB at varying snapshot intervals (the 1 TB at default
    // cadence is already covered by series A).
    let series_b: Vec<(String, u64, u64)> = vec![
        ("snap-16gb".into(), 1024, 16 * 1024 * 1024 * 1024),
        ("snap-64gb".into(), 1024, 64 * 1024 * 1024 * 1024),
        ("snap-256gb".into(), 1024, 256 * 1024 * 1024 * 1024),
        ("snap-inf".into(), 1024, SNAPSHOT_INFINITE),
    ];
    emit_series(&mut items, &base, &series_b);

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
        fill.stress_client_parameters.db_path = Some(format!("/opt/sui/db/r6/{name}"));
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
        measure.stress_client_parameters.reuse = Some(format!("/opt/sui/db/r6/{name}"));
        measure.stress_client_parameters.tldr = format!("r6-measure-crash-{replicate}");
        items.push(measure);
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_r6_recovery_ab() -> Result<()> {
    // R6 series A + B only, regenerated after the `mixed_duration_secs=0`
    // panic fix landed in `81bd854` (May 7 15:10 UTC). The original Series A
    // and B fills (May 6) hit that bug at the mixed-phase boundary; the
    // measure runs then opened post-panic DBs. The fills' write phase had
    // completed normally so recovery still succeeded, but the methodology
    // is cleaner if we re-run with the fix. Series C already ran post-fix
    // (May 7) so it isn't re-emitted here.

    const VALUE_SIZE: usize = 1024;
    const SNAPSHOT_DEFAULT: u64 = 128 * 1024 * 1024 * 1024;
    const FIRST_READ_SAMPLES: usize = 1000;
    const SNAPSHOT_INFINITE: u64 = 16 * 1024 * 1024 * 1024 * 1024;

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

    fn emit_series(
        items: &mut Vec<StressTestConfigs>,
        base: &StressTestConfigs,
        runs: &[(String, u64, u64)],
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
            fill.stress_client_parameters.db_path = Some(format!("/opt/sui/db/r6/{name}"));
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
            measure.stress_client_parameters.first_read_samples = FIRST_READ_SAMPLES;
            measure.stress_client_parameters.clean_after_measure = true;
            measure.stress_client_parameters.mixed_duration_secs = 0;
            measure.stress_client_parameters.pause_between_phases_secs = 0;
            measure.stress_client_parameters.reuse = Some(format!("/opt/sui/db/r6/{name}"));
            measure.stress_client_parameters.tldr = format!("r6-measure-{name}");
            items.push(measure);
        }
    }

    let mut items: Vec<StressTestConfigs> = Vec::new();

    let series_a: Vec<(String, u64, u64)> = vec![
        ("cold-100gb".into(), 100, SNAPSHOT_DEFAULT),
        ("cold-500gb".into(), 500, SNAPSHOT_DEFAULT),
        ("cold-1tb".into(), 1024, SNAPSHOT_DEFAULT),
        ("cold-2tb".into(), 2048, SNAPSHOT_DEFAULT),
    ];
    emit_series(&mut items, &base, &series_a);

    let series_b: Vec<(String, u64, u64)> = vec![
        ("snap-16gb".into(), 1024, 16 * 1024 * 1024 * 1024),
        ("snap-64gb".into(), 1024, 64 * 1024 * 1024 * 1024),
        ("snap-256gb".into(), 1024, 256 * 1024 * 1024 * 1024),
        ("snap-inf".into(), 1024, SNAPSHOT_INFINITE),
    ];
    emit_series(&mut items, &base, &series_b);

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_r6_recovery_supplemental() -> Result<()> {
    // R6 supplemental — extra replicates layered on top of the May 14 re-runs.
    // See the Mode docstring for the motivation. This emits 32 entries total
    // (8 batches of 4 on a 4-machine testbed), runtime ~110 min:
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
    // Same workload shape as `R6Recovery`. The Series B paths
    // (/opt/sui/db/r6/snap-{16,64,128,256}gb) are reused across rounds — the
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

    // Mirrors `emit_series` in `generate_r6_recovery` but appends a round
    // suffix to `tldr` so successive rounds against the same db_path are
    // separable in the orchestrator logs.
    fn emit_series_round(
        items: &mut Vec<StressTestConfigs>,
        base: &StressTestConfigs,
        runs: &[(String, u64, u64)],
        round: usize,
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
            fill.stress_client_parameters.db_path = Some(format!("/opt/sui/db/r6/{name}"));
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
            measure.stress_client_parameters.first_read_samples = FIRST_READ_SAMPLES;
            measure.stress_client_parameters.clean_after_measure = true;
            measure.stress_client_parameters.mixed_duration_secs = 0;
            measure.stress_client_parameters.pause_between_phases_secs = 0;
            measure.stress_client_parameters.reuse = Some(format!("/opt/sui/db/r6/{name}"));
            measure.stress_client_parameters.tldr = format!("r6-measure-{name}-r{round}");
            items.push(measure);
        }
    }

    let mut items: Vec<StressTestConfigs> = Vec::new();

    // Series B supplemental: 2 rounds × 4 snapshot cadences. snap-128 fills
    // the 4th slot so each batch is exactly 4 (the orchestrator round-robins
    // into batches of 4 across the 4 testbed machines, and a partial batch
    // would pack measure entries onto machines that don't hold the fill —
    // see comment in generate_r6_recovery's Series C). snap-128 was already
    // covered by Series A's cold-1tb runs, but the extra Series-B-labeled
    // replicates are cheap (~11 min measure) and make the data table tidier.
    let series_b: Vec<(String, u64, u64)> = vec![
        ("snap-16gb".into(), 1024, 16 * 1024 * 1024 * 1024),
        ("snap-64gb".into(), 1024, 64 * 1024 * 1024 * 1024),
        ("snap-128gb".into(), 1024, SNAPSHOT_DEFAULT),
        ("snap-256gb".into(), 1024, 256 * 1024 * 1024 * 1024),
    ];
    emit_series_round(&mut items, &base, &series_b, 1);
    emit_series_round(&mut items, &base, &series_b, 2);

    // Series C supplemental: 8 additional crash-during-relocation replicates,
    // numbered 5..=12 to extend (not collide with) the existing crash-relo-1
    // ... crash-relo-4 on disk. Matches generate_r6_recovery's Series C shape
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
            fill.stress_client_parameters.db_path = Some(format!("/opt/sui/db/r6/{name}"));
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
            measure.stress_client_parameters.reuse = Some(format!("/opt/sui/db/r6/{name}"));
            measure.stress_client_parameters.tldr = format!("r6-measure-crash-{replicate}");
            items.push(measure);
        }
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_r6_recovery_crash_single_thread_replay() -> Result<()> {
    // R6 Series C with num_replay_threads = 1 — diagnostic. See the Mode
    // docstring for the hypothesis. This emits 8 fills + 8 measures in
    // 4 batches of 4 (~48 min wall clock on a 4-machine testbed). All
    // workload parameters match generate_r6_recovery's Series C exactly
    // (200 GB write + 1200 s mixed phase + crash at +600 s); the only
    // intentional difference is db_parameters.num_replay_threads = 1.
    //
    // The replicate names use a `crash-str1-N` prefix so the new fills land
    // at distinct on-disk paths from the existing crash-relo-{1..12} and
    // can coexist on the cluster if needed.

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
    // the other R6 modes — see comment in generate_r6_recovery).
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
            fill.stress_client_parameters.db_path = Some(format!("/opt/sui/db/r6/{name}"));
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
            measure.stress_client_parameters.reuse = Some(format!("/opt/sui/db/r6/{name}"));
            measure.stress_client_parameters.tldr = format!("r6-measure-{name}");
            items.push(measure);
        }
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_r6_recovery_crash_relocation_guard_diagnostic() -> Result<()> {
    // R6 Series C relocation-guard diagnostic. See the Mode docstring for the
    // hypothesis. This emits 4 fills + 4 measures = 8 entries in 2 batches
    // (~24 min wall clock on a 4-machine testbed). The workload is identical
    // to generate_r6_recovery_supplemental's Series C: 200 GB write + 1200 s
    // mixed phase + crash at +600 s.
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
        fill.stress_client_parameters.db_path = Some(format!("/opt/sui/db/r6/{name}"));
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
        measure.stress_client_parameters.reuse = Some(format!("/opt/sui/db/r6/{name}"));
        measure.stress_client_parameters.tldr = format!("r6-measure-{name}");
        items.push(measure);
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_r4_churn_smoke() -> Result<()> {
    // R4 smoke — foreground WA and tail latency under sustained churn.
    //
    // 4 configs sized to fill one batch on a 4-machine testbed. Each run does
    // a 500 GB pre-fill (32 B keys, 1 KB values) followed by a 60-minute
    // pure-write mixed phase where every operation is either an overwrite of
    // an existing key, a delete of an existing key, or a fresh insert. The
    // `(overwrite_ratio, delete_ratio)` pair controls the mix; for the three
    // WalBased cells both ratios sum to 1.0 (no fresh inserts), keeping the
    // working set bounded so relocation has something steady to chew on.
    //
    // Cells:
    //   1. WalBased + 100% overwrite — adversarial case the reviewer flagged
    //      (constant working set, every overwrite generates a dead byte for
    //      relocation to either rewrite or skip).
    //   2. WalBased + 50/50 overwrite+delete — realistic case; also the cell
    //      R4-E2 sweeps `relocation_max_reclaim_pct` over.
    //   3. WalBased + 100% delete — easy case; live set shrinks over time,
    //      relocation should reclaim cheaply with little live-data rewriting.
    //   4. None + 50/50 overwrite+delete — no-relocation baseline; pairs with
    //      cell 2 for the headline "is relocation worth it on realistic
    //      churn?" comparison the strategy doc wants to lead with.
    //
    // Pure-write mixed phase (`read_percentage = 0`) keeps the latency signal
    // focused on write tail under churn — exactly what R4 asks for. Reads in
    // the presence of mass deletes have their own semantics that would
    // confuse the tail-latency plot.

    const VALUE_SIZE: usize = 1024;
    const FILL_GB: u64 = 500;
    const MIXED_DURATION_SECS: u64 = 3600; // 60 minutes
    const SNAPSHOT_DEFAULT: u64 = 128 * 1024 * 1024 * 1024; // 128 GB

    // (label, relocation, overwrite_ratio, delete_ratio)
    let runs: [(&str, Option<RelocationConfig>, f64, f64); 4] = [
        ("walbased-overwrite", Some(RelocationConfig::Wal), 1.0, 0.0),
        ("walbased-mixed", Some(RelocationConfig::Wal), 0.5, 0.5),
        ("walbased-delete", Some(RelocationConfig::Wal), 0.0, 1.0),
        ("none-mixed", None, 0.5, 0.5),
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
    base.stress_client_parameters.path = Some("/opt/sui/db/".to_string());
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
    for (label, relocation, overwrite_ratio, delete_ratio) in &runs {
        let mut item = base.clone();
        item.stress_client_parameters.relocation = relocation.clone();
        item.stress_client_parameters.overwrite_ratio = *overwrite_ratio;
        item.stress_client_parameters.delete_ratio = *delete_ratio;
        item.stress_client_parameters.tldr = format!("r4-smoke-{label}");
        items.push(item);
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn generate_r4_churn_full() -> Result<()> {
    // R4 full matrix — runs the 9 cells not already covered by R4ChurnSmoke.
    //
    // E1 (3×3 strategy × workload matrix): smoke covered the WalBased row
    // (overwrite / mixed / delete) plus None+mixed. The 5 remaining cells
    // are the None corners (overwrite, delete) and the full IndexBased row.
    //
    // E2 (threshold sweep on WalBased + 50/50 overwrite+delete): values
    // 1%, 10%, 25%, 50%. The 5% point is already covered by the smoke's
    // WalBased+mixed run (default reclaim_pct = 5).
    //
    // Cells are ordered so the highest-priority data lands in the first
    // 4-machine batch: completing the None row of E1 (which together with
    // the smoke's WalBased row gives the headline "None vs. WalBased"
    // contrast) plus the two threshold extremes (1% and 50%) which bound
    // the E2 curve. Batch 2 fills out IndexBased; batch 3 is the last
    // threshold point.
    //
    // Same workload shape as the smoke: 500 GB pre-fill, 60-min pure-write
    // mixed phase, 36+36 threads, 32 B keys, 1 KB values, default snapshot.

    const VALUE_SIZE: usize = 1024;
    const FILL_GB: u64 = 500;
    const MIXED_DURATION_SECS: u64 = 3600;
    const SNAPSHOT_DEFAULT: u64 = 128 * 1024 * 1024 * 1024;

    struct Cell {
        label: &'static str,
        relocation: Option<RelocationConfig>,
        overwrite_ratio: f64,
        delete_ratio: f64,
        reclaim_pct: Option<u8>,
    }
    let wal = || Some(RelocationConfig::Wal);
    let index = || Some(RelocationConfig::Index { ratio: None });
    let runs: [Cell; 9] = [
        // Batch 1: complete the None row + threshold extremes.
        Cell {
            label: "none-overwrite",
            relocation: None,
            overwrite_ratio: 1.0,
            delete_ratio: 0.0,
            reclaim_pct: None,
        },
        Cell {
            label: "none-delete",
            relocation: None,
            overwrite_ratio: 0.0,
            delete_ratio: 1.0,
            reclaim_pct: None,
        },
        Cell {
            label: "e2-walbased-reclaim1",
            relocation: wal(),
            overwrite_ratio: 0.5,
            delete_ratio: 0.5,
            reclaim_pct: Some(1),
        },
        Cell {
            label: "e2-walbased-reclaim50",
            relocation: wal(),
            overwrite_ratio: 0.5,
            delete_ratio: 0.5,
            reclaim_pct: Some(50),
        },
        // Batch 2: complete the IndexBased row + one more threshold point.
        Cell {
            label: "indexbased-overwrite",
            relocation: index(),
            overwrite_ratio: 1.0,
            delete_ratio: 0.0,
            reclaim_pct: None,
        },
        Cell {
            label: "indexbased-mixed",
            relocation: index(),
            overwrite_ratio: 0.5,
            delete_ratio: 0.5,
            reclaim_pct: None,
        },
        Cell {
            label: "indexbased-delete",
            relocation: index(),
            overwrite_ratio: 0.0,
            delete_ratio: 1.0,
            reclaim_pct: None,
        },
        Cell {
            label: "e2-walbased-reclaim10",
            relocation: wal(),
            overwrite_ratio: 0.5,
            delete_ratio: 0.5,
            reclaim_pct: Some(10),
        },
        // Batch 3: last threshold point.
        Cell {
            label: "e2-walbased-reclaim25",
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
    base.stress_client_parameters.path = Some("/opt/sui/db/".to_string());
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
        item.stress_client_parameters.tldr = format!("r4-full-{}", cell.label);
        items.push(item);
    }

    write_configs(&items, "orchestrator/assets/target_configs.yml")
}

fn writes_for_size_with_threads(size_gb: u64, write_threads: u64, write_size: usize) -> usize {
    let bytes_per_write = (KEY_LEN + write_size) as u64;
    ((size_gb * 1024 * 1024 * 1024) / (bytes_per_write * write_threads)) as usize
}

fn generate_r2d6_epoch_gc() -> Result<()> {
    // R2-D6 — epoch-based GC evaluation.
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
    base.stress_client_parameters.path = Some("/opt/sui/db/".to_string());
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
