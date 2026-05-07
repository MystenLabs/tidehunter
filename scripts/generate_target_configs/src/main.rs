use anyhow::Result;
use benchmark::configs::{Backend, KeyLayout, ReadMode, RelocationConfig, StressTestConfigs};
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

fn writes_for_size_with_threads(size_gb: u64, write_threads: u64, write_size: usize) -> usize {
    let bytes_per_write = (KEY_LEN + write_size) as u64;
    ((size_gb * 1024 * 1024 * 1024) / (bytes_per_write * write_threads)) as usize
}
