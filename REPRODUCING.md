# Reproducing the experiments in the Tidehunter paper

This document explains how to reproduce the experiments in *"Tidehunter:
Large-Value Storage With Minimal Data Relocation"* (PVLDB). All experiment
tooling lives in this repository; the configurations used for the paper are
on the
[`paper-experiments`](https://github.com/MystenLabs/tidehunter/tree/paper-experiments)
branch, so check it out before following the steps below.

Experiments are driven by the `orchestrator` crate: you generate the
experiment configurations and launch; the orchestrator SSHes into the
benchmark machine (even when it is the machine you are sitting at), builds
the code, runs the experiment cells one at a time, and downloads the logs.
Every experiment is single-node, so **one machine is enough**. This document
describes that basic single-machine setup; running on several machines in
parallel, cloud machines, and monitoring are covered in
"Advanced setups" at the end.

## Requirements

* A Linux machine with sudo access. We tested on Ubuntu; other
  distributions should work, but the setup script will need small
  adaptations.
* At least 2.5 TB of fast SSD storage, ideally NVMe.
* A bare-metal machine is recommended over a cloud instance.

## First-time setup

1. Clone this repository and check out the `paper-experiments` branch.
2. Run the setup script: `./scripts/setup_local.sh`. It installs the build
   dependencies and the Rust toolchain, and configures ssh so the
   orchestrator can drive your machine (it creates a dedicated key in
   `~/.ssh/tidehunter_local`). The script is written for Ubuntu/Debian; on
   other distributions, install the packages it lists by hand.
3. Load the environment changes the script made:
   `source ~/.bashrc && source ~/.cargo/env`.
4. Copy `orchestrator/assets/settings-local.yml` to
   `orchestrator/assets/settings.yml` and set `working_dir` to a directory
   on your fast disk. This is where the orchestrator clones the repo and
   stores the database files during experiments. The rest of the file works
   as-is for any user.
5. (Optional but recommended) Set the CPU governor to `performance`:
   `echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor`.
   This does not survive reboots.

Now you are ready to run experiments!

## Pipeline at a glance

```bash
# Generate the experiment configurations (writes orchestrator/assets/target_configs.yml)
cargo run -p generate_target_configs -- <mode>

# Run them; the orchestrator builds on the machines, runs each cell, and
# downloads the logs to logs/logs-<branch>/
cargo run --bin orchestrator -- benchmark

# Extract results from the logs (see "Extracting results")
```

The workload driver is the `benchmarks/benchmark` stress client (in this
repo). It supports three backends (`Tidehunter`, `Rocksdb`, and `Blobdb`,
i.e. RocksDB with integrated BlobDB), so all cross-system comparisons use
the same driver, key generation, and measurement code.

## Experiment configurations

`orchestrator/assets/target_configs.yml` is a list of experiment cells.
Each cell contains:

* `db_parameters`: the Tidehunter `Config` (fragment size, `max_maps`,
  snapshot cadence, relocation reclaim threshold, etc.). Ignored at
  runtime by the RocksDB/BlobDB backends.
* `stress_client_parameters`: the workload (backend, threads, number of
  writes, value size, read percentage and mode, Zipf exponent,
  overwrite/delete ratios, measurement duration, relocation, crash
  injection, ...) plus a free-form `tldr:` string that names the run and
  is echoed into the log.

Generate the file with `cargo run -p generate_target_configs -- <mode>`
(see the mapping table below). You can also edit the YAML by hand for
one-off runs; note the parser silently ignores unknown keys, so
double-check spelling against the structs in
`benchmarks/benchmark/src/configs.rs` and `tidehunter/src/config.rs` (a
typo means the default value is used, with no error).

## Running experiments

1. Generate the configs for the experiment you want (see the mapping table
   below): `cargo run -p generate_target_configs -- <mode>`.
2. Launch:

   ```bash
   cargo run --bin orchestrator -- benchmark
   ```

   (`testbed status` and friends inspect and manage the machines.)
3. Logs land in `logs/logs-<branch>/node-<timestamp>-<node_index>.log`, one
   per cell; see "Extracting results".

### Cell ordering and the recovery experiments

The orchestrator runs the cells in `target_configs.yml` in order. Most
cells are independent, but the recovery experiments come in pairs: a fill
cell creates a database that survives its run (`db_path:` plus
`preserve: true`, or a scheduled crash), and a later measure cell re-opens
it via `reuse:`. On a single machine the pairing works out of the box,
since everything runs on the same disk. Two things to watch:

* The generator lays the recovery cells out for a four-machine testbed
  (see "Advanced setups"), so on one machine up to four preserved fill
  databases exist at once; check your disk space.
* Remove leftover databases between reruns (`scripts/r6_cleanup.sh`; pass
  `--db-dir` if your `working_dir` is not `/opt/sui/db`).

### Running a single cell by hand

For quick one-off runs you can bypass the orchestrator: copy one item of
`target_configs.yml` into its own file (drop the leading `- ` and de-indent
by two spaces), set `path:` to a directory on your fast disk, and run

```bash
cargo run --release -p benchmark -- --parameters-path my_cell.yml | tee my_cell.log
```

Stdout is the same run log the orchestrator downloads, and any field can be
overridden on the command line (`--help` lists the flags). Run cells one at
a time (the client binds port 9092 for its metrics endpoint, and the
experiments assume exclusive use of the disk and CPU anyway).

## Mapping paper figures and tables to runs

Every experimental figure and table in the paper has a generator mode (or
two: Tidehunter cells and baseline cells were run separately). Figures not
listed below (the design diagrams and the production case study) are not
reproducible from this repository. The exact cell list and parameters of
every mode are in the doc comments shown by
`cargo run -p generate_target_configs -- --help`.

| Paper element | Generator mode(s) |
|---|---|
| Value-size scaling figure (Figure 1) | `value-scaling`: value size {64, 128, 256, 512, 1024} B x Zipf θ {0, 2}, 50/50 Get, 1 TiB pre-fill. Baselines: `value-scaling-baselines` (RocksDB and BlobDB over the same grid). |
| Main benchmark figures (Figures 5 and 6) | `main-benchmark`: value size {1 KB, 64 B, 128 B} x Zipf θ {0, 2} x {write-only, 50/50 Get, 50/50 Exists, 50/50 Lt, 100% Get, 100% Exists, 100% Lt}, 1 TiB pre-fill. Baselines: `main-benchmark-baselines`. |
| Stability table (Table 1) | `stability`: read percentage {0, 50, 100} x Zipf θ {0, 2} on the 1 KB config. Needs a Prometheus scraping the client: throughput CV and per-interval percentiles come from the `bench_writes` / `bench_reads` counters (`scripts/fetch_grafana_variance.py` shows the queries), the lock-overhead column from the `large_table_contention` metric. |
| Application-workload regimes figure (Figure 7) | `app-workloads`: key/value sizes {24/10, 48/43, 20/44, 38/38, 76/50} B x Zipf θ {0, 2} x backend {Tidehunter, Rocksdb, Blobdb}, 50/50 Get, 500 GiB pre-fill. |
| Relocation on/off figure (Figure 8) | `relocation`: relocation {on, off} x Zipf θ {0, 2}; 1 TiB pre-fill of 1 KB values, then a delete-only phase. Storage from filesystem usage, throughput from logs. |
| Churn tables (Tables 2 and 3) | `churn`: strategy {None, WalBased, IndexBased} x mix {100% overwrite, 50/50 overwrite+delete, 100% delete}, plus reclaim threshold {1, 10, 25, 50}% on the WalBased 50/50 cell; 500 GiB pre-fill, write-only churn phase. BlobDB rows: `churn-blobdb`. |
| Recovery table (Table 4) | `recovery`, in fill/measure pairs. Series A: cold start at {100 GiB, 500 GiB, 1 TiB (x2 replicates)}. Series B: snapshot interval {16, 64, 256 GiB, unlimited} at 1 TiB. Series C: crash during relocation (`crash_after_secs: 600`; exit code 137 is expected), then a measured re-open. Extra replicates: `recovery-replicates`. Measure cells emit the `RECOVERY:` / `FIRST_READ:` lines. |
| Runtime memory table (Table 5) | `memory-instrumented`: 4 replicates of the headline 50/50 Get 1 KB θ=0 config with metrics enabled. Needs a Prometheus scraping the client; `scripts/fetch_r3_instrumented.py` aggregates the per-keyspace gauges. |
| Memory-sensitivity sweeps table (Table 6) | One mode per table block: `sweep-bloom-fpr` (FPR {0.001, 0.01, 0.05, 0.10}, 100% Get), `sweep-mmap-window` (`max_maps` {16, 32, 64, 128}), `sweep-cell-count` (`num_mutexes` {2^14, 2^16, 2^17, 2^19, 2^20}), `sweep-dirty-keys` (`max_dirty_keys` {64, 256, 1024, 4096, 16384}). |

The index microbenchmark numbers quoted inline in the evaluation come from
a benchmark that runs locally without the stress client:
`scripts/generate.sh` builds the index files, `scripts/run.sh` sweeps
lookup window x threads x direct I/O using `benchmarks/index_benchmark`
(writing to `results-local/`), and `scripts/benchmark_all.sh` repeats the
cycle for {10k, 100k, 1M} entries/index. The quoted numbers used 10M
lookups and threads {1, 16, 48}; adjust `NUM_LOOKUPS` and `THREAD_COUNTS`
in `run.sh` to match.

## Extracting results

### Log anatomy

The run log is the stress client's stdout, downloaded by the orchestrator
as `logs/logs-<branch>/node-<timestamp>-<n>.log`. It contains, in order:

* a dump of the full effective configuration (all `db_parameters` and
  `stress_client_parameters`, including the `tldr:` run name); use it to
  verify the run did what you intended,
* `BENCHMARK_START` / phase-boundary / `BENCHMARK_END` markers with
  millisecond epoch timestamps,
* end-of-phase summary lines:
  * `Write test done ...: <N> ops/s` (fill phase),
  * `Mixed test done ...: <N> ops/s` (measurement phase),
  * `Latency(mcs): p50/p99/p99.9` per phase,
  * `App bytes written total: <N>` (cumulative over fill + measurement),
  * `Storage used: <N>`,
* for recovery runs: `RECOVERY:` (total ms, WAL replay ms, bytes replayed,
  per-phase breakdown) and `FIRST_READ:` (samples, hits, mismatches, and
  first-read latency percentiles; `mismatches: 0` is the integrity check).

Throughput and latency numbers in the paper come directly from these lines.
`scripts/summarize_benchmarks.py <log-dir>` tabulates a directory of logs;
`scripts/plot_mixed_bench.py <log-dir>` produces comparison bar plots.

### Write amplification

WA is disk bytes over application bytes for the measurement phase (the
logs call it the `Mixed` phase):

1. Application bytes: `App bytes written total` is cumulative, so subtract
   the fill phase:
   `fill_bytes = writes x write_threads x (key_len + write_size)`.
2. Disk bytes: sectors-written x 512 for the block device backing the
   database directory, read from `/proc/diskstats` on the benchmark machine
   at the start of the measurement phase (the `Write test done` log line)
   and at the end (`Mixed test done`), then diffed:

   ```bash
   # replace md1 with your block device
   awk '$3 == "md1" { print $10 * 512 }' /proc/diskstats
   ```

   Make sure the device hosts nothing else busy during the run. (On the
   cluster we recorded the same counter as node_exporter's
   `node_disk_written_bytes_total`, windowed between the log timestamps.)
3. `WA = disk_bytes_measurement / app_bytes_measurement`.

Sanity check: the same computation over the fill phase should give WA very
close to 1.0 (append-only fill).

### Prometheus-based metrics

The stress client exposes `bench_writes` / `bench_reads` counters on port
9092 and, when `metrics_enabled: true`, all Tidehunter-internal metrics
(see the Metrics section of the main README). The stability and
runtime-memory tables need this time-series data, so those two experiments
require a Prometheus scraping the client; everything else is parsed from
logs and `/proc/diskstats`. See "Monitoring with Prometheus and Grafana"
in the advanced section for how we set this up. Most paper runs used
`metrics_enabled: false` to avoid measurement overhead.

## Pitfall checklist

* Check the config dump at the top of each log: unknown YAML keys are
  silently dropped, so a typo means a default value is silently used.
* Relocation is controlled by `stress_client_parameters.relocation`
  (`Wal`, `Index`, or absent = off). The `relocation_strategy` field inside
  `db_parameters` is ignored by the stress client; editing it does nothing.
* `max_maps` is per WAL kind; total mapped memory is twice that.
* CPU governor resets to `ondemand` on reboot; set it again after
  rebooting.
* Pre-fill must be several times larger than RAM, or reads are served from
  the page cache and read results are meaningless.
* Recovery reruns: remove leftover databases first
  (`scripts/r6_cleanup.sh`).
* If the benchmark process dies before its metrics endpoint is up, the
  orchestrator can hang waiting on it; check the machine with
  `testbed status` and the node logs.

## Advanced setups

Everything above assumes the basic case: a single machine that is both the
orchestrator and the testbed. This section covers everything else.

### Multiple benchmark machines

With several machines the cells run in parallel, one cell per machine at a
time (we used four machines for the paper). Copy
`orchestrator/assets/settings-template.yml` to
`orchestrator/assets/settings.yml` and fill it in. The `Settings` struct in
`orchestrator/src/settings.rs` documents every field; the important ones:

* `custom_machines`: the benchmark machines, one `host:` entry each.
* `ssh_private_key_file`: key for SSH access to the machines.
* `repository.url` / `repository.commit`: the repo and branch/SHA the
  machines will check out and `cargo build --release`.
* `working_dir`: the database directory on the machines (put it on the
  fast disk). The config generator bakes it into the generated configs, so
  regenerate them after changing it.
* `monitoring: false`, so all machines are available for benchmarks
  (`true` reserves machine 0 for Prometheus/Grafana, which changes how
  configs map to machines).

The orchestrator runs the configs in `target_configs.yml` in batches of
N = number of machines: config `i` of a batch runs on machine `i`, one
process per machine, and the next batch starts when all machines finish.
The recovery experiments rely on this: a fill/measure pair only works if
both cells land on the **same machine**, i.e. their positions in the file
are exactly N apart. The generator emits these pairs laid out for a
4-machine testbed; with any other machine count, reorder the file so each
measure sits exactly N positions after its fill.

Multi-machine versions of the single-machine pitfalls: set the CPU
governor on **all** machines, and remove leftover recovery databases on
**all** machines between reruns.

### Cloud machines

The orchestrator can also provision cloud machines (Vultr/AWS); see
`orchestrator/readme.md`. Keep in mind that we recommend bare metal over
cloud instances for reproducing the paper's numbers.

### Monitoring with Prometheus and Grafana

Only the stability and runtime-memory experiments need a Prometheus
scraping the stress client's metrics endpoint (port 9092); see
"Prometheus-based metrics" above. On our cluster we additionally ran a
Prometheus
[node_exporter](https://github.com/prometheus/node_exporter) on each
machine (`orchestrator/assets/install_node_exporter.sh`) and used
`orchestrator/assets/grafana-dashboard.json` as the dashboard;
node_exporter's disk counters are also a convenient alternative to
`/proc/diskstats` for the write-amplification computation.

### Hardware used for the paper

The paper's numbers come from bare-metal machines (OpenMetal "Medium v4")
with:

* 2x Intel Xeon Silver 4510 (24 cores / 48 threads total, 2.4-4.1 GHz)
* 256 GB DDR5-4400 RAM
* 2x 3.2 TB NVMe (Micron 9400 PRO) in software RAID0 (`md1`), ext4,
  mounted at `/opt/sui/db` (the database directory used by all
  experiments)
* Ubuntu Linux
