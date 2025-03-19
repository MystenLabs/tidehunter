# Index Benchmark

This benchmark suite evaluates the performance of different index implementations in Tidehunter, specifically comparing the `LookupHeaderIndex` and `UniformLookupIndex` implementations.

## Overview

The benchmark suite consists of two main components:
1. A data generator that creates test files with different sizes and index configurations
2. A benchmark runner that measures lookup performance across various parameters

## Prerequisites

- Rust toolchain (latest stable version recommended)
- Sufficient disk space for benchmark data (up to 2.3TB depending on configuration)
- The Tidehunter project dependencies

## Directory Structure

- `data/` - Contains generated benchmark files
- `results-local/` - Contains benchmark results and logs
- `scripts/` - Contains automation scripts for running benchmarks

## Running the Benchmark

### Stand-alone Usage

You can run the benchmark directly using the `index_benchmark` binary. There are two main commands:

1. Generate benchmark data:
```bash
cargo run --release --bin index_benchmark -- generate \
    --num-indices <number> \
    --entries-per-index <number> \
    --header-file <path> \
    --uniform-file <path>
```

2. Run benchmarks:
```bash
cargo run --release --bin index_benchmark -- run \
    --num-lookups <number> \
    --num-runs <number> \
    --batch-size <number> \
    --window-size <number> \
    --header-file <path> \
    --uniform-file <path> \
    [--direct-io]
```

### Using the Automation Scripts

The benchmark suite includes two main scripts:

1. Generate benchmark data:
```bash
./scripts/generate.sh
```
This script generates benchmark files for different sizes (10GB, 100GB, 1TB) with configurable index sizes.

2. Run benchmarks:
```bash
./scripts/run.sh
```
This script runs benchmarks across different configurations and saves results to the `results-local` directory.

## Configuration

### Data Generation

The `generate.sh` script creates benchmark files with the following configurations:
- File sizes: 10GB, 100GB, 1TB
- Default entries per index: 1M
- Entry size: ~40 bytes

### Benchmark Parameters

The `run.sh` script uses the following default parameters:
- Number of lookups: 1,000,000
- Number of runs: 3
- Batch size: 1,000
- Window sizes: 100, 200, 400, 800
- Optional direct I/O mode

You can modify these parameters by editing the script files or using the stand-alone commands.

## Results

The benchmark results include:
- Throughput (lookups/second)
- Latency statistics (mean, standard deviation, min, max)
- I/O metrics
- Scan metrics
- Iteration statistics for the UniformLookupIndex

Results are saved in the `results-local` directory with timestamps and configuration details in the filenames.

## Notes

- Make sure you have sufficient disk space before running the benchmark
- The benchmark files can be large (up to 1TB), so plan accordingly
- Direct I/O mode can be enabled by uncommenting the `DIRECT_IO_SUFFIX` line in `run.sh`
- The index size in `run.sh` must match the entries per index in `generate.sh` 