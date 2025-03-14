#!/bin/bash

# Exit on any error
set -e

# Directory setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$PROJECT_ROOT/results"
SRC_DIR="$PROJECT_ROOT/src"

# Ensure results directory exists
mkdir -p "$RESULTS_DIR"

# File paths
UNIFORM_LOOKUP_PATH="$SRC_DIR/index/uniform_lookup.rs"
INDEX_BENCHMARK_PATH="$SRC_DIR/benchmarks/index_benchmark.rs"

# Benchmark configuration constants
INDEX_SIZE="100K"
# Array of file sizes to test
FILE_SIZES=("10GB" "100GB" "1TB")

# Array of window sizes to test
WINDOW_SIZES=(50 100 200 400)

# Run benchmarks for each combination
for file_size in "${FILE_SIZES[@]}"; do
    for window_size in "${WINDOW_SIZES[@]}"; do
        window_size=$((window_size * 2))
        echo "===== Running benchmark with file size $file_size, window size $window_size, index size $INDEX_SIZE ====="
        
        # Create timestamp for log file
        timestamp=$(date +"%Y%m%d_%H%M%S")
        log_file="$RESULTS_DIR/benchmark_${file_size}_window${window_size}_${timestamp}.log"
        
        echo "Benchmark started at $(date)" | tee -a "$log_file"
        
        # 1. Update DEFAULT_WINDOW_SIZE in uniform_lookup.rs
        sed -i.bak "s/const DEFAULT_WINDOW_SIZE: usize = [0-9]\+;/const DEFAULT_WINDOW_SIZE: usize = $window_size;/" "$UNIFORM_LOOKUP_PATH"
        
        # 2. Update file name constants in index_benchmark.rs based on file size
        sed -i.bak "s/const HEADER_INDEX_FILE: \&str = \"data\/bench-header-[0-9A-Z]\+-[0-9A-Z]\+\.dat\";/const HEADER_INDEX_FILE: \&str = \"data\/bench-header-$file_size-$INDEX_SIZE.dat\";/" "$INDEX_BENCHMARK_PATH"
        sed -i.bak "s/const UNIFORM_INDEX_FILE: \&str = \"data\/bench-uniform-[0-9A-Z]\+-[0-9A-Z]\+\.dat\";/const UNIFORM_INDEX_FILE: \&str = \"data\/bench-uniform-$file_size-$INDEX_SIZE.dat\";/" "$INDEX_BENCHMARK_PATH"
        
        # 3. Compile and run the benchmark
        echo "Building and running benchmark..." | tee -a "$log_file"
        cargo run --release --features index_benchmark_run 2>&1 | tee -a "$log_file"
        
        # 4. Log benchmark completion
        echo "Benchmark completed at $(date)" | tee -a "$log_file"
        echo "Results saved to $log_file"
        echo ""
        
        # 5. Restore original files (optional, but helpful to avoid git conflicts)
        mv "$UNIFORM_LOOKUP_PATH.bak" "$UNIFORM_LOOKUP_PATH"
        mv "$INDEX_BENCHMARK_PATH.bak" "$INDEX_BENCHMARK_PATH"
    done
done

echo "All benchmarks completed!" 