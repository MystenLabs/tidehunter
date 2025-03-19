#!/bin/bash

# Exit on any error
set -e

# Benchmark configuration constants
INDEX_SIZE="100k" # !!! MAKE SURE THIS IS THE SAME AS THE ENTRIES_PER_INDEX IN generate.sh !!!
DIRECT_IO_SUFFIX="--direct-io" # uncomment to use direct I/O
# DIRECT_IO_SUFFIX= # uncomment to not use direct I/O

# Benchmark parameters
NUM_LOOKUPS=1000000
NUM_RUNS=1
BATCH_SIZE=1000

# Directory setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$PROJECT_ROOT/results-local/index-$INDEX_SIZE$DIRECT_IO_SUFFIX"
SRC_DIR="$PROJECT_ROOT/tidehunter/src"

# Ensure results directory exists
mkdir -p "$RESULTS_DIR"

# File paths
UNIFORM_LOOKUP_PATH="$SRC_DIR/index/uniform_lookup.rs"

# Array of file sizes to test
# FILE_SIZES=("10GB" "100GB" "1TB")
FILE_SIZES=("10GB")

# Array of window sizes to test
# WINDOW_SIZES=(100 200 400 800)
WINDOW_SIZES=(200 400 800)

# Run benchmarks for each combination
for file_size in "${FILE_SIZES[@]}"; do
    for window_size in "${WINDOW_SIZES[@]}"; do
        echo "===== Running benchmark with file size $file_size, window size $window_size, index size $INDEX_SIZE ====="
        
        # Create timestamp for log file
        timestamp=$(date +"%Y%m%d_%H%M%S")
        log_file="$RESULTS_DIR/benchmark_${file_size}_window${window_size}_${timestamp}.log"
        
        echo "Benchmark started at $(date)" | tee -a "$log_file"

        # Check if the header and uniform files exist
        if [ ! -f "data/bench-header-$file_size-$INDEX_SIZE.dat" ]; then
            echo "Error: Header file data/bench-header-$file_size-$INDEX_SIZE.dat does not exist"
            exit 1
        fi
        if [ ! -f "data/bench-uniform-$file_size-$INDEX_SIZE.dat" ]; then
            echo "Error: Uniform file data/bench-uniform-$file_size-$INDEX_SIZE.dat does not exist"
            exit 1
        fi
        
        # Run the benchmark with the new command-line interface
        echo "Running benchmark..." | tee -a "$log_file"
        cargo run --release --bin index_benchmark -- run \
            --num-lookups "$NUM_LOOKUPS" \
            --num-runs "$NUM_RUNS" \
            --batch-size "$BATCH_SIZE" \
            --window-size "$window_size" \
            --header-file "data/bench-header-$file_size-$INDEX_SIZE.dat" \
            --uniform-file "data/bench-uniform-$file_size-$INDEX_SIZE.dat" \
            $DIRECT_IO_SUFFIX 2>&1 | tee -a "$log_file"
        
        # Log benchmark completion
        echo "Benchmark completed at $(date)" | tee -a "$log_file"
        echo "Results saved to $log_file"
        echo ""
    done
done

echo "All benchmarks completed!" 