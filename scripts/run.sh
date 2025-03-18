#!/bin/bash

# Exit on any error
set -e

# Benchmark configuration constants
INDEX_SIZE="10k" # !!! MAKE SURE THIS IS THE SAME AS THE ENTRIES_PER_INDEX IN generate.sh !!!
DIRECT_IO_SUFFIX="dio"

# Benchmark parameters
NUM_LOOKUPS=1000000
NUM_RUNS=10
BATCH_SIZE=1000

# Directory setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$PROJECT_ROOT/results/index-$INDEX_SIZE-$DIRECT_IO_SUFFIX"

# Ensure results directory exists
mkdir -p "$RESULTS_DIR"

# File paths
UNIFORM_LOOKUP_PATH="$SRC_DIR/index/uniform_lookup.rs"

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
        
        # Update DEFAULT_WINDOW_SIZE in uniform_lookup.rs
        sed -i.bak "s/const DEFAULT_WINDOW_SIZE: usize = [0-9]\+;/const DEFAULT_WINDOW_SIZE: usize = $window_size;/" "$UNIFORM_LOOKUP_PATH"

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
        cargo run --release -- run \
            --num-lookups "$NUM_LOOKUPS" \
            --num-runs "$NUM_RUNS" \
            --batch-size "$BATCH_SIZE" \
            --header-file "data/bench-header-$file_size-$INDEX_SIZE.dat" \
            --uniform-file "data/bench-uniform-$file_size-$INDEX_SIZE.dat" \
            --direct-io 2>&1 | tee -a "$log_file"
        
        # Log benchmark completion
        echo "Benchmark completed at $(date)" | tee -a "$log_file"
        echo "Results saved to $log_file"
        echo ""
        
        # Restore original file (optional, but helpful to avoid git conflicts)
        mv "$UNIFORM_LOOKUP_PATH.bak" "$UNIFORM_LOOKUP_PATH"
    done
done

echo "All benchmarks completed!" 