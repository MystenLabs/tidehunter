#!/bin/bash

# Exit on any error
set -e

# Default index size
DEFAULT_INDEX_SIZE="1M"

# Parse command line arguments
function usage {
    echo "Usage: $0 [--index-size SIZE]"
    echo "  --index-size SIZE: Index size to use (default: $DEFAULT_INDEX_SIZE)"
    echo "                    IMPORTANT: Must match ENTRIES_PER_INDEX in generate.sh"
    exit 1
}

# Parse arguments
INDEX_SIZE=$DEFAULT_INDEX_SIZE
while [[ $# -gt 0 ]]; do
    case "$1" in
        --index-size)
            INDEX_SIZE="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Benchmark parameters
NUM_LOOKUPS=1000000
NUM_RUNS=3
BATCH_SIZE=1000

# Directory setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$PROJECT_ROOT/results-local/index-$INDEX_SIZE"
SRC_DIR="$PROJECT_ROOT/tidehunter/src"

# Display configuration
echo "Running benchmarks with index size: $INDEX_SIZE"

# Ensure results directory exists
mkdir -p "$RESULTS_DIR"

# File paths
UNIFORM_LOOKUP_PATH="$SRC_DIR/index/uniform_lookup.rs"

# Array of file sizes to test
FILE_SIZES=("1TB")

# Array of window sizes to test
WINDOW_SIZES=(100 200 400 800 1600 3200)

# Array of thread counts to test
THREAD_COUNTS=(1 2 4 8 16)

# Direct I/O options
DIRECT_IO_OPTIONS=("on" "off")

# Run benchmarks for each combination
for direct_io in "${DIRECT_IO_OPTIONS[@]}"; do
    # Set direct I/O suffix based on the current option
    if [ "$direct_io" == "on" ]; then
        DIRECT_IO_SUFFIX="--direct-io"
        DIRECT_IO_NAME="dio"
    else
        DIRECT_IO_SUFFIX=
        DIRECT_IO_NAME="no-dio"
    fi
    
    for file_size in "${FILE_SIZES[@]}"; do
        for window_size in "${WINDOW_SIZES[@]}"; do
            for num_threads in "${THREAD_COUNTS[@]}"; do
                echo "===== Running benchmark with file size $file_size, window size $window_size, index size $INDEX_SIZE, direct I/O: $direct_io, threads: $num_threads ====="
                
                # Create timestamp for log file
                timestamp=$(date +"%Y%m%d_%H%M%S")
                log_file="$RESULTS_DIR/benchmark_${file_size}_window${window_size}_${DIRECT_IO_NAME}_threads${num_threads}_${timestamp}.log"
                
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
                echo "Running benchmark with direct I/O: $direct_io, threads: $num_threads..." | tee -a "$log_file"
                cargo run --release --bin index_benchmark -- run \
                    --num-lookups "$NUM_LOOKUPS" \
                    --num-runs "$NUM_RUNS" \
                    --batch-size "$BATCH_SIZE" \
                    --window-size "$window_size" \
                    --num-threads "$num_threads" \
                    --header-file "data/bench-header-$file_size-$INDEX_SIZE.dat" \
                    --uniform-file "data/bench-uniform-$file_size-$INDEX_SIZE.dat" \
                    $DIRECT_IO_SUFFIX 2>&1 | tee -a "$log_file"
                
                # Log benchmark completion
                echo "Benchmark completed at $(date)" | tee -a "$log_file"
                echo "Results saved to $log_file"
                echo ""
            done
        done
    done
done

echo "All benchmarks completed!" 