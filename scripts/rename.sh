#!/bin/bash

# Exit on any error
set -e

# Get directory from command line argument or use default
if [ $# -eq 0 ]; then
    echo "Usage: $0 <results_directory>"
    exit 1
fi

RESULTS_DIR="$1"

# Check if directory exists
if [ ! -d "$RESULTS_DIR" ]; then
    echo "Error: Directory $RESULTS_DIR does not exist"
    exit 1
fi

# Process each log file
for log_file in "$RESULTS_DIR"/benchmark_*.log; do
    echo "Processing $log_file..."
    
    # Extract file size and window size from filename
    filename=$(basename "$log_file")
    file_size=$(echo "$filename" | grep -o -E '[0-9]+[GMT]B' | head -1)
    window_size=$(echo "$filename" | grep -o -E 'window[0-9]+' | sed 's/window//')
    window_size=$((window_size * 2))
    new_filename="benchmark_${file_size}_window${window_size}.log"
    mv "$log_file" "$RESULTS_DIR/$new_filename"
done 