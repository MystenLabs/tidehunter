#!/bin/bash

# Exit on any error
set -e

# Index sizes to test (in entries)
INDEX_SIZES=(
  "10000"     # 10k
  "100000"    # 100k
  "1000000"   # 1M
)

# Function to format numbers with k/M suffix (similar to generate.sh)
format_number() {
  local num=$1
  if [ $num -ge 1000000 ]; then
    echo "$((num / 1000000))M"
  elif [ $num -ge 1000 ]; then
    echo "$((num / 1000))k"
  else
    echo "$num"
  fi
}

echo "Starting benchmark suite..."

for entries in "${INDEX_SIZES[@]}"; do
  # Format the entries for display and arguments
  entries_formatted=$(format_number $entries)
  
  echo "===========================================" 
  echo "Benchmarking with $entries_formatted entries per index"
  echo "===========================================" 
  
  # 1. Clean data directory
  echo "Cleaning data directory..."
  rm -f data/*.dat
  
  # 2. Generate benchmark files
  echo "Generating benchmark files with $entries entries per index..."
  ./scripts/generate.sh --entries-per-index "$entries"
  
  # 3. Run benchmarks
  echo "Running benchmarks with index size $entries_formatted..."
  ./scripts/run.sh --index-size "$entries_formatted"
  
  echo "Completed benchmark cycle for $entries_formatted"
  echo ""
done

echo "All benchmarks completed!" 