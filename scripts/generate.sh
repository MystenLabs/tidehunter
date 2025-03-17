#!/bin/bash

# Constants
ENTRIES_PER_INDEX=1000000
ENTRY_SIZE_BYTES=40  # Each entry is approximately 40 bytes

# Compute the index size in bytes (approximation)
INDEX_SIZE_BYTES=$((ENTRIES_PER_INDEX * ENTRY_SIZE_BYTES))
echo "Index size: $ENTRIES_PER_INDEX entries x $ENTRY_SIZE_BYTES bytes = $INDEX_SIZE_BYTES bytes per index"

# File sizes to generate
FILE_SIZES=(
  "10GB:10000000000"
  "100GB:100000000000"
  "1TB:1000000000000"
)

# Function to format numbers with k/M suffix
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

# Format the entries per index for filename
ENTRIES_SHORT=$(format_number $ENTRIES_PER_INDEX)
echo "Using shorthand $ENTRIES_SHORT for $ENTRIES_PER_INDEX entries in filenames"

# Function to update the benchmark file
update_benchmark_file() {
  local file_size_name=$1
  local num_indices=$2
  
  echo "Updating index_benchmark.rs for $file_size_name with $num_indices indices and $ENTRIES_PER_INDEX entries per index"
  
  # Use sed to update the constants in the file
  sed -i.bak "s|const HEADER_INDEX_FILE: \&str = \".*\";|const HEADER_INDEX_FILE: \&str = \"data/bench-header-$file_size_name-$ENTRIES_SHORT.dat\";|" src/benchmarks/index_benchmark.rs
  sed -i.bak "s|const UNIFORM_INDEX_FILE: \&str = \".*\";|const UNIFORM_INDEX_FILE: \&str = \"data/bench-uniform-$file_size_name-$ENTRIES_SHORT.dat\";|" src/benchmarks/index_benchmark.rs
  sed -i.bak "s|const NUM_INDICES: usize = [0-9_]*;|const NUM_INDICES: usize = $num_indices;|" src/benchmarks/index_benchmark.rs
  sed -i.bak "s|const ENTRIES_PER_INDEX: usize = [0-9_]*;|const ENTRIES_PER_INDEX: usize = $ENTRIES_PER_INDEX;|" src/benchmarks/index_benchmark.rs
  
  # Remove backup files
  rm src/benchmarks/index_benchmark.rs.bak
}

# Create data directory if it doesn't exist
mkdir -p data

# Process each file size
for size_pair in "${FILE_SIZES[@]}"; do
  # Split the pair into name and size
  IFS=":" read -r size_name size_bytes <<< "$size_pair"
  
  # Calculate required number of indices
  num_indices=$((size_bytes / INDEX_SIZE_BYTES))
  
  echo "Generating $size_name benchmark file:"
  echo "  - Target size: $size_bytes bytes"
  echo "  - Number of indices: $num_indices"
  
  # Update the benchmark file
  update_benchmark_file "$size_name" "$num_indices"
  
  # Run the benchmark generation
  echo "Running benchmark generation for $size_name..."
  cargo run --release --features index_benchmark_generate
  
  echo "Completed generating $size_name benchmark file"
  echo "----------------------------------------"
done

echo "All benchmark files generated successfully!" 