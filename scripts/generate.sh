#!/bin/bash

# Default entries per index
DEFAULT_ENTRIES_PER_INDEX=1000000 # Should match DEFAULT_INDEX_SIZE in run.sh (1M)
ENTRY_SIZE_BYTES=40  # Each entry is approximately 40 bytes

# Parse command line arguments
function usage {
    echo "Usage: $0 [--entries-per-index NUM]"
    echo "  --entries-per-index NUM: Number of entries per index (default: $DEFAULT_ENTRIES_PER_INDEX)"
    echo "                         IMPORTANT: Should match --index-size in run.sh"
    exit 1
}

# Parse arguments
ENTRIES_PER_INDEX=$DEFAULT_ENTRIES_PER_INDEX
while [[ $# -gt 0 ]]; do
    case "$1" in
        --entries-per-index)
            ENTRIES_PER_INDEX="$2"
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

# Compute the index size in bytes (approximation)
INDEX_SIZE_BYTES=$((ENTRIES_PER_INDEX * ENTRY_SIZE_BYTES))
echo "Index size: $ENTRIES_PER_INDEX entries x $ENTRY_SIZE_BYTES bytes = $INDEX_SIZE_BYTES bytes per index"

# File sizes to generate
FILE_SIZES=(
  "1TB:1000000000000"
  "100GB:100000000000"
  "10GB:10000000000"
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
  
  # Generate header index file
  header_file="data/bench-header-$size_name-$ENTRIES_SHORT.dat"
  uniform_file="data/bench-uniform-$size_name-$ENTRIES_SHORT.dat"
  echo "Generating header index file: $header_file"
  cargo run --release --bin index_benchmark -- generate \
    --num-indices "$num_indices" \
    --entries-per-index "$ENTRIES_PER_INDEX" \
    --header-file "$header_file" \
    --uniform-file "$uniform_file"
  
  echo "Completed generating $size_name benchmark file"
  echo "----------------------------------------"
done

echo "All benchmark files generated successfully!" 