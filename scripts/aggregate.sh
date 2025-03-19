#!/bin/bash

# Exit on any error
set -e

# Get directory from command line argument or use default
if [ $# -eq 0 ]; then
    echo "Usage: $0 <results_directory>"
    exit 1
fi

RESULTS_DIR="$1"
SUMMARY_FILE="$RESULTS_DIR/summary.log"

# Check if directory exists
if [ ! -d "$RESULTS_DIR" ]; then
    echo "Error: Directory $RESULTS_DIR does not exist"
    exit 1
fi

# Initialize summary file
echo "Benchmark Summary Report" > "$SUMMARY_FILE"
echo "======================" >> "$SUMMARY_FILE"
echo "Generated on: $(date)" >> "$SUMMARY_FILE"
echo "" >> "$SUMMARY_FILE"
printf "%-9s | %-11s | %-10s | %-12s | %-8s | %-12s | %-10s\n" "File Size" "Window Size" "Index Type" "Tput (ops/s)" "Avg Hops" "Scan us" "IO us" >> "$SUMMARY_FILE"
printf "%-9s-|-%-11s-|-%-10s-|-%-12s-|-%-8s-|-%-12s-|-%-10s\n" "---------" "-----------" "----------" "------------" "--------" "------------" "----------" >> "$SUMMARY_FILE"

# Process each log file
for log_file in "$RESULTS_DIR"/benchmark_*.log; do
    echo "Processing $log_file..."
    
    # Extract file size and window size from filename
    filename=$(basename "$log_file")
    file_size=$(echo "$filename" | grep -o -E '[0-9]+[GMT]B' | head -1)
    window_size=$(echo "$filename" | grep -o -E 'window[0-9]+' | sed 's/window//')
    
    # Initialize variables for data collection
    header_throughput=""
    uniform_throughput=""
    uniform_avg_hops=""
    
    # Extract throughput values for both index formats (from the last occurrence of each)
    header_throughput=$(grep -A 5 "HeaderLookupIndex Results:" "$log_file" | grep "Throughput:" | tail -1 | awk '{print $2}')
    uniform_throughput=$(grep -A 5 "UniformLookupIndex Results:" "$log_file" | grep "Throughput:" | tail -1 | awk '{print $2}')
    
    # Extract and process histogram buckets to calculate average hops for uniform index only
    # The header index always has exactly 2 hops
    histogram_line=$(grep "Histogram:" "$log_file")

    header_scan_mcs=$(grep "HeaderLookupIndex: scan mcs" "$log_file" | grep -o -E 'inner: [0-9]+' | head -1 | awk '{print $2}')
    header_io_mcs=$(grep "HeaderLookupIndex: io mcs" "$log_file" | grep -o -E 'inner: [0-9]+' | head -1 | awk '{print $2}')
    uniform_scan_mcs=$(grep "UniformLookupIndex: scan mcs" "$log_file" | grep -o -E 'inner: [0-9]+' | head -1 | awk '{print $2}')
    uniform_io_mcs=$(grep "UniformLookupIndex: io mcs" "$log_file" | grep -o -E 'inner: [0-9]+' | head -1 | awk '{print $2}')
    
    # Extract bucket values between square brackets after the FIRST occurrence of "buckets:"
    if [ ! -z "$histogram_line" ]; then
        # Extract the portion after the first "buckets:" and between its square brackets
        # Using a more specific pattern to find the first occurrence of buckets followed by square brackets
        first_buckets_section=$(echo "$histogram_line" | sed 's/\(buckets: \[[^]]*\]\).*/\1/' | sed 's/.*\(buckets: \[[^]]*\]\).*/\1/')
        
        # Extract all "inner: X" values
        inner_values=$(echo "$first_buckets_section" | grep -o -E 'inner: [0-9]+' | awk '{print $2}')
        
        # Calculate average hops
        weighted_sum=0
        total_count=0
        hop=1
        
        for count in $inner_values; do
            weighted_sum=$((weighted_sum + (hop * count)))
            total_count=$((total_count + count))
            hop=$((hop + 1))
        done
        
        # Calculate average
        if [ "$total_count" -gt 0 ]; then
            uniform_avg_hops=$(echo "scale=2; $weighted_sum / $total_count" | bc)
        else
            uniform_avg_hops="N/A"
        fi
    else
        uniform_avg_hops="N/A"
    fi
    
    # Add to summary file
    if [ ! -z "$header_throughput" ]; then
        printf "%-9s | %-11s | %-10s | %-12s | %-8s | %-12s | %-10s\n" "$file_size" "$window_size" "Header" "$header_throughput" "2.00" "$header_scan_mcs" "$header_io_mcs" >> "$SUMMARY_FILE"
    fi
    
    if [ ! -z "$uniform_throughput" ]; then
        printf "%-9s | %-11s | %-10s | %-12s | %-8s | %-12s | %-10s\n" "$file_size" "$window_size" "Uniform" "$uniform_throughput" "$uniform_avg_hops" "$uniform_scan_mcs" "$uniform_io_mcs" >> "$SUMMARY_FILE"
    fi
done

echo "Summary report generated at $SUMMARY_FILE" 