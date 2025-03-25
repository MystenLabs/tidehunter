#!/usr/bin/env python3

import sys
import os
import re
import numpy as np
from datetime import datetime
from pathlib import Path

def extract_metrics_from_log(log_file):
    """Extract metrics from a benchmark log file."""
    with open(log_file, 'r') as f:
        content = f.read()
    
    # Extract file size and window size from filename
    filename = os.path.basename(log_file)
    file_size = re.search(r'[0-9]+[GMT]B', filename).group(0) if re.search(r'[0-9]+[GMT]B', filename) else "N/A"
    window_size = re.search(r'window[0-9]+', filename).group(0).replace('window', '') if re.search(r'window[0-9]+', filename) else "N/A"
    
    # Extract direct I/O setting
    dio_status = "Yes" if "_dio_" in filename else "No" if "_no-dio_" in filename else "N/A"
    
    # Extract throughput values
    header_throughput = None
    uniform_throughput = None
    
    # Get all matches and calculate median using numpy
    header_matches = list(re.finditer(r'HeaderLookupIndex Results:.*?Throughput: (\d+\.?\d*)', content, re.DOTALL))
    if header_matches:
        values = np.array([float(match.group(1)) for match in header_matches])
        header_throughput = str(np.median(values))
    
    uniform_matches = list(re.finditer(r'UniformLookupIndex Results:.*?Throughput: (\d+\.?\d*)', content, re.DOTALL))
    if uniform_matches:
        values = np.array([float(match.group(1)) for match in uniform_matches])
        uniform_throughput = str(np.median(values))
    
    # Extract scan and IO metrics
    header_scan_mcs = re.search(r'HeaderLookupIndex: scan mcs.*?inner: (\d+)', content).group(1) if re.search(r'HeaderLookupIndex: scan mcs.*?inner: (\d+)', content) else "N/A"
    header_io_mcs = re.search(r'HeaderLookupIndex: io mcs.*?inner: (\d+)', content).group(1) if re.search(r'HeaderLookupIndex: io mcs.*?inner: (\d+)', content) else "N/A"
    header_io_s = float(header_io_mcs) / 1000000 if header_io_mcs != "N/A" else "N/A"
    
    uniform_scan_mcs = re.search(r'UniformLookupIndex: scan mcs.*?inner: (\d+)', content).group(1) if re.search(r'UniformLookupIndex: scan mcs.*?inner: (\d+)', content) else "N/A"
    uniform_io_mcs = re.search(r'UniformLookupIndex: io mcs.*?inner: (\d+)', content).group(1) if re.search(r'UniformLookupIndex: io mcs.*?inner: (\d+)', content) else "N/A"
    uniform_io_s = float(uniform_io_mcs) / 1000000 if uniform_io_mcs != "N/A" else "N/A"
    
    # Extract IO bytes and convert to GB
    header_io_bytes = re.search(r'HeaderLookupIndex: io bytes.*?inner: (\d+)', content).group(1) if re.search(r'HeaderLookupIndex: io bytes.*?inner: (\d+)', content) else "N/A"
    header_io_gb = float(header_io_bytes) / 1000000000 if header_io_bytes != "N/A" else "N/A"
    
    uniform_io_bytes = re.search(r'UniformLookupIndex: io bytes.*?inner: (\d+)', content).group(1) if re.search(r'UniformLookupIndex: io bytes.*?inner: (\d+)', content) else "N/A"
    uniform_io_gb = float(uniform_io_bytes) / 1000000000 if uniform_io_bytes != "N/A" else "N/A"
    
    # Calculate average hops for uniform index
    histogram_match = re.search(r'buckets: \[(.*?)\]', content)
    uniform_avg_hops = "N/A"
    
    if histogram_match:
        buckets_content = histogram_match.group(1)
        inner_values = re.findall(r'inner: (\d+)', buckets_content)
        
        if inner_values:
            weighted_sum = 0
            total_count = 0
            
            for hop, count in enumerate(inner_values, 1):
                weighted_sum += hop * int(count)
                total_count += int(count)
            
            if total_count > 0:
                uniform_avg_hops = f"{weighted_sum / total_count:.2f}"
    
    return {
        'file_size': file_size,
        'window_size': window_size,
        'dio_status': dio_status,
        'header_throughput': header_throughput,
        'uniform_throughput': uniform_throughput,
        'header_scan_mcs': header_scan_mcs,
        'header_io_s': header_io_s,
        'header_io_gb': header_io_gb,
        'uniform_scan_mcs': uniform_scan_mcs,
        'uniform_io_s': uniform_io_s,
        'uniform_io_gb': uniform_io_gb,
        'uniform_avg_hops': uniform_avg_hops
    }

def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <results_directory>")
        sys.exit(1)
    
    results_dir = sys.argv[1]
    if not os.path.isdir(results_dir):
        print(f"Error: Directory {results_dir} does not exist")
        sys.exit(1)
    
    summary_file = os.path.join(results_dir, "summary.log")
    
    # Initialize summary file
    with open(summary_file, 'w') as f:
        f.write("Benchmark Summary Report\n")
        f.write("======================\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Write header
        header = "%-9s | %-6s | %-10s | %-7s | %-12s | %-8s | %-12s | %-10s | %-10s"
        separator = "%-9s-|-%-6s-|-%-10s-|-%-7s-|-%-12s-|-%-8s-|-%-12s-|-%-10s-|-%-10s"
        
        f.write(header % ("File Size", "Window", "Index Type", "DIO", "Tput (ops/s)", "Avg Hops", "Scan us", "IO s", "IO GB") + "\n")
        f.write(separator % ("---------", "------", "----------", "-------", "------------", "--------", "------------", "----------", "----------") + "\n")
        
        # Process each log file
        for log_file in sorted(Path(results_dir).glob("benchmark_*.log")):
            print(f"Processing {log_file}...")
            metrics = extract_metrics_from_log(log_file)
            
            # Write header index results
            if metrics['header_throughput']:
                f.write(header % (
                    metrics['file_size'],
                    metrics['window_size'],
                    "Header",
                    metrics['dio_status'],
                    metrics['header_throughput'],
                    "2.00",
                    metrics['header_scan_mcs'],
                    f"{metrics['header_io_s']:.2f}" if metrics['header_io_s'] != "N/A" else "N/A",
                    f"{metrics['header_io_gb']:.2f}" if metrics['header_io_gb'] != "N/A" else "N/A"
                ) + "\n")
            
            # Write uniform index results
            if metrics['uniform_throughput']:
                f.write(header % (
                    metrics['file_size'],
                    metrics['window_size'],
                    "Uniform",
                    metrics['dio_status'],
                    metrics['uniform_throughput'],
                    metrics['uniform_avg_hops'],
                    metrics['uniform_scan_mcs'],
                    f"{metrics['uniform_io_s']:.2f}" if metrics['uniform_io_s'] != "N/A" else "N/A",
                    f"{metrics['uniform_io_gb']:.2f}" if metrics['uniform_io_gb'] != "N/A" else "N/A"
                ) + "\n")
    
    print(f"Summary report generated at {summary_file}")

if __name__ == "__main__":
    main() 