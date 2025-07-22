#!/usr/bin/env python3
"""
Plot mixed benchmark results from a directory of logs.

This script parses all .log files in a given directory, extracts benchmark
results for different configurations, and generates bar plots comparing
Tidehunter and RocksDB throughput.
"""

import os
import re
import sys
import argparse
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# --- Configuration Constants ---
# Users can modify these lists to control which plots are generated.
# The script will generate a plot for each combination of these parameters.

# Available read modes: 'Get', 'Lt'
READ_MODES = ['Exists', 'Get', 'Lt']

# Direct I/O modes: True (for direct I/O), False (for buffered I/O)
DIRECT_IO_MODES = [False]

# Read percentages to plot (e.g., 0, 50, 80, 100)
READ_PERCENTS = [0, 50, 80, 100]

# Zipf exponents for workload distribution (e.g., 0.0 for uniform, 0.8 for skewed)
ZIPF_EXPONENTS = [0, 0.8, 1.5, 2]


# --- Log Parsing ---

# Regex patterns for parsing log files
BACKEND_PATTERN = re.compile(r'backend:\s+(Tidehunter|Rocksdb)')
READ_MODE_PATTERN = re.compile(r'read_mode:\s+(Get|Exists|Lt\(\s*\d+\s*,?\s*\))', re.DOTALL)
READ_PERCENTAGE_PATTERN = re.compile(r'read_percentage:\s+(\d+)')
DIRECT_IO_PATTERN = re.compile(r'direct_io:\s+(true|false)')
ZIPF_EXPONENT_PATTERN = re.compile(r'zipf_exponent:\s+([\d.]+)')
MIXED_TEST_PATTERN = re.compile(r'Mixed test done in [^:]+:\s+([\d.]+[MK]?)\s+ops/s')

def parse_ops_value(ops_str: str) -> float:
    """Convert ops string like '1.03M' or '315.00K' to numeric value."""
    if ops_str.endswith('M'):
        return float(ops_str[:-1]) * 1_000_000
    elif ops_str.endswith('K'):
        return float(ops_str[:-1]) * 1_000
    else:
        return float(ops_str)

def parse_log_file(filepath: Path) -> Optional[Dict]:
    """Parse a single log file and extract benchmark results."""
    try:
        content = filepath.read_text()

        patterns = {
            "backend": BACKEND_PATTERN,
            "read_mode": READ_MODE_PATTERN,
            "read_percentage": READ_PERCENTAGE_PATTERN,
            "direct_io": DIRECT_IO_PATTERN,
            "zipf_exponent": ZIPF_EXPONENT_PATTERN,
            "mixed_test": MIXED_TEST_PATTERN,
        }
        matches = {name: pattern.search(content) for name, pattern in patterns.items()}

        missing_fields = [name for name, match in matches.items() if not match]
        if missing_fields:
            print(f"Warning: Could not parse fields {missing_fields} from {filepath.name}")
            return None

        # Extract values from successful matches
        backend_match = matches['backend']
        read_mode_match = matches['read_mode']
        read_percentage_match = matches['read_percentage']
        direct_io_match = matches['direct_io']
        zipf_exponent_match = matches['zipf_exponent']
        mixed_test_match = matches['mixed_test']

        # Simplify read mode (Get or Lt)
        read_mode = read_mode_match.group(1)
        if read_mode.startswith('Lt'):
            read_mode = 'Lt'
        
        return {
            'backend': backend_match.group(1),
            'read_mode': read_mode,
            'read_percentage': int(read_percentage_match.group(1)),
            'direct_io': direct_io_match.group(1) == 'true',
            'zipf_exponent': float(zipf_exponent_match.group(1)),
            'ops_per_sec': parse_ops_value(mixed_test_match.group(1)),
            'filename': filepath.name
        }
        
    except Exception as e:
        print(f"Error parsing {filepath}: {e}")
        return None

def parse_logs_to_dataframe(log_dir: Path) -> pd.DataFrame:
    """Parse all .log files in a directory and return a pandas DataFrame."""
    results = []
    log_files = sorted(log_dir.glob("*.log"))
    
    if not log_files:
        print(f"No .log files found in {log_dir}")
        return pd.DataFrame()
    
    print(f"Found {len(log_files)} log files to process...")
    
    for log_file in log_files:
        result = parse_log_file(log_file)
        if result:
            results.append(result)
        else:
            print(f"Warning: Could not parse {log_file}")
    
    if not results:
        print("No valid results found in log files")
        return pd.DataFrame()
        
    df = pd.DataFrame(results)
    
    # Drop duplicates based on key parameters, keeping the last occurrence
    # This ensures that if multiple logs have the same parameters, the later one overwrites the earlier one
    key_columns = ['backend', 'read_mode', 'read_percentage', 'direct_io', 'zipf_exponent']
    initial_count = len(df)
    df = df.drop_duplicates(subset=key_columns, keep='last')
    final_count = len(df)
    
    if initial_count > final_count:
        duplicates_removed = initial_count - final_count
        print(f"Removed {duplicates_removed} duplicate entries (kept last occurrence for each parameter combination)")
        
    print(f"Successfully parsed {len(df)} unique benchmark results.")
    return df


def plot_throughput_bars(df: pd.DataFrame, read_mode: str, direct_io: bool, zipf: float, output_dir: Path):
    """
    Generates and saves a bar plot for a given configuration.
    """
    # Filter data for the specific plot
    plot_df = df[
        (df['read_mode'] == read_mode) &
        (df['direct_io'] == direct_io) &
        (df['zipf_exponent'] == zipf)
    ]

    if plot_df.empty:
        print(f"No data for plot: read_mode={read_mode}, direct_io={direct_io}, zipf={zipf}")
        return

    # Prepare data for plotting
    read_percents = sorted(plot_df['read_percentage'].unique())
    tidehunter_perf = plot_df[plot_df['backend'] == 'Tidehunter'].set_index('read_percentage')['ops_per_sec']
    rocksdb_perf = plot_df[plot_df['backend'] == 'Rocksdb'].set_index('read_percentage')['ops_per_sec']

    # Align performance data with all read percentages for this plot
    tidehunter_perf = tidehunter_perf.reindex(read_percents, fill_value=0)
    rocksdb_perf = rocksdb_perf.reindex(read_percents, fill_value=0)

    x = np.arange(len(read_percents))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots(figsize=(12, 7))
    rects1 = ax.bar(x - width/2, tidehunter_perf, width, label='Tidehunter')
    rects2 = ax.bar(x + width/2, rocksdb_perf, width, label='RocksDB')

    # Add some text for labels, title and axes ticks
    ax.set_ylabel('Throughput (ops/s)')
    ax.set_xlabel('Read Percentage (%)')
    title = (
        f'Throughput vs. Read Percentage\n'
        f'Read Mode: {read_mode}, Direct I/O: {"ON" if direct_io else "OFF"}, Zipf: {zipf}'
    )
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(read_percents)
    ax.legend()

    ax.bar_label(rects1, padding=3, fmt='%.0f')
    ax.bar_label(rects2, padding=3, fmt='%.0f')
    
    ax.yaxis.grid(True, linestyle='--', alpha=0.6)
    fig.tight_layout()

    # Save the plot
    filename = f'plot_rm_{read_mode}_dio_{"on" if direct_io else "off"}_zipf_{zipf}.png'
    filepath = output_dir / filename
    plt.savefig(filepath)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Generate plots from mixed-benchmark log files.")
    parser.add_argument("log_dir", help="Directory containing the .log files")
    parser.add_argument("--output-dir", "-o", help="Directory to save plots (default: <log_dir>/plots)")
    
    args = parser.parse_args()
    
    log_dir = Path(args.log_dir)
    if not log_dir.is_dir():
        print(f"Error: {log_dir} is not a directory", file=sys.stderr)
        sys.exit(1)
        
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = log_dir / "plots"
        
    output_dir.mkdir(exist_ok=True)
    
    # Parse logs
    df = parse_logs_to_dataframe(log_dir)
    print(df)
    
    if df.empty:
        print("No data to plot.")
        sys.exit(1)

    # Generate a plot for each combination of parameters
    plot_count = 0
    for read_mode in READ_MODES:
        for direct_io in DIRECT_IO_MODES:
            for zipf in ZIPF_EXPONENTS:
                # Check if there's any data for this combination before plotting
                has_data = not df[
                    (df['read_mode'] == read_mode) &
                    (df['direct_io'] == direct_io) &
                    (df['zipf_exponent'] == zipf)
                ].empty
                
                if has_data:
                    plot_throughput_bars(df, read_mode, direct_io, zipf, output_dir)
                    plot_count += 1

    if plot_count > 0:
        print(f"\n{plot_count} plot(s) generated and saved to: {output_dir}")
    else:
        print("\nNo matching data found for the specified plot configurations.")


if __name__ == "__main__":
    main() 