#!/usr/bin/env python3
"""
Summarize benchmark logs from a directory.

This script parses all .log files in a given directory and creates a summary
table showing throughput (ops/s) for different configurations.

Tables are organized by:
- Direct IO mode (on/off) x Read mode (get/lt)
- Columns: Backend (Tidehunter/RocksDB)
- Rows: Read percentage (0%, 50%, 100%, etc.)
"""

import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Tuple, Optional

# Regex patterns for parsing log files
BACKEND_PATTERN = re.compile(r'backend:\s+(Tidehunter|Rocksdb)')
READ_MODE_PATTERN = re.compile(r'read_mode:\s+(Get|Lt\(\s*\d+\s*,?\s*\))', re.DOTALL)
READ_PERCENTAGE_PATTERN = re.compile(r'read_percentage:\s+(\d+)')
DIRECT_IO_PATTERN = re.compile(r'direct_io:\s+(true|false)')
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
        
        # Extract parameters
        backend_match = BACKEND_PATTERN.search(content)
        read_mode_match = READ_MODE_PATTERN.search(content)
        read_percentage_match = READ_PERCENTAGE_PATTERN.search(content)
        mixed_test_match = MIXED_TEST_PATTERN.search(content)
        
        # Check which fields are missing
        missing_fields = []
        if not backend_match:
            missing_fields.append("backend")
        if not read_mode_match:
            missing_fields.append("read_mode")
        if not read_percentage_match:
            missing_fields.append("read_percentage")
        if not mixed_test_match:
            missing_fields.append("mixed_test_result")
        
        if missing_fields:
            print(f"Warning: Could not parse fields {missing_fields} from {filepath}")
            return None
            
        # Check for direct IO
        direct_io_match = DIRECT_IO_PATTERN.search(content)
        direct_io = direct_io_match and direct_io_match.group(1) == 'true' if direct_io_match else False
        
        # Simplify read mode (Get or Lt)
        read_mode = read_mode_match.group(1)
        if read_mode.startswith('Lt'):
            read_mode = 'Lt'
        
        return {
            'backend': backend_match.group(1),
            'read_mode': read_mode,
            'read_percentage': int(read_percentage_match.group(1)),
            'direct_io': direct_io,
            'ops_per_sec': parse_ops_value(mixed_test_match.group(1)),
            'filename': filepath.name
        }
        
    except Exception as e:
        print(f"Error parsing {filepath}: {e}")
        return None

def format_ops(ops: float) -> str:
    """Format ops value for display."""
    if ops >= 1_000_000:
        return f"{ops/1_000_000:.2f}M"
    elif ops >= 1_000:
        return f"{ops/1_000:.1f}K"
    else:
        return f"{ops:.1f}"

def create_summary_table(results: list, direct_io: bool, read_mode: str) -> str:
    """Create a summary table for a specific direct_io and read_mode combination."""
    # Filter results for this table
    filtered = [r for r in results if r['direct_io'] == direct_io and r['read_mode'] == read_mode]
    
    if not filtered:
        return ""
    
    # Get unique read percentages and backends
    read_percentages = sorted(set(r['read_percentage'] for r in filtered))
    backends = sorted(set(r['backend'] for r in filtered))
    
    # Build table
    lines = []
    lines.append(f"\n{'='*60}")
    lines.append(f"Direct IO: {'ON' if direct_io else 'OFF'} | Read Mode: {read_mode}")
    lines.append(f"{'='*60}")
    
    # Header
    header = f"{'Read %':>11} |"
    for backend in backends:
        header += f" {backend:>15} |"
    lines.append(header)
    lines.append("-" * len(header))
    
    # Data rows
    for read_pct in read_percentages:
        row = f"{read_pct:>10}% |"
        for backend in backends:
            # Find matching result
            match = next((r for r in filtered if r['backend'] == backend and r['read_percentage'] == read_pct), None)
            if match:
                row += f" {format_ops(match['ops_per_sec']):>15} |"
            else:
                row += f" {'N/A':>15} |"
        lines.append(row)
    
    return "\n".join(lines)

def main():
    if len(sys.argv) != 2:
        print("Usage: python summarize_benchmarks.py <log_directory>")
        sys.exit(1)
    
    log_dir = Path(sys.argv[1])
    if not log_dir.is_dir():
        print(f"Error: {log_dir} is not a directory")
        sys.exit(1)
    
    # Parse all log files
    results = []
    log_files = sorted(log_dir.glob("*.log"))
    
    if not log_files:
        print(f"No .log files found in {log_dir}")
        sys.exit(1)
    
    print(f"Found {len(log_files)} log files to process...")
    
    for log_file in log_files:
        result = parse_log_file(log_file)
        if result:
            results.append(result)
    
    if not results:
        print("No valid results found in log files")
        sys.exit(1)
    
    print(f"Successfully parsed {len(results)} benchmark results\n")
    
    # Create summary output
    output_lines = ["BENCHMARK SUMMARY", "=" * 60]
    
    # Generate tables for each combination of direct_io and read_mode
    for direct_io in [False, True]:
        for read_mode in ['Get', 'Lt']:
            table = create_summary_table(results, direct_io, read_mode)
            if table:
                output_lines.append(table)
    
    # Add file mapping for reference
    output_lines.append(f"\n{'='*60}")
    output_lines.append("File Reference:")
    output_lines.append(f"{'='*60}")
    for result in sorted(results, key=lambda x: x['filename']):
        output_lines.append(
            f"{result['filename']}: {result['backend']}, "
            f"{'DirectIO' if result['direct_io'] else 'No-DirectIO'}, "
            f"{result['read_mode']}, {result['read_percentage']}% reads"
        )
    
    # Write to summary.txt
    summary_content = "\n".join(output_lines)
    summary_path = log_dir / "summary.txt"
    summary_path.write_text(summary_content)
    
    # Also print to console
    print(summary_content)
    print(f"\nSummary written to: {summary_path}")

if __name__ == "__main__":
    main() 