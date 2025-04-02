#!/usr/bin/env python3

import os
import re
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def parse_summary_file(file_path):
    """Parse the summary.log file and return a DataFrame."""
    # Read the file
    with open(file_path, 'r') as f:
        lines = f.readlines()
    
    # Find the header line
    header_idx = None
    for i, line in enumerate(lines):
        if "File Size" in line and "Window" in line and "Index Type" in line:
            header_idx = i
            break
    
    if header_idx is None:
        raise ValueError("Could not find header line in summary file")
    
    # Extract column names from the header
    header = lines[header_idx].strip()
    
    # Split by | and strip whitespace
    columns = [col.strip() for col in header.split('|')]
    
    # Skip the separator line
    data_start_idx = header_idx + 2
    
    # Parse the data
    data = []
    for line in lines[data_start_idx:]:
        line = line.strip()
        if not line:  # Skip empty lines
            continue
        
        # Split by | and strip whitespace
        row = [col.strip() for col in line.split('|')]
        
        # Skip if the row doesn't have the same number of columns as the header
        if len(row) != len(columns):
            continue
        
        data.append(row)
    
    # Create DataFrame
    df = pd.DataFrame(data, columns=columns)
    
    # Convert numeric columns to appropriate types
    numeric_cols = ["Tput (ops/s)", "Scan us", "IO s", "IO GB"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Convert window and threads columns to numeric
    df["Window"] = pd.to_numeric(df["Window"], errors='coerce')
    df["Threads"] = pd.to_numeric(df["Threads"], errors='coerce')
    
    return df

def plot_throughput(df, output_dir):
    """Generate throughput plot with thread count on x-axis."""
    # Create separate plots for each DIO value
    for dio_value in ["Yes", "No"]:
        plt.figure(figsize=(12, 8))
        dio_name = "DIO" if dio_value == "Yes" else "No-DIO"
        
        # Filter for each combination of Index Type and Window
        for index_type in ["Header", "Uniform"]:
            # Get unique window sizes for this combination
            filtered_df = df[(df["Index Type"] == index_type) & (df["DIO"] == dio_value)]
            window_sizes = filtered_df["Window"].unique()
            
            for window in sorted(window_sizes):
                window_df = filtered_df[filtered_df["Window"] == window]
                
                if not window_df.empty:
                    # Group by thread count and calculate mean throughput
                    grouped = window_df.groupby("Threads")["Tput (ops/s)"].mean().reset_index()
                    
                    # Sort by thread count
                    grouped = grouped.sort_values("Threads")
                    
                    # Plot
                    label = f"{index_type} Win-{window}"
                    plt.plot(grouped["Threads"], grouped["Tput (ops/s)"], marker='o', label=label)
        
        plt.title(f"Throughput vs Thread Count ({dio_name})")
        plt.xlabel("Thread Count")
        plt.ylabel("Throughput (ops/s)")
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        
        # Save the plot with DIO status in the filename
        plt.savefig(os.path.join(output_dir, f"throughput_plot_{dio_name}.png"))
        plt.close()

def plot_scan_io_time(df, output_dir):
    """Generate scan and I/O time plot with thread count on x-axis."""
    # Convert scan time from microseconds to seconds
    df["Scan s"] = df["Scan us"] / 1000000
    
    # Create separate plots for each DIO value
    for dio_value in ["Yes", "No"]:
        plt.figure(figsize=(12, 8))
        dio_name = "DIO" if dio_value == "Yes" else "No-DIO"
        
        # Filter for each index type and window size
        for index_type in ["Header", "Uniform"]:
            window_sizes = df[(df["Index Type"] == index_type) & 
                              (df["DIO"] == dio_value)]["Window"].unique()
            
            for window in sorted(window_sizes):
                filtered_df = df[(df["Index Type"] == index_type) & 
                                 (df["Window"] == window) & 
                                 (df["DIO"] == dio_value)]
                
                if not filtered_df.empty:
                    # Group by thread count and calculate mean scan and IO time
                    grouped_scan = filtered_df.groupby("Threads")["Scan s"].mean().reset_index()
                    grouped_io = filtered_df.groupby("Threads")["IO s"].mean().reset_index()
                    
                    # Sort by thread count
                    grouped_scan = grouped_scan.sort_values("Threads")
                    grouped_io = grouped_io.sort_values("Threads")
                    
                    # Plot
                    plt.plot(grouped_scan["Threads"], grouped_scan["Scan s"], marker='o', 
                             label=f"{index_type} Win-{window} Scan")
                    plt.plot(grouped_io["Threads"], grouped_io["IO s"], marker='s', 
                             label=f"{index_type} Win-{window} I/O")
        
        plt.title(f"Scan and I/O Time vs Thread Count ({dio_name})")
        plt.xlabel("Thread Count")
        plt.ylabel("Time (seconds)")
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        
        # Save the plot with DIO status in the filename
        plt.savefig(os.path.join(output_dir, f"scan_io_time_plot_{dio_name}.png"))
        plt.close()

def plot_io_amount(df, output_dir):
    """Generate I/O amount plot with thread count on x-axis."""
    # Create separate plots for each DIO value
    for dio_value in ["Yes", "No"]:
        plt.figure(figsize=(12, 8))
        dio_name = "DIO" if dio_value == "Yes" else "No-DIO"
        
        # Filter for each index type and window size
        for index_type in ["Header", "Uniform"]:
            window_sizes = df[(df["Index Type"] == index_type) & 
                              (df["DIO"] == dio_value)]["Window"].unique()
            
            for window in sorted(window_sizes):
                filtered_df = df[(df["Index Type"] == index_type) & 
                                 (df["Window"] == window) & 
                                 (df["DIO"] == dio_value)]
                
                if not filtered_df.empty:
                    # Group by thread count and calculate mean IO amount
                    grouped = filtered_df.groupby("Threads")["IO GB"].mean().reset_index()
                    
                    # Sort by thread count
                    grouped = grouped.sort_values("Threads")
                    
                    # Plot
                    plt.plot(grouped["Threads"], grouped["IO GB"], marker='o', 
                             label=f"{index_type} Win-{window}")
        
        plt.title(f"I/O Amount vs Thread Count ({dio_name})")
        plt.xlabel("Thread Count")
        plt.ylabel("I/O Amount (GB)")
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        
        # Save the plot with DIO status in the filename
        plt.savefig(os.path.join(output_dir, f"io_amount_plot_{dio_name}.png"))
        plt.close()

def main():
    parser = argparse.ArgumentParser(description="Generate plots from benchmark summary file")
    parser.add_argument("summary_file", help="Path to the summary.log file")
    parser.add_argument("--output-dir", "-o", help="Directory to save plots (default: same directory as summary_file)")
    
    args = parser.parse_args()
    
    # Use the directory of the summary file as the default output directory
    if args.output_dir is None:
        args.output_dir = os.path.dirname(args.summary_file) or '.'
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Parse the summary file
    df = parse_summary_file(args.summary_file)
    
    # Generate plots
    plot_throughput(df, args.output_dir)
    plot_scan_io_time(df, args.output_dir)
    plot_io_amount(df, args.output_dir)
    
    print(f"Plots generated and saved to {args.output_dir}/")

if __name__ == "__main__":
    main() 