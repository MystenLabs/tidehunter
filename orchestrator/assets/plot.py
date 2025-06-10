# Copyright (c) Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

import json
import matplotlib.pyplot as plt
from datetime import datetime
import glob
from os.path import join


def load_data():
    data = []
    directory = "results/"
    for filename in glob.glob(join(directory, "measurements*.json")):
        with open(filename, "r") as f:
            data += [json.load(f)]
    return data


def plot_tps_line(data):
    # Extract the +Inf bucket
    inf_bucket = data["results"]["bench_writes_bucket"]["+Inf"]

    # Compute rate: Δvalue / Δtime, then divide by 1000
    timestamps = []
    rates = []

    for i in range(1, len(inf_bucket)):
        prev = inf_bucket[i - 1]
        curr = inf_bucket[i]
        dt = curr["timestamp"] - prev["timestamp"]
        dv = curr["value"] - prev["value"]
        rate = dv / dt / 1000

        # Convert timestamp to UTC datetime string
        avg_ts = (prev["timestamp"] + curr["timestamp"]) / 2
        ts_str = datetime.utcfromtimestamp(avg_ts).strftime("%Y-%m-%d %H:%M:%S")
        timestamps.append(ts_str)
        rates.append(rate)

    # Plot
    plt.plot(timestamps, rates, marker="o", label="rate(bench_writes_count[1m]) / 1000")


def finalize_plot():
    plt.xlabel("Time (GMT)")
    plt.ylabel("Write Rate (thousands ops/sec)")
    plt.title("Write Rate Over Time (UTC)")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.ylim(bottom=0)
    plt.legend()

    # Save to PNG
    plt.savefig("sample-plot.png")
    print("Saved sample-plot.png")


if __name__ == "__main__":
    plt.figure(figsize=(10, 5))
    for filename in load_data():
        plot_tps_line(filename)
    finalize_plot()
