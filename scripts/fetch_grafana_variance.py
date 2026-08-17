#!/usr/bin/env python3
"""
Fetch throughput/latency variance data from Grafana Cloud for a benchmark run.

Parses a benchmark log file to extract phase timestamps, then queries
Grafana Cloud's Prometheus API for time-series data to compute variance.

Usage:
    Fill in GRAFANA_CLOUD_TOKEN at the top of this script, then:
    python3 scripts/fetch_grafana_variance.py <log_file>

Example:
    python3 scripts/fetch_grafana_variance.py \
        logs/logs-add-storage-backends/2025-11-21-1KB/node-2025-11-19-10-48-58-0.log
"""

import json
import re
import sys
from urllib.request import Request, urlopen
from urllib.parse import urlencode
import math

# ---- Fill these in ----
GRAFANA_CLOUD_TOKEN = ""
GRAFANA_URL = "https://metrics.sui.io"
GRAFANA_DATASOURCE_NAME = "mysten-metrics-internal"


def parse_log(log_path):
    """Parse a benchmark log file to extract phase timestamps (as unix seconds) and hostname."""
    phases = {}
    hostname = None
    current_phase = None

    with open(log_path) as f:
        for line in f:
            m = re.match(r"\[(\d+)\]\s+(.*)", line)
            if not m:
                continue
            ts_ms = int(m.group(1))
            ts_s = ts_ms / 1000.0
            text = m.group(2)

            hm = re.match(r"Hostname:\s+(\S+)", text)
            if hm:
                hostname = hm.group(1)

            if "Starting write test" in text:
                current_phase = "write"
                phases["write"] = {"start": ts_s}
            elif "Write test done" in text:
                if "write" in phases:
                    phases["write"]["end"] = ts_s
                    phases["write"]["summary"] = text
                current_phase = None
            elif "Starting mixed" in text:
                current_phase = "mixed"
                phases["mixed"] = {"start": ts_s, "description": text}
            elif "Mixed test done" in text:
                if "mixed" in phases:
                    phases["mixed"]["end"] = ts_s
                    phases["mixed"]["summary"] = text
                current_phase = None

    return phases, hostname


def resolve_datasource_id(grafana_url, datasource_name, token):
    """Resolve a Grafana datasource name to its numeric ID."""
    url = f"{grafana_url}/api/datasources/name/{datasource_name}"
    req = Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/json")
    resp = urlopen(req)
    data = json.loads(resp.read())
    return str(data["id"])


def query_prometheus(grafana_url, datasource_id, token, query, start, end, step=60):
    """Query Grafana Cloud's Prometheus-compatible API."""
    params = urlencode({
        "query": query,
        "start": f"{start:.3f}",
        "end": f"{end:.3f}",
        "step": str(step),
    })
    url = f"{grafana_url}/api/datasources/proxy/{datasource_id}/api/v1/query_range?{params}"

    req = Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/json")

    resp = urlopen(req)
    data = json.loads(resp.read())

    if data.get("status") != "success":
        print(f"Query failed: {data}", file=sys.stderr)
        return []

    return data.get("data", {}).get("result", [])


def extract_values(result):
    """Extract float values from a Prometheus query_range result, filtering NaN/Inf."""
    if not result:
        return []
    # Take the first result series (or sum across if multiple)
    values = []
    for series in result:
        for ts, val in series.get("values", []):
            v = float(val)
            if not (math.isnan(v) or math.isinf(v)):
                values.append((float(ts), v))
    return values


def compute_stats(values):
    """Compute mean, median, stddev, CV, and percentiles from (timestamp, value) pairs."""
    if not values:
        return {"mean": None, "median": None, "stddev": None, "cv": None,
                "p99": None, "p999": None, "min": None, "max": None, "n_samples": 0}
    vals = sorted([v for _, v in values])
    n = len(vals)
    mean = sum(vals) / n
    median = vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2

    if n < 2:
        return {"mean": mean, "median": median, "stddev": 0, "cv": 0,
                "p99": vals[-1], "p999": vals[-1], "min": vals[0], "max": vals[-1],
                "n_samples": n}

    variance = sum((v - mean) ** 2 for v in vals) / (n - 1)
    stddev = math.sqrt(variance)
    cv = stddev / mean if mean != 0 else float("inf")
    p99 = vals[min(int(n * 0.99), n - 1)]
    p999 = vals[min(int(n * 0.999), n - 1)]

    return {"mean": mean, "median": median, "stddev": stddev, "cv": cv,
            "p99": p99, "p999": p999, "min": vals[0], "max": vals[-1],
            "n_samples": n}


def analyze_phase(phase_name, phase_info, grafana_url, datasource_id, token, host):
    """Query and analyze metrics for a single benchmark phase."""
    start = phase_info["start"]
    end = phase_info["end"]
    duration = end - start
    host_filter = f'{{host="{host}"}}' if host else ""

    # Shift the query window forward by one rate window so that every rate(..[1m])
    # sample is fully inside the phase. Without this, the first sample at t=start
    # looks back into the inter-phase pause and yields 0, which artificially inflates
    # the coefficient of variation (e.g. 0.07 -> 0.20 for write-heavy mixed phases).
    RATE_WINDOW_S = 60
    q_start = start + RATE_WINDOW_S
    q_end = end

    print(f"\n{'=' * 60}")
    print(f"Phase: {phase_name.upper()} ({duration:.0f}s)")
    if "summary" in phase_info:
        print(f"Summary: {phase_info['summary']}")
    if "description" in phase_info:
        print(f"Description: {phase_info['description']}")
    print(f"Time window: {start:.0f} -> {end:.0f}  "
          f"(queries: {q_start:.0f} -> {q_end:.0f})")
    if host:
        print(f"Host: {host}")
    print(f"{'=' * 60}")

    # --- Throughput over time ---
    print(f"\n--- Throughput ---")
    for metric in ["bench_writes", "bench_reads"]:
        result = query_prometheus(
            grafana_url, datasource_id, token,
            f"rate({metric}_count{host_filter}[1m])",
            q_start, q_end, step=60
        )
        values = extract_values(result)
        if values:
            stats = compute_stats(values)
            print(f"  {metric}: mean={stats['mean']:.0f} ops/s, "
                  f"stddev={stats['stddev']:.0f}, "
                  f"CV={stats['cv']:.4f}, "
                  f"n={stats['n_samples']} samples")
        else:
            print(f"  {metric}: no data")

    # --- Latency percentiles ---
    print(f"\n--- Latency Percentiles (microseconds) ---")
    for metric in ["bench_writes", "bench_reads"]:
        percentiles = [0.5, 0.9, 0.99, 0.999, 0.9999]
        labels = ["p50", "p90", "p99", "p99.9", "p99.99"]
        pct_values = {}

        for pct, label in zip(percentiles, labels):
            result = query_prometheus(
                grafana_url, datasource_id, token,
                f'histogram_quantile({pct}, rate({metric}_bucket{host_filter}[1m]))',
                q_start, q_end, step=60
            )
            values = extract_values(result)
            if values:
                stats = compute_stats(values)
                pct_values[label] = stats

        if pct_values:
            parts = []
            for label in labels:
                if label in pct_values and pct_values[label]["mean"] is not None:
                    parts.append(f"{label}={pct_values[label]['mean']:.1f}")
            print(f"  {metric}: {', '.join(parts)}")
        else:
            print(f"  {metric}: no data")

    # --- Latency variance over time ---
    print(f"\n--- Latency Variance Over Time ---")
    for metric in ["bench_writes", "bench_reads"]:
        result = query_prometheus(
            grafana_url, datasource_id, token,
            f"rate({metric}_sum{host_filter}[1m]) / rate({metric}_count{host_filter}[1m])",
            q_start, q_end, step=60
        )
        values = extract_values(result)
        if values:
            stats = compute_stats(values)
            print(f"  {metric} mean latency: mean={stats['mean']:.1f}us, "
                  f"stddev={stats['stddev']:.1f}us, "
                  f"CV={stats['cv']:.4f}, "
                  f"n={stats['n_samples']} samples")
        else:
            print(f"  {metric}: no data")

    # --- Contention metrics ---
    print(f"\n--- Contention ---")
    contention_metrics = [
        ("large_table_contention", "Large table lock contention"),
        ("snapshot_lock_time_mcs", "Snapshot lock hold time"),
    ]
    for metric, description in contention_metrics:
        # Aggregate wait accumulation, summed across all threads and keyspaces, in microseconds
        # per wall-clock second. Divided by (threads * 1e6) this gives the fraction of foreground
        # thread time spent waiting on this lock.
        agg_result = query_prometheus(
            grafana_url, datasource_id, token,
            f"sum(rate({metric}_sum{host_filter}[1m]))",
            q_start, q_end, step=60
        )
        agg_values = extract_values(agg_result)
        if agg_values:
            agg_stats = compute_stats(agg_values)
            print(f"  {description} ({metric}) aggregate wait rate:")
            print(f"    Mean: {agg_stats['mean']:.1f}us/s, Median: {agg_stats['median']:.1f}us/s, "
                  f"Max: {agg_stats['max']:.1f}us/s")
            # Mixed-phase thread count is 36; adjust if needed.
            frac_pct = agg_stats["mean"] / 36_000_000 * 100
            print(f"    Fraction of 36-thread-time: {frac_pct:.6f}%")
        else:
            print(f"  {description} ({metric}) aggregate wait rate: no data")

        # Per-interval mean contention (wait per contended acquisition)
        result = query_prometheus(
            grafana_url, datasource_id, token,
            f"rate({metric}_sum{host_filter}[1m]) / rate({metric}_count{host_filter}[1m])",
            q_start, q_end, step=60
        )
        values = extract_values(result)
        if not values:
            print(f"  {description} ({metric}): no data")
            continue

        stats = compute_stats(values)
        print(f"  {description} ({metric}):")
        print(f"    Mean: {stats['mean']:.1f}us, Median: {stats['median']:.1f}us, "
              f"Stddev: {stats['stddev']:.1f}us, CV: {stats['cv']:.4f}")
        print(f"    Min: {stats['min']:.1f}us, Max: {stats['max']:.1f}us, "
              f"P99: {stats['p99']:.1f}us, P99.9: {stats['p999']:.1f}us")
        print(f"    Samples: {stats['n_samples']}")

        # Also get histogram percentiles for the raw contention distribution
        pct_parts = []
        for pct, label in [(0.5, "p50"), (0.9, "p90"), (0.99, "p99"), (0.999, "p99.9")]:
            result = query_prometheus(
                grafana_url, datasource_id, token,
                f"histogram_quantile({pct}, rate({metric}_bucket{host_filter}[1m]))",
                q_start, q_end, step=60
            )
            pct_values = extract_values(result)
            if pct_values:
                pct_stats = compute_stats(pct_values)
                pct_parts.append(f"{label}={pct_stats['mean']:.1f}")
        if pct_parts:
            print(f"    Histogram percentiles (avg over time): {', '.join(pct_parts)}")


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <log_file>", file=sys.stderr)
        sys.exit(1)

    log_file = sys.argv[1]

    if GRAFANA_CLOUD_TOKEN == "PLACEHOLDER":
        print("Error: Set GRAFANA_CLOUD_TOKEN in the script", file=sys.stderr)
        sys.exit(1)

    # Parse log file
    print(f"Parsing log: {log_file}")
    phases, hostname = parse_log(log_file)

    if not phases:
        print("Error: No benchmark phases found in log file", file=sys.stderr)
        sys.exit(1)

    print(f"Found phases: {', '.join(phases.keys())}")
    if hostname:
        print(f"Host: {hostname}")

    # Resolve datasource name to numeric ID
    print(f"Resolving datasource '{GRAFANA_DATASOURCE_NAME}'...")
    datasource_id = resolve_datasource_id(GRAFANA_URL, GRAFANA_DATASOURCE_NAME, GRAFANA_CLOUD_TOKEN)
    print(f"Datasource ID: {datasource_id}")

    # Analyze each phase
    for phase_name, phase_info in phases.items():
        if "start" not in phase_info or "end" not in phase_info:
            print(f"Skipping {phase_name}: missing start/end timestamps")
            continue
        analyze_phase(
            phase_name, phase_info,
            GRAFANA_URL, datasource_id, GRAFANA_CLOUD_TOKEN, hostname
        )

    print(f"\n{'=' * 60}")
    print("Done.")


if __name__ == "__main__":
    main()
