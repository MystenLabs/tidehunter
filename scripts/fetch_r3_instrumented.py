#!/usr/bin/env python3
"""Pull R3-instrumented metrics from Grafana for the 4 replicates.

Imports `fetch_grafana_variance` to reuse its credentialed Prometheus client.
Don't read or echo the imported module's contents.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import fetch_grafana_variance as fgv


def instant_query(datasource_id, query, t):
    """Run an instant query at time t (unix seconds). Returns list of {metric, value}."""
    from urllib.parse import urlencode
    from urllib.request import Request, urlopen

    params = urlencode({"query": query, "time": f"{t:.3f}"})
    url = (
        f"{fgv.GRAFANA_URL}/api/datasources/proxy/{datasource_id}"
        f"/api/v1/query?{params}"
    )
    req = Request(url)
    req.add_header("Authorization", f"Bearer {fgv.GRAFANA_CLOUD_TOKEN}")
    req.add_header("Accept", "application/json")
    resp = urlopen(req)
    data = json.loads(resp.read())
    if data.get("status") != "success":
        print(f"  instant query failed: {data}", file=sys.stderr)
        return []
    return data.get("data", {}).get("result", [])


def range_first_last(datasource_id, query, start, end, step=60):
    """Run a range query; return (first_sample, last_sample) per series."""
    series = fgv.query_prometheus(
        fgv.GRAFANA_URL, datasource_id, fgv.GRAFANA_CLOUD_TOKEN,
        query, start, end, step=step,
    )
    out = []
    for s in series:
        vals = s.get("values", [])
        if not vals:
            continue
        first = float(vals[0][1])
        last = float(vals[-1][1])
        out.append({"metric": s.get("metric", {}), "first": first, "last": last})
    return out


def probe_label(datasource_id, metric_name, t):
    """Discover what label identifies the host for this metric (host/instance/etc)."""
    series = instant_query(datasource_id, metric_name, t)
    if not series:
        return None, []
    labels = series[0].get("metric", {})
    host_keys = [k for k in labels if k in ("host", "instance", "hostname", "pod", "node_name")]
    return host_keys[0] if host_keys else None, list(labels.keys())


# OS hostname in the log doesn't always match the Prometheus scrape label.
# Map known aliases here.
HOSTNAME_ALIASES = {
    "lively-pigeon": "benchmark-00",
}


def analyze_log(log_path: Path, datasource_id: str):
    phases, log_hostname = fgv.parse_log(str(log_path))
    hostname = HOSTNAME_ALIASES.get(log_hostname, log_hostname)
    if hostname != log_hostname:
        print(f"  remapped hostname {log_hostname!r} -> {hostname!r}")
    if "mixed" not in phases or "end" not in phases["mixed"]:
        print(f"  no mixed phase end in {log_path.name}; skipping")
        return None
    m = phases["mixed"]
    w = phases.get("write", {})
    print(f"\n=== {log_path.name} (host={hostname}) ===")
    print(f"  write : {w.get('start'):.0f} -> {w.get('end'):.0f} "
          f"({w.get('end', 0) - w.get('start', 0):.0f}s)")
    print(f"  mixed : {m['start']:.0f} -> {m['end']:.0f} ({m['end'] - m['start']:.0f}s)")

    # Probe what host label is exposed by Tidehunter metrics
    host_key, all_keys = probe_label(datasource_id, "loaded_key_bytes", m["end"] - 60)
    if not host_key:
        # Try probing without filter
        series = instant_query(datasource_id, "loaded_key_bytes", m["end"] - 60)
        if not series:
            print(f"  no loaded_key_bytes samples at {m['end'] - 60:.0f}; "
                  f"metric not exported during this window")
            return {"log": log_path.name, "host": hostname, "available": False}
        print(f"  loaded_key_bytes labels available: {all_keys}")
        host_key = None

    print(f"  host_key inferred: {host_key}")

    # Try filtering by hostname with the discovered key
    filt = f'{{{host_key}="{hostname}"}}' if host_key else ""

    out = {"log": log_path.name, "host": hostname, "host_key": host_key,
           "phase": {"start": m["start"], "end": m["end"]}, "metrics": {}}

    # ---------- end-of-phase gauges -----------------------------------------
    t_end = m["end"] - 60
    for metric in ("loaded_key_bytes", "flat_index_bytes", "dirty_keys",
                   "memory_estimate", "wal_mmap_bytes", "value_cache_size",
                   "pending_table_len", "flush_pending", "wal_synced_position"):
        series = instant_query(datasource_id, f"{metric}{filt}", t_end)
        out["metrics"][metric] = [
            {"labels": {k: v for k, v in s["metric"].items() if k != "__name__"},
             "value": float(s["value"][1])}
            for s in series
        ]

    # ---------- counters: delta over the mixed phase ------------------------
    # Use first/last value across the phase window to avoid rate windowing.
    duration = int(m["end"] - m["start"])
    # Note: Mimir / Rust prometheus client does NOT add `_total` to counters,
    # so we query the raw counter name.
    for metric in ("lookup_result", "flush_update",
                   "flushed_keys", "flushed_bytes", "unload",
                   "read_bytes", "wal_written_bytes_type",
                   "wal_write_wait", "snapshot_written_bytes",
                   "flush_count", "flush_backpressure_count",
                   "snapshot_force_unload"):
        # increase(...) over the entire phase window:
        q = f"increase({metric}{filt}[{duration}s])"
        series = instant_query(datasource_id, q, m["end"])
        out["metrics"][metric] = [
            {"labels": {k: v for k, v in s["metric"].items() if k != "__name__"},
             "increase": float(s["value"][1])}
            for s in series
        ]

    # ---------- derived: cache-hit-ratio summary ----------------------------
    lookup = {s["labels"].get("source", "?") + "/" + s["labels"].get("result", "?"):
              s["increase"] for s in out["metrics"]["lookup_result"]}
    total = sum(lookup.values())
    if total > 0:
        out["derived"] = {
            "total_lookups": total,
            "by_source": {},
        }
        # Group by source: sum across Found/NotFound
        by_src = {}
        for k, v in lookup.items():
            src = k.split("/")[0]
            by_src[src] = by_src.get(src, 0) + v
        for src, n in sorted(by_src.items(), key=lambda kv: -kv[1]):
            out["derived"]["by_source"][src] = {"count": n, "share": n / total}

    return out


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <log_file> [<log_file> ...]", file=sys.stderr)
        sys.exit(1)

    ds_id = fgv.resolve_datasource_id(
        fgv.GRAFANA_URL, fgv.GRAFANA_DATASOURCE_NAME, fgv.GRAFANA_CLOUD_TOKEN
    )
    print(f"datasource_id = {ds_id}", file=sys.stderr)

    results = []
    for p in sys.argv[1:]:
        r = analyze_log(Path(p), ds_id)
        if r is not None:
            results.append(r)

    print("\n\n========== SUMMARY ==========")
    print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()
