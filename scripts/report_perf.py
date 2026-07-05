#!/usr/bin/env python3
"""
Collect processing-performance metrics for a RUNNING Flink job and append one
row to Results/perf.csv.

What it measures (via the JobManager REST API, sampled while the job runs):

  * throughput  — records/second emitted by the Kafka *source* vertex, both the
                  average over the active window (total records / active time)
                  and the peak instantaneous rate (sum of numRecordsOutPerSecond
                  across the source subtasks);
  * latency     — end-to-end source->sink latency in ms, read from Flink's
                  latency-marker gauges (*.latency_p50/p95/p99). Requires
                  'metrics.latency.interval' set on the cluster (done in
                  docker-compose.yml). Left blank if the gauges are absent;
  * busy %      — busyTimeMsPerSecond of the busiest vertex (pipeline
                  utilisation, i.e. how close to saturation the job runs);
  * backpressure% — backPressuredTimeMsPerSecond of the busiest vertex
                  (identifies the bottleneck operator).

Usage (run it *while* the job is processing, e.g. alongside the producer):

    python scripts/report_perf.py --engine flink --query q1 \
        --implementation table --parallelism 4 --exp 08_par2

The tool waits for the source to start emitting, samples until the record
count stops growing (idle), then writes the aggregate row and exits.

Companion of report_late_drops.py (completeness). Spark writes its own perf
row from spark_runtime.py using the same schema, so Results/perf.csv holds
both engines side by side for the report.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from merge_utils import PROJECT_ROOT

DEFAULT_FLINK_URL = "http://localhost:8081"
DEFAULT_OUTPUT = PROJECT_ROOT / "Results" / "perf.csv"

# Unified schema shared with the Spark writer in spark_runtime.py.
PERF_HEADER = [
    "timestamp_utc", "engine", "implementation", "query", "experiment",
    "parallelism", "total_records", "active_seconds",
    "throughput_rec_s_avg", "throughput_rec_s_max",
    "latency_ms_avg", "latency_ms_max",
    "busy_pct_avg", "backpressure_pct_avg", "notes",
]

# Standard task-scope metrics (no commas in the id -> fetchable in one call).
RATE_OUT = "numRecordsOutPerSecond"
COUNT_OUT = "numRecordsOut"
COUNT_IN = "numRecordsIn"
BUSY = "busyTimeMsPerSecond"
BACKPRESSURE = "backPressuredTimeMsPerSecond"
LATENCY_RE = re.compile(r"latency_p(50|95|99)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--engine", default="flink", choices=["flink"],
                        help="Only 'flink' here; Spark writes its own row.")
    parser.add_argument("--query", default="", help="q1/q2/q3 (labels the row and picks the job).")
    parser.add_argument("--implementation", default="table", help="table/datastream (labels the row).")
    parser.add_argument("--experiment", "--exp", "-e", dest="experiment", default=None,
                        help="Experiment name for the row (default: base).")
    parser.add_argument("--parallelism", type=int, default=0, help="Parallelism for the row (0 = read from job).")
    parser.add_argument("--flink-url", default=DEFAULT_FLINK_URL)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--sample-interval", type=float, default=2.0)
    parser.add_argument("--startup-timeout", type=float, default=120.0,
                        help="Give up if the source never emits within this window.")
    parser.add_argument("--idle-samples", type=int, default=4,
                        help="Consecutive unchanged samples that mean 'done'.")
    parser.add_argument("--job-id", default=None, help="Force a specific job id.")
    return parser.parse_args()


def http_get_json(url: str, timeout: float = 10.0):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.load(resp)


def pick_job(flink_url: str, query: str, job_id: str | None) -> dict:
    data = http_get_json(f"{flink_url}/jobs/overview")
    running = [j for j in data.get("jobs", []) if j.get("state") == "RUNNING"]
    if job_id:
        for j in running:
            if j["jid"] == job_id:
                return j
        raise SystemExit(f"Job id {job_id} is not RUNNING.")
    if not running:
        raise SystemExit("No RUNNING Flink job found (start the job first).")
    if query:
        matches = [j for j in running if f".{query}_" in j.get("name", "") or query in j.get("name", "")]
        if matches:
            return matches[0]
    if len(running) > 1:
        print(f"WARNING: {len(running)} running jobs, no query match; using the first.", file=sys.stderr)
    return running[0]


def vertex_metric_ids(flink_url: str, jid: str, vid: str) -> list[str]:
    data = http_get_json(f"{flink_url}/jobs/{jid}/vertices/{vid}/subtasks/metrics")
    return [m["id"] for m in data]


def fetch_metrics(flink_url: str, jid: str, vid: str, ids: list[str]) -> dict[str, dict]:
    """Return {id: {min,max,avg,sum}} for the given (comma-free) metric ids."""
    if not ids:
        return {}
    query = urllib.parse.urlencode({"get": ",".join(ids)})
    values = http_get_json(f"{flink_url}/jobs/{jid}/vertices/{vid}/subtasks/metrics?{query}")
    return {v["id"]: v for v in values}


def main() -> None:
    args = parse_args()
    experiment = args.experiment or "base"
    flink_url = args.flink_url.rstrip("/")

    try:
        job = pick_job(flink_url, args.query, args.job_id)
    except (urllib.error.URLError, OSError) as exc:
        print(f"ERROR: cannot reach Flink REST at {flink_url}: {exc}", file=sys.stderr)
        sys.exit(1)

    jid = job["jid"]
    job_name = job.get("name", "?")
    print(f"Sampling job {jid} ({job_name[:60]}...)")

    detail = http_get_json(f"{flink_url}/jobs/{jid}")
    vertices = [(v["id"], v.get("name", ""), int(v.get("parallelism", 0))) for v in detail.get("vertices", [])]
    parallelism = args.parallelism or max((p for _, _, p in vertices), default=0)

    # Cache the metric-id lists per vertex (they are stable for the job's life),
    # and pre-select the latency gauges (present only if latency tracking is on).
    latency_ids: dict[str, list[str]] = {}
    for vid, _, _ in vertices:
        ids = vertex_metric_ids(flink_url, jid, vid)
        latency_ids[vid] = [i for i in ids if LATENCY_RE.search(i)]

    samples: list[tuple[float, float, float, float, float]] = []  # t, src_out_total, src_rate, busy, bp
    lat_p50: list[float] = []
    lat_p99: list[float] = []

    started = time.monotonic()
    idle = 0
    last_total = -1.0
    seen_activity = False

    while True:
        now = time.monotonic()
        src_out_total = 0.0
        src_rate = 0.0
        busy_max = 0.0
        bp_max = 0.0

        for vid, vname, _ in vertices:
            m = fetch_metrics(flink_url, jid, vid, [COUNT_IN, COUNT_OUT, RATE_OUT, BUSY, BACKPRESSURE])
            n_in = m.get(COUNT_IN, {}).get("sum", 0.0)
            n_out = m.get(COUNT_OUT, {}).get("sum", 0.0)
            # Source vertex = reads from Kafka: numRecordsIn is 0 (no upstream).
            is_source = (float(n_in) == 0.0 and float(n_out) > 0.0) or "Source" in vname
            if is_source:
                src_out_total = max(src_out_total, float(n_out))
                src_rate = max(src_rate, float(m.get(RATE_OUT, {}).get("sum", 0.0)))
            busy_max = max(busy_max, float(m.get(BUSY, {}).get("max", 0.0)))
            bp_max = max(bp_max, float(m.get(BACKPRESSURE, {}).get("max", 0.0)))

            for lid in latency_ids.get(vid, []):
                lv = fetch_metrics(flink_url, jid, vid, [lid]).get(lid, {})
                val = float(lv.get("max", 0.0))
                if val <= 0.0:
                    continue
                if lid.endswith("p50"):
                    lat_p50.append(val)
                elif lid.endswith("p99"):
                    lat_p99.append(val)

        if src_out_total > 0.0:
            seen_activity = True
        if seen_activity:
            samples.append((now, src_out_total, src_rate, busy_max, bp_max))

        # Idle detection: source record count stopped growing.
        if seen_activity and abs(src_out_total - last_total) < 1.0:
            idle += 1
        else:
            idle = 0
        last_total = src_out_total

        if seen_activity and idle >= args.idle_samples:
            print(f"Source idle for {idle} samples; stopping.")
            break
        if not seen_activity and (now - started) > args.startup_timeout:
            print("ERROR: source never emitted within startup timeout.", file=sys.stderr)
            sys.exit(1)

        # Stop if the job left the RUNNING state.
        state = http_get_json(f"{flink_url}/jobs/{jid}").get("state")
        if state not in ("RUNNING", "RESTARTING"):
            print(f"Job no longer RUNNING (state={state}); stopping.")
            break

        time.sleep(args.sample_interval)

    if not samples:
        print("ERROR: no active samples collected.", file=sys.stderr)
        sys.exit(1)

    active = [s for s in samples if s[2] > 0.0 or s[1] > 0.0]
    first_t, first_total = active[0][0], active[0][1]
    last_t, last_total_v = active[-1][0], active[-1][1]
    active_seconds = max(last_t - first_t, 1e-6)
    total_records = max(last_total_v - first_total, last_total_v)
    thr_avg = total_records / active_seconds
    thr_max = max((s[2] for s in samples), default=0.0)
    busy_avg = sum(s[3] for s in samples) / len(samples) / 10.0     # ms/s -> %
    bp_avg = sum(s[4] for s in samples) / len(samples) / 10.0       # ms/s -> %

    lat_avg = (sum(lat_p50) / len(lat_p50)) if lat_p50 else ""
    lat_max = (max(lat_p99) if lat_p99 else "")
    notes = "flink source->sink latency markers" if lat_p50 else "latency: enable metrics.latency.interval"

    row = [
        datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "flink", args.implementation, args.query or "?", experiment, parallelism,
        int(total_records), round(active_seconds, 1),
        round(thr_avg, 1), round(thr_max, 1),
        (round(lat_avg, 1) if lat_avg != "" else ""),
        (round(lat_max, 1) if lat_max != "" else ""),
        round(busy_avg, 1), round(bp_avg, 1), notes,
    ]

    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_header = not args.output.exists()
    with args.output.open("a", encoding="utf-8", newline="") as out:
        writer = csv.writer(out)
        if write_header:
            writer.writerow(PERF_HEADER)
        writer.writerow(row)

    print(f"throughput avg={thr_avg:.0f} rec/s  max={thr_max:.0f} rec/s  "
          f"busy={busy_avg:.0f}%  backpressure={bp_avg:.0f}%  "
          f"latency_p50={lat_avg if lat_avg=='' else round(lat_avg,1)}  "
          f"records={int(total_records)}  active={active_seconds:.0f}s")
    print(f"Appended to {args.output}")


if __name__ == "__main__":
    main()
