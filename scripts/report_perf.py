#!/usr/bin/env python3

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

# Ingestion is measured at the Kafka SOURCE OPERATOR, not at the source *vertex*.
# The source is chained (e.g. "Source: flights -> Calc -> LocalWindowAggregate"),
# so the vertex-level numRecordsOut is the LAST chained operator's output (window
# pre-aggregates): it undercounts ingestion AND scales with parallelism (one set
# of partials per subtask), which fakes a linear throughput speed-up. The
# operator-scoped "Source....numRecordsOut" counts events read from Kafka, is
# invariant to parallelism, and matches how the Spark writer counts input rows,
# so the two engines stay comparable in Results/perf.csv.
SRC_COUNT_RE = re.compile(r"(?i)^Source.*\.numRecordsOut$")
SRC_RATE_RE = re.compile(r"(?i)^Source.*\.numRecordsOutPerSecond$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--engine", default="flink", choices=["flink"],
                        help="Only 'flink' here; Spark writes its own row.")
    parser.add_argument("--query", default="", help="q1/q2/q3 (labels the row and picks the job).")
    parser.add_argument("--implementation", default="table", help="implementation label written in the report row.")
    parser.add_argument("--experiment", "--exp", "-e", dest="experiment", default=None,
                        help="Experiment name for the row (default: base).")
    parser.add_argument("--parallelism", type=int, default=0, help="Parallelism for the row (0 = read from job).")
    parser.add_argument("--flink-url", default=DEFAULT_FLINK_URL)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--sample-interval", type=float, default=2.0)
    parser.add_argument("--startup-timeout", type=float, default=120.0,
                        help="Give up if the source never emits within this window.")
    # Flink exposes task metrics over REST only every
    # metrics.fetcher.update-interval (10s by default), so the source counter
    # reads "unchanged" for several samples inside one refresh window. End-of-
    # stream is therefore declared on a wall-clock idle window comfortably above
    # that interval, not on a raw count of unchanged samples.
    parser.add_argument("--idle-seconds", type=float, default=30.0,
                        help="Stop once the source count stays flat this many "
                             "seconds (must exceed Flink's metric fetch interval).")
    parser.add_argument("--min-active-seconds", type=float, default=15.0,
                        help="Never stop before this much real ingestion; keeps "
                             "the PyFlink warmup plateau from ending the run.")
    parser.add_argument("--job-id", default=None, help="Force a specific job id.")
    return parser.parse_args()


def http_get_json(url: str, timeout: float = 10.0):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.load(resp)


def pick_job(flink_url: str, query: str, job_id: str | None) -> dict | None:
    data = http_get_json(f"{flink_url}/jobs/overview")
    running = [j for j in data.get("jobs", []) if j.get("state") == "RUNNING"]
    if job_id:
        for j in running:
            if j["jid"] == job_id:
                return j
        return None
    if not running:
        return None
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
    if not ids:
        return {}
    query = urllib.parse.urlencode({"get": ",".join(ids)})
    values = http_get_json(f"{flink_url}/jobs/{jid}/vertices/{vid}/subtasks/metrics?{query}")
    return {v["id"]: v for v in values}


def main() -> None:
    args = parse_args()
    experiment = args.experiment or "base"
    flink_url = args.flink_url.rstrip("/")

    # Attende che un job sia RUNNING: i job PyFlink impiegano alcune decine di
    # secondi a partire (setup worker Python), e il runner avvia questo tool
    # subito dopo la submit.
    deadline = time.monotonic() + args.startup_timeout
    job = None
    while time.monotonic() < deadline:
        try:
            job = pick_job(flink_url, args.query, args.job_id)
        except (urllib.error.URLError, OSError):
            job = None  # cluster/REST ancora non pronto
        if job:
            break
        time.sleep(2.0)

    if not job:
        print(
            f"ERROR: no RUNNING Flink job found within {args.startup_timeout:.0f}s.",
            file=sys.stderr,
        )
        sys.exit(1)

    jid = job["jid"]
    job_name = job.get("name", "?")
    print(f"Sampling job {jid} ({job_name[:60]}...)")

    detail = http_get_json(f"{flink_url}/jobs/{jid}")
    vertices = [(v["id"], v.get("name", ""), int(v.get("parallelism", 0))) for v in detail.get("vertices", [])]
    parallelism = args.parallelism or max((p for _, _, p in vertices), default=0)

    # Cache the metric-id lists per vertex (they are stable for the job's life),
    # and pre-select the latency gauges (present only if latency tracking is on).
    # Flink registers the Kafka source operator metrics a few seconds AFTER the
    # job turns RUNNING (when the reader starts), so a one-shot discovery at
    # attach time can miss them and wrongly fall back to the vertex-level counter
    # (the chained window pre-aggregate). Retry until they appear.
    latency_ids: dict[str, list[str]] = {}
    src_count_id: dict[str, str] = {}
    src_rate_id: dict[str, str] = {}
    metric_deadline = time.monotonic() + 60.0
    while True:
        for vid, _, _ in vertices:
            ids = vertex_metric_ids(flink_url, jid, vid)
            latency_ids[vid] = [i for i in ids if LATENCY_RE.search(i)]
            for i in ids:
                if SRC_COUNT_RE.search(i):
                    src_count_id[vid] = i
                elif SRC_RATE_RE.search(i):
                    src_rate_id[vid] = i
        if src_count_id or time.monotonic() > metric_deadline:
            break
        time.sleep(1.0)

    use_source_operator = bool(src_count_id)
    if use_source_operator:
        print(f"Ingestion metric: source-operator numRecordsOut "
              f"(Kafka events read; parallelism-invariant).")
    else:
        print("WARNING: no source-operator metric found; falling back to "
              "vertex-level numRecordsOut.", file=sys.stderr)

    samples: list[tuple[float, float, float, float, float]] = []  # t, src_out_total, src_rate, busy, bp
    lat_p50: list[float] = []
    lat_p99: list[float] = []

    started = time.monotonic()
    first_active_t = 0.0
    last_growth_t = 0.0
    last_total = -1.0
    seen_activity = False

    while True:
        now = time.monotonic()
        src_out_total = 0.0
        src_rate = 0.0
        busy_max = 0.0
        bp_max = 0.0

        for vid, vname, _ in vertices:
            if use_source_operator:
                # Ingestion from the Kafka source operator (parallelism-invariant).
                cid = src_count_id.get(vid)
                rid = src_rate_id.get(vid)
                ids = [BUSY, BACKPRESSURE]
                if cid:
                    ids.append(cid)
                if rid:
                    ids.append(rid)
                m = fetch_metrics(flink_url, jid, vid, ids)
                if cid:
                    src_out_total = max(src_out_total, float(m.get(cid, {}).get("sum", 0.0)))
                if rid:
                    src_rate = max(src_rate, float(m.get(rid, {}).get("sum", 0.0)))
            else:
                # Fallback (non-Kafka source): vertex-level numRecordsOut with a
                # heuristic on which vertex is the source.
                m = fetch_metrics(flink_url, jid, vid, [COUNT_IN, COUNT_OUT, RATE_OUT, BUSY, BACKPRESSURE])
                n_in = m.get(COUNT_IN, {}).get("sum", 0.0)
                n_out = m.get(COUNT_OUT, {}).get("sum", 0.0)
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

        if src_out_total > 0.0 and not seen_activity:
            seen_activity = True
            first_active_t = now
            last_growth_t = now
        if seen_activity:
            samples.append((now, src_out_total, src_rate, busy_max, bp_max))

        # End-of-stream detection. Flink refreshes task metrics over REST only
        # every metrics.fetcher.update-interval (~10s default), so the source
        # counter is flat for several samples inside one refresh window; a naive
        # "unchanged since last sample" test therefore fires during PyFlink
        # warmup, long before the replay is done. Instead we remember the wall-
        # clock time the counter last advanced and stop only once it has stayed
        # flat for idle_seconds (comfortably above the fetch interval) AND the
        # job has been actively ingesting for at least min_active_seconds.
        if seen_activity and (src_out_total - last_total) >= 1.0:
            last_growth_t = now
        last_total = src_out_total

        if seen_activity:
            active_for = now - first_active_t
            flat_for = now - last_growth_t
            if active_for >= args.min_active_seconds and flat_for >= args.idle_seconds:
                print(f"Source drained: flat {flat_for:.0f}s after "
                      f"{active_for:.0f}s of ingestion; stopping.")
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
    thr_note = "throughput=kafka source ingestion" if use_source_operator else "throughput=vertex numRecordsOut (fallback)"
    lat_note = "latency markers on" if lat_p50 else "latency: enable metrics.latency.interval"
    notes = f"{thr_note}; {lat_note}"

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
