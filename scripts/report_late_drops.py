#!/usr/bin/env python3
"""
Report Flink 'numLateRecordsDropped' for the RUNNING job(s).

Walks every vertex of every RUNNING job via the JobManager REST API, sums
the per-subtask 'numLateRecordsDropped' counters and appends one row per
job to Results/late_drops.csv (experiment, job, total).

The counter measures events discarded by the window operators because they
arrived after the watermark had passed their window end — the number that
links the injected out-of-orderness to the completeness loss. In the Q1
SQL job the airline filter runs before the windowing, so only events of
the four target carriers can be counted.

Run it after the merge (results stable => counters are final) and BEFORE
cancelling the job: metrics disappear once the job stops.

Exit codes: 0 = metric collected; 1 = no running job, REST unreachable,
or metric not found (never silently reports 0 in those cases).
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from merge_utils import PROJECT_ROOT

METRIC = "numLateRecordsDropped"
DEFAULT_FLINK_URL = "http://localhost:8081"
DEFAULT_OUTPUT = PROJECT_ROOT / "Results" / "late_drops.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])

    parser.add_argument(
        "--experiment",
        "--exp",
        "-e",
        type=str,
        default=None,
        dest="experiment",
        help="Experiment name used to label the output row (default: base).",
    )

    parser.add_argument(
        "--flink-url",
        type=str,
        default=DEFAULT_FLINK_URL,
        help=f"JobManager REST base URL (default: {DEFAULT_FLINK_URL}).",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"CSV file the totals are appended to (default: {DEFAULT_OUTPUT}).",
    )

    return parser.parse_args()


def http_get_json(url: str, timeout: float = 10.0):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.load(resp)


def list_running_jobs(flink_url: str) -> list[dict]:
    data = http_get_json(f"{flink_url}/jobs/overview")

    return [job for job in data.get("jobs", []) if job.get("state") == "RUNNING"]


def list_vertices(flink_url: str, job_id: str) -> list[dict]:
    data = http_get_json(f"{flink_url}/jobs/{job_id}")

    return data.get("vertices", [])


def vertex_late_drops(
    flink_url: str,
    job_id: str,
    vertex_id: str,
) -> dict[str, float]:
    """Return {metric_id: summed value} for this vertex, {} if not exposed.

    Metric ids are discovered from the vertex itself (operator metrics show
    up prefixed with the operator name), then fetched one by one: Flink
    splits the 'get' query parameter on commas, so ids are never joined.
    """
    metrics_url = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics"

    available = http_get_json(metrics_url)
    metric_ids = [
        m["id"]
        for m in available
        if m["id"] == METRIC or m["id"].endswith(f".{METRIC}")
    ]

    totals: dict[str, float] = {}

    for metric_id in metric_ids:
        query = urllib.parse.urlencode({"get": metric_id})
        values = http_get_json(f"{metrics_url}?{query}")

        if not values:
            # Flink cannot fetch ids containing a comma (the 'get' filter is
            # comma-split server-side). Typical of unnamed DataStream window
            # operators; fix by setting .name(...) on the operator.
            print(
                f"WARNING: metric '{metric_id}' is listed but not fetchable "
                "via REST (comma in the operator name?) — value unknown.",
                file=sys.stderr,
            )
            continue

        totals[metric_id] = sum(float(v.get("sum", 0.0)) for v in values)

    return totals


def append_output_row(
    output_file: Path,
    experiment: str,
    job_name: str,
    job_id: str,
    late_drops: float,
) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    write_header = not output_file.exists()

    with output_file.open("a", encoding="utf-8", newline="") as out:
        writer = csv.writer(out)

        if write_header:
            writer.writerow(
                ["timestamp_utc", "experiment", "job_name", "job_id", METRIC]
            )

        writer.writerow(
            [
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
                experiment,
                job_name,
                job_id,
                int(late_drops),
            ]
        )


def main() -> None:
    args = parse_args()
    experiment = args.experiment or "base"

    try:
        jobs = list_running_jobs(args.flink_url)
    except (urllib.error.URLError, OSError) as exc:
        print(
            f"ERROR: cannot reach Flink REST API at {args.flink_url}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)

    if not jobs:
        print(
            "ERROR: no RUNNING Flink job found — run this before cancelling "
            "the job (metrics vanish once it stops).",
            file=sys.stderr,
        )
        sys.exit(1)

    metric_found = False

    for job in jobs:
        job_id = job["jid"]
        job_name = job.get("name", "?")
        job_total = 0.0

        print(f"Job {job_id} ({job_name}):")

        for vertex in list_vertices(args.flink_url, job_id):
            totals = vertex_late_drops(args.flink_url, job_id, vertex["id"])

            for metric_id, value in totals.items():
                metric_found = True
                job_total += value
                print(f"  {metric_id} = {int(value)}")

        print(f"  TOTAL {METRIC} = {int(job_total)}  [experiment: {experiment}]")

        append_output_row(
            output_file=args.output,
            experiment=experiment,
            job_name=job_name,
            job_id=job_id,
            late_drops=job_total,
        )

    if not metric_found:
        print(
            f"ERROR: no '{METRIC}' metric exposed by any vertex — cannot "
            "distinguish '0 drops' from 'not measured'.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Appended to {args.output}")


if __name__ == "__main__":
    main()
