#!/usr/bin/env python3
"""Collect Flink late-drop metrics and append them to the late-drops CSV."""

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

METRIC = "numLateRecordsDropped"
Q3_LATE_RECORDS_RE = re.compile(r"^Map__Q3LateDrops\[[^\]]+\]\.numRecordsIn$")
DEFAULT_FLINK_URL = "http://localhost:8081"
DEFAULT_OUTPUT = PROJECT_ROOT / "Results" / "late_drops.csv"
DEFAULT_WAIT_SECONDS = 60.0
DEFAULT_POLL_SECONDS = 2.0
OUTPUT_HEADER = [
    "timestamp_utc",
    "experiment",
    "query",
    "job_name",
    "job_id",
    "vertex_name",
    "metric_id",
    "operator",
    "window",
    "is_total",
    METRIC,
]
LEGACY_OUTPUT_HEADER = [column for column in OUTPUT_HEADER if column != "query"]


def parse_args() -> argparse.Namespace:
    description = (__doc__ or "Collect Flink late-drop metrics.").strip().splitlines()[0]
    parser = argparse.ArgumentParser(description=description)

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

    parser.add_argument(
        "--query",
        "-q",
        choices=("q1", "q2", "q3"),
        default=None,
        help="Query label to write in the CSV output (default: inferred from the Flink job name).",
    )

    parser.add_argument(
        "--wait-seconds",
        type=float,
        default=DEFAULT_WAIT_SECONDS,
        help=(
            "Seconds to wait for late-drop metrics to appear before failing "
            f"(default: {DEFAULT_WAIT_SECONDS:g})."
        ),
    )

    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=DEFAULT_POLL_SECONDS,
        help=(
            "Polling interval while waiting for metrics "
            f"(default: {DEFAULT_POLL_SECONDS:g})."
        ),
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


def normalize_plan_window_size(size: str) -> str:
    compact = size.strip().lower().replace(" ", "")
    if compact in {"1h", "6h", "1d", "7d"}:
        return compact
    if compact in {"365d", "365day", "365days"}:
        return "global"
    return compact


def job_plan_windows(flink_url: str, job_id: str) -> dict[str, str]:
    data = http_get_json(f"{flink_url}/jobs/{job_id}/plan")
    nodes = data.get("plan", {}).get("nodes", [])
    windows: dict[str, str] = {}

    for node in nodes:
        description = str(node.get("description", ""))
        for match in re.finditer(
            r"\[(\d+)\]:(?:WindowAggregate|GlobalWindowAggregate|LocalWindowAggregate|WindowRank)"
            r"[^<]*?size=\[([^\]]+)\]",
            description,
        ):
            windows[match.group(1)] = normalize_plan_window_size(match.group(2))

    return windows


def is_late_drop_metric_id(metric_id: str) -> bool:
    if Q3_LATE_RECORDS_RE.match(metric_id):
        return True

    if "Q3LateDrops[" in metric_id:
        return False

    return metric_id == METRIC or metric_id.endswith(f".{METRIC}")


def list_vertex_metric_ids(
    flink_url: str,
    job_id: str,
    vertex_id: str,
) -> list[str]:
    metrics_url = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics"
    available = http_get_json(metrics_url)
    return [str(metric["id"]) for metric in available]


def any_late_drop_metric_available(flink_url: str, jobs: list[dict]) -> bool:
    for job in jobs:
        job_id = job["jid"]
        for vertex in list_vertices(flink_url, job_id):
            metric_ids = list_vertex_metric_ids(flink_url, job_id, vertex["id"])
            if any(is_late_drop_metric_id(metric_id) for metric_id in metric_ids):
                return True

    return False


def wait_for_late_drop_metrics(flink_url: str, args: argparse.Namespace) -> list[dict]:
    wait_seconds = max(0.0, float(args.wait_seconds))
    poll_seconds = max(0.1, float(args.poll_seconds))
    deadline = time.monotonic() + wait_seconds
    warned = False

    while True:
        jobs = list_running_jobs(flink_url)

        if jobs and any_late_drop_metric_available(flink_url, jobs):
            return jobs

        if time.monotonic() >= deadline:
            return jobs

        if not warned:
            print(
                f"NOTE: no '{METRIC}' metric exposed yet; waiting up to "
                f"{wait_seconds:g}s...",
                file=sys.stderr,
            )
            warned = True

        time.sleep(poll_seconds)


def vertex_late_drops(
    flink_url: str,
    job_id: str,
    vertex_id: str,
) -> dict[str, float]:
    metrics_url = f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics"

    metric_ids = []
    for metric_id in list_vertex_metric_ids(flink_url, job_id, vertex_id):
        if is_late_drop_metric_id(metric_id):
            metric_ids.append(metric_id)

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


def metric_operator(metric_id: str) -> str:
    if metric_id == METRIC:
        return ""

    for suffix in (f".{METRIC}", ".numRecordsIn"):
        if metric_id.endswith(suffix):
            return metric_id[: -len(suffix)]

    return metric_id


def single_window_from_job_name(job_name: str) -> str:
    labels = []
    for label in ("1h", "6h", "global", "1d", "7d"):
        if re.search(rf"_(?:csv|kafka|jdbc|blackhole)_{label}\b", job_name):
            labels.append(label)
    labels = list(dict.fromkeys(labels))
    return labels[0] if len(labels) == 1 else ""


def infer_query_from_job_name(job_name: str) -> str:
    match = re.search(r"(?:^|[._-])(q[123])(?:[_-]|$)", job_name.lower())
    return match.group(1) if match else ""


def metric_window(
    operator: str,
    *,
    vertex_name: str,
    job_name: str,
    plan_windows: dict[str, str],
) -> str:
    q3_match = re.search(r"Q3LateDrops\[([^\]]+)\]", operator)
    if q3_match:
        return q3_match.group(1)

    match = re.search(r"\[([^\]]+)\]", operator)
    if match:
        bracket_value = match.group(1)
        if bracket_value in plan_windows:
            return plan_windows[bracket_value]
        if not bracket_value.isdigit():
            return bracket_value

    for text in (operator, vertex_name):
        for label in ("1h", "6h", "global", "1d", "7d"):
            if re.search(rf"_(?:csv|kafka|jdbc|blackhole)_{label}\b", text):
                return label

    return single_window_from_job_name(job_name)



def rotate_if_stale_schema(output_file: Path) -> None:
    if not output_file.exists():
        return

    with output_file.open("r", encoding="utf-8", newline="") as handle:
        first_line = handle.readline().strip()

    header = next(csv.reader([first_line]), [])

    if header == OUTPUT_HEADER:
        return

    if header == LEGACY_OUTPUT_HEADER:
        tmp_file = output_file.with_suffix(output_file.suffix + ".tmp")

        with output_file.open("r", encoding="utf-8", newline="") as source:
            with tmp_file.open("w", encoding="utf-8", newline="") as target:
                reader = csv.DictReader(source)
                writer = csv.DictWriter(target, fieldnames=OUTPUT_HEADER)
                writer.writeheader()

                for row in reader:
                    row["query"] = infer_query_from_job_name(row.get("job_name", ""))
                    writer.writerow(
                        {column: row.get(column, "") for column in OUTPUT_HEADER}
                    )

        tmp_file.replace(output_file)
        print(f"NOTE: migrated {output_file.name} schema by adding query column")
        return

    stale = output_file.with_suffix(output_file.suffix + ".old")
    if stale.exists():
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        stale = output_file.with_suffix(output_file.suffix + f".{timestamp}.old")

    output_file.replace(stale)
    print(f"NOTE: existing {output_file.name} had a stale schema; moved to {stale.name}")


def append_output_rows(output_file: Path, rows: list[list[object]]) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    rotate_if_stale_schema(output_file)
    write_header = not output_file.exists()

    with output_file.open("a", encoding="utf-8", newline="") as out:
        writer = csv.writer(out)

        if write_header:
            writer.writerow(OUTPUT_HEADER)

        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    experiment = args.experiment or "base"

    try:
        jobs = wait_for_late_drop_metrics(args.flink_url, args)
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
        query = args.query or infer_query_from_job_name(job_name)
        job_total = 0.0
        job_metric_found = False
        timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
        output_rows: list[list[object]] = []

        print(f"Job {job_id} ({job_name}):")
        try:
            plan_windows = job_plan_windows(args.flink_url, job_id)
        except (urllib.error.URLError, OSError, KeyError, ValueError) as exc:
            print(
                f"WARNING: cannot read Flink plan for window labels: {exc}",
                file=sys.stderr,
            )
            plan_windows = {}

        vertices = list_vertices(args.flink_url, job_id)
        for vertex in vertices:
            vertex_name = vertex.get("name", "?")
            totals = vertex_late_drops(args.flink_url, job_id, vertex["id"])

            for metric_id, value in totals.items():
                metric_found = True
                job_metric_found = True
                job_total += value

                operator = metric_operator(metric_id)
                window = metric_window(
                    operator,
                    vertex_name=vertex_name,
                    job_name=job_name,
                    plan_windows=plan_windows,
                )
                window_note = f"  [window: {window}]" if window else ""
                print(f"  {metric_id} = {int(value)}{window_note}")

                output_rows.append(
                    [
                        timestamp,
                        experiment,
                        query,
                        job_name,
                        job_id,
                        vertex_name,
                        metric_id,
                        operator,
                        window,
                        "false",
                        int(value),
                    ]
                )

        if job_metric_found:
            print(
                f"  TOTAL {METRIC} across exposed counters = {int(job_total)}  "
                f"[experiment: {experiment}]"
            )

            output_rows.append(
                [
                    timestamp,
                    experiment,
                    query,
                    job_name,
                    job_id,
                    "",
                    "TOTAL",
                    "",
                    "all_operators",
                    "true",
                    int(job_total),
                ]
            )
            append_output_rows(args.output, output_rows)
        else:
            print(f"  {METRIC}: not exposed by this job")

    if not metric_found:
        print(
            f"ERROR: no '{METRIC}' metric exposed by any vertex — cannot "
            "distinguish '0 drops' from 'not measured'.\n"
            "NOTE: Flink's Table/SQL WindowOperator exposes this counter "
            "natively. Q3 DataStream windows are measured through the "
            "Map__Q3LateDrops[window].numRecordsIn side-output counters.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Appended to {args.output}")


if __name__ == "__main__":
    main()
