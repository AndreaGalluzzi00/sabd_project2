#!/usr/bin/env python3
from __future__ import annotations

import csv
import sys
from dataclasses import dataclass
from pathlib import Path

from merge_utils import (
    add_experiment_name_to_output_file,
    configure_config_path,
    get_experiment_name,
    parse_experiment_args,
    resolve_project_path,
    stability_window_seconds,
    wait_for_stable_results,
)

ARGS = parse_experiment_args(
    description="Merge Spark Structured Q1 part files into a single sorted CSV."
)

CONFIG_PATH = configure_config_path(ARGS.experiment)

from common.config import load_config  # noqa: E402


HEADER = [
    "window_start",
    "window_end",
    "airline",
    "num_flights",
    "completed",
    "cancelled",
    "diverted",
    "dep_delay_mean",
    "cancellation_rate",
    "late_departure_rate",
]


@dataclass(frozen=True)
class MergeConfig:
    results_dir: Path
    output_file: Path
    stable_for_seconds: float


def load_merge_config() -> MergeConfig:
    cfg = load_config()
    paths = cfg["paths"]
    experiment = get_experiment_name(cfg)
    output = add_experiment_name_to_output_file(
        resolve_project_path(paths["spark_q1_merged_output_host_path"]),
        experiment,
    )
    return MergeConfig(
        results_dir=resolve_project_path(paths["spark_q1_results_host_path"]),
        output_file=output,
        stable_for_seconds=stability_window_seconds(cfg),
    )


def find_part_files(results_dir: Path) -> list[Path]:
    return sorted(p for p in results_dir.rglob("part-*") if ".inprogress" not in p.name)


def read_rows(part_files: list[Path]) -> list[tuple[str, ...]]:
    rows: list[tuple[str, ...]] = []
    for path in part_files:
        with path.open("r", encoding="utf-8", newline="") as file:
            for row in csv.reader(file):
                if row and any(cell.strip() for cell in row):
                    rows.append(tuple(row))
    return rows


def write_output(rows: list[tuple[str, ...]], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file, lineterminator="\n")
        writer.writerow(HEADER)
        writer.writerows(rows)


def main() -> None:
    print(f"Using config: {CONFIG_PATH}")
    cfg = load_merge_config()

    if ARGS.wait:
        try:
            wait_for_stable_results(
                find_part_files=lambda: find_part_files(cfg.results_dir),
                stable_for_seconds=cfg.stable_for_seconds,
                timeout_seconds=ARGS.timeout,
            )
        except TimeoutError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            sys.exit(1)

    part_files = find_part_files(cfg.results_dir)
    if not part_files:
        print(f"No finalized Spark Q1 part files found in {cfg.results_dir}")
        sys.exit(1)

    rows = list(dict.fromkeys(read_rows(part_files)))
    rows.sort(key=lambda row: (row[0], row[2]))
    write_output(rows, cfg.output_file)
    print(f"Written {len(rows)} rows -> {cfg.output_file}")


if __name__ == "__main__":
    main()
