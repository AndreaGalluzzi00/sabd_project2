#!/usr/bin/env python3
"""
Merge Q3 DataStream (job_datastream.py) part-files into three sorted CSV files.

Identical logic to merge_q3.py, but reads the q3_ds_* directories and writes
the q3_ds_* output files. The percentiles are already computed by the job
(DelayDDSketch), so this only deduplicates, sorts and writes the spec header.

Usage:
    python scripts/merge_q3_ds.py [--experiment <name>] [--wait [--timeout N]]
"""
from __future__ import annotations

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
    description="Merge all Q3 DataStream part files into sorted CSVs (one per window)."
)

CONFIG_PATH = configure_config_path(ARGS.experiment)

from common.config import load_config  # noqa: E402


# Spec output header (num_flights/delay_min/delay_max -> count/min/max)
HEADER = "ts,airline,hour,count,min,p25,p50,p75,p90,max"

COL_TS = 0
COL_AIRLINE = 1
COL_HOUR = 2


@dataclass(frozen=True)
class WindowMergeConfig:
    results_dir: Path
    output_file: Path
    stable_for_seconds: float
    label: str  # "1d" | "7d" | "global"


def load_merge_config() -> list[WindowMergeConfig]:
    cfg = load_config()
    paths = cfg["paths"]
    experiment = get_experiment_name(cfg)
    stable_for = stability_window_seconds(cfg)

    def make(dir_key: str, out_key: str, label: str) -> WindowMergeConfig:
        out = resolve_project_path(paths[out_key])
        out = add_experiment_name_to_output_file(out, experiment)
        return WindowMergeConfig(
            results_dir=resolve_project_path(paths[dir_key]),
            output_file=out,
            stable_for_seconds=stable_for,
            label=label,
        )

    return [
        make("q3_ds_results_host_path_1d",     "q3_ds_merged_output_host_path_1d",     "1d"),
        make("q3_ds_results_host_path_7d",     "q3_ds_merged_output_host_path_7d",     "7d"),
        make("q3_ds_results_host_path_global", "q3_ds_merged_output_host_path_global", "global"),
    ]


def find_finalized_part_files(results_dir: Path) -> list[Path]:
    return sorted(
        p for p in results_dir.glob("part-*")
        if ".inprogress" not in p.name
    )


def read_rows(part_files: list[Path]) -> list[str]:
    rows: list[str] = []
    for path in part_files:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(line)
    return rows


def sort_rows(rows: list[str]) -> list[str]:
    unique = list(dict.fromkeys(rows))

    def key(row: str):
        cols = row.split(",")
        return (cols[COL_TS].strip(), cols[COL_AIRLINE].strip(), int(cols[COL_HOUR].strip()))

    unique.sort(key=key)
    return unique


def write_output(rows: list[str], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as f:
        f.write(HEADER + "\n")
        for row in rows:
            f.write(row + "\n")


def merge_window(wc: WindowMergeConfig) -> None:
    print(f"\n[{wc.label}] Results dir: {wc.results_dir}")

    if ARGS.wait:
        try:
            wait_for_stable_results(
                find_part_files=lambda: find_finalized_part_files(wc.results_dir),
                stable_for_seconds=wc.stable_for_seconds,
                timeout_seconds=ARGS.timeout,
            )
        except TimeoutError as exc:
            print(f"[{wc.label}] ERROR: {exc}", file=sys.stderr)
            sys.exit(1)

    part_files = find_finalized_part_files(wc.results_dir)

    if not part_files:
        print(f"[{wc.label}] No finalised part files - skipping.")
        return

    print(f"[{wc.label}] Found {len(part_files)} part file(s) - merging ...")
    rows = sort_rows(read_rows(part_files))
    write_output(rows, wc.output_file)
    print(f"[{wc.label}] Written {len(rows)} rows -> {wc.output_file}")


def main() -> None:
    print(f"Using config: {CONFIG_PATH}")
    for wc in load_merge_config():
        merge_window(wc)


if __name__ == "__main__":
    main()
