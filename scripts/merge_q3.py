#!/usr/bin/env python3
"""
Merge Q3 part-files into sorted CSV files (one per enabled window size).

The statistics are already computed by Flink (DDSketch percentiles), so this
script only needs to:
  1. Find finalised part-files.
  2. Deduplicate rows (keep first occurrence, same policy as merge_q1.py).
  3. Sort by ts ASC, then airline ASC, then hour ASC.
  4. Write with the spec-compliant header (renames num_flights/delay_min/
     delay_max -> count/min/max, reserved words in the Flink sink DDL).

Usage:
    python scripts/merge_q3.py [--experiment <name>] [--wait [--timeout N]]
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
    description="Merge all Q3 part files into sorted CSVs (one per window)."
)

CONFIG_PATH = configure_config_path(ARGS.experiment)

from common.config import load_config  # noqa: E402


# Spec output header (num_flights/delay_min/delay_max -> count/min/max)
HEADER = "ts,airline,hour,count,min,p25,p50,p75,p90,max"

# Column indices in the Flink-produced CSV rows (no header):
#   0: ts  1: airline  2: hour  3: num_flights  4: delay_min
#   5: p25  6: p50  7: p75  8: p90  9: delay_max
COL_TS = 0
COL_AIRLINE = 1
COL_HOUR = 2
Q3_WINDOW_CHOICES = ("1d", "7d", "global", "all")


@dataclass(frozen=True)
class WindowMergeConfig:
    results_dir: Path
    output_file: Path
    stable_for_seconds: float
    label: str  # "1d" | "7d" | "global"


def selected_q3_window(cfg: dict) -> str:
    window = str(cfg.get("q3", {}).get("window", "all")).strip().lower()
    if window not in Q3_WINDOW_CHOICES:
        raise ValueError(
            "q3.window must be one of: "
            + ", ".join(Q3_WINDOW_CHOICES)
        )
    return window


def load_merge_config() -> list[WindowMergeConfig]:
    cfg = load_config()
    paths = cfg["paths"]
    experiment = get_experiment_name(cfg)
    stable_for = stability_window_seconds(cfg)
    selected_window = selected_q3_window(cfg)

    def make(dir_key: str, out_key: str, label: str) -> WindowMergeConfig:
        out = resolve_project_path(paths[out_key])
        out = add_experiment_name_to_output_file(out, experiment)
        return WindowMergeConfig(
            results_dir=resolve_project_path(paths[dir_key]),
            output_file=out,
            stable_for_seconds=stable_for,
            label=label,
        )

    configs = [
        make("q3_results_host_path_1d",     "q3_merged_output_host_path_1d",     "1d"),
        make("q3_results_host_path_7d",     "q3_merged_output_host_path_7d",     "7d"),
        make("q3_results_host_path_global", "q3_merged_output_host_path_global", "global"),
    ]
    if selected_window == "all":
        return configs
    return [config for config in configs if config.label == selected_window]


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
        # hour as int to avoid lexicographic ordering (2 < 10)
        return (cols[COL_TS].strip(), cols[COL_AIRLINE].strip(), int(cols[COL_HOUR].strip()))

    unique.sort(key=key)
    return unique


def write_output(rows: list[str], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as f:
        f.write(HEADER + "\n")
        for row in rows:
            f.write(row + "\n")

def wait_for_window(wc: WindowMergeConfig) -> None:
    try:
        wait_for_stable_results(
            find_part_files=lambda: find_finalized_part_files(wc.results_dir),
            stable_for_seconds=wc.stable_for_seconds,
            timeout_seconds=ARGS.timeout,
        )
    except TimeoutError as exc:
        print(f"[{wc.label}] ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


def merge_window(wc: WindowMergeConfig) -> None:
    print(f"\n[{wc.label}] Results dir: {wc.results_dir}")

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
    window_configs = load_merge_config()

    if ARGS.wait:
        wait_config = window_configs[-1]
        print(f"\n[{wait_config.label}] Waiting for enabled Q3 output before merging...")
        wait_for_window(wait_config)

    for wc in window_configs:
        merge_window(wc)


if __name__ == "__main__":
    main()
