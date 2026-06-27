#!/usr/bin/env python3
"""
Merge Q2 part-files into three sorted CSV files (one per window size).

The ranking is already computed by Flink's Top10AllWindowFunction, so this
script only needs to:
  1. Find finalised part-files.
  2. Deduplicate rows (keep first occurrence, same policy as merge_q1.py).
  3. Sort by ts ASC, then airport_rank ASC.
  4. Write with the spec-compliant header (renames airport_rank → rank).

Usage:
    python scripts/merge_q2.py [--experiment <name>]
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
)

ARGS = parse_experiment_args(
    description="Merge all Q2 part files into sorted CSVs (one per window)."
)

CONFIG_PATH = configure_config_path(ARGS.experiment)

from common.config import load_config  # noqa: E402


# Spec output header (airport_rank → rank as required by the spec schema)
HEADER = (
    "ts,rank,origin_airport_id,num_flights,severe_delays,"
    "dep_delay_mean,dep_delay_max,delayed_flights"
)

# Column indices in the Flink-produced CSV rows (no header):
#   0: ts  1: airport_rank  2: origin_airport_id  3: num_flights
#   4: severe_delays  5: dep_delay_mean  6: dep_delay_max  7: delayed_flights
COL_TS = 0
COL_RANK = 1


@dataclass(frozen=True)
class WindowMergeConfig:
    results_dir: Path
    output_file: Path
    label: str  # "1h" | "6h" | "global"


def load_merge_config() -> list[WindowMergeConfig]:
    cfg = load_config()
    paths = cfg["paths"]
    experiment = get_experiment_name(cfg)

    def make(dir_key: str, out_key: str, label: str) -> WindowMergeConfig:
        out = resolve_project_path(paths[out_key])
        out = add_experiment_name_to_output_file(out, experiment)
        return WindowMergeConfig(
            results_dir=resolve_project_path(paths[dir_key]),
            output_file=out,
            label=label,
        )

    return [
        make("q2_results_host_path_1h",     "q2_merged_output_host_path_1h",     "1h"),
        make("q2_results_host_path_6h",     "q2_merged_output_host_path_6h",     "6h"),
        make("q2_results_host_path_global", "q2_merged_output_host_path_global", "global"),
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
    # Sort by ts ASC, then airport_rank ASC (as int to avoid lexicographic issues)
    unique.sort(key=lambda r: (r.split(",")[COL_TS].strip(), int(r.split(",")[COL_RANK].strip())))
    return unique


def write_output(rows: list[str], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as f:
        f.write(HEADER + "\n")
        for row in rows:
            f.write(row + "\n")


def merge_window(wc: WindowMergeConfig) -> None:
    print(f"\n[{wc.label}] Results dir: {wc.results_dir}")
    part_files = find_finalized_part_files(wc.results_dir)

    if not part_files:
        print(f"[{wc.label}] No finalised part files — skipping.")
        return

    print(f"[{wc.label}] Found {len(part_files)} part file(s) — merging …")
    rows = sort_rows(read_rows(part_files))
    write_output(rows, wc.output_file)
    print(f"[{wc.label}] Written {len(rows)} rows → {wc.output_file}")


def main() -> None:
    print(f"Using config: {CONFIG_PATH}")
    for wc in load_merge_config():
        merge_window(wc)


if __name__ == "__main__":
    main()
