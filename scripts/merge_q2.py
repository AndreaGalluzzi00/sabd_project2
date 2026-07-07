#!/usr/bin/env python3

from __future__ import annotations

import sys
import csv
import re
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
    description="Merge all Q2 part files into sorted CSVs (one per window)."
)

CONFIG_PATH = configure_config_path(ARGS.experiment)

from common.config import load_config  # noqa: E402


# Spec output header (airport_rank -> rank as required by the spec schema)
HEADER = [
    "ts",
    "rank",
    "origin_airport_id",
    "num_flights",
    "severe_delays",
    "dep_delay_mean",
    "dep_delay_max",
    "delayed_flights",
]


COL_TS = 0
COL_RANK = 1
Q2_WINDOW_CHOICES = ("1h", "6h", "global", "cumulative", "all")


@dataclass(frozen=True)
class WindowMergeConfig:
    results_dir: Path
    output_file: Path
    stable_for_seconds: float
    label: str  # "1h" | "6h" | "global" | "cumulative"


def selected_q2_window(cfg: dict) -> str:
    window = str(cfg.get("q2", {}).get("window", "all")).strip().lower()
    if window not in Q2_WINDOW_CHOICES:
        raise ValueError(
            "q2.window must be one of: "
            + ", ".join(Q2_WINDOW_CHOICES)
        )
    return window


def load_merge_config() -> list[WindowMergeConfig]:
    cfg = load_config()
    paths = cfg["paths"]
    experiment = get_experiment_name(cfg)
    stable_for = stability_window_seconds(cfg)
    selected_window = selected_q2_window(cfg)

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
        make("q2_results_host_path_1h",     "q2_merged_output_host_path_1h",     "1h"),
        make("q2_results_host_path_6h",     "q2_merged_output_host_path_6h",     "6h"),
        make("q2_results_host_path_global", "q2_merged_output_host_path_global", "global"),
        make("q2_results_host_path_cumulative", "q2_merged_output_host_path_cumulative", "cumulative"),
    ]
    if selected_window == "all":
        return configs
    return [config for config in configs if config.label == selected_window]


def part_file_sort_key(path: Path) -> tuple[int, str]:
    match = re.search(r"-(\d+)(?:\.inprogress|$)", path.name)
    sequence = int(match.group(1)) if match else -1
    return sequence, path.name


def find_part_files(results_dir: Path, *, include_inprogress: bool = False) -> list[Path]:
    return sorted(
        (
            p
            for p in results_dir.iterdir()
            if p.is_file()
            and "part-" in p.name
            and (include_inprogress or ".inprogress" not in p.name)
        ),
        key=part_file_sort_key,
    )


def read_rows(part_files: list[Path]) -> list[tuple[str, ...]]:
    rows: list[tuple[str, ...]] = []
    for path in part_files:
        with path.open("r", encoding="utf-8", newline="") as f:
            for row in csv.reader(f):
                if row and any(cell.strip() for cell in row):
                    rows.append(tuple(row))
    return rows


def dedupe_rows(rows: list[tuple[str, ...]], label: str) -> list[tuple[str, ...]]:
    if label == "cumulative":
        latest_by_snapshot_rank: dict[tuple[str, str], tuple[str, ...]] = {}
        for row in rows:
            latest_by_snapshot_rank[(row[COL_TS], row[COL_RANK])] = row
        return list(latest_by_snapshot_rank.values())

    return list(dict.fromkeys(rows))


def sort_rows(rows: list[tuple[str, ...]], label: str) -> list[tuple[str, ...]]:
    unique = dedupe_rows(rows, label)
    # Sort by ts ASC, then airport_rank ASC (as int to avoid lexicographic issues)
    unique.sort(key=lambda r: (r[COL_TS].strip(), int(r[COL_RANK].strip())))
    return unique


def write_output(rows: list[tuple[str, ...]], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(HEADER)
        writer.writerows(rows)


def wait_for_window(wc: WindowMergeConfig) -> None:
    include_inprogress = wc.label == "cumulative"
    try:
        wait_for_stable_results(
            find_part_files=lambda: find_part_files(
                wc.results_dir,
                include_inprogress=include_inprogress,
            ),
            stable_for_seconds=wc.stable_for_seconds,
            timeout_seconds=ARGS.timeout,
        )
    except TimeoutError as exc:
        print(f"[{wc.label}] ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


def merge_window(wc: WindowMergeConfig) -> None:
    print(f"\n[{wc.label}] Results dir: {wc.results_dir}")
    include_inprogress = wc.label == "cumulative"
    part_files = find_part_files(wc.results_dir, include_inprogress=include_inprogress)

    if not part_files:
        print(f"[{wc.label}] No finalised part files - skipping.")
        return

    print(f"[{wc.label}] Found {len(part_files)} part file(s) - merging ...")
    rows = sort_rows(read_rows(part_files), wc.label)
    write_output(rows, wc.output_file)
    print(f"[{wc.label}] Written {len(rows)} rows -> {wc.output_file}")


def main() -> None:
    print(f"Using config: {CONFIG_PATH}")
    window_configs = load_merge_config()

    if ARGS.wait:
        wait_config = window_configs[-1]
        print(f"\n[{wait_config.label}] Waiting for enabled Q2 output before merging...")
        wait_for_window(wait_config)

    for wc in window_configs:
        merge_window(wc)


if __name__ == "__main__":
    main()
