#!/usr/bin/env python3
from __future__ import annotations

import csv
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
    description="Merge Spark Structured Q3 part files into sorted CSVs."
)

CONFIG_PATH = configure_config_path(ARGS.experiment)

from common.config import load_config  # noqa: E402


HEADER = ["ts", "airline", "hour", "count", "min", "p25", "p50", "p75", "p90", "max"]
Q3_WINDOW_CHOICES = ("1d", "7d", "global", "cumulative", "all")


@dataclass(frozen=True)
class WindowMergeConfig:
    results_dir: Path
    output_file: Path
    label: str


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
    selected_window = selected_q3_window(cfg)

    def make(dir_key: str, out_key: str, label: str) -> WindowMergeConfig:
        output = add_experiment_name_to_output_file(
            resolve_project_path(paths[out_key]),
            experiment,
        )
        return WindowMergeConfig(
            results_dir=resolve_project_path(paths[dir_key]),
            output_file=output,
            label=label,
        )

    configs = [
        make("spark_q3_results_host_path_1d", "spark_q3_merged_output_host_path_1d", "1d"),
        make("spark_q3_results_host_path_7d", "spark_q3_merged_output_host_path_7d", "7d"),
        make("spark_q3_results_host_path_global", "spark_q3_merged_output_host_path_global", "global"),
        make("spark_q3_results_host_path_cumulative", "spark_q3_merged_output_host_path_cumulative", "cumulative"),
    ]
    if selected_window == "all":
        return configs
    return [config for config in configs if config.label == selected_window]


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


def merge_window(cfg: WindowMergeConfig) -> None:
    print(f"\n[{cfg.label}] Results dir: {cfg.results_dir}")
    part_files = find_part_files(cfg.results_dir)
    if not part_files:
        print(f"[{cfg.label}] No finalized Spark Q3 part files - skipping.")
        return

    rows = list(dict.fromkeys(read_rows(part_files)))
    rows.sort(key=lambda row: (row[0], row[1], int(row[2])))
    write_output(rows, cfg.output_file)
    print(f"[{cfg.label}] Written {len(rows)} rows -> {cfg.output_file}")


def main() -> None:
    print(f"Using config: {CONFIG_PATH}")
    for cfg in load_merge_config():
        merge_window(cfg)


if __name__ == "__main__":
    main()
