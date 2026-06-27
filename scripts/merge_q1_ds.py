#!/usr/bin/env python3
"""
Merge dei part-file prodotti dal job Q1 DataStream API (job_datastream.py).

Funzionamento identico a merge_q1.py: legge i part-file dalla directory
q1_ds_results_host_path, rimuove duplicati, ordina per window_start + airline
e scrive il CSV finale con header in q1_ds_merged_output_host_path.
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
    description="Merge all Q1-DataStream part files into a single sorted CSV."
)

CONFIG_PATH = configure_config_path(ARGS.experiment)

from common.config import load_config  # noqa: E402


HEADER = (
    "window_start,window_end,airline,num_flights,completed,"
    "cancelled,diverted,dep_delay_mean,"
    "cancellation_rate,late_departure_rate"
)


@dataclass(frozen=True)
class MergeConfig:
    results_dir: Path
    output_file: Path


def load_merge_config() -> MergeConfig:
    cfg = load_config()
    paths_cfg = cfg["paths"]
    experiment_name = get_experiment_name(cfg)

    results_dir = resolve_project_path(paths_cfg["q1_ds_results_host_path"])
    output_file = resolve_project_path(paths_cfg["q1_ds_merged_output_host_path"])
    output_file = add_experiment_name_to_output_file(
        output_file=output_file,
        experiment_name=experiment_name,
    )

    return MergeConfig(results_dir=results_dir, output_file=output_file)


def find_finalized_part_files(results_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in results_dir.rglob("part-*")
        if ".inprogress" not in path.name
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
    unique_rows = list(dict.fromkeys(rows))
    unique_rows.sort(key=lambda row: (row.split(",")[0], row.split(",")[2]))
    return unique_rows


def write_output(rows: list[str], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as out:
        out.write(HEADER + "\n")
        for row in rows:
            out.write(row + "\n")


def main() -> None:
    print(f"Using config: {CONFIG_PATH}")

    cfg = load_merge_config()
    part_files = find_finalized_part_files(cfg.results_dir)

    if not part_files:
        print(f"No finalized part files found in {cfg.results_dir}")
        sys.exit(1)

    print(f"Found {len(part_files)} part file(s) in {cfg.results_dir} — merging...")

    rows = read_rows(part_files)
    rows = sort_rows(rows)
    write_output(rows=rows, output_file=cfg.output_file)

    print(f"Written {len(rows)} rows → {cfg.output_file}")


if __name__ == "__main__":
    main()
