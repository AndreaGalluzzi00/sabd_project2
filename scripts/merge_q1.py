#!/usr/bin/env python3
"""Merge all Q1 part files into a single sorted CSV."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Allow importing common.config when running:
# python scripts/merge_q1.py
sys.path.insert(0, str(PROJECT_ROOT))

# Local scripts use config/base.yml by default.
# Docker containers override CONFIG_PATH with /config/base.yml.
os.environ.setdefault("CONFIG_PATH", str(PROJECT_ROOT / "config" / "base.yml"))

from common.config import load_config  # noqa: E402


HEADER = (
    "window_start,window_end,airline,num_flights,completed,"
    "cancelled_count,diverted_count,dep_delay_mean,"
    "cancellation_rate,late_departure_rate"
)


@dataclass(frozen=True)
class MergeConfig:
    results_dir: Path
    output_file: Path


def resolve_project_path(path_value: str) -> Path:
    path = Path(path_value)

    if path.is_absolute():
        return path

    return PROJECT_ROOT / path


def load_merge_config() -> MergeConfig:
    cfg = load_config()
    paths_cfg = cfg["paths"]

    return MergeConfig(
        results_dir=resolve_project_path(paths_cfg["q1_results_host_path"]),
        output_file=resolve_project_path(paths_cfg["q1_merged_output_host_path"]),
    )


def find_finalized_part_files(results_dir: Path) -> list[Path]:
    part_files = sorted(results_dir.glob("part-*"))

    return [
        path
        for path in part_files
        if ".inprogress" not in path.name
    ]


def read_rows(part_files: list[Path]) -> list[str]:
    rows: list[str] = []

    for path in part_files:
        with path.open("r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()

                if line:
                    rows.append(line)

    return rows


def sort_rows(rows: list[str]) -> list[str]:
    # Remove duplicates while preserving first occurrence.
    unique_rows = list(dict.fromkeys(rows))

    # Sort by window_start, then airline.
    unique_rows.sort(key=lambda row: (row.split(",")[0], row.split(",")[2]))

    return unique_rows


def write_output(rows: list[str], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", encoding="utf-8") as out:
        out.write(HEADER + "\n")

        for row in rows:
            out.write(row + "\n")


def main() -> None:
    cfg = load_merge_config()

    part_files = find_finalized_part_files(cfg.results_dir)

    if not part_files:
        print(f"No finalized part files found in {cfg.results_dir}")
        sys.exit(1)

    print(f"Found {len(part_files)} part file(s) in {cfg.results_dir} — merging...")

    rows = read_rows(part_files)
    rows = sort_rows(rows)

    write_output(
        rows=rows,
        output_file=cfg.output_file,
    )

    print(f"Written {len(rows)} rows → {cfg.output_file}")


if __name__ == "__main__":
    main()