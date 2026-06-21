#!/usr/bin/env python3
"""Merge all Q1 part files into a single sorted CSV."""

from __future__ import annotations

import glob
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Permette allo script di importare common.config anche se viene lanciato come:
# python scripts/merge_q1.py
sys.path.insert(0, str(PROJECT_ROOT))

# Se lo script gira sull'host, CONFIG_PATH di default deve puntare al config locale.
# Dentro i container invece viene passato /config/base.yml dal docker-compose.
os.environ.setdefault("CONFIG_PATH", str(PROJECT_ROOT / "config" / "base.yml"))

from common.config import load_config  # noqa: E402


HEADER = (
    "window_start,window_end,airline,num_flights,completed,"
    "cancelled,diverted,dep_delay_mean,cancellation_rate,late_departure_rate"
)


def resolve_project_path(path_value: str) -> Path:
    """
    Resolve a path from config.

    If the path is absolute, keep it.
    If the path is relative, interpret it relative to the project root.
    """
    path = Path(path_value)

    if path.is_absolute():
        return path

    return PROJECT_ROOT / path


def load_merge_paths() -> tuple[Path, Path]:
    cfg = load_config()

    paths_cfg = cfg["paths"]

    results_dir = resolve_project_path(paths_cfg["q1_results_host_path"])
    output_file = resolve_project_path(paths_cfg["q1_merged_output_host_path"])

    return results_dir, output_file


def main() -> None:
    results_dir, output_file = load_merge_paths()

    part_files = sorted(glob.glob(str(results_dir / "part-*")))
    part_files = [path for path in part_files if ".inprogress" not in path]

    if not part_files:
        print(f"No finalized part files found in {results_dir}")
        sys.exit(1)

    print(f"Found {len(part_files)} part file(s) in {results_dir} — merging...")

    rows: list[str] = []

    for path in part_files:
        with open(path, encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if line:
                    rows.append(line)

    # Remove duplicates while preserving first occurrence.
    rows = list(dict.fromkeys(rows))

    # Sort by window_start, then airline.
    rows.sort(key=lambda row: (row.split(",")[0], row.split(",")[2]))

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as out:
        out.write(HEADER + "\n")
        for row in rows:
            out.write(row + "\n")

    print(f"Written {len(rows)} rows → {output_file}")


if __name__ == "__main__":
    main()