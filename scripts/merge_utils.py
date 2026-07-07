from __future__ import annotations

import argparse
import os
import sys
import time
from collections.abc import Callable
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(PROJECT_ROOT))


def parse_experiment_args(description: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument(
        "--experiment",
        "--exp",
        "-e",
        type=str,
        default=None,
        dest="experiment",
        help="Experiment name under config/experiments, e.g. 02_ooo_safe",
    )

    parser.add_argument(
        "--wait",
        action="store_true",
        help=(
            "Wait until the finalized part files are stable (no new files or "
            "rows for at least ~2 Flink checkpoint intervals) before merging."
        ),
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=180.0,
        help="Max seconds to wait for stable results with --wait (default: 180).",
    )

    return parser.parse_args()


def stability_window_seconds(cfg: dict) -> float:

    checkpoint_ms = float(cfg.get("flink", {}).get("checkpoint_interval_ms", 10_000))

    return max(2.5 * checkpoint_ms / 1000.0, 15.0)


def _snapshot_results(
    find_part_files: Callable[[], list[Path]],
) -> tuple[tuple[str, ...], int]:
    files = find_part_files()
    names = tuple(sorted(str(path) for path in files))

    total_rows = 0
    for path in files:
        with path.open("r", encoding="utf-8") as file:
            total_rows += sum(1 for line in file if line.strip())

    return names, total_rows


def wait_for_stable_results(
    find_part_files: Callable[[], list[Path]],
    stable_for_seconds: float,
    poll_seconds: float = 2.0,
    timeout_seconds: float = 180.0,
) -> None:

    deadline = time.monotonic() + timeout_seconds
    previous: tuple[tuple[str, ...], int] | None = None
    stable_since = time.monotonic()

    while True:
        current = _snapshot_results(find_part_files)
        now = time.monotonic()

        if current != previous:
            print(
                f"Waiting for stable results: {len(current[0])} finalized "
                f"part file(s), {current[1]} row(s)..."
            )
            previous = current
            stable_since = now
        elif current[1] > 0 and now - stable_since >= stable_for_seconds:
            print(
                f"Results stable for {stable_for_seconds:.0f}s: "
                f"{len(current[0])} part file(s), {current[1]} row(s)."
            )
            return

        if now >= deadline:
            raise TimeoutError(
                f"Results not stable after {timeout_seconds:.0f}s "
                f"(finalized part files: {len(current[0])}, "
                f"rows: {current[1]}). The Flink job may still be writing, "
                "or checkpointing may be stuck."
            )

        time.sleep(poll_seconds)


def resolve_config_path(experiment: str | None) -> Path:
    if experiment:
        return PROJECT_ROOT / "config" / "experiments" / f"{experiment}.yml"

    return PROJECT_ROOT / "config" / "base.yml"


def configure_config_path(experiment: str | None) -> Path:
    config_path = resolve_config_path(experiment)
    os.environ["CONFIG_PATH"] = str(config_path)
    return config_path


def resolve_project_path(path_value: str) -> Path:
    path = Path(path_value)

    if path.is_absolute():
        return path

    return PROJECT_ROOT / path


def add_experiment_name_to_output_file(
    output_file: Path,
    experiment_name: str,
) -> Path:
    experiment_name = experiment_name.strip() or "base"

    suffix = output_file.suffix or ".csv"
    stem = output_file.stem

    if stem.endswith(f"_{experiment_name}"):
        return output_file

    return output_file.with_name(f"{stem}_{experiment_name}{suffix}")


def get_experiment_name(cfg: dict) -> str:
    experiment_cfg = cfg.get("experiment", {})
    return str(experiment_cfg.get("name", "base"))
