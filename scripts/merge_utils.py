from __future__ import annotations

import argparse
import os
import sys
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

    return parser.parse_args()


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