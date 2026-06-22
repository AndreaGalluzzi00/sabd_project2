from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG_PATH = "/config/base.yml"


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)

    for key, value in override.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value

    return merged


def _load_yaml_file(config_path: Path) -> dict[str, Any]:
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def load_config() -> dict[str, Any]:
    config_path = Path(os.getenv("CONFIG_PATH", DEFAULT_CONFIG_PATH)).resolve()

    cfg = _load_yaml_file(config_path)

    extends_path = cfg.pop("extends", None)

    if extends_path is None:
        return cfg

    extends_path = Path(extends_path)

    if not extends_path.is_absolute():
        extends_path = config_path.parent / extends_path

    base_cfg = _load_yaml_file(extends_path.resolve())

    return deep_merge(base_cfg, cfg)