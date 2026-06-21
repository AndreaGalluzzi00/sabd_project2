from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG_PATH = "/config/base.yml"


def load_config() -> dict[str, Any]:
    config_path = Path(os.getenv("CONFIG_PATH", DEFAULT_CONFIG_PATH))

    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}