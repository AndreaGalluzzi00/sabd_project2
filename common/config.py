from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG_PATH = "/config/base.yml"
BASE_EXPERIMENT_NAME = "base"


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


def _load_config_with_extends(
    config_path: Path,
    seen: set[Path] | None = None,
) -> dict[str, Any]:
    config_path = config_path.resolve()
    seen = set() if seen is None else set(seen)

    if config_path in seen:
        chain = " -> ".join(str(path) for path in [*seen, config_path])
        raise ValueError(f"Cyclic config extends detected: {chain}")

    seen.add(config_path)
    cfg = _load_yaml_file(config_path)
    extends_path = cfg.pop("extends", None)

    if extends_path is None:
        return cfg

    extends_path = Path(extends_path)

    if not extends_path.is_absolute():
        extends_path = config_path.parent / extends_path

    base_cfg = _load_config_with_extends(
        config_path=extends_path,
        seen=seen,
    )

    return deep_merge(base_cfg, cfg)


def _join_path(root: str, *parts: str) -> str:
    root = str(root).rstrip("/")
    clean_parts = [str(part).strip("/") for part in parts if str(part)]

    if not root:
        return "/".join(clean_parts)

    if not clean_parts:
        return root

    return "/".join([root, *clean_parts])


def _ensure_child_path(root: str, child: str) -> str:
    root = str(root).rstrip("/")
    last_part = root.replace("\\", "/").split("/")[-1]

    if last_part == child:
        return root

    return _join_path(root, child)


def _setdefault_path(paths: dict[str, Any], key: str, value: str) -> None:
    if paths.get(key) in (None, ""):
        paths[key] = value


def _result_roots(cfg: dict[str, Any]) -> tuple[str, str, str]:
    experiment_cfg = cfg.setdefault("experiment", {})
    paths = cfg.setdefault("paths", {})

    name = str(experiment_cfg.get("name", BASE_EXPERIMENT_NAME)).strip()
    name = name or BASE_EXPERIMENT_NAME

    flink_root = str(paths.setdefault("flink_results_root", "/opt/flink/results"))
    spark_root = str(paths.setdefault("spark_results_root", "/opt/spark/results"))
    host_root = str(paths.setdefault("host_results_root", "Results"))

    if name == BASE_EXPERIMENT_NAME:
        return flink_root, spark_root, host_root

    host_parent = str(
        experiment_cfg.get("output_dir_host", _join_path(host_root, "experiments"))
    )
    flink_parent = str(
        experiment_cfg.get("output_dir_flink", _join_path(flink_root, "experiments"))
    )
    spark_parent = str(
        experiment_cfg.get("output_dir_spark", _join_path(spark_root, "experiments"))
    )

    return (
        _ensure_child_path(flink_parent, name),
        _ensure_child_path(spark_parent, name),
        _ensure_child_path(host_parent, name),
    )


def _materialize_derived_paths(cfg: dict[str, Any]) -> dict[str, Any]:
    experiment_cfg = cfg.setdefault("experiment", {})
    paths = cfg.setdefault("paths", {})

    flink_root, spark_root, host_root = _result_roots(cfg)
    merged_host_root = str(paths["host_results_root"])

    def merged_output_host_path(
        query: str,
        engine: str,
        implementation: str,
        label: str | None = None,
    ) -> str:
        parts = [query]

        if label:
            parts.append(label)

        parts.extend([engine, implementation])

        return _join_path(merged_host_root, f"{'_'.join(parts)}.csv")

    def flink_table_root(query: str) -> str:
        return _join_path(flink_root, "flink", "table", query)

    def flink_table_host_root(query: str) -> str:
        return _join_path(host_root, "flink", "table", query)

    def spark_structured_root(query: str) -> str:
        return _join_path(spark_root, "spark", "structured", query)

    def spark_structured_host_root(query: str) -> str:
        return _join_path(host_root, "spark", "structured", query)

    _setdefault_path(
        paths,
        "q1_results_path",
        _join_path(flink_table_root("q1"), "part_files"),
    )
    _setdefault_path(
        paths,
        "q1_results_host_path",
        _join_path(flink_table_host_root("q1"), "part_files"),
    )
    _setdefault_path(
        paths,
        "q1_merged_output_host_path",
        merged_output_host_path("q1", "flink", "table"),
    )

    for label in ("1h", "6h", "global", "cumulative"):
        _setdefault_path(
            paths,
            f"q2_results_path_{label}",
            _join_path(flink_table_root("q2"), label, "part_files"),
        )
        _setdefault_path(
            paths,
            f"q2_results_host_path_{label}",
            _join_path(flink_table_host_root("q2"), label, "part_files"),
        )
        _setdefault_path(
            paths,
            f"q2_merged_output_host_path_{label}",
            merged_output_host_path("q2", "flink", "table", label),
        )

    for label in ("1d", "7d", "global", "cumulative"):
        _setdefault_path(
            paths,
            f"q3_results_path_{label}",
            _join_path(flink_table_root("q3"), label, "part_files"),
        )
        _setdefault_path(
            paths,
            f"q3_results_host_path_{label}",
            _join_path(flink_table_host_root("q3"), label, "part_files"),
        )
        _setdefault_path(
            paths,
            f"q3_merged_output_host_path_{label}",
            merged_output_host_path("q3", "flink", "table", label),
        )

    _setdefault_path(
        paths,
        "spark_q1_results_path",
        _join_path(spark_structured_root("q1"), "part_files"),
    )
    _setdefault_path(
        paths,
        "spark_q1_results_host_path",
        _join_path(spark_structured_host_root("q1"), "part_files"),
    )
    _setdefault_path(
        paths,
        "spark_q1_merged_output_host_path",
        merged_output_host_path("q1", "spark", "structured"),
    )

    for query, labels in {
        "q2": ("1h", "6h", "global", "cumulative"),
        "q3": ("1d", "7d", "global", "cumulative"),
    }.items():
        for label in labels:
            _setdefault_path(
                paths,
                f"spark_{query}_results_path_{label}",
                _join_path(spark_structured_root(query), label, "part_files"),
            )
            _setdefault_path(
                paths,
                f"spark_{query}_results_host_path_{label}",
                _join_path(spark_structured_host_root(query), label, "part_files"),
            )
            _setdefault_path(
                paths,
                f"spark_{query}_merged_output_host_path_{label}",
                merged_output_host_path(query, "spark", "structured", label),
            )

    _setdefault_path(
        experiment_cfg,
        "producer_log_path",
        _join_path(flink_root, "producer.log"),
    )

    return cfg


def load_config() -> dict[str, Any]:
    config_path = Path(os.getenv("CONFIG_PATH", DEFAULT_CONFIG_PATH)).resolve()

    cfg = _load_config_with_extends(config_path)

    return _materialize_derived_paths(cfg)
