#!/usr/bin/env python3
from __future__ import annotations

import logging
import sys

from common.config import load_config
from common.logging_utils import configure_logging
from global_state import (
    build_q3_global_snapshots,
    build_q3_window_snapshots,
)
from spark_runtime import (
    await_queries_until_idle,
    build_spark_runtime_config,
    checkpoint_path,
    create_spark_session,
    read_flights_stream,
    write_csv_stream,
)


Q3_WINDOW_CHOICES = ("1d", "7d", "global", "all")
DAY_MS = 86_400_000


def selected_q3_window(value: object) -> str:
    window = str(value or "all").strip().lower()
    if window not in Q3_WINDOW_CHOICES:
        raise ValueError(
            "q3.window must be one of: "
            + ", ".join(Q3_WINDOW_CHOICES)
        )
    return window


def enabled_q3_windows(
    selected_window: str,
    paths: dict,
) -> list[tuple[str, int, int, str]]:
    windows = [
        ("1d", DAY_MS, 0, paths["spark_q3_results_path_1d"]),
        ("7d", 7 * DAY_MS, 6 * DAY_MS, paths["spark_q3_results_path_7d"]),
    ]
    if selected_window == "all":
        return windows
    return [window for window in windows if window[0] == selected_window]


def global_q3_enabled(selected_window: str) -> bool:
    return selected_window in {"all", "global"}


def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)

    cfg = load_config()
    runtime_cfg = build_spark_runtime_config(cfg)
    spark = create_spark_session("SparkStructuredQ3", runtime_cfg)

    paths = cfg["paths"]
    q3_cfg = cfg["q3"]
    selected_window = selected_q3_window(q3_cfg.get("window", "all"))
    watermark_delay = int(q3_cfg["watermark_delay_seconds"])
    sketch_alpha = float(q3_cfg.get("sketch_alpha", 0.01))

    logger.info(
        "Spark Q3 | Kafka: %s topic: %s",
        runtime_cfg.kafka_bootstrap,
        runtime_cfg.kafka_topic,
    )
    logger.info("Spark Q3 | Enabled window(s): %s", selected_window)
    logger.info(
        "Spark Q3 | DDSketch DataDog alpha: %.4f (tutte le finestre)",
        sketch_alpha,
    )
    logger.info(
        "Spark Q3 | Stateful engine: applyInPandasWithState "
        "(distributed + checkpointed)",
    )

    source = read_flights_stream(spark, runtime_cfg, "q3")
    queries = []

    for label, duration_ms, offset_ms, output_path in enabled_q3_windows(
        selected_window,
        paths,
    ):
        output = build_q3_window_snapshots(
            source,
            watermark_delay_seconds=watermark_delay,
            alpha=sketch_alpha,
            window_duration_ms=duration_ms,
            window_offset_ms=offset_ms,
        )
        queries.append(
            write_csv_stream(
                output,
                path=output_path,
                checkpoint=checkpoint_path(runtime_cfg, "q3", label),
                query_name=f"spark-q3-{label}",
                runtime_cfg=runtime_cfg,
            )
        )
        logger.info(
            "Spark Q3 [%s] | Official DDSketch state -> %s",
            label,
            output_path,
        )

    if global_q3_enabled(selected_window):
        output_path = paths["spark_q3_results_path_global"]
        snapshots = build_q3_global_snapshots(
            source,
            watermark_delay_seconds=watermark_delay,
            alpha=sketch_alpha,
        )
        queries.append(
            write_csv_stream(
                snapshots,
                path=output_path,
                checkpoint=checkpoint_path(runtime_cfg, "q3", "global"),
                query_name="spark-q3-global",
                runtime_cfg=runtime_cfg,
            )
        )
        logger.info(
            "Spark Q3 [global] | Official DDSketch daily snapshots -> %s",
            output_path,
        )

    await_queries_until_idle(
        queries,
        runtime_cfg,
        logger,
        query_label="q3",
        experiment=str(cfg.get("experiment", {}).get("name", "base")),
    )
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.getLogger(__name__).exception("Spark Q3 failed")
        sys.exit(1)
