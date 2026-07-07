#!/usr/bin/env python3
from __future__ import annotations

import logging
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.delay_sketch import DelayDDSketch
from common.config import load_config
from common.logging_utils import configure_logging
from spark_runtime import (
    await_queries_until_idle,
    build_spark_runtime_config,
    checkpoint_path,
    create_spark_session,
    format_ts,
    not_cancelled,
    not_diverted,
    read_flights_stream,
    write_csv_stream,
    write_foreach_batch_stream,
)


AIRLINES = ["AA", "DL", "UA", "WN"]
Q3_WINDOW_CHOICES = ("1d", "7d", "global", "cumulative", "all")
DATASET_START_TS = "2025-01-01 00:00:00"


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
) -> list[tuple[str, str, str | None, str]]:
    windows = [
        # Spec Q3 windows: 1 day, 7 days, and global.
        ("1d", "1 day", None, paths["spark_q3_results_path_1d"]),
        # Start offset 6 days aligns weekly buckets to 2025-01-01.
        ("7d", "7 days", "6 days", paths["spark_q3_results_path_7d"]),
        # Start offset 14 days aligns the 365-day bucket to 2025-01-01.
        ("global", "365 days", "14 days", paths["spark_q3_results_path_global"]),
    ]
    if selected_window == "all":
        return windows
    if selected_window == "cumulative":
        return []
    return [window for window in windows if window[0] == selected_window]


def cumulative_q3_enabled(selected_window: str) -> bool:
    return selected_window in {"all", "cumulative"}


def build_distribution(
    flights: DataFrame,
    *,
    window_duration: str,
    accuracy: int,
    start_time: str | None = None,
) -> DataFrame:
    if start_time is None:
        window_col = F.window("rowtime", window_duration)
    else:
        window_col = F.window("rowtime", window_duration, window_duration, start_time)

    is_eos = F.col("airline") == "__EOS__"
    is_real = (
        F.col("airline").isin(AIRLINES)
        & not_cancelled()
        & not_diverted()
        & F.col("dep_delay").isNotNull()
    )

    relevant = (
        flights.filter(is_real | is_eos)
        .withColumn("hour", F.pmod(F.floor(F.col("crs_dep_time") / 100), F.lit(24)).cast("int"))
        .withColumn("_is_real", is_real)
    )

    aggregated = (
        relevant.groupBy(window_col.alias("w"), "airline", "hour")
        .agg(
            F.sum(F.when(F.col("_is_real"), 1).otherwise(0)).cast("long").alias("num_flights"),
            F.min(F.when(F.col("_is_real"), F.col("dep_delay"))).alias("delay_min"),
            F.expr(
                "percentile_approx(IF(_is_real, dep_delay, NULL), array(0.25D, 0.5D, 0.75D, 0.9D), "
                f"{accuracy})"
            ).alias("q"),
            F.max(F.when(F.col("_is_real"), F.col("dep_delay"))).alias("delay_max"),
        )
        .filter(F.col("num_flights") > 0)
    )

    return aggregated.select(
        format_ts(F.col("w.start")).alias("ts"),
        "airline",
        "hour",
        "num_flights",
        "delay_min",
        F.col("q")[0].alias("p25"),
        F.col("q")[1].alias("p50"),
        F.col("q")[2].alias("p75"),
        F.col("q")[3].alias("p90"),
        "delay_max",
    )


def cumulative_batch_writer(output_path: str, alpha: float):
    sketches: dict[tuple[str, int], DelayDDSketch] = {}

    def snapshot_rows() -> list[tuple]:
        rows = []
        for (airline, hour), sketch in sorted(sketches.items()):
            if sketch.count == 0:
                continue
            rows.append(
                (
                    DATASET_START_TS,
                    airline,
                    int(hour),
                    int(sketch.count),
                    float(sketch.min_value),
                    round(sketch.quantile(0.25), 2),
                    round(sketch.quantile(0.50), 2),
                    round(sketch.quantile(0.75), 2),
                    round(sketch.quantile(0.90), 2),
                    float(sketch.max_value),
                )
            )
        return rows

    def write(batch_df: DataFrame, batch_id: int) -> None:
        dirty = False
        saw_eos = False

        rows = batch_df.select(
            "airline",
            "crs_dep_time",
            "dep_delay",
            "cancelled",
            "diverted",
        ).toLocalIterator()

        for row in rows:
            if row.airline == "__EOS__":
                saw_eos = True
                continue

            cancelled = row.cancelled or 0.0
            diverted = row.diverted or 0.0
            if (
                row.airline not in AIRLINES
                or cancelled >= 0.5
                or diverted >= 0.5
                or row.dep_delay is None
                or row.crs_dep_time is None
            ):
                continue

            hour = (int(row.crs_dep_time) // 100) % 24
            key = (row.airline, hour)
            sketch = sketches.setdefault(key, DelayDDSketch(alpha))
            sketch.add(float(row.dep_delay))
            dirty = True

        if not dirty and not saw_eos:
            return

        output_rows = snapshot_rows()
        if not output_rows:
            return

        out = batch_df.sparkSession.createDataFrame(
            output_rows,
            schema=[
                "ts",
                "airline",
                "hour",
                "num_flights",
                "delay_min",
                "p25",
                "p50",
                "p75",
                "p90",
                "delay_max",
            ],
        )
        out.coalesce(1).write.mode("append").option("header", "false").csv(output_path)

    return write


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
    accuracy = int(q3_cfg.get("spark_percentile_accuracy", 10000))

    logger.info("Spark Q3 | Kafka: %s topic: %s", runtime_cfg.kafka_bootstrap, runtime_cfg.kafka_topic)
    logger.info("Spark Q3 | Enabled window(s): %s", selected_window)
    logger.info("Spark Q3 | percentile_approx accuracy: %d", accuracy)

    flights = (
        read_flights_stream(spark, runtime_cfg, "q3")
        .withWatermark("rowtime", f"{watermark_delay} seconds")
    )

    windows = enabled_q3_windows(selected_window, paths)

    queries = []
    for label, duration, start_time, output_path in windows:
        output = build_distribution(
            flights,
            window_duration=duration,
            start_time=start_time,
            accuracy=accuracy,
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
        logger.info("Spark Q3 [%s] | Results path: %s", label, output_path)

    if cumulative_q3_enabled(selected_window):
        output_path = paths["spark_q3_results_path_cumulative"]
        queries.append(
            write_foreach_batch_stream(
                flights,
                writer=cumulative_batch_writer(
                    output_path,
                    alpha=float(q3_cfg.get("sketch_alpha", 0.01)),
                ),
                checkpoint=checkpoint_path(runtime_cfg, "q3", "cumulative"),
                query_name="spark-q3-cumulative",
                runtime_cfg=runtime_cfg,
            )
        )
        logger.info("Spark Q3 [cumulative] | Results path: %s", output_path)

    await_queries_until_idle(
        queries, runtime_cfg, logger,
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
