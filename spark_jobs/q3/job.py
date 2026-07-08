#!/usr/bin/env python3
from __future__ import annotations

import logging
import math
import sys
from datetime import datetime, timezone

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
Q3_QUANTILE_IMPL_CHOICES = ("ddsketch", "native")
Q3_ZERO_EPSILON = 1e-9
DATASET_START_TS = "2025-01-01 00:00:00"
DATASET_START_MS = int(
    datetime.strptime(DATASET_START_TS, "%Y-%m-%d %H:%M:%S")
    .replace(tzinfo=timezone.utc)
    .timestamp()
    * 1000
)


def _format_event_time_ms(event_time_ms: int | None) -> str:
    if event_time_ms is None:
        return DATASET_START_TS
    return datetime.fromtimestamp(
        event_time_ms / 1000.0,
        tz=timezone.utc,
    ).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _next_emit_after(event_time_ms: int, snapshot_event_step_ms: int) -> int:
    if event_time_ms < DATASET_START_MS:
        return DATASET_START_MS
    elapsed = event_time_ms - DATASET_START_MS
    return DATASET_START_MS + ((elapsed // snapshot_event_step_ms) + 1) * snapshot_event_step_ms


def selected_q3_window(value: object) -> str:
    window = str(value or "all").strip().lower()
    if window not in Q3_WINDOW_CHOICES:
        raise ValueError(
            "q3.window must be one of: "
            + ", ".join(Q3_WINDOW_CHOICES)
        )
    return window


def selected_q3_quantile_impl(value: object) -> str:
    impl = str(value or "ddsketch").strip().lower()
    aliases = {
        "percentile_approx": "native",
        "spark": "native",
    }
    impl = aliases.get(impl, impl)
    if impl not in Q3_QUANTILE_IMPL_CHOICES:
        raise ValueError(
            "q3.spark_quantile_impl must be one of: "
            + ", ".join(Q3_QUANTILE_IMPL_CHOICES)
            + " (aliases: percentile_approx, spark)"
        )
    return impl


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


def build_distribution_native(
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


def build_distribution_ddsketch(
    flights: DataFrame,
    *,
    window_duration: str,
    alpha: float,
    start_time: str | None = None,
) -> DataFrame:
    if start_time is None:
        window_col = F.window("rowtime", window_duration)
    else:
        window_col = F.window("rowtime", window_duration, window_duration, start_time)

    gamma = (1.0 + alpha) / (1.0 - alpha)
    log_gamma = math.log(gamma)

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
        .withColumn(
            "_bucket_kind",
            F.when(~F.col("_is_real"), F.lit("eos"))
            .when(F.col("dep_delay") > F.lit(Q3_ZERO_EPSILON), F.lit("pos"))
            .when(F.col("dep_delay") < F.lit(-Q3_ZERO_EPSILON), F.lit("neg"))
            .otherwise(F.lit("zero")),
        )
        .withColumn(
            "_bucket_index",
            F.when(
                F.col("_bucket_kind").isin("pos", "neg"),
                F.ceil(F.log(F.abs(F.col("dep_delay"))) / F.lit(log_gamma)).cast("int"),
            ).otherwise(F.lit(0)),
        )
    )

    return (
        relevant.groupBy(
            window_col.alias("w"),
            "airline",
            "hour",
            "_bucket_kind",
            "_bucket_index",
        )
        .agg(
            F.sum(F.when(F.col("_is_real"), 1).otherwise(0)).cast("long").alias("bucket_count"),
            F.min(F.when(F.col("_is_real"), F.col("dep_delay"))).alias("delay_min"),
            F.max(F.when(F.col("_is_real"), F.col("dep_delay"))).alias("delay_max"),
        )
        .select(
            format_ts(F.col("w.start")).alias("ts"),
            "airline",
            "hour",
            F.col("_bucket_kind").alias("bucket_kind"),
            F.col("_bucket_index").alias("bucket_index"),
            "bucket_count",
            "delay_min",
            "delay_max",
        )
    )


def _add_bucket(
    sketch: DelayDDSketch,
    bucket_kind: str,
    bucket_index: int,
    count: int,
) -> None:
    if count <= 0:
        return

    if bucket_kind == "pos":
        sketch._pos[bucket_index] = sketch._pos.get(bucket_index, 0) + count
    elif bucket_kind == "neg":
        sketch._neg[bucket_index] = sketch._neg.get(bucket_index, 0) + count
    elif bucket_kind == "zero":
        sketch._zero_count += count
    else:
        return

    sketch.count += count


def sketch_bucket_batch_writer(output_path: str, alpha: float):
    def write(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        states: dict[tuple[str, str, int], dict] = {}

        rows = batch_df.select(
            "ts",
            "airline",
            "hour",
            "bucket_kind",
            "bucket_index",
            "bucket_count",
            "delay_min",
            "delay_max",
        ).toLocalIterator()

        for row in rows:
            count = int(row.bucket_count or 0)
            if count <= 0 or row.airline not in AIRLINES or row.hour is None:
                continue

            key = (row.ts, row.airline, int(row.hour))
            state = states.setdefault(
                key,
                {
                    "sketch": DelayDDSketch(alpha),
                    "delay_min": math.inf,
                    "delay_max": -math.inf,
                },
            )
            _add_bucket(
                state["sketch"],
                str(row.bucket_kind),
                int(row.bucket_index or 0),
                count,
            )
            if row.delay_min is not None:
                state["delay_min"] = min(state["delay_min"], float(row.delay_min))
            if row.delay_max is not None:
                state["delay_max"] = max(state["delay_max"], float(row.delay_max))

        output_rows = []
        for (ts, airline, hour), state in sorted(states.items()):
            sketch = state["sketch"]
            if sketch.count == 0:
                continue

            sketch.min_value = state["delay_min"]
            sketch.max_value = state["delay_max"]

            output_rows.append(
                (
                    ts,
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


def cumulative_batch_writer(output_path: str, alpha: float, snapshot_event_step_ms: int):
    sketches: dict[tuple[str, int], DelayDDSketch] = {}
    max_event_time_ms: int | None = None
    next_emit_event_time_ms = DATASET_START_MS + max(1, int(snapshot_event_step_ms))
    last_snapshot_event_time_ms: int | None = None
    dirty_since_snapshot = False

    def snapshot_rows() -> list[tuple]:
        snapshot_ts = _format_event_time_ms(max_event_time_ms)
        rows = []
        for (airline, hour), sketch in sorted(sketches.items()):
            if sketch.count == 0:
                continue
            rows.append(
                (
                    snapshot_ts,
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
        nonlocal dirty_since_snapshot
        nonlocal last_snapshot_event_time_ms
        nonlocal max_event_time_ms
        nonlocal next_emit_event_time_ms

        saw_eos = False
        output_rows = []

        rows = batch_df.select(
            "event_time",
            "airline",
            "crs_dep_time",
            "dep_delay",
            "cancelled",
            "diverted",
        ).orderBy(
            F.col("event_time").asc_nulls_last()
        ).toLocalIterator()

        for row in rows:
            if row.airline == "__EOS__":
                saw_eos = True
                continue

            if row.event_time is not None:
                event_time = int(row.event_time)
                if max_event_time_ms is None or event_time > max_event_time_ms:
                    max_event_time_ms = event_time

            cancelled = row.cancelled or 0.0
            diverted = row.diverted or 0.0
            if (
                row.airline not in AIRLINES
                or cancelled >= 0.5
                or diverted >= 0.5
                or row.dep_delay is None
                or row.crs_dep_time is None
            ):
                if (
                    dirty_since_snapshot
                    and max_event_time_ms is not None
                    and max_event_time_ms >= next_emit_event_time_ms
                ):
                    rows_to_emit = snapshot_rows()
                    if rows_to_emit:
                        output_rows.extend(rows_to_emit)
                        last_snapshot_event_time_ms = max_event_time_ms
                        dirty_since_snapshot = False
                    next_emit_event_time_ms = _next_emit_after(
                        max_event_time_ms,
                        snapshot_event_step_ms,
                    )
                continue

            hour = (int(row.crs_dep_time) // 100) % 24
            key = (row.airline, hour)
            sketch = sketches.setdefault(key, DelayDDSketch(alpha))
            sketch.add(float(row.dep_delay))
            dirty_since_snapshot = True

            if max_event_time_ms is not None and max_event_time_ms >= next_emit_event_time_ms:
                rows_to_emit = snapshot_rows()
                if rows_to_emit:
                    output_rows.extend(rows_to_emit)
                    last_snapshot_event_time_ms = max_event_time_ms
                    dirty_since_snapshot = False
                next_emit_event_time_ms = _next_emit_after(
                    max_event_time_ms,
                    snapshot_event_step_ms,
                )

        if saw_eos and (
            dirty_since_snapshot
            or max_event_time_ms != last_snapshot_event_time_ms
        ):
            rows_to_emit = snapshot_rows()
            if rows_to_emit:
                output_rows.extend(rows_to_emit)
                last_snapshot_event_time_ms = max_event_time_ms
            dirty_since_snapshot = False

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
    quantile_impl = selected_q3_quantile_impl(q3_cfg.get("spark_quantile_impl", "ddsketch"))
    watermark_delay = int(q3_cfg["watermark_delay_seconds"])
    accuracy = int(q3_cfg.get("spark_percentile_accuracy", 10000))
    sketch_alpha = float(q3_cfg.get("sketch_alpha", 0.01))

    logger.info("Spark Q3 | Kafka: %s topic: %s", runtime_cfg.kafka_bootstrap, runtime_cfg.kafka_topic)
    logger.info("Spark Q3 | Enabled window(s): %s", selected_window)
    logger.info("Spark Q3 | Quantile implementation: %s", quantile_impl)
    if quantile_impl == "native":
        logger.info("Spark Q3 | percentile_approx accuracy: %d", accuracy)
    else:
        logger.info("Spark Q3 | DDSketch alpha: %.4f", sketch_alpha)

    flights = (
        read_flights_stream(spark, runtime_cfg, "q3")
        .withWatermark("rowtime", f"{watermark_delay} seconds")
    )

    windows = enabled_q3_windows(selected_window, paths)

    queries = []
    for label, duration, start_time, output_path in windows:
        if quantile_impl == "native":
            output = build_distribution_native(
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
        else:
            buckets = build_distribution_ddsketch(
                flights,
                window_duration=duration,
                start_time=start_time,
                alpha=sketch_alpha,
            )
            queries.append(
                write_foreach_batch_stream(
                    buckets,
                    writer=sketch_bucket_batch_writer(output_path, sketch_alpha),
                    checkpoint=checkpoint_path(runtime_cfg, "q3", label),
                    query_name=f"spark-q3-{label}",
                    runtime_cfg=runtime_cfg,
                )
            )
        logger.info("Spark Q3 [%s] | Results path: %s", label, output_path)

    if cumulative_q3_enabled(selected_window):
        output_path = paths["spark_q3_results_path_cumulative"]
        snapshot_event_step_ms = int(q3_cfg.get("cumulative_snapshot_event_step_ms", 2_700_000))
        queries.append(
            write_foreach_batch_stream(
                flights,
                writer=cumulative_batch_writer(
                    output_path,
                    alpha=sketch_alpha,
                    snapshot_event_step_ms=snapshot_event_step_ms,
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
