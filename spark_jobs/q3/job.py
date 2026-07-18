#!/usr/bin/env python3
from __future__ import annotations

import logging
import math
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.delay_sketch import DelayDDSketch
from common.config import load_config
from common.logging_utils import configure_logging
from global_state import build_q3_global_snapshots
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
Q3_WINDOW_CHOICES = ("1d", "7d", "global", "all")
Q3_QUANTILE_IMPL_CHOICES = ("ddsketch", "native")
Q3_ZERO_EPSILON = 1e-9


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
        ("1d", "1 day", None, paths["spark_q3_results_path_1d"]),
        ("7d", "7 days", "6 days", paths["spark_q3_results_path_7d"]),
    ]
    if selected_window == "all":
        return windows
    return [window for window in windows if window[0] == selected_window]


def global_q3_enabled(selected_window: str) -> bool:
    return selected_window in {"all", "global"}


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


def global_batch_writer(output_path: str):
    def write(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        (
            batch_df.select(
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
            )
            .coalesce(1)
            .write.mode("append")
            .option("header", "false")
            .csv(output_path)
        )

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
    logger.info("Spark Q3 | Bounded-window quantile implementation: %s", quantile_impl)
    if quantile_impl == "native":
        logger.info("Spark Q3 | percentile_approx accuracy: %d", accuracy)
    else:
        logger.info("Spark Q3 | DDSketch alpha: %.4f", sketch_alpha)
    if global_q3_enabled(selected_window):
        logger.info(
            "Spark Q3 [global] | DDSketch alpha: %.4f (aligned with Flink)",
            sketch_alpha,
        )

    source = read_flights_stream(spark, runtime_cfg, "q3")
    flights = source.withWatermark("rowtime", f"{watermark_delay} seconds")

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

    if global_q3_enabled(selected_window):
        output_path = paths["spark_q3_results_path_global"]
        snapshots = build_q3_global_snapshots(
            source,
            watermark_delay_seconds=watermark_delay,
            alpha=sketch_alpha,
        )
        queries.append(
            write_foreach_batch_stream(
                snapshots,
                writer=global_batch_writer(output_path),
                checkpoint=checkpoint_path(runtime_cfg, "q3", "global"),
                query_name="spark-q3-global",
                runtime_cfg=runtime_cfg,
            )
        )
        logger.info(
            "Spark Q3 [global] | Checkpointed daily event-time snapshots -> %s",
            output_path,
        )

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
