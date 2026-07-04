#!/usr/bin/env python3
from __future__ import annotations

import logging
import sys

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

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
    write_foreach_batch_stream,
)


def build_stats(
    flights: DataFrame,
    *,
    window_duration: str,
    start_time: str | None = None,
) -> DataFrame:
    is_eos = F.col("airline") == "__EOS__"
    is_real = (not_cancelled() & not_diverted()) & ~is_eos
    completed = flights.filter(is_real | is_eos)
    severe = is_real & (F.col("dep_delay") > F.lit(30.0))

    if start_time is None:
        window_col = F.window("rowtime", window_duration)
    else:
        window_col = F.window("rowtime", window_duration, window_duration, start_time)

    delayed_flight = F.when(
        severe,
        F.struct(
            (-F.col("dep_delay")).alias("sort_delay"),
            F.col("airline").alias("airline"),
            F.col("dest_airport_id").alias("dest_airport_id"),
            F.col("dep_delay").alias("dep_delay"),
        ),
    )

    stats = (
        completed.groupBy(window_col.alias("w"), F.col("origin_airport_id"))
        .agg(
            F.sum(F.when(is_real, 1).otherwise(0)).cast("long").alias("num_flights"),
            F.sum(F.when(severe, 1).otherwise(0)).cast("long").alias("severe_delays"),
            F.avg(F.when(is_real, F.col("dep_delay"))).alias("dep_delay_mean"),
            F.max(F.when(is_real, F.col("dep_delay"))).alias("dep_delay_max"),
            F.collect_list(delayed_flight).alias("_delayed_raw"),
        )
        .filter(F.col("num_flights") >= 30)
        .withColumn(
            "delayed_flights",
            F.expr(
                """
                concat(
                  '[',
                  array_join(
                    transform(
                      slice(sort_array(filter(_delayed_raw, x -> x is not null)), 1, 20),
                      x -> concat(
                        '(',
                        x.airline,
                        ',',
                        cast(x.dest_airport_id as string),
                        ',',
                        format_string('%.2f', x.dep_delay),
                        ')'
                      )
                    ),
                    ', '
                  ),
                  ']'
                )
                """
            ),
        )
    )

    return stats.select(
        format_ts(F.col("w.start")).alias("ts"),
        "origin_airport_id",
        "num_flights",
        "severe_delays",
        "dep_delay_mean",
        "dep_delay_max",
        "delayed_flights",
    )


def ranked_batch_writer(output_path: str):
    def write(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        rank_window = Window.partitionBy("ts").orderBy(
            F.desc("severe_delays"),
            F.asc("origin_airport_id"),
        )

        ranked = (
            batch_df.withColumn("rank", F.row_number().over(rank_window))
            .filter(F.col("rank") <= 10)
            .select(
                "ts",
                "rank",
                "origin_airport_id",
                "num_flights",
                "severe_delays",
                "dep_delay_mean",
                "dep_delay_max",
                "delayed_flights",
            )
        )

        ranked.coalesce(1).write.mode("append").option("header", "false").csv(output_path)

    return write


def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)

    cfg = load_config()
    runtime_cfg = build_spark_runtime_config(cfg)
    spark = create_spark_session("SparkStructuredQ2", runtime_cfg)

    paths = cfg["paths"]
    watermark_delay = int(cfg["q2"]["watermark_delay_seconds"])

    logger.info("Spark Q2 | Kafka: %s topic: %s", runtime_cfg.kafka_bootstrap, runtime_cfg.kafka_topic)

    flights = (
        read_flights_stream(spark, runtime_cfg, "q2")
        .withWatermark("rowtime", f"{watermark_delay} seconds")
    )

    windows = [
        ("1h", "1 hour", None, paths["spark_q2_results_path_1h"]),
        ("6h", "6 hours", None, paths["spark_q2_results_path_6h"]),
        ("global", "365 days", "14 days", paths["spark_q2_results_path_global"]),
    ]

    queries = []
    for label, duration, start_time, output_path in windows:
        stats = build_stats(
            flights,
            window_duration=duration,
            start_time=start_time,
        )
        queries.append(
            write_foreach_batch_stream(
                stats,
                writer=ranked_batch_writer(output_path),
                checkpoint=checkpoint_path(runtime_cfg, "q2", label),
                query_name=f"spark-q2-{label}",
                runtime_cfg=runtime_cfg,
            )
        )
        logger.info("Spark Q2 [%s] | Results path: %s", label, output_path)

    await_queries_until_idle(queries, runtime_cfg, logger)
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.getLogger(__name__).exception("Spark Q2 failed")
        sys.exit(1)
