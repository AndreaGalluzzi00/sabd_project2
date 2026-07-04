#!/usr/bin/env python3
from __future__ import annotations

import logging
import sys

from pyspark.sql import DataFrame
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
    write_csv_stream,
)


AIRLINES = ["AA", "DL", "UA", "WN"]


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


def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)

    cfg = load_config()
    runtime_cfg = build_spark_runtime_config(cfg)
    spark = create_spark_session("SparkStructuredQ3", runtime_cfg)

    paths = cfg["paths"]
    q3_cfg = cfg["q3"]
    watermark_delay = int(q3_cfg["watermark_delay_seconds"])
    accuracy = int(q3_cfg.get("spark_percentile_accuracy", 10000))

    logger.info("Spark Q3 | Kafka: %s topic: %s", runtime_cfg.kafka_bootstrap, runtime_cfg.kafka_topic)
    logger.info("Spark Q3 | percentile_approx accuracy: %d", accuracy)

    flights = (
        read_flights_stream(spark, runtime_cfg, "q3")
        .withWatermark("rowtime", f"{watermark_delay} seconds")
    )

    windows = [
        ("1d", "1 day", None, paths["spark_q3_results_path_1d"]),
        ("7d", "7 days", "6 days", paths["spark_q3_results_path_7d"]),
        ("global", "365 days", "14 days", paths["spark_q3_results_path_global"]),
    ]

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

    await_queries_until_idle(queries, runtime_cfg, logger)
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.getLogger(__name__).exception("Spark Q3 failed")
        sys.exit(1)
