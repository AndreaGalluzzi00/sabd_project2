#!/usr/bin/env python3
from __future__ import annotations

import logging
import sys

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


def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)

    cfg = load_config()
    runtime_cfg = build_spark_runtime_config(cfg)
    spark = create_spark_session("SparkStructuredQ1", runtime_cfg)

    paths = cfg["paths"]
    watermark_delay = int(cfg["q1"]["watermark_delay_seconds"])

    logger.info("Spark Q1 | Kafka: %s topic: %s", runtime_cfg.kafka_bootstrap, runtime_cfg.kafka_topic)
    logger.info("Spark Q1 | Results path: %s", paths["spark_q1_results_path"])

    flights = (
        read_flights_stream(spark, runtime_cfg, "q1")
        .withWatermark("rowtime", f"{watermark_delay} seconds")
    )

    # Keep the EOS marker through the stateful operator so its far-future
    # event time advances the watermark and closes the last real 1h window.
    # It is filtered out after aggregation, so it never appears in the CSV.
    q1_input = flights.filter(
        F.col("airline").isin(AIRLINES) | (F.col("airline") == "__EOS__")
    )

    is_not_cancelled = not_cancelled()
    is_not_diverted = not_diverted()

    aggregated = (
        q1_input.groupBy(F.window("rowtime", "1 hour").alias("w"), F.col("airline"))
        .agg(
            F.count(F.lit(1)).alias("num_flights"),
            F.sum(F.when(is_not_cancelled & is_not_diverted, 1).otherwise(0)).cast("long").alias("completed"),
            F.sum(F.when(~is_not_cancelled, 1).otherwise(0)).cast("long").alias("cancelled"),
            F.sum(F.when(~is_not_diverted, 1).otherwise(0)).cast("long").alias("diverted"),
            F.avg(F.when(is_not_cancelled, F.col("dep_delay"))).alias("dep_delay_mean"),
            F.sum(F.when(is_not_cancelled, 1).otherwise(0)).cast("long").alias("_non_cancelled"),
            F.sum(F.when(is_not_cancelled & (F.col("dep_delay") > 15.0), 1).otherwise(0)).cast("long").alias("_late_departures"),
        )
        .withColumn(
            "cancellation_rate",
            F.col("cancelled").cast("double") * F.lit(100.0) / F.col("num_flights"),
        )
        .withColumn(
            "late_departure_rate",
            F.when(
                F.col("_non_cancelled") > 0,
                F.col("_late_departures").cast("double") * F.lit(100.0) / F.col("_non_cancelled"),
            ),
        )
    )

    output = aggregated.select(
        format_ts(F.col("w.start")).alias("window_start"),
        format_ts(F.col("w.end")).alias("window_end"),
        "airline",
        "num_flights",
        "completed",
        "cancelled",
        "diverted",
        "dep_delay_mean",
        "cancellation_rate",
        "late_departure_rate",
    ).filter(
        F.col("airline").isin(AIRLINES)
    )

    query = write_csv_stream(
        output,
        path=paths["spark_q1_results_path"],
        checkpoint=checkpoint_path(runtime_cfg, "q1"),
        query_name="spark-q1",
        runtime_cfg=runtime_cfg,
    )

    await_queries_until_idle([query], runtime_cfg, logger)
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.getLogger(__name__).exception("Spark Q1 failed")
        sys.exit(1)
