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

    is_eos = F.col("airline") == "__EOS__"
    is_target_airline = F.col("airline").isin(AIRLINES)

    # Keep EOS rows inside the stateful aggregation. A post-aggregate filter on
    # the grouping key can be pushed below the aggregate by Spark's optimizer,
    # which would drop "__EOS__" before it advances the watermark. Instead, map
    # every EOS marker to the real Q1 airline keys and mark it as non-real: it
    # advances event time but contributes zero to all metrics.
    q1_input = (
        flights.filter(is_target_airline | is_eos)
        .withColumn("_source_airline", F.col("airline"))
        .withColumn(
            "_q1_airlines",
            F.when(
                F.col("_source_airline") == "__EOS__",
                F.array(*[F.lit(airline) for airline in AIRLINES]),
            ).otherwise(F.array(F.col("_source_airline"))),
        )
        .withColumn("airline", F.explode(F.col("_q1_airlines")))
        .withColumn("_is_real", F.col("_source_airline") != "__EOS__")
    )

    is_not_cancelled = not_cancelled()
    is_not_diverted = not_diverted()
    is_real = F.col("_is_real")

    aggregated = (
        q1_input.groupBy(F.window("rowtime", "1 hour").alias("w"), F.col("airline"))
        .agg(
            F.sum(F.when(is_real, 1).otherwise(0)).cast("long").alias("num_flights"),
            F.sum(F.when(is_real & is_not_cancelled & is_not_diverted, 1).otherwise(0)).cast("long").alias("completed"),
            F.sum(F.when(is_real & ~is_not_cancelled, 1).otherwise(0)).cast("long").alias("cancelled"),
            F.sum(F.when(is_real & ~is_not_diverted, 1).otherwise(0)).cast("long").alias("diverted"),
            F.avg(F.when(is_real & is_not_cancelled, F.col("dep_delay"))).alias("dep_delay_mean"),
            F.sum(F.when(is_real & is_not_cancelled, 1).otherwise(0)).cast("long").alias("_non_cancelled"),
            F.sum(F.when(is_real & is_not_cancelled & (F.col("dep_delay") > 15.0), 1).otherwise(0)).cast("long").alias("_late_departures"),
        )
        .filter(F.col("num_flights") > 0)
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
    )

    query = write_csv_stream(
        output,
        path=paths["spark_q1_results_path"],
        checkpoint=checkpoint_path(runtime_cfg, "q1"),
        query_name="spark-q1",
        runtime_cfg=runtime_cfg,
    )

    await_queries_until_idle(
        [query], runtime_cfg, logger,
        query_label="q1",
        experiment=str(cfg.get("experiment", {}).get("name", "base")),
    )
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.getLogger(__name__).exception("Spark Q1 failed")
        sys.exit(1)
