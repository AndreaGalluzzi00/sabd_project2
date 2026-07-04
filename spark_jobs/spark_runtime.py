from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro


@dataclass(frozen=True)
class SparkRuntimeConfig:
    kafka_bootstrap: str
    kafka_topic: str
    consumer_group_prefix: str
    checkpoint_base_path: str
    shuffle_partitions: int
    trigger_processing_time: str
    idle_timeout_seconds: int
    startup_timeout_seconds: int
    max_offsets_per_trigger: int | None


def build_spark_runtime_config(cfg: dict[str, Any]) -> SparkRuntimeConfig:
    spark_cfg = cfg.get("spark", {})

    max_offsets = spark_cfg.get("max_offsets_per_trigger")

    return SparkRuntimeConfig(
        kafka_bootstrap=cfg["kafka"]["bootstrap_servers"],
        kafka_topic=cfg["kafka"]["topic"],
        consumer_group_prefix=str(
            spark_cfg.get("consumer_group", "spark-flight-analysis")
        ),
        checkpoint_base_path=str(
            spark_cfg.get("checkpoint_base_path", "/opt/spark/checkpoints")
        ),
        shuffle_partitions=int(spark_cfg.get("shuffle_partitions", 4)),
        trigger_processing_time=str(
            spark_cfg.get("trigger_processing_time", "5 seconds")
        ),
        idle_timeout_seconds=int(spark_cfg.get("idle_timeout_seconds", 60)),
        startup_timeout_seconds=int(spark_cfg.get("startup_timeout_seconds", 900)),
        max_offsets_per_trigger=(
            int(max_offsets) if max_offsets is not None else None
        ),
    )


def create_spark_session(
    app_name: str,
    runtime_cfg: SparkRuntimeConfig,
) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", runtime_cfg.shuffle_partitions)
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .getOrCreate()
    )


def read_schema(schema_path: str = "/opt/spark/app/schema/flight.avsc") -> str:
    return Path(schema_path).read_text(encoding="utf-8")


def read_flights_stream(
    spark: SparkSession,
    runtime_cfg: SparkRuntimeConfig,
    query_suffix: str,
) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", runtime_cfg.kafka_bootstrap)
        .option("subscribe", runtime_cfg.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("groupIdPrefix", f"{runtime_cfg.consumer_group_prefix}-{query_suffix}")
    )

    if runtime_cfg.max_offsets_per_trigger is not None:
        reader = reader.option(
            "maxOffsetsPerTrigger",
            runtime_cfg.max_offsets_per_trigger,
        )

    raw = reader.load()
    schema_json = read_schema()

    # Kafka values use the Confluent Avro wire format:
    # magic byte + 4-byte schema id + plain Avro payload.
    payload = raw.select(
        F.expr("substring(value, 6, length(value) - 5)").alias("avro_payload")
    )

    decoded = payload.select(
        from_avro(F.col("avro_payload"), schema_json).alias("flight")
    )

    return (
        decoded.select("flight.*")
        .withColumn(
            "rowtime",
            F.to_timestamp(F.from_unixtime(F.col("event_time") / F.lit(1000.0))),
        )
    )


def not_cancelled() -> F.Column:
    return F.coalesce(F.col("cancelled"), F.lit(0.0)) < F.lit(0.5)


def not_diverted() -> F.Column:
    return F.coalesce(F.col("diverted"), F.lit(0.0)) < F.lit(0.5)


def checkpoint_path(runtime_cfg: SparkRuntimeConfig, *parts: str) -> str:
    return str(Path(runtime_cfg.checkpoint_base_path, *parts))


def format_ts(column: F.Column) -> F.Column:
    return F.date_format(column, "yyyy-MM-dd HH:mm:ss")


def write_csv_stream(
    df: DataFrame,
    *,
    path: str,
    checkpoint: str,
    query_name: str,
    runtime_cfg: SparkRuntimeConfig,
):
    return (
        df.writeStream.queryName(query_name)
        .outputMode("append")
        .format("csv")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
        .option("header", "false")
        .trigger(processingTime=runtime_cfg.trigger_processing_time)
        .start()
    )


def write_foreach_batch_stream(
    df: DataFrame,
    *,
    writer,
    checkpoint: str,
    query_name: str,
    runtime_cfg: SparkRuntimeConfig,
):
    return (
        df.writeStream.queryName(query_name)
        .outputMode("append")
        .foreachBatch(writer)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=runtime_cfg.trigger_processing_time)
        .start()
    )


def await_queries_until_idle(
    queries,
    runtime_cfg: SparkRuntimeConfig,
    logger: logging.Logger,
) -> None:
    """Stop local Spark jobs after the finite Kafka replay has gone idle."""
    started_at = time.monotonic()
    last_activity_at = started_at
    seen_input = False
    seen_batches: set[tuple[str, int]] = set()

    while any(query.isActive for query in queries):
        now = time.monotonic()

        for query in queries:
            progress = query.lastProgress
            if not progress:
                continue

            batch_id = int(progress.get("batchId", -1))
            key = (str(query.id), batch_id)
            if key in seen_batches:
                continue

            seen_batches.add(key)
            input_rows = int(progress.get("numInputRows", 0))

            if input_rows > 0:
                seen_input = True
                last_activity_at = now
                logger.info(
                    "%s | batch=%s input_rows=%s",
                    query.name,
                    batch_id,
                    input_rows,
                )

        if seen_input and now - last_activity_at >= runtime_cfg.idle_timeout_seconds:
            logger.info(
                "No new Kafka input for %d s; stopping Spark query/queries.",
                runtime_cfg.idle_timeout_seconds,
            )
            for query in queries:
                if query.isActive:
                    query.stop()
            break

        if (
            not seen_input
            and now - started_at >= runtime_cfg.startup_timeout_seconds
        ):
            for query in queries:
                if query.isActive:
                    query.stop()
            raise TimeoutError(
                "Spark query started but no Kafka rows arrived before "
                f"{runtime_cfg.startup_timeout_seconds} s."
            )

        time.sleep(5.0)

    for query in queries:
        query.awaitTermination(30)
