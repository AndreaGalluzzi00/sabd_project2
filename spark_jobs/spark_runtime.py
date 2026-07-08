from __future__ import annotations

import csv
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
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
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
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


# Unified perf schema, shared with scripts/report_perf.py (Flink writer), so
# Results/perf.csv holds both engines side by side for the report.
_PERF_HEADER = [
    "timestamp_utc", "engine", "implementation", "query", "experiment",
    "parallelism", "total_records", "active_seconds",
    "throughput_rec_s_avg", "throughput_rec_s_max",
    "busy_pct_avg", "backpressure_pct_avg", "notes",
]

# Default host location (./Results is bind-mounted at /opt/spark/results).
_PERF_OUTPUT = "/opt/spark/results/perf.csv"


def _append_perf_row(output_path: str, row: list[Any], logger: logging.Logger) -> None:
    try:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        write_header = not out.exists()
        with out.open("a", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            if write_header:
                writer.writerow(_PERF_HEADER)
            writer.writerow(row)
        logger.info("Spark perf row appended to %s", output_path)
    except Exception as exc:  # noqa: BLE001 - metrics must never fail the job
        logger.warning("Could not write Spark perf row: %s", exc)


def _recent_progress(query) -> list[dict[str, Any]]:
    progress = list(query.recentProgress or [])
    if progress:
        return progress

    last = query.lastProgress
    return [last] if last else []


def _progress_event_time(progress: dict[str, Any]) -> tuple[str, str]:
    event_time = progress.get("eventTime") or {}
    return (
        str(event_time.get("watermark", "")),
        str(event_time.get("max", "")),
    )


def await_queries_until_idle(
    queries,
    runtime_cfg: SparkRuntimeConfig,
    logger: logging.Logger,
    *,
    query_label: str = "",
    experiment: str = "base",
    perf_output: str = _PERF_OUTPUT,
) -> None:
    started_at = time.monotonic()
    last_activity_at = started_at
    seen_input = False
    seen_batches: set[tuple[str, int]] = set()
    input_seen_by_query: set[str] = set()
    drained_after_input_by_query: set[str] = set()
    last_input_batch_by_query: dict[str, int] = {}
    query_names: dict[str, str] = {str(query.id): query.name for query in queries}
    final_drain_started_at: float | None = None

    input_by_query: dict[str, int] = {}
    proc_rates: list[float] = []
    batch_latencies_ms: list[float] = []
    first_activity_at: float | None = None

    while any(query.isActive for query in queries):
        now = time.monotonic()

        for query in queries:
            for progress in _recent_progress(query):
                batch_id = int(progress.get("batchId", -1))
                key = (str(query.id), batch_id)
                if key in seen_batches:
                    continue

                seen_batches.add(key)
                input_rows = int(progress.get("numInputRows", 0))
                watermark, max_event_time = _progress_event_time(progress)
                qid = str(query.id)

                if input_rows > 0:
                    seen_input = True
                    last_activity_at = now
                    final_drain_started_at = None
                    if first_activity_at is None:
                        first_activity_at = now

                    input_seen_by_query.add(qid)
                    drained_after_input_by_query.discard(qid)
                    last_input_batch_by_query[qid] = batch_id
                    input_by_query[qid] = input_by_query.get(qid, 0) + input_rows

                    rate = float(progress.get("processedRowsPerSecond", 0.0) or 0.0)
                    if rate > 0.0:
                        proc_rates.append(rate)

                    duration = progress.get("durationMs") or {}
                    trigger_ms = float(duration.get("triggerExecution", 0.0) or 0.0)
                    if trigger_ms > 0.0:
                        batch_latencies_ms.append(trigger_ms)

                    logger.info(
                        "%s | batch=%s input_rows=%s watermark=%s max_event_time=%s",
                        query.name,
                        batch_id,
                        input_rows,
                        watermark,
                        max_event_time,
                    )
                    continue

                if (
                    qid in input_seen_by_query
                    and batch_id > last_input_batch_by_query.get(qid, -1)
                ):
                    drained_after_input_by_query.add(qid)
                    logger.info(
                        "%s | final-drain batch=%s input_rows=0 watermark=%s",
                        query.name,
                        batch_id,
                        watermark,
                    )

        if seen_input and now - last_activity_at >= runtime_cfg.idle_timeout_seconds:
            pending_drain = sorted(
                query_names.get(qid, qid)
                for qid in (input_seen_by_query - drained_after_input_by_query)
            )

            if not pending_drain:
                logger.info(
                    "No new Kafka input for %d s and final drain batch observed; "
                    "stopping Spark query/queries.",
                    runtime_cfg.idle_timeout_seconds,
                )
                for query in queries:
                    if query.isActive:
                        query.stop()
                break

            if final_drain_started_at is None:
                final_drain_started_at = now
                logger.info(
                    "No new Kafka input for %d s; waiting for Spark final "
                    "no-input micro-batch to close event-time windows: %s",
                    runtime_cfg.idle_timeout_seconds,
                    ", ".join(pending_drain),
                )
            elif now - final_drain_started_at >= runtime_cfg.idle_timeout_seconds:
                logger.warning(
                    "Final no-input micro-batch was not observed after another "
                    "%d s for %s; stopping Spark query/queries.",
                    runtime_cfg.idle_timeout_seconds,
                    ", ".join(pending_drain),
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

    # ── Emit the aggregate perf row ────────────────────────────────────────────
    total_records = max(input_by_query.values(), default=0)
    active_seconds = max((last_activity_at - first_activity_at), 1e-6) if first_activity_at else 0.0
    thr_avg = (total_records / active_seconds) if active_seconds > 0 else 0.0
    thr_max = max(proc_rates, default=0.0)
    lat_avg = (sum(batch_latencies_ms) / len(batch_latencies_ms)) if batch_latencies_ms else ""
    lat_max = max(batch_latencies_ms, default="") if batch_latencies_ms else ""

    row = [
        datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "spark", "structured", query_label or "?", experiment,
        runtime_cfg.shuffle_partitions, int(total_records), round(active_seconds, 1),
        round(thr_avg, 1), round(thr_max, 1),
        "", "", f"spark local[*], trigger={runtime_cfg.trigger_processing_time}",
    ]
    _append_perf_row(perf_output, row, logger)
