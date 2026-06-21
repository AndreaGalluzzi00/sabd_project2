#!/usr/bin/env python3

from __future__ import annotations

import heapq
import json
import logging
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError, NoBrokersAvailable

from common.config import load_config
from flink_runtime import (
    FlinkRuntimeConfig,
    build_flink_runtime_config,
    create_table_environment,
)
import logging

from common.logging_utils import configure_logging


configure_logging()

logger = logging.getLogger(__name__)



@dataclass(frozen=True)
class ProducerConfig:
    kafka_bootstrap_servers: str
    kafka_topic: str
    prepared_path: Path

    acceleration_factor: float
    log_interval: int
    flush_interval: int
    skip_if_topic_has_messages: bool

    holdback_probability: float
    holdback_delay: float

    emit_end_of_stream: bool

    kafka_acks: str
    kafka_retries: int
    kafka_linger_ms: int
    kafka_batch_size: int
    kafka_max_in_flight_requests_per_connection: int


def load_producer_config() -> ProducerConfig:

    cfg = load_config()

    kafka_cfg = cfg["kafka"]
    paths_cfg = cfg["paths"]
    producer_cfg = cfg["producer"]

    return ProducerConfig(
        kafka_bootstrap_servers=kafka_cfg["bootstrap_servers"],
        kafka_topic=kafka_cfg["topic"],
        prepared_path=Path(paths_cfg["prepared_path"]),

        acceleration_factor=float(producer_cfg["acceleration_factor"]),
        log_interval=int(producer_cfg["log_interval"]),
        flush_interval=int(producer_cfg["flush_interval"]),
        skip_if_topic_has_messages=bool(producer_cfg["skip_if_topic_has_messages"]),

        holdback_probability=float(producer_cfg["holdback_probability"]),
        holdback_delay=float(producer_cfg["holdback_delay"]),

        emit_end_of_stream=bool(producer_cfg.get("emit_end_of_stream", True)),

        kafka_acks=str(producer_cfg["acks"]),
        kafka_retries=int(producer_cfg["retries"]),
        kafka_linger_ms=int(producer_cfg["linger_ms"]),
        kafka_batch_size=int(producer_cfg["batch_size"]),
        kafka_max_in_flight_requests_per_connection=int(
            producer_cfg["max_in_flight_requests_per_connection"])
    )


def load_prepared(path: Path, acceleration_factor: float) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(
            f"Prepared dataset not found: {path}. "
            "Run the preprocessing stage first."
        )

    logger.info("Loading prepared dataset %s …", path)

    df = pd.read_parquet(path)
    df = df.reset_index(drop=True)

    logger.info("Loaded %d prepared events", len(df))

    if df.empty:
        return df

    first_ms = int(df["event_time"].iloc[0])
    last_ms = int(df["event_time"].iloc[-1])
    span_days = (last_ms - first_ms) / 86_400_000

    logger.info(
        "Event range: %s → %s (%.1f days)",
        datetime.fromtimestamp(first_ms / 1000, tz=timezone.utc),
        datetime.fromtimestamp(last_ms / 1000, tz=timezone.utc),
        span_days,
    )

    logger.info(
        "Estimated replay duration at %gx: %.1f minutes",
        acceleration_factor,
        span_days * 86_400 / acceleration_factor / 60,
    )

    return df


def _nullable_value(value: Any) -> Any:
    if pd.isna(value):
        return None

    return value


def _row_to_payload(row: pd.Series) -> dict[str, Any]:
    return {
        "event_time": int(row["event_time"]),
        "year": int(row.get("YEAR", 0)),
        "month": int(row.get("MONTH", 0)),
        "day_of_month": int(row.get("DAY_OF_MONTH", 0)),
        "airline": str(row.get("OP_UNIQUE_CARRIER", "")),
        "origin_airport_id": int(row.get("ORIGIN_AIRPORT_ID", 0)),
        "dest_airport_id": int(row.get("DEST_AIRPORT_ID", 0)),
        "crs_dep_time": int(row.get("CRS_DEP_TIME", 0)),
        "dep_delay": _nullable_value(row.get("DEP_DELAY")),
        "arr_delay": _nullable_value(row.get("ARR_DELAY")),
        "cancelled": _nullable_value(row.get("CANCELLED")),
        "diverted": _nullable_value(row.get("DIVERTED")),
        "carrier_delay": _nullable_value(row.get("CARRIER_DELAY")),
        "weather_delay": _nullable_value(row.get("WEATHER_DELAY")),
        "nas_delay": _nullable_value(row.get("NAS_DELAY")),
        "security_delay": _nullable_value(row.get("SECURITY_DELAY")),
        "late_aircraft_delay": _nullable_value(row.get("LATE_AIRCRAFT_DELAY")),
    }


def create_producer(
    cfg: ProducerConfig,
    max_retries: int = 30,
    retry_interval: int = 5,
) -> KafkaProducer:
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=cfg.kafka_bootstrap_servers,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                acks=cfg.kafka_acks,
                retries=cfg.kafka_retries,
                max_in_flight_requests_per_connection=(
                    cfg.kafka_max_in_flight_requests_per_connection
                ),
                linger_ms=cfg.kafka_linger_ms,
                batch_size=cfg.kafka_batch_size,
            )

            producer.partitions_for(cfg.kafka_topic)

            logger.info(
                "Connected to Kafka (%s), topic '%s'",
                cfg.kafka_bootstrap_servers,
                cfg.kafka_topic,
            )

            return producer

        except (NoBrokersAvailable, KafkaError, Exception) as exc:
            logger.warning(
                "Kafka not ready (attempt %d/%d): %s",
                attempt,
                max_retries,
                exc,
            )

            if attempt < max_retries:
                time.sleep(retry_interval)

    raise RuntimeError(f"Could not connect to Kafka after {max_retries} attempts")


def topic_has_messages(cfg: ProducerConfig) -> bool:
    consumer: KafkaConsumer | None = None

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=cfg.kafka_bootstrap_servers,
            enable_auto_commit=False,
        )

        partitions = consumer.partitions_for_topic(cfg.kafka_topic)

        if not partitions:
            return False

        topic_partitions = [
            TopicPartition(cfg.kafka_topic, partition)
            for partition in partitions
        ]

        end_offsets = consumer.end_offsets(topic_partitions)

        return sum(end_offsets.values()) > 0

    except Exception as exc:
        logger.warning(
            "Could not check topic offsets: %s — proceeding with production.",
            exc,
        )
        return False

    finally:
        if consumer is not None:
            consumer.close()


def _sleep_until(wall_start: float, target_wall: float) -> None:
    """Sleep until `target_wall` seconds have elapsed since `wall_start`."""
    wait = target_wall - (time.monotonic() - wall_start)

    if wait > 0.001:
        time.sleep(wait)


def replay_events(
    producer: KafkaProducer,
    df: pd.DataFrame,
    cfg: ProducerConfig,
) -> None:

    n = len(df)

    if n == 0:
        logger.warning("No events to replay.")
        return

    if cfg.acceleration_factor <= 0:
        raise ValueError("acceleration_factor must be greater than zero")

    event_times = df["event_time"].to_numpy()

    first_ts = int(event_times[0])
    wall_start = time.monotonic()

    # Out-of-orderness is simulated by DELAYING DELIVERY, never by altering
    # event_time. A held-back event keeps its true event_time but is sent later,
    # so it lands further ahead in the Kafka log → genuine out-of-order arrival.
    # The delay is drawn uniformly in [0, holdback_delay]; holdback_delay is thus
    # the maximum out-of-orderness bound to compare against the watermark delay W:
    #   W >= bound → late events still land in the CORRECT window (complete, +latency)
    #   W <  bound → some events arrive after the watermark and are dropped (incomplete)
    max_delay_wall = cfg.holdback_delay / cfg.acceleration_factor

    logger.info(
        "Replay started: %d events, factor=%gx, out-of-order prob=%.2f, "
        "max delivery delay=%ds (event time)",
        n,
        cfg.acceleration_factor,
        cfg.holdback_probability,
        int(cfg.holdback_delay),
    )

    # Min-heap of held-back events ordered by scheduled wall-clock send time.
    # Entries are (scheduled_wall, seq, payload); seq breaks ties so the heap
    # never has to compare payload dicts.
    held_buffer: list[tuple[float, int, dict[str, Any]]] = []
    held_total = 0
    seq = 0
    sent = 0

    def dispatch(payload: dict[str, Any]) -> None:
        nonlocal sent
        producer.send(cfg.kafka_topic, value=payload)
        sent += 1

        if sent % cfg.flush_interval == 0:
            producer.flush()

        if sent % cfg.log_interval == 0:
            elapsed = time.monotonic() - wall_start
            throughput = sent / elapsed if elapsed > 0 else 0.0

            logger.info(
                "Sent %d / %d  (%.1f%%)  elapsed=%.1fs  throughput=%.0f ev/s",
                sent,
                n,
                sent / n * 100,
                elapsed,
                throughput,
            )

    for index, row in df.iterrows():
        target_wall = (
            (int(event_times[index]) - first_ts) / 1000.0
        ) / cfg.acceleration_factor

        # Emit any held-back events whose delayed send time has arrived, in
        # order, before the next in-order event.
        while held_buffer and held_buffer[0][0] <= target_wall:
            sched_wall, _, late_payload = heapq.heappop(held_buffer)
            _sleep_until(wall_start, sched_wall)
            dispatch(late_payload)

        payload = _row_to_payload(row)  # event_time left untouched

        if cfg.holdback_probability > 0.0 and random.random() < cfg.holdback_probability:
            delay = random.uniform(0.0, max_delay_wall)
            heapq.heappush(held_buffer, (target_wall + delay, seq, payload))
            seq += 1
            held_total += 1
            continue

        _sleep_until(wall_start, target_wall)
        dispatch(payload)

    # Drain events still held back after the last in-order event.
    while held_buffer:
        sched_wall, _, late_payload = heapq.heappop(held_buffer)
        _sleep_until(wall_start, sched_wall)
        dispatch(late_payload)

    producer.flush()

    total = time.monotonic() - wall_start
    avg_throughput = sent / total if total > 0 else 0.0

    logger.info(
        "Replay complete: %d events in %.1f s (avg %.0f ev/s) — delayed: %d (%.1f%%)",
        sent,
        total,
        avg_throughput,
        held_total,
        held_total / n * 100,
    )


# Event time well beyond the dataset (year 2200) carried by the end-of-stream
# markers. Sending one marker per partition pushes every per-partition watermark
# past the last real window, so Flink fires and commits the final window without
# a manual 'flink stop --drain'. Equivalent effect to emitting MAX_WATERMARK.
END_OF_STREAM_EVENT_TIME_MS = int(
    datetime(2200, 1, 1, tzinfo=timezone.utc).timestamp() * 1000
)


def emit_end_of_stream_markers(producer: KafkaProducer, cfg: ProducerConfig) -> None:
    """Append a future-dated marker as the LAST record of every partition.

    Correctness depends on the marker being the highest offset on each partition:
    any real event read after it would fall below the (now max) watermark and be
    dropped as late. We therefore flush all real events first (with acks=all they
    are durably in the log), then send one marker per partition, then flush again.
    Combined with max_in_flight_requests_per_connection=1 this guarantees ordering.
    """
    partitions = producer.partitions_for(cfg.kafka_topic)

    if not partitions:
        logger.warning(
            "Could not resolve partitions for '%s' — skipping end-of-stream markers.",
            cfg.kafka_topic,
        )
        return

    # Ensure every real event is acknowledged before any marker is enqueued.
    producer.flush()

    # Marker excluded by every query: airline outside the Q1/Q3 carrier list and
    # flagged cancelled+diverted so the "non cancellati e non deviati" filters of
    # Q2/Q3 drop it too. Its event_time only drives the watermark.
    marker = {
        "event_time": END_OF_STREAM_EVENT_TIME_MS,
        "airline": "__EOS__",
        "cancelled": 1.0,
        "diverted": 1.0,
    }

    for partition in sorted(partitions):
        producer.send(cfg.kafka_topic, value=marker, partition=partition)

    producer.flush()

    logger.info(
        "End-of-stream markers sent to %d partition(s) (event_time=%s) — "
        "final event-time windows will flush at the next checkpoint.",
        len(partitions),
        datetime.fromtimestamp(END_OF_STREAM_EVENT_TIME_MS / 1000, tz=timezone.utc),
    )


def main() -> None:
    cfg = load_producer_config()

    logger.info("=== Flight Event Producer ===")
    logger.info(" Prepared data : %s", cfg.prepared_path)
    logger.info(" Kafka brokers : %s", cfg.kafka_bootstrap_servers)
    logger.info(" Topic         : %s", cfg.kafka_topic)
    logger.info(" Acceleration  : %gx", cfg.acceleration_factor)
    logger.info(" Flush every   : %d events", cfg.flush_interval)
    logger.info(" Log every     : %d events", cfg.log_interval)

    if cfg.holdback_probability > 0.0:
        logger.info(
            " Out-of-order  : %.0f%% of events, delivery delayed up to %ds (event time)",
            cfg.holdback_probability * 100,
            int(cfg.holdback_delay),
        )
    else:
        logger.info(" Out-of-order  : disabled")

    logger.info(
        " End-of-stream : %s",
        "marker per partition" if cfg.emit_end_of_stream else "disabled",
    )

    if cfg.skip_if_topic_has_messages and topic_has_messages(cfg):
        logger.info(
            "Topic '%s' already has messages — skipping production.",
            cfg.kafka_topic,
        )
        return

    df = load_prepared(path=cfg.prepared_path,acceleration_factor=cfg.acceleration_factor)

    if df.empty:
        logger.error("Prepared dataset is empty — nothing to produce. Exiting.")
        sys.exit(1)

    producer = create_producer(cfg)

    try:
        replay_events(producer=producer,df=df,cfg=cfg)

        if cfg.emit_end_of_stream:
            emit_end_of_stream_markers(producer=producer, cfg=cfg)
    finally:
        producer.close()

    logger.info("Producer closed.")


if __name__ == "__main__":
    main()