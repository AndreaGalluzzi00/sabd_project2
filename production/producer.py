#!/usr/bin/env python3
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError, NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "flights")
PREPARED_PATH: str = os.getenv("PREPARED_PATH", "/prepared/flights_prepared.parquet")
ACCELERATION_FACTOR: float = float(os.getenv("ACCELERATION_FACTOR", "3600"))
LOG_INTERVAL: int = int(os.getenv("LOG_INTERVAL", "100000"))
FLUSH_INTERVAL: int = int(os.getenv("FLUSH_INTERVAL", "5000"))


def load_prepared(path: str) -> pd.DataFrame:
    """Load the typed, globally-sorted dataset produced by the preprocess stage.

    Ordering and event_time (epoch ms) are established upstream; here we only
    read the prepared Parquet and replay it — no parsing, typing or sorting.
    """
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(
            f"Prepared dataset not found: {path}. "
            "Run the preprocessing stage first (it writes the sorted Parquet)."
        )

    logger.info("Loading prepared dataset %s …", path)
    df = pd.read_parquet(path)
    logger.info("Loaded %d prepared events", len(df))
    if df.empty:
        return df

    first_ms = int(df["event_time"].iloc[0])
    last_ms  = int(df["event_time"].iloc[-1])
    span_days = (last_ms - first_ms) / 86_400_000
    logger.info(
        "Event range: %s → %s (%.1f days)",
        datetime.fromtimestamp(first_ms / 1000, tz=timezone.utc),
        datetime.fromtimestamp(last_ms / 1000, tz=timezone.utc),
        span_days,
    )
    logger.info(
        "Estimated replay duration at %gx: %.1f minutes",
        ACCELERATION_FACTOR,
        span_days * 86400 / ACCELERATION_FACTOR / 60,
    )
    return df


def _row_to_payload(row: "pd.Series") -> dict:
    def _f(v):
        if pd.isna(v):
            return None
        return v

    return {
        "event_time":        int(row["event_time"]),
        "year":              int(row.get("YEAR",             0)),
        "month":             int(row.get("MONTH",            0)),
        "day_of_month":      int(row.get("DAY_OF_MONTH",     0)),
        "airline":           str(row.get("OP_UNIQUE_CARRIER", "")),
        "origin_airport_id": int(row.get("ORIGIN_AIRPORT_ID", 0)),
        "dest_airport_id":   int(row.get("DEST_AIRPORT_ID",   0)),
        "crs_dep_time":      int(row.get("CRS_DEP_TIME",      0)),
        "dep_delay":         _f(row.get("DEP_DELAY")),
        "arr_delay":         _f(row.get("ARR_DELAY")),
        "cancelled":         _f(row.get("CANCELLED")),
        "diverted":          _f(row.get("DIVERTED")),
        "carrier_delay":     _f(row.get("CARRIER_DELAY")),
        "weather_delay":     _f(row.get("WEATHER_DELAY")),
        "nas_delay":         _f(row.get("NAS_DELAY")),
        "security_delay":    _f(row.get("SECURITY_DELAY")),
        "late_aircraft_delay": _f(row.get("LATE_AIRCRAFT_DELAY")),
    }


def create_producer(max_retries: int = 30, retry_interval: int = 5) -> KafkaProducer:
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                # Guarantee in-partition ordering: with retries enabled, more than
                # one in-flight request could let a later batch overtake a retried
                # one, introducing *accidental* out-of-orderness. Replay is paced by
                # the event-time schedule, so throughput is not the bottleneck here.
                max_in_flight_requests_per_connection=1,
                linger_ms=20,
                batch_size=32768,
            )
            producer.partitions_for(KAFKA_TOPIC)
            logger.info("Connected to Kafka (%s), topic '%s'", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
            return producer
        except (NoBrokersAvailable, KafkaError, Exception) as exc:
            logger.warning("Kafka not ready (attempt %d/%d): %s", attempt, max_retries, exc)
            if attempt < max_retries:
                time.sleep(retry_interval)

    raise RuntimeError(f"Could not connect to Kafka after {max_retries} attempts")


def replay_events(producer: KafkaProducer, df: pd.DataFrame) -> None:
    n = len(df)
    if n == 0:
        logger.warning("No events to replay.")
        return

    event_times = df["event_time"].to_numpy()
    first_ts    = int(event_times[0])
    wall_start  = time.monotonic()

    logger.info("Replay started: %d events, factor=%gx", n, ACCELERATION_FACTOR)

    sent = 0
    for i, row in df.iterrows():
        # event_time is in ms; convert the elapsed event-time span to seconds,
        # then compress by the acceleration factor (event-seconds per wall-second).
        target_wall = ((int(event_times[i]) - first_ts) / 1000.0) / ACCELERATION_FACTOR
        wait = target_wall - (time.monotonic() - wall_start)
        if wait > 0.001:
            time.sleep(wait)

        payload = _row_to_payload(row)
        producer.send(KAFKA_TOPIC, value=payload)
        sent += 1

        if sent % FLUSH_INTERVAL == 0:
            producer.flush()

        if sent % LOG_INTERVAL == 0:
            elapsed = time.monotonic() - wall_start
            logger.info(
                "Sent %d / %d  (%.1f%%)  elapsed=%.1fs  throughput=%.0f ev/s",
                sent, n, sent / n * 100, elapsed, sent / elapsed,
            )

    producer.flush()
    total = time.monotonic() - wall_start
    logger.info(
        "Replay complete: %d events in %.1f s (avg %.0f ev/s)", sent, total, sent / total
    )


def topic_has_messages() -> bool:
    """Return True if the Kafka topic already contains messages."""
    try:
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
        if not partitions:
            consumer.close()
            return False
        tps = [TopicPartition(KAFKA_TOPIC, p) for p in partitions]
        end_offsets = consumer.end_offsets(tps)
        consumer.close()
        return sum(end_offsets.values()) > 0
    except Exception as exc:
        logger.warning("Could not check topic offsets: %s — proceeding with production.", exc)
        return False


def main() -> None:
    logger.info("=== Flight Event Producer ===")
    logger.info("  Prepared data : %s", PREPARED_PATH)
    logger.info("  Kafka brokers : %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("  Topic         : %s", KAFKA_TOPIC)
    logger.info("  Acceleration  : %gx", ACCELERATION_FACTOR)

    if topic_has_messages():
        logger.info("Topic '%s' already has messages — skipping production.", KAFKA_TOPIC)
        return

    df = load_prepared(PREPARED_PATH)
    if df.empty:
        logger.error("Prepared dataset is empty — nothing to produce. Exiting.")
        sys.exit(1)

    producer = create_producer()
    try:
        replay_events(producer, df)
    finally:
        producer.close()
        logger.info("Producer closed.")


if __name__ == "__main__":
    main()
