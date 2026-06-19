#!/usr/bin/env python3
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "flights")
DATASET_PATH: str = os.getenv("DATASET_PATH", "/data")
ACCELERATION_FACTOR: float = float(os.getenv("ACCELERATION_FACTOR", "3600"))
LOG_INTERVAL: int = int(os.getenv("LOG_INTERVAL", "100000"))
FLUSH_INTERVAL: int = int(os.getenv("FLUSH_INTERVAL", "5000"))

USECOLS = [
    "YEAR", "MONTH", "DAY_OF_MONTH",
    "OP_UNIQUE_CARRIER",
    "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID",
    "CRS_DEP_TIME",
    "DEP_DELAY", "ARR_DELAY",
    "CANCELLED", "DIVERTED",
    "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY",
    "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY",
]


def _compute_event_timestamps(df: pd.DataFrame) -> pd.Series:
    crs = df["CRS_DEP_TIME"].fillna(0).astype(int)
    crs = crs.where(crs != 2400, 0)

    hours   = (crs // 100) % 24
    minutes = crs % 100

    year  = df["YEAR"].fillna(2025).astype(int)
    month = df["MONTH"].fillna(1).astype(int).clip(1, 12)
    day   = df["DAY_OF_MONTH"].fillna(1).astype(int).clip(1, 28)

    dt_strings = (
        year.astype(str).str.zfill(4) + "-" +
        month.astype(str).str.zfill(2) + "-" +
        day.astype(str).str.zfill(2) + " " +
        hours.astype(str).str.zfill(2) + ":" +
        minutes.astype(str).str.zfill(2)
    )
    timestamps = pd.to_datetime(dt_strings, format="%Y-%m-%d %H:%M", errors="coerce", utc=True)

    fallback_str = (
        year.astype(str).str.zfill(4) + "-" +
        month.astype(str).str.zfill(2) + "-01 00:00"
    )
    fallback = pd.to_datetime(fallback_str, format="%Y-%m-%d %H:%M", errors="coerce", utc=True)
    timestamps = timestamps.fillna(fallback)

    return timestamps.astype("int64") // 10**9


def load_dataset(path: str) -> pd.DataFrame:
    p = Path(path)
    if p.is_dir():
        csv_files = sorted(p.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in: {path}")
    elif p.is_file():
        csv_files = [p]
    else:
        raise FileNotFoundError(f"Dataset path not found: {path}")

    chunks: list[pd.DataFrame] = []
    for csv_file in csv_files:
        logger.info("Reading %s …", csv_file.name)
        df_chunk = pd.read_csv(
            csv_file,
            usecols=lambda c: c in USECOLS,
            dtype=str,
            encoding="utf-8",
            on_bad_lines="skip",
        )
        chunks.append(df_chunk)
        logger.info("  → %d rows loaded so far", sum(len(c) for c in chunks))

    df = pd.concat(chunks, ignore_index=True)
    logger.info("Total rows loaded: %d", len(df))

    int_cols   = ["YEAR", "MONTH", "DAY_OF_MONTH", "ORIGIN_AIRPORT_ID",
                  "DEST_AIRPORT_ID", "CRS_DEP_TIME"]
    float_cols = ["DEP_DELAY", "ARR_DELAY", "CANCELLED", "DIVERTED",
                  "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY",
                  "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"]

    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "OP_UNIQUE_CARRIER" in df.columns:
        df["OP_UNIQUE_CARRIER"] = df["OP_UNIQUE_CARRIER"].fillna("").str.strip()

    logger.info("Computing event timestamps …")
    df["event_time"] = _compute_event_timestamps(df)

    logger.info("Sorting %d events by event time …", len(df))
    df = df.sort_values("event_time", kind="stable").reset_index(drop=True)

    t_first = datetime.fromtimestamp(int(df["event_time"].iloc[0]),  tz=timezone.utc)
    t_last  = datetime.fromtimestamp(int(df["event_time"].iloc[-1]), tz=timezone.utc)
    span_days = (int(df["event_time"].iloc[-1]) - int(df["event_time"].iloc[0])) / 86400
    logger.info("Event range: %s → %s (%.1f days)", t_first, t_last, span_days)
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
        target_wall = (int(event_times[i]) - first_ts) / ACCELERATION_FACTOR
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


def main() -> None:
    logger.info("=== Flight Event Producer ===")
    logger.info("  Dataset path  : %s", DATASET_PATH)
    logger.info("  Kafka brokers : %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("  Topic         : %s", KAFKA_TOPIC)
    logger.info("  Acceleration  : %gx", ACCELERATION_FACTOR)

    df = load_dataset(DATASET_PATH)
    if df.empty:
        logger.error("Dataset is empty — nothing to produce. Exiting.")
        sys.exit(1)

    producer = create_producer()
    try:
        replay_events(producer, df)
    finally:
        producer.close()
        logger.info("Producer closed.")


if __name__ == "__main__":
    main()
