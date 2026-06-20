import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

RAW_DATASET_PATH: str = os.getenv("RAW_DATASET_PATH", "/data")
PREPARED_PATH: str = os.getenv("PREPARED_PATH", "/prepared/flights_prepared.parquet")

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
    # Keep real days: 29/30/31 are valid for most months. Only guard against
    # 0/negative values. Genuinely impossible dates (e.g. Feb 30) become NaT
    # in to_datetime below and are caught by the first-of-month fallback.
    day   = df["DAY_OF_MONTH"].fillna(1).astype(int).clip(lower=1)

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

    return timestamps.astype("int64") // 10**6  # epoch milliseconds (Flink convention)


def load_and_prepare(path: str) -> pd.DataFrame:
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

    # Delay fields keep NaN for missing values (do NOT collapse to 0): the
    # distinction "present vs missing" is preserved through the pipeline and the
    # null-handling policy is applied per query in Flink.
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "OP_UNIQUE_CARRIER" in df.columns:
        df["OP_UNIQUE_CARRIER"] = df["OP_UNIQUE_CARRIER"].fillna("").str.strip()

    logger.info("Computing event timestamps …")
    df["event_time"] = _compute_event_timestamps(df)

    logger.info("Sorting %d events by event time …", len(df))
    df = df.sort_values("event_time", kind="stable").reset_index(drop=True)

    first_ms = int(df["event_time"].iloc[0])
    last_ms  = int(df["event_time"].iloc[-1])
    span_days = (last_ms - first_ms) / 86_400_000
    logger.info(
        "Event range: %s → %s (%.1f days)",
        datetime.fromtimestamp(first_ms / 1000, tz=timezone.utc),
        datetime.fromtimestamp(last_ms / 1000, tz=timezone.utc),
        span_days,
    )

    return df


def main() -> None:
    logger.info("=== Flight Dataset Preprocessing ===")
    logger.info("  Raw dataset   : %s", RAW_DATASET_PATH)
    logger.info("  Prepared file : %s", PREPARED_PATH)

    out = Path(PREPARED_PATH)
    if out.exists():
        logger.info("Prepared file already exists — skipping preprocessing.")
        return

    df = load_and_prepare(RAW_DATASET_PATH)
    if df.empty:
        logger.error("Dataset is empty — nothing to prepare. Exiting.")
        sys.exit(1)

    out = Path(PREPARED_PATH)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    logger.info("Wrote %d prepared events → %s", len(df), out)


if __name__ == "__main__":
    main()
