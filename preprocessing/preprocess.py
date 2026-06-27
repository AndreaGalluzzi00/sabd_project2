import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from common.config import load_config
import pandas as pd
from dataclasses import dataclass
import logging

from common.logging_utils import configure_logging


configure_logging()
logger = logging.getLogger(__name__)


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

INT_COLS = [
    "YEAR",
    "MONTH",
    "DAY_OF_MONTH",
    "ORIGIN_AIRPORT_ID",
    "DEST_AIRPORT_ID",
    "CRS_DEP_TIME",
]

FLOAT_COLS = [
    "DEP_DELAY",
    "ARR_DELAY",
    "CANCELLED",
    "DIVERTED",
    "CARRIER_DELAY",
    "WEATHER_DELAY",
    "NAS_DELAY",
    "SECURITY_DELAY",
    "LATE_AIRCRAFT_DELAY",
]


@dataclass(frozen=True)
class PreprocessConfig:
    raw_dataset_path: Path
    prepared_path: Path
    numeric_missing_policy: str

def load_preprocess_config() -> PreprocessConfig:
    cfg = load_config()

    numeric_missing_policy = cfg.get("preprocessing", {}).get(
        "numeric_missing_policy",
        "null",
    )

    if numeric_missing_policy not in {"null", "zero"}:
        raise ValueError(
            "Invalid preprocessing.numeric_missing_policy. "
            "Allowed values are: 'null', 'zero'."
        )

    return PreprocessConfig(
        raw_dataset_path=Path(cfg["paths"]["raw_dataset_path"]),
        prepared_path=Path(cfg["paths"]["prepared_path"]),
        numeric_missing_policy=numeric_missing_policy,
    )

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


def log_event_range(df: pd.DataFrame) -> None:
    if df.empty:
        logger.warning("No events available to log event range.")
        return

    first_ms = int(df["event_time"].iloc[0])
    last_ms = int(df["event_time"].iloc[-1])
    span_days = (last_ms - first_ms) / 86_400_000

    logger.info(
        "Event range: %s → %s (%.1f days)",
        datetime.fromtimestamp(first_ms / 1000, tz=timezone.utc),
        datetime.fromtimestamp(last_ms / 1000, tz=timezone.utc),
        span_days,
    )

def sort_by_event_time(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Sorting %d events by event time …", len(df))

    return df.sort_values("event_time", kind="stable").reset_index(drop=True)


def add_event_time(df: pd.DataFrame) -> pd.DataFrame:


    logger.info("Computing event timestamps …")
    df["event_time"] = _compute_event_timestamps(df)

    return df


def normalize_columns(
    df: pd.DataFrame,
    numeric_missing_policy: str,
) -> pd.DataFrame:

    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

            if numeric_missing_policy == "zero":
                df[col] = df[col].fillna(0.0)

    if "OP_UNIQUE_CARRIER" in df.columns:
        df["OP_UNIQUE_CARRIER"] = df["OP_UNIQUE_CARRIER"].fillna("").str.strip()

    return df


def read_raw_dataset(csv_files: list[Path]) -> pd.DataFrame:
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

    return df


def discover_csv_files(raw_dataset_path: Path) -> list[Path]:
    if raw_dataset_path.is_dir():
        csv_files = sorted(raw_dataset_path.glob("*.csv"))

        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in: {raw_dataset_path}")

        return csv_files

    if raw_dataset_path.is_file():
        return [raw_dataset_path]

    raise FileNotFoundError(f"Dataset path not found: {raw_dataset_path}")



def load_and_prepare(
    raw_dataset_path: Path,
    numeric_missing_policy: str,
) -> pd.DataFrame:

    csv_files = discover_csv_files(raw_dataset_path)
    df = read_raw_dataset(csv_files)
    normalize_columns(df, numeric_missing_policy)
    add_event_time(df)
    df = sort_by_event_time(df)
    log_event_range(df)

    return df


def write_prepared(df: pd.DataFrame, prepared_path: Path) -> None:
    prepared_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(prepared_path, index=False)

    logger.info("Wrote %d prepared events → %s", len(df), prepared_path)


def main() -> None:
    cfg = load_preprocess_config()

    logger.info("=== Flight Dataset Preprocessing ===")
    logger.info(" Raw dataset    : %s", cfg.raw_dataset_path)
    logger.info(" Prepared file  : %s", cfg.prepared_path)
    logger.info(" Missing policy : %s", cfg.numeric_missing_policy)

    if cfg.prepared_path.exists():
        logger.info("Prepared file already exists — skipping preprocessing.")
        return

    df = load_and_prepare(
        raw_dataset_path=cfg.raw_dataset_path,
        numeric_missing_policy=cfg.numeric_missing_policy,
    )

    if df.empty:
        logger.error("Dataset is empty — nothing to prepare. Exiting.")
        sys.exit(1)

    write_prepared(df, cfg.prepared_path)


if __name__ == "__main__":
    main()
