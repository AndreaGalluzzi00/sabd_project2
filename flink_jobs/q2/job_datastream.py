#!/usr/bin/env python3
"""
Q2 – Real-time ranking of departure airports with significant delays (DataStream API).

Pure DataStream reimplementation of q2/job.py (which uses Flink SQL Window Top-N),
kept side by side for the Table-API-vs-DataStream comparison — same pattern as Q1
(q1/job.py vs q1/job_datastream.py). Produces the same result under q2_ds/.

Pipeline (three window sizes 1h / 6h / 365-day global, in one job):

  Stage 1 — keyBy(origin_airport_id) → TumblingEventTimeWindow → AggregateFunction
    computes per (window, airport): num_flights, severe_delays (dep_delay > 30),
    dep_delay_mean, dep_delay_max and the top-20 severe flights. The window
    aggregate fires once per window (append-only, state released on fire), so —
    unlike a SQL Python UDAF — there are no intermediate snapshots. The
    ProcessWindowFunction drops airports with < 30 flights (HAVING) and formats
    the flight list.

  Stage 2 — windowAll(TumblingEventTimeWindow) collects every airport of a
    completed window and emits the top-10 ranked by severe_delays.

Kafka is read exactly like q1/job_datastream.py: SimpleStringSchema("ISO-8859-1")
carries the raw Confluent-Avro bytes losslessly, decoded by a minimal pure-Python
reader. Q2 also needs origin_airport_id and dest_airport_id (Q1 skips them).

Output schema (per window type, one file each):
    ts, airport_rank, origin_airport_id, num_flights, severe_delays,
    dep_delay_mean, dep_delay_max, delayed_flights
"""
from __future__ import annotations

import struct
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration, Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import (
    AggregateFunction,
    MapFunction,
    ProcessAllWindowFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.window import TumblingEventTimeWindows

from common.config import load_config
from common.logging_utils import configure_logging
from flink_runtime import FlinkRuntimeConfig, build_flink_runtime_config

import logging

configure_logging()
logger = logging.getLogger(__name__)

# Max severe flights listed per airport (spec: at most 20, sorted by delay desc).
TOP_N = 20


# ── Config ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Q2DSConfig(FlinkRuntimeConfig):
    results_path_1h: str
    results_path_6h: str
    results_path_global: str
    watermark_delay_seconds: int


def load_q2_ds_config() -> Q2DSConfig:
    cfg = load_config()
    flink_cfg = build_flink_runtime_config(cfg)
    return Q2DSConfig(
        **asdict(flink_cfg),
        results_path_1h=cfg["paths"]["q2_ds_results_path_1h"],
        results_path_6h=cfg["paths"]["q2_ds_results_path_6h"],
        results_path_global=cfg["paths"]["q2_ds_results_path_global"],
        watermark_delay_seconds=int(cfg["q2"]["watermark_delay_seconds"]),
    )


# ── Pure-Python Avro binary decoder ──────────────────────────────────────────
# Same encoding rules as q1/job_datastream.py; here we also keep
# origin_airport_id and dest_airport_id.

def _avro_decode_varint(buf: bytes, pos: int):
    b, shift = 0, 0
    while True:
        byte = buf[pos]; pos += 1
        b |= (byte & 0x7F) << shift
        shift += 7
        if not (byte & 0x80):
            break
    return (b >> 1) ^ -(b & 1), pos


def _avro_skip_varint(buf: bytes, pos: int) -> int:
    while buf[pos] & 0x80:
        pos += 1
    return pos + 1


def _avro_decode_string(buf: bytes, pos: int):
    n, pos = _avro_decode_varint(buf, pos)
    return buf[pos:pos + n].decode('utf-8'), pos + n


def _avro_decode_nullable_double(buf: bytes, pos: int):
    branch, pos = _avro_decode_varint(buf, pos)
    if branch == 0:
        return None, pos
    return struct.unpack_from('<d', buf, pos)[0], pos + 8


def _decode_flight_record(data: bytes):
    """
    Decode a Confluent-Avro wire-format Flight record (0x00 + 4-byte schema_id +
    Avro binary). Field order from schema/flight.avsc.

    Returns (event_time_ms, airline, origin_airport_id, dest_airport_id,
             dep_delay, cancelled, diverted) or None if malformed.
    """
    if len(data) < 6 or data[0] != 0:
        return None
    pos = 5  # skip magic byte + schema_id

    event_time, pos = _avro_decode_varint(data, pos)            # event_time : long
    pos = _avro_skip_varint(data, pos)                          # year
    pos = _avro_skip_varint(data, pos)                          # month
    pos = _avro_skip_varint(data, pos)                          # day_of_month
    airline,   pos = _avro_decode_string(data, pos)            # airline : string
    origin,    pos = _avro_decode_varint(data, pos)            # origin_airport_id : int
    dest,      pos = _avro_decode_varint(data, pos)            # dest_airport_id   : int
    pos = _avro_skip_varint(data, pos)                          # crs_dep_time
    dep_delay, pos = _avro_decode_nullable_double(data, pos)   # dep_delay
    _,         pos = _avro_decode_nullable_double(data, pos)   # arr_delay (skipped)
    cancelled, pos = _avro_decode_nullable_double(data, pos)   # cancelled
    diverted,  pos = _avro_decode_nullable_double(data, pos)   # diverted

    return event_time, airline, origin, dest, dep_delay, cancelled, diverted


class _AvroDeserializer(MapFunction):
    """
    ISO-8859-1 str -> raw bytes -> Flight record. Keeps only non-cancelled and
    non-diverted flights (this also drops the '__EOS__' marker, which carries
    cancelled=1). Malformed records are dropped. Returns
    (event_time_ms, airline, origin_airport_id, dest_airport_id, dep_delay) or None.
    """

    def map(self, value: str):
        try:
            result = _decode_flight_record(value.encode('iso-8859-1'))
            if result is None:
                return None
            event_time, airline, origin, dest, dep_delay, cancelled, diverted = result
            if (cancelled or 0.0) >= 0.5 or (diverted or 0.0) >= 0.5:
                return None
            return (event_time, airline, origin, dest, dep_delay)
        except Exception:
            return None


# ── Timestamp assigners ───────────────────────────────────────────────────────

class FlightTimestampAssigner:
    """event_time (ms) is tuple[0]."""
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return value[0]


class WindowEndTimestampAssigner:
    """Stage-2 event time = window_end (index 1 of the Stage-1 stats row, epoch ms)."""
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return value[1]


# ── Stage 1: per-airport aggregation ──────────────────────────────────────────
# Accumulator layout:
#   [num_flights, severe_delays, dep_delay_sum, dep_delay_count,
#    dep_delay_max|None, severe_list]
# severe_list: list of (dep_delay, carrier, dest) kept bounded to TOP_N.

class Q2AggFunction(AggregateFunction):

    def create_accumulator(self):
        return [0, 0, 0.0, 0, None, []]

    def add(self, value, acc):
        _, airline, _origin, dest, dep_delay = value
        acc[0] += 1
        if dep_delay is not None:
            acc[2] += dep_delay
            acc[3] += 1
            if acc[4] is None or dep_delay > acc[4]:
                acc[4] = dep_delay
            if dep_delay > 30.0:
                acc[1] += 1
                acc[5].append((dep_delay, airline, dest))
                if len(acc[5]) > TOP_N:
                    acc[5].sort(key=lambda x: x[0], reverse=True)
                    del acc[5][TOP_N:]
        return acc

    def get_result(self, acc):
        return acc

    def merge(self, acc, other):
        acc[0] += other[0]
        acc[1] += other[1]
        acc[2] += other[2]
        acc[3] += other[3]
        if other[4] is not None and (acc[4] is None or other[4] > acc[4]):
            acc[4] = other[4]
        acc[5].extend(other[5])
        acc[5].sort(key=lambda x: x[0], reverse=True)
        del acc[5][TOP_N:]
        return acc


class Q2WindowFunction(ProcessWindowFunction):
    """
    Emits per (window, airport) stats, dropping airports with < 30 flights
    (HAVING) and formatting the top-20 severe-flight list. window_start/end are
    epoch milliseconds (LONG) to avoid a java.sql.Timestamp bridge.
    """

    def process(self, key, context: ProcessWindowFunction.Context, elements):
        acc = next(iter(elements))
        num_flights, severe_delays, dep_sum, dep_count, dep_max, severe_list = acc
        if num_flights < 30:          # HAVING COUNT(*) >= 30
            return

        dep_delay_mean = dep_sum / dep_count if dep_count > 0 else None
        top = sorted(severe_list, key=lambda x: x[0], reverse=True)[:TOP_N]
        delayed_flights = "[" + ",".join(
            f"({carrier},{dest},{delay:.2f})" for delay, carrier, dest in top
        ) + "]"

        window = context.window()
        yield Row(
            window.start,            # window_start (epoch ms)
            window.end,              # window_end   (epoch ms)
            key,                     # origin_airport_id
            num_flights,
            severe_delays,
            dep_delay_mean,
            dep_max,
            delayed_flights,
        )


Q2_STATS_TYPE = Types.ROW_NAMED(
    ['window_start', 'window_end', 'origin_airport_id', 'num_flights',
     'severe_delays', 'dep_delay_mean', 'dep_delay_max', 'delayed_flights'],
    [Types.LONG(), Types.LONG(), Types.INT(), Types.LONG(),
     Types.LONG(), Types.DOUBLE(), Types.DOUBLE(), Types.STRING()],
)


# ── Stage 2: top-10 ranking per window ────────────────────────────────────────

class Q2RankFunction(ProcessAllWindowFunction):
    """
    Receives every per-airport stats row of a completed window (each fired
    exactly once by Stage 1 — no intermediate snapshots) and emits the top-10
    ranked by severe_delays DESC (ties broken by dep_delay_mean DESC).
    """

    def process(self, context, elements):
        airports = sorted(
            elements,
            key=lambda r: (-(r[4] or 0), -(r[5] or 0.0)),
        )
        for rank, r in enumerate(airports[:10], 1):
            yield Row(r[0], rank, r[2], r[3], r[4], r[5], r[6], r[7])


Q2_OUTPUT_TYPE = Types.ROW_NAMED(
    ['ts', 'airport_rank', 'origin_airport_id', 'num_flights', 'severe_delays',
     'dep_delay_mean', 'dep_delay_max', 'delayed_flights'],
    [Types.LONG(), Types.LONG(), Types.INT(), Types.LONG(),
     Types.LONG(), Types.DOUBLE(), Types.DOUBLE(), Types.STRING()],
)


# ── CSV sink ─────────────────────────────────────────────────────────────────

class _CsvWriter(MapFunction):
    """Writes each CSV line to part-{subtask}-{run_ts} and re-emits it unchanged
    (consumed by .print() as the mandatory sink). Unique filenames avoid Windows
    bind-mount permission clashes with previous-run files."""

    def __init__(self, base_path: str):
        self._base_path = base_path
        self._file = None

    def open(self, runtime_context):
        import os
        import time as _time
        os.makedirs(self._base_path, exist_ok=True)
        idx = runtime_context.get_index_of_this_subtask()
        run_ts = int(_time.time())
        self._file = open(
            os.path.join(self._base_path, f"part-{idx}-{run_ts}"),
            'w', encoding='utf-8',
        )

    def map(self, value):
        self._file.write(value + '\n')
        self._file.flush()
        return value

    def close(self):
        if self._file:
            self._file.close()


def _format_csv_row(row) -> str:
    """Row (ts_ms, rank, origin, num_flights, severe, mean, max, delayed) -> CSV.
    ts and delayed_flights are quoted (delayed_flights contains commas)."""
    ts = datetime.fromtimestamp(row[0] / 1000.0, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    mean = '' if row[5] is None else str(row[5])
    mx   = '' if row[6] is None else str(row[6])
    return f'"{ts}",{row[1]},{row[2]},{row[3]},{row[4]},{mean},{mx},"{row[7]}"'


# ── Per-window pipeline ───────────────────────────────────────────────────────

def build_window_pipeline(flights_ds, window_time: Time, results_path: str, label: str) -> None:
    """Stage 1 (keyed window aggregate) -> Stage 2 (windowAll top-10) -> CSV."""
    stats_ds = (
        flights_ds
        .key_by(lambda v: v[2], key_type=Types.INT())                 # origin_airport_id
        .window(TumblingEventTimeWindows.of(window_time))
        .aggregate(
            Q2AggFunction(),
            window_function=Q2WindowFunction(),
            accumulator_type=Types.PICKLED_BYTE_ARRAY(),
            output_type=Q2_STATS_TYPE,
        )
    )

    # Re-key event time to window_end so every airport of a window lands in the
    # same Stage-2 bucket; Stage 1 emits windows in order -> monotonous.
    stats_ds = stats_ds.assign_timestamps_and_watermarks(
        WatermarkStrategy
        .for_monotonous_timestamps()
        .with_timestamp_assigner(WindowEndTimestampAssigner())
    )

    ranked_ds = (
        stats_ds
        .window_all(TumblingEventTimeWindows.of(window_time))
        .process(Q2RankFunction(), output_type=Q2_OUTPUT_TYPE)
    )

    (
        ranked_ds
        .map(_format_csv_row, output_type=Types.STRING())
        .map(_CsvWriter(results_path), output_type=Types.STRING())
        .print()
    )
    logger.info("Q2-DS [%s] | CSV sink → %s", label, results_path)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = load_q2_ds_config()

    logger.info("Q2-DS | Kafka: %s  topic: %s", cfg.kafka_bootstrap, cfg.kafka_topic)
    logger.info("Q2-DS | Consumer group: %s", cfg.kafka_consumer_group + "-q2-ds")
    logger.info("Q2-DS | Parallelism: %d", cfg.parallelism)
    logger.info("Q2-DS | Watermark delay: %d s", cfg.watermark_delay_seconds)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(cfg.parallelism)
    env.enable_checkpointing(cfg.checkpoint_interval_ms)
    env.get_config().set_auto_watermark_interval(cfg.auto_watermark_interval_ms)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(cfg.kafka_bootstrap)
        .set_topics(cfg.kafka_topic)
        .set_group_id(cfg.kafka_consumer_group + "-q2-ds")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema("ISO-8859-1"))
        .build()
    )
    raw_ds = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "KafkaSource[flights]")

    flights_ds = (
        raw_ds
        .map(_AvroDeserializer(), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda v: v is not None)
    )
    flights_ds = flights_ds.assign_timestamps_and_watermarks(
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(cfg.watermark_delay_seconds))
        .with_timestamp_assigner(FlightTimestampAssigner())
    )

    windows = [
        ("1h",     Time.hours(1),   cfg.results_path_1h),
        ("6h",     Time.hours(6),   cfg.results_path_6h),
        ("global", Time.days(365),  cfg.results_path_global),
    ]
    for label, window_time, results_path in windows:
        build_window_pipeline(flights_ds, window_time, results_path, label)

    logger.info("Q2-DS | Submitting job (3 window pipelines) …")
    env.execute("Q2-DS")
    logger.info("Q2-DS | Job submitted successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()
