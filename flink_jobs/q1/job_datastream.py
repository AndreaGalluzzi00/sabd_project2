#!/usr/bin/env python3
"""
Q1 – Real-time airline operational status monitoring (DataStream API).

Pure DataStream pipeline: no Table API, no external Avro library.

KafkaSource uses SimpleStringSchema("ISO-8859-1") to receive raw Kafka bytes
as a Java String without data loss (ISO-8859-1 maps every byte 0-255 to the
matching Unicode code point).  Python recovers the original bytes with
str.encode('iso-8859-1') inside _AvroDeserializer.

The Confluent Avro wire format (magic byte 0x00 + 4-byte big-endian schema_id
+ Avro binary payload) is decoded with a minimal pure-Python reader that
follows the exact field order in schema/flight.avsc:

    event_time (long), year (int), month (int), day_of_month (int),
    airline (string), origin_airport_id (int), dest_airport_id (int),
    crs_dep_time (int), dep_delay (["null","double"]),
    arr_delay (["null","double"]), cancelled (["null","double"]),
    diverted (["null","double"]), …

Output schema (identical to job.py):
    window_start, window_end, airline, num_flights, completed, cancelled,
    diverted, dep_delay_mean, cancellation_rate, late_departure_rate
"""
from __future__ import annotations

import struct
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration, Time
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import AggregateFunction, MapFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows

from common.config import load_config
from common.logging_utils import configure_logging
from flink_runtime import (
    FlinkRuntimeConfig,
    build_flink_runtime_config,
    create_stream_execution_environment,
)

import logging

configure_logging()
logger = logging.getLogger(__name__)

AIRLINES: frozenset[str] = frozenset({"AA", "DL", "UA", "WN"})


# ── Config ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Q1DSConfig(FlinkRuntimeConfig):
    results_path: str
    watermark_delay_seconds: int


def load_q1_ds_config() -> Q1DSConfig:
    cfg = load_config()
    flink_cfg = build_flink_runtime_config(cfg)
    return Q1DSConfig(
        **asdict(flink_cfg),
        results_path=cfg["paths"]["q1_ds_results_path"],
        watermark_delay_seconds=int(cfg["q1"]["watermark_delay_seconds"]),
    )


# ── Pure-Python Avro binary decoder ──────────────────────────────────────────
# Avro binary encoding rules used below:
#   long / int  zigzag varint: 7 bits per byte (LSB first), MSB = continuation bit.
#               ZigZag decode: (encoded >> 1) ^ -(encoded & 1)
#   string      long (byte count) + raw UTF-8 bytes
#   ["null","double"]
#               varint branch index (0 = null; 1 → zigzag = 2 = double)
#               + optional 8-byte little-endian IEEE 754 double

def _avro_decode_varint(buf: bytes, pos: int):
    """Decode a zigzag-encoded varint. Returns (value, new_pos)."""
    b, shift = 0, 0
    while True:
        byte = buf[pos]; pos += 1
        b |= (byte & 0x7F) << shift
        shift += 7
        if not (byte & 0x80):
            break
    return (b >> 1) ^ -(b & 1), pos


def _avro_skip_varint(buf: bytes, pos: int) -> int:
    """Skip a varint without decoding it; return the new position."""
    while buf[pos] & 0x80:
        pos += 1
    return pos + 1


def _avro_decode_string(buf: bytes, pos: int):
    """Decode an Avro string (long byte-length + UTF-8). Returns (str, new_pos)."""
    n, pos = _avro_decode_varint(buf, pos)
    return buf[pos:pos + n].decode('utf-8'), pos + n


def _avro_decode_nullable_double(buf: bytes, pos: int):
    """Decode union ["null","double"]. Returns (float|None, new_pos)."""
    branch, pos = _avro_decode_varint(buf, pos)
    if branch == 0:          # null branch
        return None, pos
    # double branch (index 1, zigzag-encoded as 2)
    return struct.unpack_from('<d', buf, pos)[0], pos + 8


def _decode_flight_record(data: bytes):
    """
    Decode a Confluent Avro wire-format Flight record.

    Wire format:  0x00 (magic byte)  |  4 bytes big-endian schema_id  |  Avro binary
    Field order:  flight.avsc (it.uniroma2.sabd.Flight) — see module docstring.

    Returns (event_time_ms, airline, dep_delay, cancelled, diverted) or None
    if the message is malformed (wrong magic byte or too short).
    """
    if len(data) < 6 or data[0] != 0:
        return None
    pos = 5  # skip magic byte (1) + schema_id (4)

    event_time, pos = _avro_decode_varint(data, pos)            # event_time : long
    pos = _avro_skip_varint(data, pos)                          # year       : int
    pos = _avro_skip_varint(data, pos)                          # month      : int
    pos = _avro_skip_varint(data, pos)                          # day_of_month: int
    airline,    pos = _avro_decode_string(data, pos)            # airline    : string
    pos = _avro_skip_varint(data, pos)                          # origin_airport_id: int
    pos = _avro_skip_varint(data, pos)                          # dest_airport_id  : int
    pos = _avro_skip_varint(data, pos)                          # crs_dep_time     : int
    dep_delay,  pos = _avro_decode_nullable_double(data, pos)   # dep_delay  : ["null","double"]
    _,          pos = _avro_decode_nullable_double(data, pos)   # arr_delay  (skipped)
    cancelled,  pos = _avro_decode_nullable_double(data, pos)   # cancelled  : ["null","double"]
    diverted,   pos = _avro_decode_nullable_double(data, pos)   # diverted   : ["null","double"]

    return event_time, airline, dep_delay, cancelled, diverted


# ── Kafka+Avro deserializer MapFunction ──────────────────────────────────────

class _AvroDeserializer(MapFunction):
    """
    Converts the ISO-8859-1 Java String (from SimpleStringSchema("ISO-8859-1"))
    back to the original raw bytes, then decodes the Confluent Avro Flight record.

    Returns (event_time_ms, airline, dep_delay, cancelled, diverted) or None.
    Non-target airlines and EOS markers ('__EOS__') produce None.
    Malformed records are silently dropped (return None).
    """

    def map(self, value: str):
        try:
            raw = value.encode('iso-8859-1')    # recover original Kafka bytes
            result = _decode_flight_record(raw)
            if result is None:
                return None
            event_time, airline, dep_delay, cancelled, diverted = result
            if airline not in AIRLINES:         # filters EOS markers too
                return None
            return (event_time, airline, dep_delay, cancelled, diverted)
        except Exception:
            return None


# ── Timestamp assigner ────────────────────────────────────────────────────────

class FlightTimestampAssigner:
    """Duck-type timestamp assigner for PyFlink 1.20. Extracts event_time (ms) from tuple[0]."""
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return value[0]  # event_time in milliseconds


# ── Accumulator ───────────────────────────────────────────────────────────────
# List layout: [num_flights, completed, cancelled, diverted,
#               dep_delay_sum, dep_delay_count, late_departures, non_cancelled_count]

class Q1AggFunction(AggregateFunction):
    """Incremental aggregation for a 1-hour tumbling window."""

    def create_accumulator(self):
        return [0, 0, 0, 0, 0.0, 0, 0, 0]

    def add(self, value, acc):
        # value: (event_time_ms, airline, dep_delay|None, cancelled|None, diverted|None)
        _, _, dep_delay, cancelled, diverted = value
        is_cancelled = (cancelled or 0.0) >= 0.5
        is_diverted  = (diverted  or 0.0) >= 0.5

        acc[0] += 1                                  # num_flights
        if not is_cancelled and not is_diverted:
            acc[1] += 1                              # completed
        if is_cancelled:
            acc[2] += 1                              # cancelled
        if is_diverted:
            acc[3] += 1                              # diverted

        if not is_cancelled:
            acc[7] += 1                              # non_cancelled_count
            if dep_delay is not None:
                acc[4] += dep_delay                  # dep_delay_sum
                acc[5] += 1                          # dep_delay_count
                if dep_delay > 15.0:
                    acc[6] += 1                      # late_departures

        return acc

    def get_result(self, acc):
        return acc

    def merge(self, acc, other):
        return [a + b for a, b in zip(acc, other)]


# ── ProcessWindowFunction ─────────────────────────────────────────────────────

class Q1WindowFunction(ProcessWindowFunction):
    """
    Receives the final accumulator for (window, airline) and emits a Row with
    all statistics plus window_start / window_end as epoch milliseconds (LONG).
    """

    def process(self, key: str, context: ProcessWindowFunction.Context, elements):
        acc = next(iter(elements))
        num_flights, completed, cancelled, diverted, \
            dep_delay_sum, dep_delay_count, late_departures, non_cancelled_count = acc

        window = context.window()
        dep_delay_mean      = dep_delay_sum / dep_delay_count if dep_delay_count > 0 else None
        cancellation_rate   = cancelled / num_flights * 100.0 if num_flights > 0     else 0.0
        late_departure_rate = late_departures / non_cancelled_count * 100.0 \
                              if non_cancelled_count > 0 else None

        yield Row(
            window.start,          # epoch ms (LONG) — avoids java.sql.Timestamp bridge
            window.end,
            key,
            num_flights,
            completed,
            cancelled,
            diverted,
            dep_delay_mean,
            cancellation_rate,
            late_departure_rate,
        )


Q1_DS_OUTPUT_TYPE = Types.ROW_NAMED(
    ['window_start', 'window_end', 'airline', 'num_flights', 'completed',
     'cancelled', 'diverted', 'dep_delay_mean', 'cancellation_rate', 'late_departure_rate'],
    [Types.LONG(), Types.LONG(), Types.STRING(),
     Types.LONG(), Types.LONG(), Types.LONG(), Types.LONG(),
     Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()],
)


# ── CSV sink ─────────────────────────────────────────────────────────────────

class _CsvWriter(MapFunction):
    """
    MapFunction that writes each CSV row to part-{subtask_index}-{run_ts} and
    re-emits the value unchanged (consumed by .print() as the mandatory sink).
    Unique timestamped filenames avoid PermissionError on Windows Docker bind mounts
    where previous-run files have a different UID.
    """
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


# ── CSV formatter ────────────────────────────────────────────────────────────

def _format_csv_row(row) -> str:
    """Convert an output Row to a CSV string."""
    ws = datetime.fromtimestamp(row[0] / 1000.0, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    we = datetime.fromtimestamp(row[1] / 1000.0, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    dep_mean  = '' if row[7] is None else str(row[7])
    late_rate = '' if row[9] is None else str(row[9])
    return f"{ws},{we},{row[2]},{row[3]},{row[4]},{row[5]},{row[6]},{dep_mean},{row[8]},{late_rate}"


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = load_q1_ds_config()

    logger.info("Q1-DS | Kafka: %s  topic: %s", cfg.kafka_bootstrap, cfg.kafka_topic)
    logger.info("Q1-DS | Consumer group: %s", cfg.kafka_consumer_group + "-ds")
    logger.info("Q1-DS | Results path: %s", cfg.results_path)
    logger.info("Q1-DS | Parallelism: %d", cfg.parallelism)
    logger.info("Q1-DS | Watermark delay: %d s", cfg.watermark_delay_seconds)

    # Checkpointing EXACTLY_ONCE + restart strategy per la tolleranza ai guasti
    # degli operatori con stato (configurazione uniforme a tutti i job Flink).
    env = create_stream_execution_environment(cfg)

    # ── KafkaSource (raw bytes via ISO-8859-1 trick) ──────────────────────────
    # SimpleStringSchema("ISO-8859-1") converts each Kafka message byte[i] to
    # the Java char with code point == byte[i].  Because ISO-8859-1 is a bijection
    # on 0-255, no byte information is lost.  The Python worker receives a str whose
    # code points equal the original bytes; .encode('iso-8859-1') recovers them.
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(cfg.kafka_bootstrap)
        .set_topics(cfg.kafka_topic)
        .set_group_id(cfg.kafka_consumer_group + "-ds")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema("ISO-8859-1"))
        .build()
    )

    raw_ds = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "KafkaSource[flights]",
    )

    # ── Avro decode + airline filter ─────────────────────────────────────────
    flights_ds = (
        raw_ds
        .map(_AvroDeserializer(), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda v: v is not None)
    )

    # ── Watermark assignment ──────────────────────────────────────────────────
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(cfg.watermark_delay_seconds))
        .with_timestamp_assigner(FlightTimestampAssigner())
    )
    flights_ds = flights_ds.assign_timestamps_and_watermarks(watermark_strategy)

    # ── KeyBy + TumblingEventTimeWindow + Aggregate ───────────────────────────
    result_ds = (
        flights_ds
        .key_by(lambda v: v[1], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.hours(1)))
        .aggregate(
            Q1AggFunction(),
            window_function=Q1WindowFunction(),
            accumulator_type=Types.PICKLED_BYTE_ARRAY(),
            output_type=Q1_DS_OUTPUT_TYPE,
        )
    )

    # ── CSV sink ──────────────────────────────────────────────────────────────
    (
        result_ds
        .map(_format_csv_row, output_type=Types.STRING())
        .map(_CsvWriter(cfg.results_path), output_type=Types.STRING())
        .print()
    )

    logger.info("Q1-DS | Submitting job …")
    env.execute("Q1-DS")
    logger.info("Q1-DS | Job submitted successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()
