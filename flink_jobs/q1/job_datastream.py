#!/usr/bin/env python3
"""
Q1 – Real-time airline operational status monitoring (DataStream API).

Reimplementazione di q1/job.py usando la DataStream API di PyFlink al posto
della Table API / SQL.  Produce lo stesso schema CSV del job Table API così
che merge_q1.py possa essere riusato puntando a q1_ds_results_host_path.

Pipeline:
    KafkaSource (JSON)
      → map(parse_json)  → filter(airline ∈ AIRLINES)
      → assign_timestamps_and_watermarks(BoundedOutOfOrderness)
      → key_by(airline)
      → window(TumblingEventTimeWindows 1h)
      → aggregate(Q1AggFunction, Q1WindowFunction)
      → from_data_stream()            ← torna a Table solo per il sink CSV
      → filesystem CSV sink

Output schema (identico a job.py):
    window_start, window_end, airline, num_flights, completed, cancelled,
    diverted, dep_delay_mean, cancellation_rate, late_departure_rate
"""
from __future__ import annotations

import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.time import Duration, Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import DataTypes, Schema, StreamTableEnvironment

from common.config import load_config
from common.logging_utils import configure_logging
from flink_runtime import FlinkRuntimeConfig, build_flink_runtime_config

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


# ── Timestamp assigner ────────────────────────────────────────────────────────

class FlightTimestampAssigner:
    """Duck-type timestamp assigner for PyFlink 1.20. Extracts event_time (ms) from tuple[0]."""
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return value[0]  # event_time in millisecondi


# ── Accumulator ───────────────────────────────────────────────────────────────
# Lista mutabile: [num_flights, completed, cancelled, diverted,
#                  dep_delay_sum, dep_delay_count, late_departures, non_cancelled_count]
# dep_delay_count e late_departures contano solo i voli non cancellati con dep_delay non nullo.
# non_cancelled_count conta tutti i voli non cancellati (denominatore di late_departure_rate).

class Q1AggFunction(AggregateFunction):
    """
    Aggregazione incrementale per finestra tumbling 1h.
    Ogni chiamata ad add() aggiorna l'accumulatore con un singolo volo.
    Serializzato come PICKLED_BYTE_ARRAY: Flink usa pickle Python per lo stato.
    """

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

        # Solo voli non cancellati contribuiscono al ritardo
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
    Riceve l'accumulatore finale per (finestra, airline) e emette un Row con
    tutte le statistiche più i metadati window_start / window_end.

    Semantica identica al GROUP BY + FILTER della Table API in job.py:
      - dep_delay_mean:      AVG(dep_delay) sui non cancellati (None se 0 voli)
      - cancellation_rate:   cancelled / num_flights * 100
      - late_departure_rate: late_departures / non_cancelled_count * 100  (None se 0 non-cancellati)
    """

    def process(self, key: str, context: ProcessWindowFunction.Context, elements):
        acc = next(iter(elements))
        num_flights, completed, cancelled, diverted, \
            dep_delay_sum, dep_delay_count, late_departures, non_cancelled_count = acc

        window = context.window()
        window_start = datetime.fromtimestamp(window.start / 1000.0, tz=timezone.utc) \
                               .replace(tzinfo=None)   # naive UTC, coerente con LOCAL_DATE_TIME
        window_end   = datetime.fromtimestamp(window.end   / 1000.0, tz=timezone.utc) \
                               .replace(tzinfo=None)

        dep_delay_mean      = dep_delay_sum / dep_delay_count if dep_delay_count > 0         else None
        cancellation_rate   = cancelled / num_flights * 100.0  if num_flights > 0            else 0.0
        late_departure_rate = late_departures / non_cancelled_count * 100.0 \
                              if non_cancelled_count > 0 else None

        yield Row(
            window_start,
            window_end,
            key,
            num_flights,
            completed,
            cancelled,
            diverted,
            dep_delay_mean,
            cancellation_rate,
            late_departure_rate,
        )


# TypeInformation del Row emesso da Q1WindowFunction.
# LOCAL_DATE_TIME ↔ TIMESTAMP_LTZ(3) nel bridge Java/Python di PyFlink.
Q1_DS_OUTPUT_TYPE = Types.ROW([
    Types.SQL_TIMESTAMP(),  # window_start
    Types.SQL_TIMESTAMP(),  # window_end
    Types.STRING(),           # airline
    Types.LONG(),             # num_flights
    Types.LONG(),             # completed
    Types.LONG(),             # cancelled
    Types.LONG(),             # diverted
    Types.DOUBLE(),           # dep_delay_mean  (nullable)
    Types.DOUBLE(),           # cancellation_rate
    Types.DOUBLE(),           # late_departure_rate (nullable)
])


# ── Avro tuple extractor ──────────────────────────────────────────────────────

def _extract_flight_tuple(row):
    """
    Converte un Row proveniente dal Table source avro-confluent in una tupla
    (event_time_ms, airline, dep_delay|None, cancelled|None, diverted|None).
    Restituisce None per i marker EOS (airline='__EOS__') e per le compagnie
    non di interesse.
    """
    airline = row.airline
    if not isinstance(airline, str) or airline not in AIRLINES:
        return None
    return (
        int(row.event_time),
        airline,
        float(row.dep_delay)  if row.dep_delay  is not None else None,
        float(row.cancelled)  if row.cancelled  is not None else None,
        float(row.diverted)   if row.diverted   is not None else None,
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = load_q1_ds_config()

    logger.info("Q1-DS | Kafka: %s  topic: %s", cfg.kafka_bootstrap, cfg.kafka_topic)
    logger.info("Q1-DS | Consumer group: %s", cfg.kafka_consumer_group + "-ds")
    logger.info("Q1-DS | Results path: %s", cfg.results_path)
    logger.info("Q1-DS | Parallelism: %d", cfg.parallelism)
    logger.info("Q1-DS | Watermark delay: %d s", cfg.watermark_delay_seconds)

    # ── Ambiente ──────────────────────────────────────────────────────────────
    env   = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)          # usato solo per il sink CSV

    env.set_parallelism(cfg.parallelism)
    env.enable_checkpointing(cfg.checkpoint_interval_ms)
    env.get_config().set_auto_watermark_interval(cfg.auto_watermark_interval_ms)

    # ── Kafka source via Table API (avro-confluent) ───────────────────────────
    # La DataStream API non ha un deserializzatore Avro+SchemaRegistry nativo
    # in PyFlink. Si usa la Table API per il source e si converte in DataStream.
    t_env.execute_sql(f"""
        CREATE TABLE flights_src (
            event_time  BIGINT,
            airline     STRING,
            dep_delay   DOUBLE,
            cancelled   DOUBLE,
            diverted    DOUBLE
        ) WITH (
            'connector'                                = 'kafka',
            'topic'                                    = '{cfg.kafka_topic}',
            'properties.bootstrap.servers'             = '{cfg.kafka_bootstrap}',
            'properties.group.id'                      = '{cfg.kafka_consumer_group}-ds',
            'scan.startup.mode'                        = 'earliest-offset',
            'format'                                   = 'avro-confluent',
            'avro-confluent.schema-registry.url'       = 'http://schema-registry:8081',
            'avro-confluent.schema-registry.subject'   = 'flights-value'
        )
    """)

    raw_ds = t_env.to_data_stream(t_env.from_path("flights_src"))

    # ── Extract tuple + filter ────────────────────────────────────────────────
    flights_ds = (
        raw_ds
        .map(_extract_flight_tuple, output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda v: v is not None)
    )

    # ── Assegnazione timestamp e watermark ────────────────────────────────────
    # BoundedOutOfOrderness con lo stesso delay del job Table API (q1.watermark_delay_seconds).
    # FlightTimestampAssigner estrae event_time (ms) dalla tupla parsata.
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

    # ── Da DataStream a Table → sink CSV (filesystem connector) ──────────────
    # Pattern identico a Q2: from_data_stream() riporta il risultato nel mondo
    # Table API. Il sink filesystem è identico a quello del job Table API così
    # che merge_q1.py possa processare i part-file senza modifiche.
    result_table = t_env.from_data_stream(
        result_ds,
        Schema.new_builder()
            .column("window_start",        DataTypes.TIMESTAMP_LTZ(3))
            .column("window_end",          DataTypes.TIMESTAMP_LTZ(3))
            .column("airline",             DataTypes.STRING())
            .column("num_flights",         DataTypes.BIGINT())
            .column("completed",           DataTypes.BIGINT())
            .column("cancelled",           DataTypes.BIGINT())
            .column("diverted",            DataTypes.BIGINT())
            .column("dep_delay_mean",      DataTypes.DOUBLE())
            .column("cancellation_rate",   DataTypes.DOUBLE())
            .column("late_departure_rate", DataTypes.DOUBLE())
            .build()
    )
    t_env.create_temporary_view("q1_ds_agg", result_table)

    t_env.execute_sql(f"""
        CREATE TABLE q1_ds_results (
            window_start        TIMESTAMP(3),
            window_end          TIMESTAMP(3),
            airline             STRING,
            num_flights         BIGINT,
            completed           BIGINT,
            cancelled           BIGINT,
            diverted            BIGINT,
            dep_delay_mean      DOUBLE,
            cancellation_rate   DOUBLE,
            late_departure_rate DOUBLE
        ) WITH (
            'connector'                              = 'filesystem',
            'path'                                   = '{cfg.results_path}',
            'format'                                 = 'csv',
            'sink.rolling-policy.rollover-interval'  = '10 s',
            'sink.rolling-policy.check-interval'     = '5 s'
        )
    """)

    logger.info("Q1-DS | Submitting job …")
    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql("INSERT INTO q1_ds_results SELECT * FROM q1_ds_agg")
    stmt_set.execute()

    logger.info("Q1-DS | Job submitted successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()
