#!/usr/bin/env python3
"""
Q3 – Distribuzione in tempo reale dei ritardi in partenza per compagnia e
fascia oraria (percentili approssimati con DDSketch).

Il job combina le due API già usate per Q1:

  Computazione (DataStream API, come q1/job_datastream.py):
    KafkaSource (byte grezzi via ISO-8859-1) → decoder Confluent Avro
    puro-Python (esteso con crs_dep_time) → timestamp/watermark →
    filtro (AA/DL/UA/WN, non cancellati, non deviati, dep_delay presente) →
    keyBy(compagnia|fascia) → finestre event-time → AggregateFunction il cui
    accumulatore è un DDSketch (percentili senza accumulare i valori).

  Sink (Table API, come q1/job.py):
    ogni DataStream di risultati diventa una vista e uno StatementSet fa
    fan-out verso CSV (sempre) + Kafka→InfluxDB e JDBC→TimescaleDB
    (opzionali, per la dashboard Grafana). Un solo job Flink.

Finestre (tutte event-time, allineate all'inizio del dataset 2025-01-01
tramite offset del tumbling):
    1 giorno · 7 giorni · dall'inizio del dataset (365 giorni, un solo bucket
    che copre gen–apr 2025 e si chiude col marker EOS, come il global di Q2).

Nota watermark: timestamp e watermark sono assegnati PRIMA del filtro sulle
compagnie, quindi il marker EOS (event_time nel 2200) avanza il watermark e
drena l'ultima finestra anche senza 'flink stop --drain' (fix del gap noto
di Q1-DS).

Output schema (header finale scritto da scripts/merge_q3.py):
    ts, airline, hour, count, min, p25, p50, p75, p90, max
"""
from __future__ import annotations

import sys
from dataclasses import asdict, dataclass

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration, Time
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import DataTypes, Schema, StreamTableEnvironment

from common.config import load_config
from common.logging_utils import configure_logging
from flink_runtime import (
    FlinkRuntimeConfig,
    build_flink_runtime_config,
    create_stream_execution_environment,
)
from flight_avro import decode_flight_for_q3, hour_band
from window_ops import (
    CSV_SINK_DDL,
    JDBC_SINK_DDL,
    KAFKA_SINK_DDL,
    Q3_OUTPUT_TYPE,
    Q3AggregateFunction,
    Q3WindowFunction,
    group_key,
)

import logging

configure_logging()
logger = logging.getLogger(__name__)

AIRLINES: frozenset[str] = frozenset({"AA", "DL", "UA", "WN"})


# ── Config ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Q3Config(FlinkRuntimeConfig):
    results_path_1d: str
    results_path_7d: str
    results_path_global: str
    watermark_delay_seconds: int
    sketch_alpha: float

    influx_enabled: bool
    influx_topic_1d: str
    influx_topic_7d: str
    influx_topic_global: str

    timescale_enabled: bool
    timescale_url: str
    timescale_table_1d: str
    timescale_table_7d: str
    timescale_table_global: str
    timescale_username: str
    timescale_password: str


def load_q3_config() -> Q3Config:
    cfg = load_config()
    flink_cfg = build_flink_runtime_config(cfg)

    # Q3 usa un proprio consumer group per poter girare insieme a Q1/Q2.
    flink_dict = asdict(flink_cfg)
    flink_dict["kafka_consumer_group"] = cfg["flink"].get(
        "consumer_group_q3", cfg["flink"]["consumer_group"] + "-q3"
    )

    dashboard = cfg.get("dashboard", {})
    influx = dashboard.get("influx", {})
    timescale = dashboard.get("timescale", {})
    q3_influx = influx.get("q3", {})
    q3_timescale = timescale.get("q3", {})

    return Q3Config(
        **flink_dict,
        results_path_1d=cfg["paths"]["q3_results_path_1d"],
        results_path_7d=cfg["paths"]["q3_results_path_7d"],
        results_path_global=cfg["paths"]["q3_results_path_global"],
        watermark_delay_seconds=int(cfg["q3"]["watermark_delay_seconds"]),
        sketch_alpha=float(cfg["q3"].get("sketch_alpha", 0.01)),
        influx_enabled=bool(influx.get("enabled", False)),
        influx_topic_1d=str(q3_influx.get("topic_1d", "q3_results_1d")),
        influx_topic_7d=str(q3_influx.get("topic_7d", "q3_results_7d")),
        influx_topic_global=str(q3_influx.get("topic_global", "q3_results_global")),
        timescale_enabled=bool(timescale.get("enabled", False)),
        timescale_url=str(timescale.get("url", "")),
        timescale_table_1d=str(q3_timescale.get("table_1d", "q3_results_1d")),
        timescale_table_7d=str(q3_timescale.get("table_7d", "q3_results_7d")),
        timescale_table_global=str(q3_timescale.get("table_global", "q3_results_global")),
        timescale_username=str(timescale.get("username", "")),
        timescale_password=str(timescale.get("password", "")),
    )


# ── Decodifica Kafka+Avro ─────────────────────────────────────────────────────

class _FlightDecoder(MapFunction):
    """
    Recupera i byte Kafka originali dalla String ISO-8859-1 e decodifica il
    record Flight. Ritorna (event_time_ms, airline, crs_dep_time, dep_delay,
    cancelled, diverted) oppure None per i messaggi malformati.

    A differenza di Q1-DS qui NON si filtra sulla compagnia: anche il marker
    EOS deve attraversare l'assegnazione del watermark (vedi docstring modulo).
    """

    def map(self, value: str):
        try:
            return decode_flight_for_q3(value.encode('iso-8859-1'))
        except Exception:
            return None


class FlightTimestampAssigner:
    """Duck-type timestamp assigner (PyFlink 1.20): event_time ms in tuple[0]."""
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return value[0]


def _is_q3_relevant(value) -> bool:
    """Voli delle 4 compagnie, non cancellati, non deviati, con DEP_DELAY.

    NULL trattato come 0 ("non cancellato"/"non deviato"), coerente con Q1/Q2
    e con la policy 'null' del preprocessing. I voli senza dep_delay non
    contribuiscono alcun valore alla distribuzione, quindi non rientrano nel
    conteggio dei "voli considerati". Il marker EOS (cancelled=1) cade qui.
    """
    _, airline, _, dep_delay, cancelled, diverted = value
    return (
        airline in AIRLINES
        and (cancelled or 0.0) < 0.5
        and (diverted or 0.0) < 0.5
        and dep_delay is not None
    )


# ── Per-window pipeline ───────────────────────────────────────────────────────

def build_window_pipeline(
    label: str,
    window_assigner: TumblingEventTimeWindows,
    results_path: str,
    influx_topic: str,
    timescale_table: str,
    keyed_stream,
    t_env: StreamTableEnvironment,
    stmt_set,
    cfg: Q3Config,
) -> None:
    """Finestra → sketch → vista Table → sink (CSV + dashboard opzionali)."""

    result_ds = (
        keyed_stream
        .window(window_assigner)
        .aggregate(
            Q3AggregateFunction(cfg.sketch_alpha),
            window_function=Q3WindowFunction(),
            accumulator_type=Types.PICKLED_BYTE_ARRAY(),
            output_type=Q3_OUTPUT_TYPE,
        )
        # Nome esplicito senza virgole: gli operatori window anonimi hanno un
        # nome con virgole che rende numLateRecordsDropped non interrogabile
        # via REST (scripts/report_late_drops.py).
        .name(f"Q3Window[{label}]")
    )

    view = f"q3_{label}"
    table = t_env.from_data_stream(
        result_ds,
        Schema.new_builder()
            .column("ts",          DataTypes.TIMESTAMP(3))
            .column("airline",     DataTypes.STRING())
            .column("hour",        DataTypes.INT())
            .column("num_flights", DataTypes.BIGINT())
            .column("delay_min",   DataTypes.DOUBLE())
            .column("p25",         DataTypes.DOUBLE())
            .column("p50",         DataTypes.DOUBLE())
            .column("p75",         DataTypes.DOUBLE())
            .column("p90",         DataTypes.DOUBLE())
            .column("delay_max",   DataTypes.DOUBLE())
            .build(),
    )
    t_env.create_temporary_view(view, table)

    # ── CSV sink (sempre attivo: output certificato) ──────────────────────────
    csv_sink = f"q3_csv_{label}"
    t_env.execute_sql(CSV_SINK_DDL.format(name=csv_sink, path=results_path))
    stmt_set.add_insert_sql(f"INSERT INTO {csv_sink} SELECT * FROM {view}")
    logger.info("Q3 [%s] | CSV sink → %s", label, results_path)

    # ── InfluxDB sink opzionale (Kafka → Telegraf) ────────────────────────────
    if cfg.influx_enabled:
        kafka_sink = f"q3_kafka_{label}"
        t_env.execute_sql(KAFKA_SINK_DDL.format(
            name=kafka_sink,
            topic=influx_topic,
            bootstrap=cfg.kafka_bootstrap,
        ))
        # hour → STRING: Telegraf usa solo stringhe come tag InfluxDB.
        stmt_set.add_insert_sql(f"""
            INSERT INTO {kafka_sink}
            SELECT ts, airline, CAST(hour AS STRING), num_flights,
                   delay_min, p25, p50, p75, p90, delay_max
            FROM {view}
        """)
        logger.info("Q3 [%s] | InfluxDB sink → Kafka topic '%s'", label, influx_topic)

    # ── TimescaleDB sink opzionale (JDBC nativo) ──────────────────────────────
    if cfg.timescale_enabled:
        jdbc_sink = f"q3_jdbc_{label}"
        t_env.execute_sql(JDBC_SINK_DDL.format(
            name=jdbc_sink,
            url=cfg.timescale_url,
            table=timescale_table,
            username=cfg.timescale_username,
            password=cfg.timescale_password,
        ))
        stmt_set.add_insert_sql(f"INSERT INTO {jdbc_sink} SELECT * FROM {view}")
        logger.info("Q3 [%s] | TimescaleDB sink → table '%s'", label, timescale_table)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = load_q3_config()

    logger.info("Q3 | Kafka: %s  topic: %s", cfg.kafka_bootstrap, cfg.kafka_topic)
    logger.info("Q3 | Consumer group: %s", cfg.kafka_consumer_group)
    logger.info("Q3 | Parallelism: %d", cfg.parallelism)
    logger.info("Q3 | Watermark delay: %d s (event time)", cfg.watermark_delay_seconds)
    logger.info("Q3 | DDSketch alpha: %g (errore relativo max sui percentili)", cfg.sketch_alpha)

    env = create_stream_execution_environment(cfg)
    t_env = StreamTableEnvironment.create(env)

    # ── KafkaSource (byte grezzi via trucco ISO-8859-1, come Q1-DS) ───────────
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(cfg.kafka_bootstrap)
        .set_topics(cfg.kafka_topic)
        .set_group_id(cfg.kafka_consumer_group)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema("ISO-8859-1"))
        .build()
    )

    raw_ds = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "KafkaSource[flights]",
    )

    decoded_ds = (
        raw_ds
        .map(_FlightDecoder(), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda v: v is not None)
    )

    # ── Watermark PRIMA del filtro: l'EOS avanza il watermark e chiude le
    #    finestre finali; il filtro successivo lo esclude dai risultati. ───────
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(cfg.watermark_delay_seconds))
        .with_timestamp_assigner(FlightTimestampAssigner())
    )
    decoded_ds = decoded_ds.assign_timestamps_and_watermarks(watermark_strategy)

    # ── Filtro Q3 + proiezione (event_time, airline, hour, dep_delay) ─────────
    flights_ds = (
        decoded_ds
        .filter(_is_q3_relevant)
        .map(
            lambda v: (v[0], v[1], hour_band(v[2]), float(v[3])),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
    )

    keyed_stream = flights_ds.key_by(
        lambda v: group_key(v[1], v[2]),
        key_type=Types.STRING(),
    )

    # ── Tre finestre tumbling event-time ──────────────────────────────────────
    # I tumbling di Flink sono allineati a epoch (1970-01-01, un giovedì): senza
    # offset le finestre da 7 e 365 giorni partirebbero rispettivamente il
    # 2024-12-26 e il 2024-12-18. L'offset le allinea all'inizio del dataset:
    #   2025-01-01 = epoch + 20089 giorni; 20089 mod 7 = 6; 20089 mod 365 = 14
    # → settimane 01/01, 08/01, … e finestra globale [2025-01-01, 2026-01-01),
    # che copre l'intero dataset (gen–apr 2025) in un solo bucket e si chiude
    # col watermark spinto dal marker EOS (stessa semantica del global di Q2).
    windows = [
        ("1d",     TumblingEventTimeWindows.of(Time.days(1)),
         cfg.results_path_1d,     cfg.influx_topic_1d,     cfg.timescale_table_1d),
        ("7d",     TumblingEventTimeWindows.of(Time.days(7),   Time.days(6)),
         cfg.results_path_7d,     cfg.influx_topic_7d,     cfg.timescale_table_7d),
        ("global", TumblingEventTimeWindows.of(Time.days(365), Time.days(14)),
         cfg.results_path_global, cfg.influx_topic_global, cfg.timescale_table_global),
    ]

    stmt_set = t_env.create_statement_set()

    for label, window_assigner, results_path, influx_topic, timescale_table in windows:
        build_window_pipeline(
            label=label,
            window_assigner=window_assigner,
            results_path=results_path,
            influx_topic=influx_topic,
            timescale_table=timescale_table,
            keyed_stream=keyed_stream,
            t_env=t_env,
            stmt_set=stmt_set,
            cfg=cfg,
        )

    if not (cfg.influx_enabled or cfg.timescale_enabled):
        logger.info("Q3 | Dashboard sinks disabled (CSV-only run).")

    # ── Submit: un solo job, sorgente letta una volta, fan-out sui sink ───────
    logger.info("Q3 | Submitting job (3 window pipelines) …")
    stmt_set.execute()
    logger.info("Q3 | Job submitted successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()
