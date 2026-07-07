#!/usr/bin/env python3
from __future__ import annotations

import sys
from dataclasses import asdict, dataclass

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.time import Duration, Time
from pyflink.datastream import OutputTag
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import DataTypes, Schema, StreamTableEnvironment

from common.config import load_config
from common.logging_utils import configure_logging
from flink_runtime import (
    FlinkRuntimeConfig,
    build_flink_runtime_config,
    create_stream_execution_environment,
)
from q3_window_ops import (
    CSV_SINK_DDL,
    JDBC_SINK_DDL,
    KAFKA_SINK_DDL,
    Q3_OUTPUT_TYPE,
    Q3AggregateFunction,
    Q3LateDropCounter,
    Q3WindowFunction,
    group_key,
)

import logging

configure_logging()
logger = logging.getLogger(__name__)

AIRLINES: frozenset[str] = frozenset({"AA", "DL", "UA", "WN"})
Q3_WINDOW_CHOICES = ("1d", "7d", "global", "all")


def hour_band(crs_dep_time: int) -> int:
    """Scheduled departure hour bucket (0-23) from CRS_DEP_TIME in hhmm."""
    return (crs_dep_time // 100) % 24



@dataclass(frozen=True)
class Q3Config(FlinkRuntimeConfig):
    results_path_1d: str
    results_path_7d: str
    results_path_global: str
    window: str
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


def selected_q3_window(value: object) -> str:
    window = str(value or "all").strip().lower()
    if window not in Q3_WINDOW_CHOICES:
        raise ValueError(
            "q3.window must be one of: "
            + ", ".join(Q3_WINDOW_CHOICES)
        )
    return window


def load_q3_config() -> Q3Config:
    cfg = load_config()
    flink_cfg = build_flink_runtime_config(cfg)
    q3_cfg = cfg["q3"]

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
        window=selected_q3_window(q3_cfg.get("window", "all")),
        watermark_delay_seconds=int(q3_cfg["watermark_delay_seconds"]),
        sketch_alpha=float(q3_cfg.get("sketch_alpha", 0.01)),
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


def register_q3_flights_source(t_env: StreamTableEnvironment, cfg: Q3Config) -> None:
    t_env.execute_sql(f"""
        CREATE TABLE q3_flights_source (
            event_time   BIGINT,
            airline      STRING,
            crs_dep_time INT,
            dep_delay    DOUBLE,
            cancelled    DOUBLE,
            diverted     DOUBLE
        ) WITH (
            'connector'                              = 'kafka',
            'topic'                                  = '{cfg.kafka_topic}',
            'properties.bootstrap.servers'           = '{cfg.kafka_bootstrap}',
            'properties.group.id'                    = '{cfg.kafka_consumer_group}',
            'scan.startup.mode'                      = 'earliest-offset',
            'format'                                 = 'avro-confluent',
            'avro-confluent.schema-registry.url'     = '{cfg.schema_registry_url}',
            'avro-confluent.schema-registry.subject' = '{cfg.schema_registry_subject}'
        )
    """)


class FlightTimestampAssigner:
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return value[0]


def _is_q3_relevant(value) -> bool:
    _, airline, _, dep_delay, cancelled, diverted = value
    return (
        airline in AIRLINES
        and (cancelled or 0.0) < 0.5
        and (diverted or 0.0) < 0.5
        and dep_delay is not None
    )


def enabled_q3_windows(
    cfg: Q3Config,
) -> list[tuple[str, TumblingEventTimeWindows, str, str, str]]:
    windows = [
        # Spec Q3 windows: 1 day, 7 days, and global.
        ("1d", TumblingEventTimeWindows.of(Time.days(1)),
         cfg.results_path_1d, cfg.influx_topic_1d, cfg.timescale_table_1d),
        # Offset 6 days aligns weekly buckets to 2025-01-01.
        ("7d", TumblingEventTimeWindows.of(Time.days(7), Time.days(6)),
         cfg.results_path_7d, cfg.influx_topic_7d, cfg.timescale_table_7d),
        # Offset 14 days aligns the 365-day bucket to 2025-01-01.
        ("global", TumblingEventTimeWindows.of(Time.days(365), Time.days(14)),
         cfg.results_path_global, cfg.influx_topic_global, cfg.timescale_table_global),
    ]
    if cfg.window == "all":
        return windows
    return [window for window in windows if window[0] == cfg.window]


def _project_q3_event(value):
    return (value[0], value[1], hour_band(value[2]), float(value[3]))



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

    late_tag = OutputTag(f"q3-late-{label}", Types.PICKLED_BYTE_ARRAY())

    result_ds = (
        keyed_stream
        .window(window_assigner)
        .side_output_late_data(late_tag)
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


    # Il branch del side output termina su un sink blackhole aggiunto allo
    # StatementSet: senza un sink Table il ramo DataStream non verrebbe eseguito
    # da stmt_set.execute(). Il conteggio avviene come side-effect nella map.
    late_ds = (
        result_ds
        .get_side_output(late_tag)
        .map(Q3LateDropCounter(), output_type=Types.LONG())
        .name(f"Q3LateDrops[{label}]")
    )
    late_view = f"q3_late_{label}"
    t_env.create_temporary_view(late_view, t_env.from_data_stream(late_ds))
    blackhole = f"q3_blackhole_{label}"
    t_env.execute_sql(
        f"CREATE TABLE {blackhole} (c BIGINT) WITH ('connector' = 'blackhole')"
    )
    stmt_set.add_insert_sql(f"INSERT INTO {blackhole} SELECT * FROM {late_view}")
    logger.info("Q3 [%s] | late-drop counter → blackhole sink", label)

    view = f"q3_{label}"
    table = t_env.from_data_stream(
        result_ds,
        Schema.new_builder()
            .column("ts",          DataTypes.TIMESTAMP(3).bridged_to("java.sql.Timestamp"))
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
            SELECT ts, airline, CAST(`hour` AS STRING), num_flights,
                   delay_min, p25, p50, p75, p90, delay_max
            FROM {view}
        """)
        logger.info("Q3 [%s] | InfluxDB sink → Kafka topic '%s'", label, influx_topic)

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



def main() -> None:
    cfg = load_q3_config()

    logger.info("Q3 | Kafka: %s  topic: %s", cfg.kafka_bootstrap, cfg.kafka_topic)
    logger.info("Q3 | Consumer group: %s", cfg.kafka_consumer_group)
    logger.info("Q3 | Parallelism: %d", cfg.parallelism)
    logger.info("Q3 | Enabled window(s): %s", cfg.window)
    logger.info("Q3 | Watermark delay: %d s (event time)", cfg.watermark_delay_seconds)
    logger.info("Q3 | DDSketch alpha: %g (errore relativo max sui percentili)", cfg.sketch_alpha)

    env = create_stream_execution_environment(cfg)
    t_env = StreamTableEnvironment.create(env)

    register_q3_flights_source(t_env, cfg)
    logger.info(
        "Q3 | Source format: avro-confluent via Schema Registry %s subject %s",
        cfg.schema_registry_url,
        cfg.schema_registry_subject,
    )

    decoded_ds = t_env.to_data_stream(t_env.from_path("q3_flights_source"))

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(cfg.watermark_delay_seconds))
        .with_timestamp_assigner(FlightTimestampAssigner())
    )
    decoded_ds = decoded_ds.assign_timestamps_and_watermarks(watermark_strategy)

    flights_ds = (
        decoded_ds
        .filter(_is_q3_relevant)
        .map(
            _project_q3_event,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .name("Q3Events[python]")
    )

    keyed_stream = flights_ds.key_by(
        lambda v: group_key(v[1], v[2]),
        key_type=Types.STRING(),
    )


    windows = enabled_q3_windows(cfg)

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

    logger.info("Q3 | Submitting job (%d enabled window pipeline(s)) ...", len(windows))
    stmt_set.execute()
    logger.info("Q3 | Job submitted successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()
