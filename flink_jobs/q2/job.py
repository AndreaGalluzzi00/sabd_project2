#!/usr/bin/env python3
from __future__ import annotations

import sys
from dataclasses import asdict, dataclass

from pyflink.common import Types
from pyflink.table import DataTypes, Schema

from common.config import load_config
from common.logging_utils import configure_logging
from cumulative_ops import Q2CumulativeTopN, Q2_CUMULATIVE_OUTPUT_TYPE
from flink_runtime import FlinkRuntimeConfig, build_flink_runtime_config, create_table_environment
from window_ops import (
    format_top_delayed,
    make_stats_view_sql,
    make_topn_sql,
    CSV_SINK_DDL,
    KAFKA_SINK_DDL,
    KAFKA_CUMULATIVE_SINK_DDL,
    JDBC_SINK_DDL,
)

import logging

configure_logging()
logger = logging.getLogger(__name__)

Q2_WINDOW_CHOICES = ("1h", "6h", "global", "cumulative", "all")



@dataclass(frozen=True)
class Q2Config(FlinkRuntimeConfig):
    results_path_1h: str
    results_path_6h: str
    results_path_global: str
    results_path_cumulative: str
    window: str
    watermark_delay_seconds: int
    cumulative_snapshot_event_step_ms: int

    influx_enabled: bool
    influx_topic_1h: str
    influx_topic_6h: str
    influx_topic_global: str
    influx_topic_cumulative: str

    timescale_enabled: bool
    timescale_url: str
    timescale_table_1h: str
    timescale_table_6h: str
    timescale_table_global: str
    timescale_table_cumulative: str
    timescale_username: str
    timescale_password: str


def selected_q2_window(value: object) -> str:
    window = str(value or "all").strip().lower()
    if window not in Q2_WINDOW_CHOICES:
        raise ValueError(
            "q2.window must be one of: "
            + ", ".join(Q2_WINDOW_CHOICES)
        )
    return window


def load_q2_config() -> Q2Config:
    cfg = load_config()
    flink_cfg = build_flink_runtime_config(cfg)
    q2_cfg = cfg["q2"]

    flink_dict = asdict(flink_cfg)
    flink_dict["kafka_consumer_group"] = cfg["flink"].get(
        "consumer_group_q2", cfg["flink"]["consumer_group"] + "-q2"
    )

    dashboard = cfg.get("dashboard", {})
    influx = dashboard.get("influx", {})
    timescale = dashboard.get("timescale", {})
    q2_influx = influx.get("q2", {})
    q2_timescale = timescale.get("q2", {})

    return Q2Config(
        **flink_dict,
        results_path_1h=cfg["paths"]["q2_results_path_1h"],
        results_path_6h=cfg["paths"]["q2_results_path_6h"],
        results_path_global=cfg["paths"]["q2_results_path_global"],
        results_path_cumulative=cfg["paths"]["q2_results_path_cumulative"],
        window=selected_q2_window(q2_cfg.get("window", "all")),
        watermark_delay_seconds=int(q2_cfg["watermark_delay_seconds"]),
        cumulative_snapshot_event_step_ms=int(
            q2_cfg.get("cumulative_snapshot_event_step_ms", 2_700_000)
        ),
        influx_enabled=bool(influx.get("enabled", False)),
        influx_topic_1h=str(q2_influx.get("topic_1h", "q2_results_1h")),
        influx_topic_6h=str(q2_influx.get("topic_6h", "q2_results_6h")),
        influx_topic_global=str(q2_influx.get("topic_global", "q2_results_global")),
        influx_topic_cumulative=str(q2_influx.get("topic_cumulative", "q2_results_cumulative")),
        timescale_enabled=bool(timescale.get("enabled", False)),
        timescale_url=str(timescale.get("url", "")),
        timescale_table_1h=str(q2_timescale.get("table_1h", "q2_results_1h")),
        timescale_table_6h=str(q2_timescale.get("table_6h", "q2_results_6h")),
        timescale_table_global=str(q2_timescale.get("table_global", "q2_results_global")),
        timescale_table_cumulative=str(q2_timescale.get("table_cumulative", "q2_results_cumulative")),
        timescale_username=str(timescale.get("username", "")),
        timescale_password=str(timescale.get("password", "")),
    )


def sql_watermark_interval(seconds: int) -> str:
    if seconds == 0:
        return "INTERVAL '0' SECOND"
    if seconds % 3600 == 0:
        return f"INTERVAL '{seconds // 3600}' HOUR"
    if seconds % 60 == 0:
        return f"INTERVAL '{seconds // 60}' MINUTE"
    return f"INTERVAL '{seconds}' SECOND"


def enabled_q2_windows(cfg: Q2Config) -> list[tuple[str, str, str, str, str]]:
    windows = [
        (
            "1h",
            "INTERVAL '1' HOUR",
            cfg.results_path_1h,
            cfg.influx_topic_1h,
            cfg.timescale_table_1h,
        ),
        (
            "6h",
            "INTERVAL '6' HOUR",
            cfg.results_path_6h,
            cfg.influx_topic_6h,
            cfg.timescale_table_6h,
        ),
        (
            "global",
            "INTERVAL '365' DAY(3)",
            cfg.results_path_global,
            cfg.influx_topic_global,
            cfg.timescale_table_global,
        ),
    ]
    if cfg.window == "all":
        return windows
    if cfg.window == "cumulative":
        return []
    return [window for window in windows if window[0] == cfg.window]


def cumulative_q2_enabled(cfg: Q2Config) -> bool:
    return cfg.window in {"all", "cumulative"}


def _project_q2_event(value):
    return (
        value[0],
        value[1],
        value[2],
        value[3],
        value[4],
        value[5],
        value[6],
    )


def _register_q2_result_view(t_env, view: str, result_ds) -> None:
    table = t_env.from_data_stream(
        result_ds,
        Schema.new_builder()
            .column("ts", DataTypes.TIMESTAMP(3).bridged_to("java.sql.Timestamp"))
            .column("airport_rank", DataTypes.BIGINT())
            .column("origin_airport_id", DataTypes.INT())
            .column("num_flights", DataTypes.BIGINT())
            .column("severe_delays", DataTypes.BIGINT())
            .column("dep_delay_mean", DataTypes.DOUBLE())
            .column("dep_delay_max", DataTypes.DOUBLE())
            .column("delayed_flights", DataTypes.STRING())
            .build(),
    )
    t_env.create_temporary_view(view, table)


def build_cumulative_pipeline(
    t_env,
    stmt_set,
    cfg: Q2Config,
) -> None:
    label = "cumulative"

    result_ds = (
        t_env.to_data_stream(t_env.from_path("flights"))
        .map(_project_q2_event, output_type=Types.PICKLED_BYTE_ARRAY())
        .key_by(lambda _: "q2-cumulative", key_type=Types.STRING())
        .process(
            Q2CumulativeTopN(cfg.cumulative_snapshot_event_step_ms),
            output_type=Q2_CUMULATIVE_OUTPUT_TYPE,
        )
        .name("Q2CumulativeTopN")
    )

    view = "q2_cumulative"
    _register_q2_result_view(t_env, view, result_ds)

    csv_sink = f"q2_csv_{label}"
    t_env.execute_sql(CSV_SINK_DDL.format(name=csv_sink, path=cfg.results_path_cumulative))
    stmt_set.add_insert_sql(f"INSERT INTO {csv_sink} SELECT * FROM {view}")
    logger.info("Q2 [%s] | CSV sink -> %s", label, cfg.results_path_cumulative)

    if cfg.influx_enabled:
        kafka_sink = f"q2_kafka_{label}"
        t_env.execute_sql(KAFKA_CUMULATIVE_SINK_DDL.format(
            name=kafka_sink,
            topic=cfg.influx_topic_cumulative,
            bootstrap=cfg.kafka_bootstrap,
        ))

        stmt_set.add_insert_sql(f"""
            INSERT INTO {kafka_sink}
            SELECT ts,
                   CAST(airport_rank AS STRING)      AS airport_rank,
                   origin_airport_id,
                   num_flights, severe_delays, dep_delay_mean, dep_delay_max,
                   delayed_flights
            FROM {view}
        """)
        logger.info("Q2 [%s] | InfluxDB sink -> Kafka topic '%s'", label, cfg.influx_topic_cumulative)

    if cfg.timescale_enabled:
        jdbc_sink = f"q2_jdbc_{label}"
        t_env.execute_sql(JDBC_SINK_DDL.format(
            name=jdbc_sink,
            url=cfg.timescale_url,
            table=cfg.timescale_table_cumulative,
            username=cfg.timescale_username,
            password=cfg.timescale_password,
        ))
        stmt_set.add_insert_sql(f"INSERT INTO {jdbc_sink} SELECT * FROM {view}")
        logger.info("Q2 [%s] | TimescaleDB sink -> table '%s'", label, cfg.timescale_table_cumulative)



def build_window_pipeline(
    label: str,
    window_sql: str,
    results_path: str,
    influx_topic: str,
    timescale_table: str,
    t_env,
    stmt_set,
    cfg: Q2Config,
) -> None:


    stats_view = f"q2_stats_{label}"
    t_env.execute_sql(f"""
        CREATE TEMPORARY VIEW {stats_view} AS
        {make_stats_view_sql(window_sql)}
    """)

    ranked_sql = make_topn_sql(stats_view)

    # ── CSV sink (always active) ───────────────────────────────────────────────
    csv_sink = f"q2_csv_{label}"
    t_env.execute_sql(CSV_SINK_DDL.format(name=csv_sink, path=results_path))
    stmt_set.add_insert_sql(f"INSERT INTO {csv_sink} {ranked_sql}")
    logger.info("Q2 [%s] | CSV sink → %s", label, results_path)

    if cfg.influx_enabled:
        kafka_sink = f"q2_kafka_{label}"
        t_env.execute_sql(KAFKA_SINK_DDL.format(
            name=kafka_sink,
            topic=influx_topic,
            bootstrap=cfg.kafka_bootstrap,
        ))

        stmt_set.add_insert_sql(f"""
            INSERT INTO {kafka_sink}
            SELECT ts,
                   CAST(airport_rank AS STRING)      AS airport_rank,
                   CAST(origin_airport_id AS STRING) AS origin_airport_id,
                   num_flights, severe_delays, dep_delay_mean, dep_delay_max,
                   delayed_flights
            FROM ({ranked_sql})
        """)
        logger.info("Q2 [%s] | InfluxDB sink → Kafka topic '%s'", label, influx_topic)

    if cfg.timescale_enabled:
        jdbc_sink = f"q2_jdbc_{label}"
        t_env.execute_sql(JDBC_SINK_DDL.format(
            name=jdbc_sink,
            url=cfg.timescale_url,
            table=timescale_table,
            username=cfg.timescale_username,
            password=cfg.timescale_password,
        ))
        stmt_set.add_insert_sql(f"INSERT INTO {jdbc_sink} {ranked_sql}")
        logger.info("Q2 [%s] | TimescaleDB sink → table '%s'", label, timescale_table)




def main() -> None:
    cfg = load_q2_config()
    watermark_interval = sql_watermark_interval(cfg.watermark_delay_seconds)
    t_env = create_table_environment(cfg)

    logger.info("Q2 | Kafka: %s  topic: %s", cfg.kafka_bootstrap, cfg.kafka_topic)
    logger.info("Q2 | Consumer group: %s", cfg.kafka_consumer_group)
    logger.info("Q2 | Enabled window(s): %s", cfg.window)
    logger.info("Q2 | Watermark delay: %d s (event time)", cfg.watermark_delay_seconds)


    t_env.create_temporary_function("format_top_delayed", format_top_delayed)


    t_env.execute_sql(f"""
        CREATE TABLE flights (
            event_time        BIGINT,
            airline           STRING,
            origin_airport_id INT,
            dest_airport_id   INT,
            dep_delay         DOUBLE,
            cancelled         DOUBLE,
            diverted          DOUBLE,
            rowtime           AS TO_TIMESTAMP_LTZ(event_time, 3),
            WATERMARK FOR rowtime AS rowtime - {watermark_interval}
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{cfg.kafka_topic}',
            'properties.bootstrap.servers' = '{cfg.kafka_bootstrap}',
            'properties.group.id'          = '{cfg.kafka_consumer_group}',
            'scan.startup.mode'                        = 'earliest-offset',
            'format'                                   = 'avro-confluent',
            'avro-confluent.schema-registry.url'       = '{cfg.schema_registry_url}',
            'avro-confluent.schema-registry.subject'   = '{cfg.schema_registry_subject}'
        )
    """)

    # ── Pre-filter: non-cancelled, non-diverted only ───────────────────────────
    # Shared across all three window pipelines. NULL treated as 0 (not
    # cancelled/diverted), consistent with Q1 and base.yml null policy.
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW completed_flights AS
        SELECT *
        FROM flights
        WHERE COALESCE(cancelled, 0.0) < 0.5
          AND COALESCE(diverted,  0.0) < 0.5
    """)

    # ── Build one pipeline per window size ────────────────────────────────────
    stmt_set = t_env.create_statement_set()

    windows = enabled_q2_windows(cfg)

    for label, window_sql, results_path, influx_topic, timescale_table in windows:
        build_window_pipeline(
            label=label,
            window_sql=window_sql,
            results_path=results_path,
            influx_topic=influx_topic,
            timescale_table=timescale_table,
            t_env=t_env,
            stmt_set=stmt_set,
            cfg=cfg,
        )

    if cumulative_q2_enabled(cfg):
        build_cumulative_pipeline(
            t_env=t_env,
            stmt_set=stmt_set,
            cfg=cfg,
        )

    # ── Submit ────────────────────────────────────────────────────────────────
    pipeline_count = len(windows) + (1 if cumulative_q2_enabled(cfg) else 0)
    logger.info("Q2 | Submitting job (%d enabled pipeline(s)) ...", pipeline_count)
    stmt_set.execute()
    logger.info("Q2 | Job submitted successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()
