#!/usr/bin/env python3
"""
Q1 – Real-time airline operational status monitoring.

Tumbling 1-hour event-time windows over the 'flights' Kafka topic.
Filters AA, DL, UA, WN and computes per-window, per-airline statistics.

Output schema:
    window_start, window_end, airline, num_flights, completed, cancelled,
    diverted, dep_delay_mean, cancellation_rate, late_departure_rate
"""
from __future__ import annotations

import sys
from dataclasses import asdict, dataclass

from common.config import load_config
from flink_runtime import (
    FlinkRuntimeConfig,
    build_flink_runtime_config,
    create_table_environment,
)
import logging

from common.logging_utils import configure_logging


configure_logging()
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Q1Config(FlinkRuntimeConfig):
    results_path: str
    watermark_delay_seconds: int

    # Optional live dashboard sinks (Grafana). Two independent backends:
    #   - InfluxDB via Kafka -> Telegraf
    #   - TimescaleDB via native Flink JDBC sink
    # Both disabled by default so the certified CSV pipeline runs unchanged.
    influx_enabled: bool
    influx_results_topic: str

    timescale_enabled: bool
    timescale_url: str
    timescale_table: str
    timescale_username: str
    timescale_password: str


def load_q1_config() -> Q1Config:
    cfg = load_config()
    flink_cfg = build_flink_runtime_config(cfg)
    dashboard_cfg = cfg.get("dashboard", {})
    influx_cfg = dashboard_cfg.get("influx", {})
    timescale_cfg = dashboard_cfg.get("timescale", {})

    return Q1Config(
        **asdict(flink_cfg),
        results_path=cfg["paths"]["q1_results_path"],
        watermark_delay_seconds=int(cfg["q1"]["watermark_delay_seconds"]),
        influx_enabled=bool(influx_cfg.get("enabled", False)),
        influx_results_topic=str(influx_cfg.get("results_topic", "q1_results")),
        timescale_enabled=bool(timescale_cfg.get("enabled", False)),
        timescale_url=str(timescale_cfg.get("url", "")),
        timescale_table=str(timescale_cfg.get("table", "q1_results")),
        timescale_username=str(timescale_cfg.get("username", "")),
        timescale_password=str(timescale_cfg.get("password", "")),
    )

def sql_watermark_interval(seconds: int) -> str:
    if seconds < 0:
        raise ValueError("watermark_delay_seconds must be >= 0")

    if seconds == 0:
        return "INTERVAL '0' SECOND"

    if seconds % 3600 == 0:
        hours = seconds // 3600
        return f"INTERVAL '{hours}' HOUR"

    if seconds % 60 == 0:
        minutes = seconds // 60
        return f"INTERVAL '{minutes}' MINUTE"

    return f"INTERVAL '{seconds}' SECOND"

def main() -> None:
    q1_cfg = load_q1_config()
    watermark_interval = sql_watermark_interval(q1_cfg.watermark_delay_seconds)
    t_env = create_table_environment(q1_cfg)

    logger.info(
        "Q1 | Kafka: %s topic: %s",
        q1_cfg.kafka_bootstrap,
        q1_cfg.kafka_topic,
    )
    logger.info("Q1 | Consumer group: %s", q1_cfg.kafka_consumer_group)
    logger.info("Q1 | Results path: %s", q1_cfg.results_path)
    logger.info("Q1 | Parallelism: %d", q1_cfg.parallelism)
    logger.info("Q1 | Checkpoint interval: %d ms", q1_cfg.checkpoint_interval_ms)
    logger.info(
        "Q1 | Watermark delay: %d s (event time)",
        q1_cfg.watermark_delay_seconds,
    )

    # ── Source: Kafka 'flights' topic ────────────────────────────────────────
    # Only the fields needed for Q1 are declared; unknown JSON keys are ignored.
    t_env.execute_sql(f"""
        CREATE TABLE flights (
            event_time  BIGINT,
            airline     STRING,
            dep_delay   DOUBLE,
            cancelled   DOUBLE,
            diverted    DOUBLE,
            rowtime     AS TO_TIMESTAMP_LTZ(event_time, 3),
            WATERMARK FOR rowtime AS rowtime - {watermark_interval}
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{q1_cfg.kafka_topic}',
            'properties.bootstrap.servers' = '{q1_cfg.kafka_bootstrap}',
            'properties.group.id'          = '{q1_cfg.kafka_consumer_group}',
            'scan.startup.mode'                        = 'earliest-offset',
            'format'                                   = 'avro-confluent',
            'avro-confluent.schema-registry.url'       = 'http://schema-registry:8081',
            'avro-confluent.schema-registry.subject'   = 'flights-value'
        )
    """)

    # ── Sink: CSV files under configured result path ─────────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE q1_results (
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
            'path'                                   = '{q1_cfg.results_path}',
            'format'                                 = 'csv',
            'sink.rolling-policy.rollover-interval'  = '10 s',
            'sink.rolling-policy.check-interval'     = '5 s'
        )
    """)

    # ── Q1 aggregation (single source of truth for every sink) ───────────────
    # Filter the target airlines before windowing so Flink does not assign
    # events from irrelevant carriers to tumbling windows only to discard them.
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW flights_q1 AS
        SELECT * FROM flights
        WHERE airline IN ('AA', 'DL', 'UA', 'WN')
    """)

    # Computed once as a view. The CSV sink (certified output) and the optional
    # Kafka sink (dashboard) both read identical rows from it, so the live
    # dashboard can never diverge from the delivered CSV.
    # Watermark semantics:
    #   - COALESCE(cancelled, 0.0) treats missing values as "not cancelled"
    #   - AVG ignores NULL dep_delay rows automatically (standard SQL)
    #   - dep_delay > 15 is FALSE for NULL dep_delay (treated as not late)
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW q1_agg AS
        SELECT
            window_start,
            window_end,
            airline,

            COUNT(*) AS num_flights,

            COUNT(*) FILTER (WHERE COALESCE(cancelled, 0.0) < 0.5
                             AND   COALESCE(diverted,  0.0) < 0.5) AS completed,

            COUNT(*) FILTER (WHERE COALESCE(cancelled, 0.0) >= 0.5) AS cancelled,

            COUNT(*) FILTER (WHERE COALESCE(diverted,  0.0) >= 0.5) AS diverted,

            -- Mean dep_delay of non-cancelled flights (AVG skips NULLs)
            AVG(dep_delay) FILTER (WHERE COALESCE(cancelled, 0.0) < 0.5) AS dep_delay_mean,

            -- % cancelled over total flights in the window
            CAST(COUNT(*) FILTER (WHERE COALESCE(cancelled, 0.0) >= 0.5) AS DOUBLE)
                * 100.0 / COUNT(*) AS cancellation_rate,

            -- % non-cancelled flights with dep_delay > 15 min
            CAST(COUNT(*) FILTER (WHERE COALESCE(cancelled, 0.0) < 0.5
                                  AND   dep_delay > 15) AS DOUBLE)
                * 100.0
                / NULLIF(COUNT(*) FILTER (WHERE COALESCE(cancelled, 0.0) < 0.5), 0)
                AS late_departure_rate

        FROM TABLE(
            TUMBLE(TABLE flights_q1, DESCRIPTOR(rowtime), INTERVAL '1' HOUR)
        )
        GROUP BY window_start, window_end, airline
    """)

    # ── Optional dashboard sink #1: InfluxDB via Kafka topic ─────────────────
    # Append-only (windowing TVF output), so the plain 'kafka' connector applies.
    # Telegraf consumes this topic and ships it to InfluxDB. Timestamps use the
    # SQL standard ('2025-01-01 08:00:00'); window bounds fall on whole seconds.
    if q1_cfg.influx_enabled:
        logger.info(
            "Q1 | InfluxDB sink ENABLED → Kafka topic '%s'",
            q1_cfg.influx_results_topic,
        )
        t_env.execute_sql(f"""
            CREATE TABLE q1_results_kafka (
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
                'connector'                     = 'kafka',
                'topic'                         = '{q1_cfg.influx_results_topic}',
                'properties.bootstrap.servers'  = '{q1_cfg.kafka_bootstrap}',
                'format'                        = 'json',
                'json.timestamp-format.standard' = 'SQL'
            )
        """)

    # ── Optional dashboard sink #2: TimescaleDB via native JDBC ──────────────
    # PRIMARY KEY (window_start, airline) NOT ENFORCED → the JDBC connector runs
    # in upsert mode (INSERT ... ON CONFLICT DO UPDATE), so re-running the job is
    # idempotent. The target table must already exist (see dashboard/timescaledb).
    if q1_cfg.timescale_enabled:
        logger.info(
            "Q1 | TimescaleDB sink ENABLED → %s (table '%s')",
            q1_cfg.timescale_url,
            q1_cfg.timescale_table,
        )
        t_env.execute_sql(f"""
            CREATE TABLE q1_results_jdbc (
                window_start        TIMESTAMP(3),
                window_end          TIMESTAMP(3),
                airline             STRING,
                num_flights         BIGINT,
                completed           BIGINT,
                cancelled           BIGINT,
                diverted            BIGINT,
                dep_delay_mean      DOUBLE,
                cancellation_rate   DOUBLE,
                late_departure_rate DOUBLE,
                PRIMARY KEY (window_start, airline) NOT ENFORCED
            ) WITH (
                'connector'  = 'jdbc',
                'url'        = '{q1_cfg.timescale_url}',
                'table-name' = '{q1_cfg.timescale_table}',
                'username'   = '{q1_cfg.timescale_username}',
                'password'   = '{q1_cfg.timescale_password}'
            )
        """)

    if not (q1_cfg.influx_enabled or q1_cfg.timescale_enabled):
        logger.info("Q1 | Dashboard sinks disabled (CSV-only run).")

    # ── Submit: CSV sink always, dashboard sinks only when enabled ───────────
    # One StatementSet → one Flink job: the Kafka source is read once and the
    # aggregation fans out to every sink (keeps the stop --drain / EOS workflow).
    logger.info("Q1 | Submitting job …")
    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql("INSERT INTO q1_results SELECT * FROM q1_agg")
    if q1_cfg.influx_enabled:
        stmt_set.add_insert_sql("INSERT INTO q1_results_kafka SELECT * FROM q1_agg")
    if q1_cfg.timescale_enabled:
        stmt_set.add_insert_sql("INSERT INTO q1_results_jdbc SELECT * FROM q1_agg")
    stmt_set.execute()

    logger.info("Q1 | Job submitted successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()