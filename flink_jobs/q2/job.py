#!/usr/bin/env python3
"""
Q2 – Real-time ranking of departure airports with significant delays.

Two-stage pipeline per window size (1h, 6h, 365-day global):

  Stage 1 (SQL / Table API):
    TUMBLE aggregation → per-airport stats (num_flights, severe_delays,
    dep_delay_mean, dep_delay_max, delayed_flights).
    Airports with < 30 completed flights are excluded (HAVING).

  Stage 2 (DataStream API):
    All airports for a completed window arrive as a burst; a
    ProcessAllWindowFunction sorts them and emits the top-10 ranked rows.
    Output is append-only → compatible with the filesystem CSV sink.

The same ranked DataStream feeds optional dashboard sinks (Kafka → InfluxDB,
JDBC → TimescaleDB) using the same pattern as Q1.

Output schema (per window type, one file each):
    ts, airport_rank, origin_airport_id, num_flights, severe_delays,
    dep_delay_mean, dep_delay_max, delayed_flights

The merge script (merge_q2.py) deduplicates part-files and writes the
final CSVs with the correct spec header (rank instead of airport_rank).
"""
from __future__ import annotations

import sys
from dataclasses import asdict, dataclass

from pyflink.common import WatermarkStrategy
from pyflink.common.time import Time
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import DataTypes, Schema

from common.config import load_config
from common.logging_utils import configure_logging
from flink_runtime import FlinkRuntimeConfig, build_flink_runtime_config, create_table_environment
from window_ops import (
    Top10AllWindowFunction,
    TOP10_OUTPUT_TYPE,
    STATS_VIEW_TYPE,
    WindowEndTimestampAssigner,
    make_stats_view_sql,
    make_top_delayed_udaf,
    CSV_SINK_DDL,
    KAFKA_SINK_DDL,
    JDBC_SINK_DDL,
)

import logging

configure_logging()
logger = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Q2Config(FlinkRuntimeConfig):
    results_path_1h: str
    results_path_6h: str
    results_path_global: str
    watermark_delay_seconds: int

    influx_enabled: bool
    influx_topic_1h: str
    influx_topic_6h: str
    influx_topic_global: str

    timescale_enabled: bool
    timescale_url: str
    timescale_table_1h: str
    timescale_table_6h: str
    timescale_table_global: str
    timescale_username: str
    timescale_password: str


def load_q2_config() -> Q2Config:
    cfg = load_config()
    flink_cfg = build_flink_runtime_config(cfg)

    # Q2 uses its own consumer group so it can run concurrently with Q1.
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
        watermark_delay_seconds=int(cfg["q2"]["watermark_delay_seconds"]),
        influx_enabled=bool(influx.get("enabled", False)),
        influx_topic_1h=str(q2_influx.get("topic_1h", "q2_results_1h")),
        influx_topic_6h=str(q2_influx.get("topic_6h", "q2_results_6h")),
        influx_topic_global=str(q2_influx.get("topic_global", "q2_results_global")),
        timescale_enabled=bool(timescale.get("enabled", False)),
        timescale_url=str(timescale.get("url", "")),
        timescale_table_1h=str(q2_timescale.get("table_1h", "q2_results_1h")),
        timescale_table_6h=str(q2_timescale.get("table_6h", "q2_results_6h")),
        timescale_table_global=str(q2_timescale.get("table_global", "q2_results_global")),
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


# ── Per-window pipeline ───────────────────────────────────────────────────────

def build_window_pipeline(
    label: str,
    window_sql: str,
    window_time: Time,
    results_path: str,
    influx_topic: str,
    timescale_table: str,
    t_env,
    stmt_set,
    cfg: Q2Config,
) -> None:
    """
    Wires Stage 1 (SQL TUMBLE) → Stage 2 (DataStream top-10) → sinks for one
    window size. All three window sizes share the same completed_flights view.

    Stage 2 uses windowAll(TumblingEventTimeWindows) with the same interval as
    Stage 1. Because all airports from a given Stage-1 window carry event_time =
    window_end, they all land in the same Stage-2 window bucket and are processed
    together by Top10AllWindowFunction. Output is append-only (no retracts).
    """

    # ── Stage 1: SQL aggregation ──────────────────────────────────────────────
    stats_view = f"q2_stats_{label}"
    t_env.execute_sql(f"""
        CREATE TEMPORARY VIEW {stats_view} AS
        {make_stats_view_sql(window_sql)}
    """)

    # ── Stage 2: DataStream top-10 ranking ────────────────────────────────────
    # The Python UDAF (top_delayed) causes Flink to plan Stage 1 as
    # PythonGroupAggregate (retract mode), so neither to_data_stream() nor
    # to_append_stream() is accepted by the planner. to_retract_stream()
    # handles retract semantics and returns (bool, Row) tuples. TUMBLE windows
    # are append-only in practice (one emit per bucket, no retracts), so we
    # filter for True records and unwrap the Row.
    retract_ds = t_env.to_retract_stream(t_env.from_path(stats_view), STATS_VIEW_TYPE)
    stats_ds = (
        retract_ds
        .filter(lambda t: t[0])                             # inserts only
        .map(lambda t: t[1], output_type=STATS_VIEW_TYPE)  # unwrap Row
    )

    # Re-assign timestamps from window_end (index 1) and emit monotonous
    # watermarks. The SQL pipeline already processes windows in order, so
    # for_monotonous_timestamps() is correct and tight.
    stats_ds = stats_ds.assign_timestamps_and_watermarks(
        WatermarkStrategy
            .for_monotonous_timestamps()
            .with_timestamp_assigner(WindowEndTimestampAssigner())
    )

    ranked_ds = (
        stats_ds
        .window_all(TumblingEventTimeWindows.of(window_time))
        .process(Top10AllWindowFunction(), TOP10_OUTPUT_TYPE)
    )

    # ── Convert ranked DataStream back to Table ────────────────────────────────
    ranked_view = f"q2_ranked_{label}"
    ranked_table = t_env.from_data_stream(
        ranked_ds,
        Schema.new_builder()
            .column("ts",                DataTypes.TIMESTAMP(3))   # SQL_TIMESTAMP → java.sql.Timestamp → TIMESTAMP(3)
            .column("airport_rank",      DataTypes.BIGINT())
            .column("origin_airport_id", DataTypes.INT())
            .column("num_flights",       DataTypes.BIGINT())
            .column("severe_delays",     DataTypes.BIGINT())
            .column("dep_delay_mean",    DataTypes.DOUBLE())
            .column("dep_delay_max",     DataTypes.DOUBLE())
            .column("delayed_flights",   DataTypes.STRING())
            .build()
    )
    t_env.create_temporary_view(ranked_view, ranked_table)

    # ── CSV sink (always active) ───────────────────────────────────────────────
    csv_sink = f"q2_csv_{label}"
    t_env.execute_sql(CSV_SINK_DDL.format(name=csv_sink, path=results_path))
    stmt_set.add_insert_sql(f"INSERT INTO {csv_sink} SELECT * FROM {ranked_view}")
    logger.info("Q2 [%s] | CSV sink → %s", label, results_path)

    # ── InfluxDB sink (optional) ───────────────────────────────────────────────
    if cfg.influx_enabled:
        kafka_sink = f"q2_kafka_{label}"
        t_env.execute_sql(KAFKA_SINK_DDL.format(
            name=kafka_sink,
            topic=influx_topic,
            bootstrap=cfg.kafka_bootstrap,
        ))
        stmt_set.add_insert_sql(f"INSERT INTO {kafka_sink} SELECT * FROM {ranked_view}")
        logger.info("Q2 [%s] | InfluxDB sink → Kafka topic '%s'", label, influx_topic)

    # ── TimescaleDB sink (optional) ────────────────────────────────────────────
    if cfg.timescale_enabled:
        jdbc_sink = f"q2_jdbc_{label}"
        t_env.execute_sql(JDBC_SINK_DDL.format(
            name=jdbc_sink,
            url=cfg.timescale_url,
            table=timescale_table,
            username=cfg.timescale_username,
            password=cfg.timescale_password,
        ))
        stmt_set.add_insert_sql(f"INSERT INTO {jdbc_sink} SELECT * FROM {ranked_view}")
        logger.info("Q2 [%s] | TimescaleDB sink → table '%s'", label, timescale_table)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = load_q2_config()
    watermark_interval = sql_watermark_interval(cfg.watermark_delay_seconds)
    t_env = create_table_environment(cfg)

    logger.info("Q2 | Kafka: %s  topic: %s", cfg.kafka_bootstrap, cfg.kafka_topic)
    logger.info("Q2 | Consumer group: %s", cfg.kafka_consumer_group)
    logger.info("Q2 | Watermark delay: %d s (event time)", cfg.watermark_delay_seconds)

    # ── Register UDAF ─────────────────────────────────────────────────────────
    t_env.create_temporary_function("top_delayed", make_top_delayed_udaf())

    # ── Kafka source ──────────────────────────────────────────────────────────
    # Q2 needs more fields than Q1: airline and dest_airport_id for the
    # delayed_flights list; origin_airport_id as the grouping key.
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
            'avro-confluent.schema-registry.url'       = 'http://schema-registry:8081',
            'avro-confluent.schema-registry.subject'   = 'flights-value'
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

    windows = [
        # (label,    SQL interval,          DataStream Time,  results path,              influx topic,           timescale table)
        ("1h",     "INTERVAL '1' HOUR",   Time.hours(1),    cfg.results_path_1h,     cfg.influx_topic_1h,     cfg.timescale_table_1h),
        ("6h",     "INTERVAL '6' HOUR",   Time.hours(6),    cfg.results_path_6h,     cfg.influx_topic_6h,     cfg.timescale_table_6h),
        # 365-day window: the entire Jan–Apr 2025 dataset fits in one bucket.
        # The window fires once after the EOS marker. ts = Flink's epoch-aligned
        # window_start (late 2024); document this alignment in the report.
        # DAY(3) precision required: default DAY(2) only allows up to 99 days.
        ("global", "INTERVAL '365' DAY(3)",  Time.days(365),   cfg.results_path_global, cfg.influx_topic_global, cfg.timescale_table_global),
    ]

    for label, window_sql, window_time, results_path, influx_topic, timescale_table in windows:
        build_window_pipeline(
            label=label,
            window_sql=window_sql,
            window_time=window_time,
            results_path=results_path,
            influx_topic=influx_topic,
            timescale_table=timescale_table,
            t_env=t_env,
            stmt_set=stmt_set,
            cfg=cfg,
        )

    # ── Submit ────────────────────────────────────────────────────────────────
    logger.info("Q2 | Submitting job (3 window pipelines) …")
    stmt_set.execute()
    logger.info("Q2 | Job submitted successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()
