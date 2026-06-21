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


def load_q1_config() -> Q1Config:
    cfg = load_config()
    flink_cfg = build_flink_runtime_config(cfg)

    return Q1Config(
        **asdict(flink_cfg),
        results_path=cfg["paths"]["q1_results_path"],
        watermark_delay_seconds=int(cfg["q1"]["watermark_delay_seconds"]),
    )

def main() -> None:
    q1_cfg = load_q1_config()

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
            WATERMARK FOR rowtime AS rowtime - INTERVAL '{q1_cfg.watermark_delay_seconds}' SECOND
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{q1_cfg.kafka_topic}',
            'properties.bootstrap.servers' = '{q1_cfg.kafka_bootstrap}',
            'properties.group.id'          = '{q1_cfg.kafka_consumer_group}',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json',
            'json.ignore-parse-errors'     = 'true'
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
            cancelled_count     BIGINT,
            diverted_count      BIGINT,
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

    # ── Q1 aggregation ───────────────────────────────────────────────────────
    # Watermark semantics:
    #   - COALESCE(cancelled, 0.0) treats missing values as "not cancelled"
    #   - AVG ignores NULL dep_delay rows automatically (standard SQL)
    #   - dep_delay > 15 is FALSE for NULL dep_delay (treated as not late)
    logger.info("Q1 | Submitting job …")
    t_env.execute_sql("""
        INSERT INTO q1_results
        SELECT
            window_start,
            window_end,
            airline,

            COUNT(*) AS num_flights,

            COUNT(*) FILTER (WHERE COALESCE(cancelled, 0.0) < 0.5
                             AND   COALESCE(diverted,  0.0) < 0.5) AS completed,

            COUNT(*) FILTER (WHERE COALESCE(cancelled, 0.0) >= 0.5) AS cancelled_count,

            COUNT(*) FILTER (WHERE COALESCE(diverted,  0.0) >= 0.5) AS diverted_count,

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
            TUMBLE(TABLE flights, DESCRIPTOR(rowtime), INTERVAL '1' HOUR)
        )
        WHERE airline IN ('AA', 'DL', 'UA', 'WN')
        GROUP BY window_start, window_end, airline
    """)

    logger.info("Q1 | Job submitted successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()