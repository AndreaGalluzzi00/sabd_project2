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

import logging
import sys
from dataclasses import dataclass

from common.config import load_config
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Q1RuntimeConfig:
    kafka_bootstrap: str
    kafka_topic: str
    kafka_consumer_group: str

    results_path: str

    parallelism: int
    checkpoint_interval_ms: int
    watermark_delay_seconds: int


def load_q1_runtime_config() -> Q1RuntimeConfig:
    cfg = load_config()

    return Q1RuntimeConfig(
        kafka_bootstrap=cfg["kafka"]["bootstrap_servers"],
        kafka_topic=cfg["kafka"]["topic"],
        kafka_consumer_group=cfg["flink"]["consumer_group"],

        results_path=cfg["paths"]["q1_results_path"],

        parallelism=int(cfg["flink"]["parallelism"]),
        checkpoint_interval_ms=int(cfg["flink"]["checkpoint_interval_ms"]),
        watermark_delay_seconds=int(cfg["q1"]["watermark_delay_seconds"]),
    )


def main() -> None:
    runtime_cfg = load_q1_runtime_config()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(runtime_cfg.parallelism)
    env.enable_checkpointing(runtime_cfg.checkpoint_interval_ms)

    t_env = StreamTableEnvironment.create(env)

    logger.info(
        "Q1 | Kafka: %s topic: %s",
        runtime_cfg.kafka_bootstrap,
        runtime_cfg.kafka_topic,
    )
    logger.info("Q1 | Consumer group: %s", runtime_cfg.kafka_consumer_group)
    logger.info("Q1 | Results path: %s", runtime_cfg.results_path)
    logger.info("Q1 | Parallelism: %d", runtime_cfg.parallelism)
    logger.info("Q1 | Checkpoint interval: %d ms", runtime_cfg.checkpoint_interval_ms)
    logger.info(
        "Q1 | Watermark delay: %d s (event time)",
        runtime_cfg.watermark_delay_seconds,
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
            WATERMARK FOR rowtime AS rowtime - INTERVAL '{runtime_cfg.watermark_delay_seconds}' SECOND
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{runtime_cfg.kafka_topic}',
            'properties.bootstrap.servers' = '{runtime_cfg.kafka_bootstrap}',
            'properties.group.id'          = '{runtime_cfg.kafka_consumer_group}',
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
            'path'                                   = '{runtime_cfg.results_path}',
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