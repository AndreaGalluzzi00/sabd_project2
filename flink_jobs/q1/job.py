#!/usr/bin/env python3
"""
Q1 – Real-time airline operational status monitoring.

Tumbling 1-hour event-time windows over the 'flights' Kafka topic.
Filters AA, DL, UA, WN and computes per-window, per-airline statistics.

Output schema:
    window_start, window_end, airline, num_flights, completed, cancelled,
    diverted, dep_delay_mean, cancellation_rate, late_departure_rate
"""
import argparse
import logging
import os
import sys

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "flights")
RESULTS_PATH    = os.getenv("RESULTS_PATH", "/opt/flink/results/q1")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Q1 Flink job")
    parser.add_argument(
        "--watermark-delay",
        type=int,
        default=int(os.getenv("WATERMARK_DELAY_SECONDS", "30")),
        metavar="SECONDS",
        help=(
            "Bounded out-of-orderness watermark delay in event-time seconds. "
            "Config A (strict): 30  — Config B (permissive): 40. "
            "Falls back to WATERMARK_DELAY_SECONDS env var (default: 30)."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args  = parse_args()
    watermark_delay = args.watermark_delay

    env   = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.enable_checkpointing(10_000)  # ogni 10s → finalizza i file CSV sul sink
    t_env = StreamTableEnvironment.create(env)

    logger.info("Q1 | Kafka: %s  topic: %s", KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    logger.info("Q1 | Results path: %s", RESULTS_PATH)
    logger.info("Q1 | Watermark delay: %d s (event time)", watermark_delay)

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
            WATERMARK FOR rowtime AS rowtime - INTERVAL '{watermark_delay}' SECOND
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id'          = 'flink-q1',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json',
            'json.ignore-parse-errors'     = 'true'
        )
    """)

    # ── Sink: CSV files under /opt/flink/results/q1/ ────────────────────────
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
            'path'                                   = '{RESULTS_PATH}',
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
    sys.exit(0)  # forza la chiusura del gateway py4j


if __name__ == "__main__":
    main()
