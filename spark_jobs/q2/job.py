#!/usr/bin/env python3
from __future__ import annotations

import logging
import heapq
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common.config import load_config
from common.logging_utils import configure_logging
from spark_runtime import (
    await_queries_until_idle,
    build_spark_runtime_config,
    checkpoint_path,
    create_spark_session,
    format_ts,
    not_cancelled,
    not_diverted,
    read_flights_stream,
    write_foreach_batch_stream,
)

Q2_WINDOW_CHOICES = ("1h", "6h", "global", "cumulative", "all")
DATASET_START_TS = "2025-01-01 00:00:00"
DATASET_START_MS = int(
    datetime.strptime(DATASET_START_TS, "%Y-%m-%d %H:%M:%S")
    .replace(tzinfo=timezone.utc)
    .timestamp()
    * 1000
)
TOP_N_FLIGHTS = 20


def _format_event_time_ms(event_time_ms: int | None) -> str:
    if event_time_ms is None:
        return DATASET_START_TS
    return datetime.fromtimestamp(
        event_time_ms / 1000.0,
        tz=timezone.utc,
    ).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _next_emit_after(event_time_ms: int, emit_interval_ms: int) -> int:
    if event_time_ms < DATASET_START_MS:
        return DATASET_START_MS
    elapsed = event_time_ms - DATASET_START_MS
    return DATASET_START_MS + ((elapsed // emit_interval_ms) + 1) * emit_interval_ms


def selected_q2_window(value: object) -> str:
    window = str(value or "all").strip().lower()
    if window not in Q2_WINDOW_CHOICES:
        raise ValueError(
            "q2.window must be one of: "
            + ", ".join(Q2_WINDOW_CHOICES)
        )
    return window


def enabled_q2_windows(
    selected_window: str,
    paths: dict,
) -> list[tuple[str, str, str | None, str]]:
    windows = [
        # Spec Q2 windows: 1 hour, 6 hours, and global.
        ("1h", "1 hour", None, paths["spark_q2_results_path_1h"]),
        ("6h", "6 hours", None, paths["spark_q2_results_path_6h"]),
        # Start offset 14 days aligns the 365-day bucket to 2025-01-01.
        ("global", "365 days", "14 days", paths["spark_q2_results_path_global"]),
    ]
    if selected_window == "all":
        return windows
    if selected_window == "cumulative":
        return []
    return [window for window in windows if window[0] == selected_window]


def cumulative_q2_enabled(selected_window: str) -> bool:
    return selected_window in {"all", "cumulative"}


def _new_airport_state() -> dict:
    return {
        "num_flights": 0,
        "mean_count": 0,
        "mean": 0.0,
        "severe_delays": 0,
        "dep_delay_max": None,
        "top20": [],
    }


def _update_top20(
    heap: list[tuple[float, str, int]],
    dep_delay: float,
    airline: str,
    dest_airport_id: int | None,
) -> None:
    item = (dep_delay, airline or "", 0 if dest_airport_id is None else int(dest_airport_id))
    if len(heap) < TOP_N_FLIGHTS:
        heapq.heappush(heap, item)
    elif item > heap[0]:
        heapq.heapreplace(heap, item)


def _format_top_delayed(flights: list[tuple[float, str, int]]) -> str:
    top = sorted(flights, key=lambda row: (-row[0], row[1], row[2]))
    return "[" + ",".join(
        f"({carrier},{dest},{delay:.2f})"
        for delay, carrier, dest in top
    ) + "]"


def cumulative_batch_writer(output_path: str, emit_event_interval_ms: int):
    airport_states: dict[int, dict] = {}
    max_event_time_ms: int | None = None
    next_emit_event_time_ms = DATASET_START_MS + max(1, int(emit_event_interval_ms))
    last_snapshot_event_time_ms: int | None = None
    dirty_since_snapshot = False

    def snapshot_rows() -> list[tuple]:
        snapshot_ts = _format_event_time_ms(max_event_time_ms)
        ranked = [
            (airport_id, state)
            for airport_id, state in airport_states.items()
            if state["num_flights"] >= 30
        ]
        ranked.sort(
            key=lambda item: (
                -item[1]["severe_delays"],
                -item[1]["mean"],
                item[0],
            )
        )
        return [
            (
                snapshot_ts,
                rank,
                int(airport_id),
                int(state["num_flights"]),
                int(state["severe_delays"]),
                float(state["mean"]),
                float(state["dep_delay_max"] or 0.0),
                _format_top_delayed(state["top20"]),
            )
            for rank, (airport_id, state) in enumerate(ranked[:10], start=1)
        ]

    def write(batch_df: DataFrame, batch_id: int) -> None:
        nonlocal dirty_since_snapshot
        nonlocal last_snapshot_event_time_ms
        nonlocal max_event_time_ms
        nonlocal next_emit_event_time_ms

        saw_eos = False
        output_rows = []

        rows = batch_df.select(
            "event_time",
            "airline",
            "origin_airport_id",
            "dest_airport_id",
            "dep_delay",
            "cancelled",
            "diverted",
        ).orderBy(
            F.col("event_time").asc_nulls_last()
        ).toLocalIterator()

        for row in rows:
            if row.airline == "__EOS__":
                saw_eos = True
                continue

            if row.event_time is not None:
                event_time = int(row.event_time)
                if max_event_time_ms is None or event_time > max_event_time_ms:
                    max_event_time_ms = event_time

            cancelled = row.cancelled or 0.0
            diverted = row.diverted or 0.0
            if (
                cancelled >= 0.5
                or diverted >= 0.5
                or row.dep_delay is None
                or row.origin_airport_id is None
            ):
                if (
                    dirty_since_snapshot
                    and max_event_time_ms is not None
                    and max_event_time_ms >= next_emit_event_time_ms
                ):
                    rows_to_emit = snapshot_rows()
                    if rows_to_emit:
                        output_rows.extend(rows_to_emit)
                        last_snapshot_event_time_ms = max_event_time_ms
                        dirty_since_snapshot = False
                    next_emit_event_time_ms = _next_emit_after(
                        max_event_time_ms,
                        emit_event_interval_ms,
                    )
                continue

            airport_id = int(row.origin_airport_id)
            dep_delay = float(row.dep_delay)
            state = airport_states.setdefault(airport_id, _new_airport_state())

            state["num_flights"] += 1
            state["mean_count"] += 1
            state["mean"] += (dep_delay - state["mean"]) / state["mean_count"]
            state["dep_delay_max"] = (
                dep_delay
                if state["dep_delay_max"] is None
                else max(state["dep_delay_max"], dep_delay)
            )
            if dep_delay > 30.0:
                state["severe_delays"] += 1
                _update_top20(
                    state["top20"],
                    dep_delay,
                    row.airline,
                    row.dest_airport_id,
                )
            dirty_since_snapshot = True

            if max_event_time_ms is not None and max_event_time_ms >= next_emit_event_time_ms:
                rows_to_emit = snapshot_rows()
                if rows_to_emit:
                    output_rows.extend(rows_to_emit)
                    last_snapshot_event_time_ms = max_event_time_ms
                    dirty_since_snapshot = False
                next_emit_event_time_ms = _next_emit_after(
                    max_event_time_ms,
                    emit_event_interval_ms,
                )

        if saw_eos and (
            dirty_since_snapshot
            or max_event_time_ms != last_snapshot_event_time_ms
        ):
            rows_to_emit = snapshot_rows()
            if rows_to_emit:
                output_rows.extend(rows_to_emit)
                last_snapshot_event_time_ms = max_event_time_ms
            dirty_since_snapshot = False

        if not output_rows:
            return

        out = batch_df.sparkSession.createDataFrame(
            output_rows,
            schema=[
                "ts",
                "rank",
                "origin_airport_id",
                "num_flights",
                "severe_delays",
                "dep_delay_mean",
                "dep_delay_max",
                "delayed_flights",
            ],
        )
        out.coalesce(1).write.mode("append").option("header", "false").csv(output_path)

    return write


def build_stats(
    flights: DataFrame,
    *,
    window_duration: str,
    start_time: str | None = None,
) -> DataFrame:
    is_eos = F.col("airline") == "__EOS__"
    is_real = (not_cancelled() & not_diverted()) & ~is_eos
    completed = flights.filter(is_real | is_eos)
    severe = is_real & (F.col("dep_delay") > F.lit(30.0))

    if start_time is None:
        window_col = F.window("rowtime", window_duration)
    else:
        window_col = F.window("rowtime", window_duration, window_duration, start_time)

    delayed_flight = F.when(
        severe,
        F.struct(
            (-F.col("dep_delay")).alias("sort_delay"),
            F.col("airline").alias("airline"),
            F.coalesce(F.col("dest_airport_id"), F.lit(0)).alias("dest_airport_id"),
            F.col("dep_delay").alias("dep_delay"),
        ),
    )

    stats = (
        completed.groupBy(window_col.alias("w"), F.col("origin_airport_id"))
        .agg(
            F.sum(F.when(is_real, 1).otherwise(0)).cast("long").alias("num_flights"),
            F.sum(F.when(severe, 1).otherwise(0)).cast("long").alias("severe_delays"),
            F.avg(F.when(is_real, F.col("dep_delay"))).alias("dep_delay_mean"),
            F.max(F.when(is_real, F.col("dep_delay"))).alias("dep_delay_max"),
            F.collect_list(delayed_flight).alias("_delayed_raw"),
        )
        .filter(F.col("num_flights") >= 30)
        .withColumn(
            "delayed_flights",
            F.expr(
                """
                concat(
                  '[',
                  array_join(
                    transform(
                      slice(sort_array(filter(_delayed_raw, x -> x is not null)), 1, 20),
                      x -> concat(
                        '(',
                        x.airline,
                        ',',
                        cast(x.dest_airport_id as string),
                        ',',
                        format_string('%.2f', x.dep_delay),
                        ')'
                      )
                    ),
                    ','
                  ),
                  ']'
                )
                """
            ),
        )
    )

    return stats.select(
        format_ts(F.col("w.start")).alias("ts"),
        "origin_airport_id",
        "num_flights",
        "severe_delays",
        "dep_delay_mean",
        "dep_delay_max",
        "delayed_flights",
    )


def ranked_batch_writer(output_path: str):
    def write(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        rank_window = Window.partitionBy("ts").orderBy(
            F.desc("severe_delays"),
            F.desc("dep_delay_mean"),
            F.asc("origin_airport_id"),
        )

        ranked = (
            batch_df.withColumn("rank", F.row_number().over(rank_window))
            .filter(F.col("rank") <= 10)
            .select(
                "ts",
                "rank",
                "origin_airport_id",
                "num_flights",
                "severe_delays",
                "dep_delay_mean",
                "dep_delay_max",
                "delayed_flights",
            )
        )

        ranked.coalesce(1).write.mode("append").option("header", "false").csv(output_path)

    return write


def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)

    cfg = load_config()
    runtime_cfg = build_spark_runtime_config(cfg)
    spark = create_spark_session("SparkStructuredQ2", runtime_cfg)

    paths = cfg["paths"]
    q2_cfg = cfg["q2"]
    selected_window = selected_q2_window(q2_cfg.get("window", "all"))
    watermark_delay = int(q2_cfg["watermark_delay_seconds"])

    logger.info("Spark Q2 | Kafka: %s topic: %s", runtime_cfg.kafka_bootstrap, runtime_cfg.kafka_topic)
    logger.info("Spark Q2 | Enabled window(s): %s", selected_window)

    flights = (
        read_flights_stream(spark, runtime_cfg, "q2")
        .withWatermark("rowtime", f"{watermark_delay} seconds")
    )

    windows = enabled_q2_windows(selected_window, paths)

    queries = []
    for label, duration, start_time, output_path in windows:
        stats = build_stats(
            flights,
            window_duration=duration,
            start_time=start_time,
        )
        queries.append(
            write_foreach_batch_stream(
                stats,
                writer=ranked_batch_writer(output_path),
                checkpoint=checkpoint_path(runtime_cfg, "q2", label),
                query_name=f"spark-q2-{label}",
                runtime_cfg=runtime_cfg,
            )
        )
        logger.info("Spark Q2 [%s] | Results path: %s", label, output_path)

    if cumulative_q2_enabled(selected_window):
        output_path = paths["spark_q2_results_path_cumulative"]
        emit_event_interval_ms = int(q2_cfg.get("cumulative_emit_event_interval_ms", 2_700_000))
        queries.append(
            write_foreach_batch_stream(
                flights,
                writer=cumulative_batch_writer(output_path, emit_event_interval_ms),
                checkpoint=checkpoint_path(runtime_cfg, "q2", "cumulative"),
                query_name="spark-q2-cumulative",
                runtime_cfg=runtime_cfg,
            )
        )
        logger.info("Spark Q2 [cumulative] | Results path: %s", output_path)

    await_queries_until_idle(
        queries, runtime_cfg, logger,
        query_label="q2",
        experiment=str(cfg.get("experiment", {}).get("name", "base")),
    )
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.getLogger(__name__).exception("Spark Q2 failed")
        sys.exit(1)
