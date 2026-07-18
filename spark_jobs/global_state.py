from __future__ import annotations

import heapq
import pickle
from datetime import datetime, timezone
from typing import Iterator

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout

from common.delay_sketch import DelayDDSketch
from spark_runtime import not_cancelled, not_diverted


DATASET_START_MS = int(
    datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000
)
GLOBAL_SNAPSHOT_INTERVAL_MS = 86_400_000
EOS_AIRLINE = "__EOS__"
TOP_N_FLIGHTS = 20

Q2_GLOBAL_OUTPUT_SCHEMA = """
    ts string,
    origin_airport_id int,
    num_flights long,
    severe_delays long,
    dep_delay_mean double,
    dep_delay_max double,
    delayed_flights string
"""

Q3_GLOBAL_OUTPUT_SCHEMA = """
    ts string,
    airline string,
    hour int,
    num_flights long,
    delay_min double,
    p25 double,
    p50 double,
    p75 double,
    p90 double,
    delay_max double
"""

GLOBAL_STATE_SCHEMA = "payload binary, next_snapshot_ms long"


def _snapshot_ts(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(
        timestamp_ms / 1000.0,
        tz=timezone.utc,
    ).strftime("%Y-%m-%d %H:%M:%S")


def _bucket_end_ms(event_time_ms: int) -> int:
    if event_time_ms < DATASET_START_MS:
        return DATASET_START_MS
    elapsed = event_time_ms - DATASET_START_MS
    return (
        DATASET_START_MS
        + ((elapsed // GLOBAL_SNAPSHOT_INTERVAL_MS) + 1)
        * GLOBAL_SNAPSHOT_INTERVAL_MS
    )


def _raw_event_time(column: str = "event_time"):
    return F.timestamp_millis(F.col(column))


def _new_q2_stats() -> dict:
    return {
        "num_flights": 0,
        "delay_count": 0,
        "delay_sum": 0.0,
        "severe_delays": 0,
        "dep_delay_max": None,
        "top20": [],
    }


def _update_top20(
    heap: list[tuple[float, str, int]],
    dep_delay: float,
    airline: str | None,
    dest_airport_id: int | None,
) -> None:
    item = (
        float(dep_delay),
        airline or "",
        0 if dest_airport_id is None else int(dest_airport_id),
    )
    if len(heap) < TOP_N_FLIGHTS:
        heapq.heappush(heap, item)
    elif item > heap[0]:
        heapq.heapreplace(heap, item)


def _add_q2_flight(
    stats: dict,
    *,
    dep_delay: float | None,
    airline: str | None,
    dest_airport_id: int | None,
) -> None:
    stats["num_flights"] += 1
    if dep_delay is None:
        return

    delay = float(dep_delay)
    stats["delay_count"] += 1
    stats["delay_sum"] += delay
    stats["dep_delay_max"] = (
        delay
        if stats["dep_delay_max"] is None
        else max(stats["dep_delay_max"], delay)
    )
    if delay > 30.0:
        stats["severe_delays"] += 1
        _update_top20(stats["top20"], delay, airline, dest_airport_id)


def _merge_q2_stats(target: dict, source: dict) -> None:
    target["num_flights"] += source["num_flights"]
    target["delay_count"] += source["delay_count"]
    target["delay_sum"] += source["delay_sum"]
    target["severe_delays"] += source["severe_delays"]
    if source["dep_delay_max"] is not None:
        target["dep_delay_max"] = (
            source["dep_delay_max"]
            if target["dep_delay_max"] is None
            else max(target["dep_delay_max"], source["dep_delay_max"])
        )
    for delay, airline, destination in source["top20"]:
        _update_top20(target["top20"], delay, airline, destination)


def _format_top_delayed(flights: list[tuple[float, str, int]]) -> str:
    top = sorted(flights, key=lambda row: (-row[0], row[1], row[2]))
    return "[" + ",".join(
        f"({airline},{destination},{delay:.2f})"
        for delay, airline, destination in top
    ) + "]"


def _q2_state_function(
    key: tuple,
    pdf_iter: Iterator[pd.DataFrame],
    state,
) -> Iterator[pd.DataFrame]:
    airport_id = int(key[0])

    if state.exists:
        payload, next_snapshot_ms = state.get
        stored = pickle.loads(bytes(payload))
    else:
        stored = {"committed": _new_q2_stats(), "pending": {}}
        next_snapshot_ms = DATASET_START_MS + GLOBAL_SNAPSHOT_INTERVAL_MS

    committed = stored["committed"]
    pending = stored["pending"]

    for pdf in pdf_iter:
        for row in pdf.itertuples(index=False):
            if row.airline == EOS_AIRLINE or airport_id < 0:
                continue

            event_time_ms = int(row.event_time)
            bucket_end = _bucket_end_ms(event_time_ms)
            target = (
                committed
                if bucket_end < int(next_snapshot_ms)
                else pending.setdefault(bucket_end, _new_q2_stats())
            )

            dep_delay = None if pd.isna(row.dep_delay) else float(row.dep_delay)
            destination = (
                None
                if pd.isna(row.dest_airport_id)
                else int(row.dest_airport_id)
            )
            _add_q2_flight(
                target,
                dep_delay=dep_delay,
                airline=None if pd.isna(row.airline) else str(row.airline),
                dest_airport_id=destination,
            )

    output_rows = []
    current_watermark_ms = int(state.getCurrentWatermarkMs())
    while airport_id >= 0 and int(next_snapshot_ms) < current_watermark_ms:
        due_buckets = sorted(
            bucket_end
            for bucket_end in pending
            if bucket_end <= int(next_snapshot_ms)
        )
        for bucket_end in due_buckets:
            _merge_q2_stats(committed, pending.pop(bucket_end))

        if committed["num_flights"] >= 30:
            delay_count = int(committed["delay_count"])
            output_rows.append(
                {
                    "ts": _snapshot_ts(int(next_snapshot_ms)),
                    "origin_airport_id": airport_id,
                    "num_flights": int(committed["num_flights"]),
                    "severe_delays": int(committed["severe_delays"]),
                    "dep_delay_mean": (
                        float(committed["delay_sum"]) / delay_count
                        if delay_count
                        else None
                    ),
                    "dep_delay_max": (
                        float(committed["dep_delay_max"])
                        if committed["dep_delay_max"] is not None
                        else None
                    ),
                    "delayed_flights": _format_top_delayed(committed["top20"]),
                }
            )

        next_snapshot_ms = int(next_snapshot_ms) + GLOBAL_SNAPSHOT_INTERVAL_MS
    if airport_id >= 0:
        state.update((
            bytearray(pickle.dumps(stored, protocol=pickle.HIGHEST_PROTOCOL)),
            int(next_snapshot_ms),
        ))
        state.setTimeoutTimestamp(int(next_snapshot_ms))

    if output_rows:
        yield pd.DataFrame(output_rows)


def build_q2_global_snapshots(
    flights: DataFrame,
    *,
    watermark_delay_seconds: int,
) -> DataFrame:
    is_eos = F.col("airline") == F.lit(EOS_AIRLINE)
    is_real = (
        ~is_eos
        & not_cancelled()
        & not_diverted()
        & F.col("origin_airport_id").isNotNull()
    )

    relevant = (
        flights.withColumn("global_rowtime", _raw_event_time())
        .withWatermark("global_rowtime", f"{watermark_delay_seconds} seconds")
        .filter(is_real | is_eos)
        .withColumn(
            "_global_key",
            F.when(is_eos, F.lit(-1)).otherwise(F.col("origin_airport_id")),
        )
        .select(
            "_global_key",
            "global_rowtime",
            "event_time",
            "airline",
            "dest_airport_id",
            "dep_delay",
        )
    )

    return relevant.groupBy("_global_key").applyInPandasWithState(
        _q2_state_function,
        outputStructType=Q2_GLOBAL_OUTPUT_SCHEMA,
        stateStructType=GLOBAL_STATE_SCHEMA,
        outputMode="Append",
        timeoutConf=GroupStateTimeout.EventTimeTimeout,
    )


def _new_q3_state(alpha: float) -> dict:
    return {
        "committed": DelayDDSketch(alpha),
        "pending": {},
    }


def _q3_result(
    timestamp_ms: int,
    airline: str,
    hour: int,
    sketch: DelayDDSketch,
) -> dict:
    return {
        "ts": _snapshot_ts(timestamp_ms),
        "airline": airline,
        "hour": int(hour),
        "num_flights": int(sketch.count),
        "delay_min": float(sketch.min_value),
        "p25": round(sketch.quantile(0.25), 2),
        "p50": round(sketch.quantile(0.50), 2),
        "p75": round(sketch.quantile(0.75), 2),
        "p90": round(sketch.quantile(0.90), 2),
        "delay_max": float(sketch.max_value),
    }


def q3_state_function(alpha: float):
    def update(
        key: tuple,
        pdf_iter: Iterator[pd.DataFrame],
        state,
    ) -> Iterator[pd.DataFrame]:
        airline = str(key[0])
        hour = int(key[1])

        if state.exists:
            payload, next_snapshot_ms = state.get
            stored = pickle.loads(bytes(payload))
        else:
            stored = _new_q3_state(alpha)
            next_snapshot_ms = DATASET_START_MS + GLOBAL_SNAPSHOT_INTERVAL_MS

        committed: DelayDDSketch = stored["committed"]
        pending: dict[int, DelayDDSketch] = stored["pending"]

        for pdf in pdf_iter:
            for row in pdf.itertuples(index=False):
                if row.airline == EOS_AIRLINE or airline == EOS_AIRLINE:
                    continue

                event_time_ms = int(row.event_time)
                bucket_end = _bucket_end_ms(event_time_ms)
                if bucket_end < int(next_snapshot_ms):
                    target = committed
                else:
                    target = pending.setdefault(bucket_end, DelayDDSketch(alpha))
                target.add(float(row.dep_delay))

        output_rows = []
        current_watermark_ms = int(state.getCurrentWatermarkMs())
        while airline != EOS_AIRLINE and int(next_snapshot_ms) < current_watermark_ms:
            due_buckets = sorted(
                bucket_end
                for bucket_end in pending
                if bucket_end <= int(next_snapshot_ms)
            )
            for bucket_end in due_buckets:
                committed.merge(pending.pop(bucket_end))

            if committed.count > 0:
                output_rows.append(
                    _q3_result(
                        int(next_snapshot_ms),
                        airline,
                        hour,
                        committed,
                    )
                )

            next_snapshot_ms = int(next_snapshot_ms) + GLOBAL_SNAPSHOT_INTERVAL_MS

        if airline != EOS_AIRLINE:
            state.update((
                bytearray(pickle.dumps(stored, protocol=pickle.HIGHEST_PROTOCOL)),
                int(next_snapshot_ms),
            ))
            state.setTimeoutTimestamp(int(next_snapshot_ms))

        if output_rows:
            yield pd.DataFrame(output_rows)

    return update


def build_q3_global_snapshots(
    flights: DataFrame,
    *,
    watermark_delay_seconds: int,
    alpha: float,
) -> DataFrame:
    is_eos = F.col("airline") == F.lit(EOS_AIRLINE)
    is_real = (
        F.col("airline").isin("AA", "DL", "UA", "WN")
        & not_cancelled()
        & not_diverted()
        & F.col("dep_delay").isNotNull()
        & F.col("crs_dep_time").isNotNull()
    )

    relevant = (
        flights.withColumn("global_rowtime", _raw_event_time())
        .withWatermark("global_rowtime", f"{watermark_delay_seconds} seconds")
        .filter(is_real | is_eos)
        .withColumn(
            "hour",
            F.when(is_eos, F.lit(-1)).otherwise(
                F.pmod(
                    F.floor(F.col("crs_dep_time") / F.lit(100)),
                    F.lit(24),
                ).cast("int")
            ),
        )
        .select(
            "airline",
            "hour",
            "global_rowtime",
            "event_time",
            "dep_delay",
        )
    )

    return relevant.groupBy("airline", "hour").applyInPandasWithState(
        q3_state_function(alpha),
        outputStructType=Q3_GLOBAL_OUTPUT_SCHEMA,
        stateStructType=GLOBAL_STATE_SCHEMA,
        outputMode="Append",
        timeoutConf=GroupStateTimeout.EventTimeTimeout,
    )
