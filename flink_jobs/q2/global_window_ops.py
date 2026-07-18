from __future__ import annotations

import heapq
from datetime import datetime, timezone

from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction


DATASET_START_TS = datetime(2025, 1, 1)
DATASET_START_MS = int(DATASET_START_TS.replace(tzinfo=timezone.utc).timestamp() * 1000)
GLOBAL_SNAPSHOT_INTERVAL_MS = 86_400_000
TOP_N_AIRPORTS = 10
TOP_N_FLIGHTS = 20
EOS_AIRLINE = "__EOS__"


# ── Filtro e proiezione dell'evento ──────────────────────────────────────────
# Row sorgente `flights`: (event_time, airline, origin_airport_id,
#                          dest_airport_id, dep_delay, cancelled, diverted)

def is_q2_global_event(value) -> bool:
    """Keep completed flights with a known origin; exclude the EOS marker.

    Null departure delays still contribute to ``num_flights``, consistently
    with the SQL implementation of the 1h and 6h windows.
    """

    airline = value[1]
    cancelled = value[5] or 0.0
    diverted = value[6] or 0.0
    return (
        airline != EOS_AIRLINE
        and cancelled < 0.5
        and diverted < 0.5
        and value[2] is not None
    )


def project_q2_global_event(value):
    # (event_time, origin_airport_id, dest_airport_id, dep_delay, airline)
    return (value[0], value[2], value[3], value[4], value[1])


class Q2GlobalTimestampAssigner:
    """Use the raw event timestamp, including the bounded EOS timestamp."""

    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return value[0]


# ── Stato per-aeroporto e ranking (stessa logica delle finestre 1h/6h) ────────

def _new_airport_state() -> dict:
    return {
        "num_flights": 0,   # voli completati (incl. dep_delay nullo)
        "mean_count": 0,    # voli con dep_delay non nullo
        "mean": 0.0,
        "severe_delays": 0,
        "dep_delay_max": None,
        "top20": [],
    }


def _update_top20(heap, dep_delay: float, airline: str, dest_airport_id) -> None:
    item = (dep_delay, airline or "", 0 if dest_airport_id is None else int(dest_airport_id))
    if len(heap) < TOP_N_FLIGHTS:
        heapq.heappush(heap, item)
    elif item > heap[0]:
        heapq.heapreplace(heap, item)


def _format_top_delayed(flights) -> str:
    if not flights:
        return "[]"
    top = sorted(flights, key=lambda row: (-row[0], row[1], row[2]))
    return "[" + ",".join(
        f"({carrier},{dest},{delay:.2f})" for delay, carrier, dest in top
    ) + "]"


def _snapshot_ts(current_watermark_ms: int) -> datetime:
    if current_watermark_ms < DATASET_START_MS:
        return DATASET_START_TS

    snapshot_ms = (
        DATASET_START_MS
        + ((current_watermark_ms - DATASET_START_MS) // GLOBAL_SNAPSHOT_INTERVAL_MS)
        * GLOBAL_SNAPSHOT_INTERVAL_MS
    )
    return datetime.fromtimestamp(
        snapshot_ms / 1000.0, tz=timezone.utc
    ).replace(tzinfo=None)


def _ranked_rows(airports: dict, current_watermark_ms: int):
    ts = _snapshot_ts(current_watermark_ms)

    ranked = [
        (airport_id, state)
        for airport_id, state in airports.items()
        if state["num_flights"] >= 30
    ]
    ranked.sort(
        key=lambda item: (
            -item[1]["severe_delays"],
            -item[1]["mean"],
            item[0],
        )
    )

    for rank, (airport_id, state) in enumerate(ranked[:TOP_N_AIRPORTS], start=1):
        yield Row(
            ts,
            rank,
            int(airport_id),
            int(state["num_flights"]),
            int(state["severe_delays"]),
            float(state["mean"]),
            float(state["dep_delay_max"] or 0.0),
            _format_top_delayed(state["top20"]),
        )


# The native ContinuousEventTimeTrigger is configured in job.py.

# ── Aggregazione incrementale sull'intera GlobalWindow ────────────────────────

class Q2GlobalAggregate(AggregateFunction):
    """Bounded accumulator: one state entry per origin airport."""

    def create_accumulator(self):
        return {"airports": {}}

    def add(self, value, acc):
        # value: (event_time, origin_airport_id, dest_airport_id, dep_delay, airline)
        airport_id = int(value[1])
        state = acc["airports"].get(airport_id)
        if state is None:
            state = _new_airport_state()
            acc["airports"][airport_id] = state

        state["num_flights"] += 1

        dep_delay = value[3]
        if dep_delay is not None:
            dep_delay = float(dep_delay)
            state["mean_count"] += 1
            state["mean"] += (dep_delay - state["mean"]) / state["mean_count"]
            state["dep_delay_max"] = (
                dep_delay if state["dep_delay_max"] is None
                else max(state["dep_delay_max"], dep_delay)
            )
            if dep_delay > 30.0:
                state["severe_delays"] += 1
                _update_top20(state["top20"], dep_delay, value[4], value[2])

        return acc

    def get_result(self, acc):
        return acc

    def merge(self, acc, other):
        # GlobalWindows non fa merge di finestre; definito per completezza.
        for airport_id, o in other["airports"].items():
            s = acc["airports"].get(airport_id)
            if s is None:
                acc["airports"][airport_id] = o
                continue
            total = s["mean_count"] + o["mean_count"]
            if total > 0:
                s["mean"] = (s["mean"] * s["mean_count"] + o["mean"] * o["mean_count"]) / total
            s["mean_count"] = total
            s["num_flights"] += o["num_flights"]
            s["severe_delays"] += o["severe_delays"]
            if o["dep_delay_max"] is not None:
                s["dep_delay_max"] = (
                    o["dep_delay_max"] if s["dep_delay_max"] is None
                    else max(s["dep_delay_max"], o["dep_delay_max"])
                )
            for delay, carrier, dest in o["top20"]:
                _update_top20(s["top20"], delay, carrier, dest)
        return acc


class Q2GlobalWindowFunction(ProcessWindowFunction):
    """Emit the cumulative top-10 on every daily event-time firing."""

    def process(self, key, context, elements):
        acc = next(iter(elements))
        yield from _ranked_rows(acc["airports"], context.current_watermark())


Q2_GLOBAL_OUTPUT_TYPE = Types.ROW_NAMED(
    [
        "ts",
        "airport_rank",
        "origin_airport_id",
        "num_flights",
        "severe_delays",
        "dep_delay_mean",
        "dep_delay_max",
        "delayed_flights",
    ],
    [
        Types.SQL_TIMESTAMP(),
        Types.LONG(),
        Types.INT(),
        Types.LONG(),
        Types.LONG(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.STRING(),
    ],
)
