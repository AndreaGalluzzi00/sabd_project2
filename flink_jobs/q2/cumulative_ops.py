from __future__ import annotations

import heapq
from datetime import datetime

from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.functions import KeyedProcessFunction


DATASET_START_TS = datetime(2025, 1, 1)
TOP_N_AIRPORTS = 10
TOP_N_FLIGHTS = 20


def _is_eos(value) -> bool:
    return value[1] == "__EOS__"


def _is_completed(value) -> bool:
    cancelled = value[5] or 0.0
    diverted = value[6] or 0.0
    return cancelled < 0.5 and diverted < 0.5


def _format_top_delayed(flights: list[tuple[float, str, int]]) -> str:
    if not flights:
        return "[]"

    top = sorted(flights, key=lambda row: (-row[0], row[1], row[2]))
    return "[" + ",".join(
        f"({carrier},{dest},{delay:.2f})"
        for delay, carrier, dest in top
    ) + "]"


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


def _snapshot_rows(airports: dict[int, dict]):
    ranked = []
    for airport_id, state in airports.items():
        if state["num_flights"] < 30:
            continue
        ranked.append((airport_id, state))

    ranked.sort(
        key=lambda item: (
            -item[1]["severe_delays"],
            -item[1]["mean"],
            item[0],
        )
    )

    for rank, (airport_id, state) in enumerate(ranked[:TOP_N_AIRPORTS], start=1):
        yield Row(
            DATASET_START_TS,
            rank,
            int(airport_id),
            int(state["num_flights"]),
            int(state["severe_delays"]),
            float(state["mean"]),
            float(state["dep_delay_max"] or 0.0),
            _format_top_delayed(state["top20"]),
        )


class Q2CumulativeTopN(KeyedProcessFunction):
    """Running Q2 state from dataset start, emitted as throttled snapshots."""

    def __init__(self, emit_interval_ms: int):
        self._emit_interval_ms = emit_interval_ms
        self._airports_state = None
        self._timer_state = None
        self._dirty_state = None

    def open(self, runtime_context) -> None:
        self._airports_state = runtime_context.get_state(
            ValueStateDescriptor("q2-cumulative-airports", Types.PICKLED_BYTE_ARRAY())
        )
        self._timer_state = runtime_context.get_state(
            ValueStateDescriptor("q2-cumulative-next-timer", Types.LONG())
        )
        self._dirty_state = runtime_context.get_state(
            ValueStateDescriptor("q2-cumulative-dirty", Types.BOOLEAN())
        )

    def process_element(self, value, ctx):
        airports = self._airports_state.value() or {}

        if _is_eos(value):
            yield from _snapshot_rows(airports)
            self._dirty_state.update(False)
            return

        if not _is_completed(value) or value[4] is None or value[2] is None:
            return

        airline = value[1]
        airport_id = int(value[2])
        dest_airport_id = value[3]
        dep_delay = float(value[4])

        state = airports.get(airport_id)
        if state is None:
            state = _new_airport_state()
            airports[airport_id] = state

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
            _update_top20(state["top20"], dep_delay, airline, dest_airport_id)

        self._airports_state.update(airports)
        self._dirty_state.update(True)
        self._ensure_timer(ctx)

    def on_timer(self, timestamp: int, ctx):
        self._timer_state.clear()

        if not self._dirty_state.value():
            return

        airports = self._airports_state.value() or {}
        yield from _snapshot_rows(airports)
        self._dirty_state.update(False)

    def _ensure_timer(self, ctx) -> None:
        if self._timer_state.value() is not None:
            return

        now = ctx.timer_service().current_processing_time()
        next_timer = now + self._emit_interval_ms
        self._timer_state.update(next_timer)
        ctx.timer_service().register_processing_time_timer(next_timer)


Q2_CUMULATIVE_OUTPUT_TYPE = Types.ROW_NAMED(
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
