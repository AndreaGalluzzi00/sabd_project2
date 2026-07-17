from __future__ import annotations

import heapq
from datetime import datetime, timezone

from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import Trigger, TriggerResult


DATASET_START_TS = datetime(2025, 1, 1)
DATASET_START_MS = int(DATASET_START_TS.replace(tzinfo=timezone.utc).timestamp() * 1000)
TOP_N_AIRPORTS = 10
TOP_N_FLIGHTS = 20
EOS_AIRLINE = "__EOS__"


# ── Filtro e proiezione dell'evento ──────────────────────────────────────────
# Row sorgente `flights`: (event_time, airline, origin_airport_id,
#                          dest_airport_id, dep_delay, cancelled, diverted)

def is_q2_global_event(value) -> bool:
    """Voli non cancellati/deviati con aeroporto di partenza noto (marker escluso).

    NON richiede dep_delay non nullo: come nelle finestre SQL 1h/6h, il volo
    conta comunque in num_flights; il dep_delay nullo e' solo escluso da
    media/max/severe.
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
    """event_time (ms) come timestamp del record: guida GlobalWindow trigger e
    watermark. Applicato PRIMA del filtro del marker, cosi' il watermark
    raggiunge 2200 e fa scattare lo snapshot finale (stesso schema di Q3)."""

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


def _snapshot_ts(max_event_time_ms):
    if max_event_time_ms is None:
        return DATASET_START_TS
    return datetime.fromtimestamp(
        max_event_time_ms / 1000.0, tz=timezone.utc
    ).replace(tzinfo=None)


def _ranked_rows(airports: dict, max_event_time_ms):
    ts = _snapshot_ts(max_event_time_ms)

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


# ── Trigger continuo event-time (equivalente nativo mancante in PyFlink) ──────

class ContinuousEventTimeTrigger(Trigger):
    """Fa scattare la finestra a ogni confine event-time (FIRE, senza purge).

    Registra il prossimo confine SOLO in on_element: cosi' un salto di watermark
    (marker a 2200) non genera una catena infinita di firing, ma solo l'ultimo
    confine registrato dagli eventi reali.
    """

    def __init__(self, interval_ms: int, align_start_ms: int):
        self._interval = max(1, int(interval_ms))
        self._start = int(align_start_ms)
        self._next_fire = ValueStateDescriptor("q2-global-next-fire", Types.LONG())

    def _boundary_after(self, value_ms: int) -> int:
        # Primo confine (allineato a _start) STRETTAMENTE maggiore di value_ms.
        return self._start + ((value_ms - self._start) // self._interval + 1) * self._interval

    def on_element(self, element, timestamp, window, ctx):
        state = ctx.get_partitioned_state(self._next_fire)
        if state.value() is None:
            watermark = ctx.get_current_watermark()
            base = timestamp if timestamp > watermark else watermark
            next_fire = self._boundary_after(base)
            ctx.register_event_time_timer(next_fire)
            state.update(next_fire)
        return TriggerResult.CONTINUE

    def on_event_time(self, time, window, ctx):
        state = ctx.get_partitioned_state(self._next_fire)
        pending = state.value()
        if pending is not None and time == pending:
            state.clear()  # il prossimo confine verra' registrato dal prossimo elemento
            return TriggerResult.FIRE
        return TriggerResult.CONTINUE

    def on_processing_time(self, time, window, ctx):
        return TriggerResult.CONTINUE

    def clear(self, window, ctx):
        state = ctx.get_partitioned_state(self._next_fire)
        pending = state.value()
        if pending is not None:
            ctx.delete_event_time_timer(pending)
        state.clear()

    def on_merge(self, window, ctx):
        # GlobalWindows non e' un merging assigner: mai chiamato.
        pass


# ── Aggregazione incrementale sull'intera GlobalWindow ────────────────────────

class Q2GlobalAggregate(AggregateFunction):
    """Accumulatore = {airports: {id -> stato}, max_event_time}. Bounded: uno
    stato per aeroporto, indipendente dal numero di voli."""

    def create_accumulator(self):
        return {"airports": {}, "max_event_time": None}

    def add(self, value, acc):
        # value: (event_time, origin_airport_id, dest_airport_id, dep_delay, airline)
        event_time = value[0]
        if event_time is not None:
            event_time = int(event_time)
            if acc["max_event_time"] is None or event_time > acc["max_event_time"]:
                acc["max_event_time"] = event_time

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
        if other["max_event_time"] is not None:
            acc["max_event_time"] = (
                other["max_event_time"] if acc["max_event_time"] is None
                else max(acc["max_event_time"], other["max_event_time"])
            )
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
    """Su ogni firing del trigger emette la top-10 corrente dall'inizio dataset."""

    def process(self, key, context, elements):
        acc = next(iter(elements))
        yield from _ranked_rows(acc["airports"], acc["max_event_time"])


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
