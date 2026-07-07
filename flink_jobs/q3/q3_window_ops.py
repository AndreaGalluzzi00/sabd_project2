
from __future__ import annotations

from datetime import datetime, timezone

from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import (
    AggregateFunction,
    KeyedProcessFunction,
    MapFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.state import ValueStateDescriptor

from delay_sketch import DelayDDSketch


LATE_DROPS_METRIC = "numLateRecordsDropped"
DATASET_START_TS = datetime(2025, 1, 1)
DATASET_START_MS = int(DATASET_START_TS.replace(tzinfo=timezone.utc).timestamp() * 1000)
AIRLINES: frozenset[str] = frozenset({"AA", "DL", "UA", "WN"})


class Q3LateDropCounter(MapFunction):

    def __init__(self) -> None:
        self._counter = None

    def open(self, runtime_context) -> None:
        self._counter = runtime_context.get_metrics_group().counter(LATE_DROPS_METRIC)

    def map(self, value):
        self._counter.inc()
        return 1


def group_key(airline: str, hour: int) -> str:
    return f"{airline}|{hour:02d}"


def _snapshot_ts(max_event_time_ms: int | None) -> datetime:
    if max_event_time_ms is None:
        return DATASET_START_TS
    return datetime.fromtimestamp(
        max_event_time_ms / 1000.0,
        tz=timezone.utc,
    ).replace(tzinfo=None)


def _next_emit_after(event_time_ms: int, snapshot_event_step_ms: int) -> int:
    if event_time_ms < DATASET_START_MS:
        return DATASET_START_MS
    elapsed = event_time_ms - DATASET_START_MS
    return DATASET_START_MS + ((elapsed // snapshot_event_step_ms) + 1) * snapshot_event_step_ms



class Q3AggregateFunction(AggregateFunction):

    def __init__(self, alpha: float):
        self._alpha = alpha

    def create_accumulator(self) -> DelayDDSketch:
        return DelayDDSketch(self._alpha)

    def add(self, value, acc: DelayDDSketch) -> DelayDDSketch:
        # value: (event_time_ms, airline, hour, dep_delay)
        acc.add(value[3])
        return acc

    def get_result(self, acc: DelayDDSketch) -> DelayDDSketch:
        return acc

    def merge(self, acc: DelayDDSketch, other: DelayDDSketch) -> DelayDDSketch:
        acc.merge(other)
        return acc


class Q3WindowFunction(ProcessWindowFunction):

    def process(self, key: str, context: ProcessWindowFunction.Context, elements):
        sketch = next(iter(elements))
        airline, hour_str = key.split("|")

        ts = datetime.fromtimestamp(
            context.window().start / 1000.0, tz=timezone.utc
        ).replace(tzinfo=None)  # naive UTC → java.sql.Timestamp → TIMESTAMP(3)

        yield Row(
            ts,
            airline,
            int(hour_str),
            sketch.count,
            sketch.min_value,
            round(sketch.quantile(0.25), 2),
            round(sketch.quantile(0.50), 2),
            round(sketch.quantile(0.75), 2),
            round(sketch.quantile(0.90), 2),
            sketch.max_value,
        )


class Q3CumulativeFunction(KeyedProcessFunction):
    """Running Q3 state from dataset start, emitted on event-time steps."""

    def __init__(self, alpha: float, snapshot_event_step_ms: int):
        self._alpha = alpha
        self._snapshot_event_step_ms = max(1, int(snapshot_event_step_ms))
        self._sketches_state = None
        self._max_event_time_state = None
        self._next_emit_event_time_state = None
        self._last_snapshot_event_time_state = None
        self._dirty_state = None

    def open(self, runtime_context) -> None:
        self._sketches_state = runtime_context.get_state(
            ValueStateDescriptor("q3-cumulative-sketches", Types.PICKLED_BYTE_ARRAY())
        )
        self._max_event_time_state = runtime_context.get_state(
            ValueStateDescriptor("q3-cumulative-max-event-time", Types.LONG())
        )
        self._next_emit_event_time_state = runtime_context.get_state(
            ValueStateDescriptor("q3-cumulative-next-emit-event-time", Types.LONG())
        )
        self._last_snapshot_event_time_state = runtime_context.get_state(
            ValueStateDescriptor("q3-cumulative-last-snapshot-event-time", Types.LONG())
        )
        self._dirty_state = runtime_context.get_state(
            ValueStateDescriptor("q3-cumulative-dirty", Types.BOOLEAN())
        )

    def process_element(self, value, ctx):
        # value: (event_time_ms, airline, hour, dep_delay, cancelled, diverted)
        sketches = self._sketches_state.value() or {}

        if value[1] == "__EOS__":
            max_event_time_ms = self._max_event_time_state.value()
            if (
                self._dirty_state.value()
                or max_event_time_ms != self._last_snapshot_event_time_state.value()
            ):
                rows = list(self._snapshot_rows(sketches, max_event_time_ms))
                for row in rows:
                    yield row
                if rows and max_event_time_ms is not None:
                    self._last_snapshot_event_time_state.update(max_event_time_ms)
            self._dirty_state.update(False)
            return

        max_event_time_ms = self._max_event_time_state.value()
        event_time = value[0]
        if event_time is not None:
            event_time = int(event_time)
            if max_event_time_ms is None or event_time > max_event_time_ms:
                max_event_time_ms = event_time
                self._max_event_time_state.update(event_time)

        if not self._is_relevant(value):
            yield from self._maybe_emit_snapshot(sketches, max_event_time_ms)
            return

        airline = value[1]
        hour = int(value[2])
        key = (airline, hour)
        sketch = sketches.get(key)
        if sketch is None:
            sketch = DelayDDSketch(self._alpha)
            sketches[key] = sketch

        sketch.add(float(value[3]))
        self._sketches_state.update(sketches)
        self._dirty_state.update(True)
        yield from self._maybe_emit_snapshot(sketches, max_event_time_ms)

    @staticmethod
    def _is_relevant(value) -> bool:
        cancelled = value[4] or 0.0
        diverted = value[5] or 0.0
        return (
            value[1] in AIRLINES
            and cancelled < 0.5
            and diverted < 0.5
            and value[3] is not None
        )

    def _maybe_emit_snapshot(self, sketches: dict[tuple[str, int], DelayDDSketch], max_event_time_ms: int | None):
        if max_event_time_ms is None or not self._dirty_state.value():
            return

        next_emit = self._next_emit_event_time_state.value()
        if next_emit is None:
            next_emit = DATASET_START_MS + self._snapshot_event_step_ms

        if max_event_time_ms < next_emit:
            return

        rows = list(self._snapshot_rows(sketches, max_event_time_ms))
        self._next_emit_event_time_state.update(
            _next_emit_after(max_event_time_ms, self._snapshot_event_step_ms)
        )

        if not rows:
            return

        for row in rows:
            yield row

        self._last_snapshot_event_time_state.update(max_event_time_ms)
        self._dirty_state.update(False)

    def _snapshot_rows(self, sketches: dict[tuple[str, int], DelayDDSketch], max_event_time_ms: int | None):
        ts = _snapshot_ts(max_event_time_ms)
        for (airline, hour), sketch in sorted(sketches.items()):
            if sketch.count == 0:
                continue
            yield Row(
                ts,
                airline,
                int(hour),
                sketch.count,
                sketch.min_value,
                round(sketch.quantile(0.25), 2),
                round(sketch.quantile(0.50), 2),
                round(sketch.quantile(0.75), 2),
                round(sketch.quantile(0.90), 2),
                sketch.max_value,
            )



Q3_OUTPUT_TYPE = Types.ROW_NAMED(
    ['ts', 'airline', 'hour', 'num_flights',
     'delay_min', 'p25', 'p50', 'p75', 'p90', 'delay_max'],
    [Types.SQL_TIMESTAMP(), Types.STRING(), Types.INT(), Types.LONG(),
     Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
     Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()],
)


CSV_SINK_DDL = """
    CREATE TABLE {name} (
        ts          TIMESTAMP(3),
        airline     STRING,
        `hour`      INT,
        num_flights BIGINT,
        delay_min   DOUBLE,
        p25         DOUBLE,
        p50         DOUBLE,
        p75         DOUBLE,
        p90         DOUBLE,
        delay_max   DOUBLE
    ) WITH (
        'connector'                             = 'filesystem',
        'path'                                  = '{path}',
        'format'                                = 'csv',
        'sink.rolling-policy.rollover-interval' = '10 s',
        'sink.rolling-policy.check-interval'    = '5 s'
    )
"""


KAFKA_SINK_DDL = """
    CREATE TABLE {name} (
        ts          TIMESTAMP(3),
        airline     STRING,
        `hour`      STRING,
        num_flights BIGINT,
        delay_min   DOUBLE,
        p25         DOUBLE,
        p50         DOUBLE,
        p75         DOUBLE,
        p90         DOUBLE,
        delay_max   DOUBLE
    ) WITH (
        'connector'                      = 'kafka',
        'topic'                          = '{topic}',
        'properties.bootstrap.servers'   = '{bootstrap}',
        'format'                         = 'json',
        'json.timestamp-format.standard' = 'SQL'
    )
"""

JDBC_SINK_DDL = """
    CREATE TABLE {name} (
        ts          TIMESTAMP(3),
        airline     STRING,
        `hour`      INT,
        num_flights BIGINT,
        delay_min   DOUBLE,
        p25         DOUBLE,
        p50         DOUBLE,
        p75         DOUBLE,
        p90         DOUBLE,
        delay_max   DOUBLE,
        PRIMARY KEY (ts, airline, `hour`) NOT ENFORCED
    ) WITH (
        'connector'  = 'jdbc',
        'url'        = '{url}',
        'table-name' = '{table}',
        'username'   = '{username}',
        'password'   = '{password}',
        'sink.buffer-flush.max-rows' = '5000',
        'sink.buffer-flush.interval' = '2s'
    )
"""
