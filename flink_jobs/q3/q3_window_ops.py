
from __future__ import annotations

from datetime import datetime, timezone

from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import (
    AggregateFunction,
    MapFunction,
    ProcessWindowFunction,
)

from delay_sketch import DelayDDSketch


LATE_DROPS_METRIC = "numLateRecordsDropped"


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
        'password'   = '{password}'
    )
"""
