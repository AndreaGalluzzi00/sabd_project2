"""
Q2 shared components: UDAF, Stage-2 ranking function, SQL helpers.
"""
from __future__ import annotations

from datetime import timezone

from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import ProcessAllWindowFunction
from pyflink.table import DataTypes
from pyflink.table.udf import AggregateFunction, udaf


# ── UDAF: top-20 severely delayed flights per airport per window ──────────────

class TopDelayedCollector(AggregateFunction):
    """
    Accumulates (carrier, dest_airport_id, dep_delay) tuples.
    The SQL caller passes NULL for non-qualifying rows (dep_delay <= 30);
    NULLs are silently ignored.
    Returns the top-20 by dep_delay descending as a formatted string.
    """

    def create_accumulator(self):
        return []

    def accumulate(self, acc, carrier, dest_airport_id, dep_delay):
        if carrier is not None and dep_delay is not None:
            acc.append((str(carrier), int(dest_airport_id) if dest_airport_id is not None else 0, float(dep_delay)))

    def get_value(self, acc):
        if not acc:
            return "[]"
        top20 = sorted(acc, key=lambda x: x[2], reverse=True)[:20]
        return "[" + ",".join(f"({f[0]},{f[1]},{f[2]:.2f})" for f in top20) + "]"

    def get_result_type(self):
        return DataTypes.STRING()

    def get_accumulator_type(self):
        return DataTypes.ARRAY(DataTypes.STRING())


def make_top_delayed_udaf():
    return udaf(
        TopDelayedCollector(),
        result_type=DataTypes.STRING(),
        accumulator_type=DataTypes.ARRAY(DataTypes.STRING()),
    )


# ── Stage 2: top-10 ranking per window (DataStream ProcessAllWindowFunction) ──

class Top10AllWindowFunction(ProcessAllWindowFunction):
    """
    Receives all per-airport stats for a completed TUMBLE window (emitted as a
    burst by Stage 1), sorts by severe_delays DESC (ties broken by dep_delay_mean
    DESC), and emits exactly the top-10 with ranks 1–10.

    Input Row column order (from make_stats_view_sql):
        0  window_start     TIMESTAMP_LTZ(3)
        1  window_end       TIMESTAMP_LTZ(3)
        2  origin_airport_id  INT
        3  num_flights        BIGINT
        4  severe_delays      BIGINT
        5  dep_delay_mean     DOUBLE (nullable)
        6  dep_delay_max      DOUBLE (nullable)
        7  delayed_flights    STRING

    Output Row column order:
        0  ts                 TIMESTAMP_LTZ(3)  (= window_start)
        1  airport_rank       BIGINT
        2  origin_airport_id  INT
        3  num_flights        BIGINT
        4  severe_delays      BIGINT
        5  dep_delay_mean     DOUBLE
        6  dep_delay_max      DOUBLE
        7  delayed_flights    STRING
    """

    def process(self, context, elements):
        # PyFlink 1.20 calls process(context, elements) — use yield, not out.collect().
        airports = sorted(
            list(elements),          # materialise one-shot iterator
            key=lambda r: (-(r[4] or 0), -(r[5] or 0.0)),  # severe_delays DESC, mean DESC
        )
        for rank, r in enumerate(airports[:10], 1):
            yield Row(r[0], rank, r[2], r[3], r[4], r[5], r[6], r[7])


# TypeInformation for Top10AllWindowFunction output.
# TIMESTAMP_LTZ(3) → SQL_TIMESTAMP in PyFlink 1.20's Java type bridge.
# Types.ROW_NAMED gives named fields so that from_data_stream(schema=...) can
# resolve column references like 'ts', 'airport_rank', etc. by name.
# Types.ROW([...]) would produce anonymous f0/f1/... fields which fail schema matching.
TOP10_OUTPUT_TYPE = Types.ROW_NAMED(
    ['ts', 'airport_rank', 'origin_airport_id', 'num_flights',
     'severe_delays', 'dep_delay_mean', 'dep_delay_max', 'delayed_flights'],
    [Types.SQL_TIMESTAMP(), Types.LONG(), Types.INT(), Types.LONG(),
     Types.LONG(), Types.DOUBLE(), Types.DOUBLE(), Types.STRING()],
)

# TypeInformation for the Stage-1 stats view rows.
# Used with to_append_stream() to bypass the PythonGroupAggregate retract-mode
# check: TUMBLE windows are semantically append-only (fire once per bucket)
# but the Python UDAF makes Flink think the plan needs retract support.
STATS_VIEW_TYPE = Types.ROW([
    Types.SQL_TIMESTAMP(),  # window_start
    Types.SQL_TIMESTAMP(),  # window_end
    Types.INT(),              # origin_airport_id
    Types.LONG(),             # num_flights
    Types.LONG(),             # severe_delays
    Types.DOUBLE(),           # dep_delay_mean  (nullable at Python level)
    Types.DOUBLE(),           # dep_delay_max   (nullable at Python level)
    Types.STRING(),           # delayed_flights
])


# ── Timestamp assigner for Stage 2 watermark re-assignment ───────────────────

class WindowEndTimestampAssigner:
    """
    Duck-type timestamp assigner for PyFlink 1.20 (no base class needed).
    Extracts window_end (row[1]) as epoch milliseconds.
    Handles both tz-aware (TIMESTAMP_LTZ → UTC datetime) and naive datetimes.
    Used after to_retract_stream() + map() to ensure Stage 2
    TumblingEventTimeWindows fire at the correct event-time boundaries.
    """
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        dt = value[1]  # window_end
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)


# ── SQL helpers ───────────────────────────────────────────────────────────────

def make_stats_view_sql(window_interval: str) -> str:
    """
    Per-airport TUMBLE aggregation (Stage 1). Includes window_end so that
    to_data_stream() can extract it as the event-time for Stage 2.
    HAVING filters airports with < 30 completed flights.
    """
    return f"""
        SELECT
            window_start,
            window_end,
            origin_airport_id,
            COUNT(*)                                   AS num_flights,
            COUNT(*) FILTER (WHERE dep_delay > 30.0)   AS severe_delays,
            AVG(dep_delay)                             AS dep_delay_mean,
            MAX(dep_delay)                             AS dep_delay_max,
            top_delayed(
                CASE WHEN dep_delay > 30.0 THEN airline         ELSE NULL END,
                CASE WHEN dep_delay > 30.0 THEN dest_airport_id ELSE NULL END,
                CASE WHEN dep_delay > 30.0 THEN dep_delay       ELSE NULL END
            )                                          AS delayed_flights
        FROM TABLE(
            TUMBLE(TABLE completed_flights, DESCRIPTOR(rowtime), {window_interval})
        )
        GROUP BY window_start, window_end, origin_airport_id
        HAVING COUNT(*) >= 30
    """


CSV_SINK_DDL = """
    CREATE TABLE {name} (
        ts                TIMESTAMP(3),
        airport_rank      BIGINT,
        origin_airport_id INT,
        num_flights       BIGINT,
        severe_delays     BIGINT,
        dep_delay_mean    DOUBLE,
        dep_delay_max     DOUBLE,
        delayed_flights   STRING
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
        ts                TIMESTAMP(3),
        airport_rank      BIGINT,
        origin_airport_id INT,
        num_flights       BIGINT,
        severe_delays     BIGINT,
        dep_delay_mean    DOUBLE,
        dep_delay_max     DOUBLE,
        delayed_flights   STRING
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
        ts                TIMESTAMP(3),
        airport_rank      BIGINT,
        origin_airport_id INT,
        num_flights       BIGINT,
        severe_delays     BIGINT,
        dep_delay_mean    DOUBLE,
        dep_delay_max     DOUBLE,
        delayed_flights   STRING,
        PRIMARY KEY (ts, airport_rank) NOT ENFORCED
    ) WITH (
        'connector'  = 'jdbc',
        'url'        = '{url}',
        'table-name' = '{table}',
        'username'   = '{username}',
        'password'   = '{password}'
    )
"""
