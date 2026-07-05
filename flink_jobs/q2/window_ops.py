"""
Q2 shared components — Option B: pure Flink SQL Window Top-N.

The whole pipeline is append-only, so it maps 1:1 onto the filesystem CSV sink
and never needs retract handling:

    Windowing TVF aggregation  ->  Window Top-N  ->  format list  ->  CSV sink
    (GlobalWindowAggregate)        (WindowRank)      (Python scalar UDF)

Why no Python UDAF: a Python *UDAF* inside the windowing query forces the planner
out of the window-aggregate operator into a continuously-updating group aggregate
(retract mode) — that was the defect of the previous implementation (same airport
occupying several ranks, unbounded state). Here the severely-delayed flights are
collected with the built-in ARRAY_AGG (kept inside the append-only window
aggregate) and turned into the required string by a Python *scalar* UDF, which
does not change the changelog mode.
"""
from __future__ import annotations

from pyflink.table import DataTypes
from pyflink.table.udf import udf


# Max flights listed per airport (spec: at most 20, sorted by dep_delay desc).
TOP_N = 20

# Element type collected by ARRAY_AGG for every severe-delay flight.
SEVERE_ROW = DataTypes.ROW([
    DataTypes.FIELD("dep_delay", DataTypes.DOUBLE()),
    DataTypes.FIELD("carrier",   DataTypes.STRING()),
    DataTypes.FIELD("dest",      DataTypes.INT()),
])


@udf(result_type=DataTypes.STRING(), input_types=[DataTypes.ARRAY(SEVERE_ROW)])
def format_top_delayed(flights) -> str:
    """
    Sort the severely-delayed flights by dep_delay descending, keep the TOP_N
    largest and format them as ``[(carrier,dest,delay), ...]``.

    Scalar (one row -> one value) function: it runs after the append-only Window
    Top-N, so it does not affect the changelog mode of the stream.
    """
    if not flights:
        return "[]"
    top = sorted(
        flights,
        key=lambda r: r[0] if r[0] is not None else float("-inf"),
        reverse=True,
    )[:TOP_N]
    return "[" + ",".join(
        f"({r[1]},{0 if r[2] is None else r[2]},{r[0]:.2f})" for r in top
    ) + "]"


# ── SQL helpers ───────────────────────────────────────────────────────────────

def make_stats_view_sql(window_interval: str) -> str:
    """
    Stage 1 — Windowing TVF aggregation (append-only, fires once per window,
    per-window state released on fire). Per (window, airport):
      * num_flights    : non-cancelled/non-diverted flights in the window
      * severe_delays  : of those, how many with dep_delay > 30
      * dep_delay_mean : average dep_delay
      * dep_delay_max  : maximum dep_delay
      * severe_flights : ARRAY_AGG of the severe flights (dep_delay, carrier, dest)
    HAVING keeps only airports with at least 30 flights.

    `completed_flights` is expected to already be filtered to non-cancelled and
    non-diverted rows.
    """
    return f"""
        SELECT
            window_start,
            window_end,
            origin_airport_id,
            COUNT(*)                                     AS num_flights,
            COUNT(*) FILTER (WHERE dep_delay > 30.0)     AS severe_delays,
            AVG(dep_delay)                               AS dep_delay_mean,
            MAX(dep_delay)                               AS dep_delay_max,
            ARRAY_AGG(
                CAST(ROW(dep_delay, airline, dest_airport_id) AS
                     ROW<dep_delay DOUBLE, carrier STRING, dest INT>)
            ) FILTER (WHERE dep_delay > 30.0)            AS severe_flights
        FROM TABLE(
            TUMBLE(TABLE completed_flights, DESCRIPTOR(rowtime), {window_interval})
        )
        GROUP BY window_start, window_end, origin_airport_id
        HAVING COUNT(*) >= 30
    """


def make_topn_sql(stats_view: str, top_n_airports: int = 10) -> str:
    """
    Stage 2 — Window Top-N: the top `top_n_airports` airports per window ranked
    by severe_delays (ties broken by dep_delay_mean), planned by Flink as the
    native append-only WindowRank operator.
    Stage 3 — the scalar UDF formats the flight list for the surviving rows.

    Output columns match the CSV sink / spec schema:
        ts, airport_rank, origin_airport_id, num_flights, severe_delays,
        dep_delay_mean, dep_delay_max, delayed_flights
    """
    return f"""
        SELECT
            window_start                       AS ts,
            rn                                 AS airport_rank,
            origin_airport_id,
            num_flights,
            severe_delays,
            dep_delay_mean,
            dep_delay_max,
            format_top_delayed(severe_flights) AS delayed_flights
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY window_start, window_end
                    ORDER BY severe_delays DESC, dep_delay_mean DESC
                ) AS rn
            FROM {stats_view}
        )
        WHERE rn <= {top_n_airports}
    """


# ── Sink DDLs ───────────────────────────────────────────────────────────────

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

# airport_rank e origin_airport_id come STRING: Telegraf (parser JSON) accetta
# solo stringhe come tag InfluxDB e qui i due identificano la serie (rank +
# aeroporto). Stesso trattamento di 'hour' in Q3 (q3_window_ops.KAFKA_SINK_DDL).
KAFKA_SINK_DDL = """
    CREATE TABLE {name} (
        ts                TIMESTAMP(3),
        airport_rank      STRING,
        origin_airport_id STRING,
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
