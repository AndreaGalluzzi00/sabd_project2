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

    if not flights:
        return "[]"
    def sort_key(row):
        delay = row[0] if row[0] is not None else float("-inf")
        carrier = row[1] or ""
        dest = 0 if row[2] is None else row[2]
        return (-delay, carrier, dest)

    top = sorted(flights, key=sort_key)[:TOP_N]
    return "[" + ",".join(
        f"({r[1]},{0 if r[2] is None else r[2]},{r[0]:.2f})" for r in top
    ) + "]"



def make_stats_view_sql(window_interval: str) -> str:

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
                    ORDER BY severe_delays DESC, dep_delay_mean DESC, origin_airport_id ASC
                ) AS rn
            FROM {stats_view}
        )
        WHERE rn <= {top_n_airports}
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

KAFKA_CUMULATIVE_SINK_DDL = """
    CREATE TABLE {name} (
        ts                TIMESTAMP(3),
        airport_rank      STRING,
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
        'password'   = '{password}',
        'sink.buffer-flush.max-rows' = '5000',
        'sink.buffer-flush.interval' = '2s'
    )
"""
