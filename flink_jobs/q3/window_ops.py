"""
Q3 shared components: sketch aggregator, window function, tipi e DDL dei sink.

L'aggregazione è incrementale (AggregateFunction): l'accumulatore per ogni
chiave (compagnia, fascia oraria) e finestra è UN DelayDDSketch, non la lista
dei valori — requisito esplicito della specifica ("senza ordinare tutti i
valori e possibilmente senza accumularli interamente"). Flink invoca add()
per ogni evento e la finestra mantiene solo lo sketch; alla chiusura la
ProcessWindowFunction estrae i percentili approssimati e min/max/count esatti.
"""
from __future__ import annotations

from datetime import datetime, timezone

from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction

from delay_sketch import DelayDDSketch


# ── Chiave di raggruppamento (compagnia, fascia oraria) ───────────────────────
# Chiave stringa "AA|08": evita il bridge dei tuple-key Python↔Java; la window
# function la riscompone nei due campi di output.

def group_key(airline: str, hour: int) -> str:
    return f"{airline}|{hour:02d}"


# ── Aggregazione incrementale: accumulatore = DDSketch ────────────────────────

class Q3AggregateFunction(AggregateFunction):
    """Sketch per (compagnia, fascia) su una finestra event-time."""

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


# ── ProcessWindowFunction: sketch → riga di output ────────────────────────────

class Q3WindowFunction(ProcessWindowFunction):
    """
    Riceve lo sketch finale per (finestra, chiave) ed emette la riga Q3:
    ts = inizio finestra, percentili approssimati arrotondati a 2 decimali
    (formato dell'esempio di output della specifica), min/max/count esatti.
    """

    def process(self, key: str, context: ProcessWindowFunction.Context, elements):
        sketch = next(iter(elements))
        airline, hour_str = key.split("|")

        ts = datetime.fromtimestamp(
            context.window().start / 1000.0, tz=timezone.utc
        ).replace(tzinfo=None)  # naive UTC → LOCAL_DATE_TIME → TIMESTAMP(3)

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


# TypeInformation dell'output. LOCAL_DATE_TIME → TIMESTAMP(3) nel bridge
# from_data_stream (stessa combinazione provata dallo Stage 2 di Q2).
# 'num_flights'/'delay_min'/'delay_max' invece di count/min/max: evita le
# keyword SQL riservate nei DDL dei sink; merge_q3.py scrive l'header
# richiesto dalla specifica (ts, airline, hour, count, min, ..., max).
Q3_OUTPUT_TYPE = Types.ROW_NAMED(
    ['ts', 'airline', 'hour', 'num_flights',
     'delay_min', 'p25', 'p50', 'p75', 'p90', 'delay_max'],
    [Types.LOCAL_DATE_TIME(), Types.STRING(), Types.INT(), Types.LONG(),
     Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
     Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()],
)


# ── DDL dei sink (stesso pattern di Q1/Q2: CSV sempre, Kafka/JDBC opzionali) ──

CSV_SINK_DDL = """
    CREATE TABLE {name} (
        ts          TIMESTAMP(3),
        airline     STRING,
        hour        INT,
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

# hour come STRING: Telegraf (parser JSON) accetta solo stringhe come tag e
# 'hour' deve essere un tag InfluxDB insieme ad 'airline'.
KAFKA_SINK_DDL = """
    CREATE TABLE {name} (
        ts          TIMESTAMP(3),
        airline     STRING,
        hour        STRING,
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

# PRIMARY KEY NOT ENFORCED → il connettore JDBC lavora in upsert
# (INSERT ... ON CONFLICT DO UPDATE): ri-esecuzioni idempotenti.
JDBC_SINK_DDL = """
    CREATE TABLE {name} (
        ts          TIMESTAMP(3),
        airline     STRING,
        hour        INT,
        num_flights BIGINT,
        delay_min   DOUBLE,
        p25         DOUBLE,
        p50         DOUBLE,
        p75         DOUBLE,
        p90         DOUBLE,
        delay_max   DOUBLE,
        PRIMARY KEY (ts, airline, hour) NOT ENFORCED
    ) WITH (
        'connector'  = 'jdbc',
        'url'        = '{url}',
        'table-name' = '{table}',
        'username'   = '{username}',
        'password'   = '{password}'
    )
"""
