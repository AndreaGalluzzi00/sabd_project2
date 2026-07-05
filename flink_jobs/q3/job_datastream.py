#!/usr/bin/env python3
"""
Q3 – versione DataStream API PURA (percentili approssimati con DDSketch).

Come q1/job_datastream.py e q2/job_datastream.py: NESSUNA Table API. Il calcolo
è identico a q3/job.py (KafkaSource byte grezzi → decoder Avro puro-Python →
watermark → filtro AA/DL/UA/WN non cancellati/deviati con DEP_DELAY → keyBy
(compagnia|fascia) → finestre event-time → AggregateFunction il cui accumulatore
è un DelayDDSketch), ma i risultati sono scritti in CSV da una MapFunction
custom (_CsvWriter + .print() come sink obbligatorio), senza sink Table/Kafka/JDBC.

Finestre (allineate all'inizio del dataset via offset del tumbling):
    1 giorno · 7 giorni · dall'inizio del dataset (365 giorni).

Nota watermark: timestamp e watermark sono assegnati PRIMA del filtro sulle
compagnie, quindi il marker EOS avanza il watermark e drena l'ultima finestra
anche senza 'flink stop --drain' (stesso comportamento di q3/job.py).

Output CSV (header finale scritto da scripts/merge_q3_ds.py):
    ts, airline, hour, count, min, p25, p50, p75, p90, max
"""
from __future__ import annotations

import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration, Time
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.window import TumblingEventTimeWindows

from common.config import load_config
from common.logging_utils import configure_logging
from flink_runtime import (
    FlinkRuntimeConfig,
    build_flink_runtime_config,
    create_stream_execution_environment,
)
from flight_avro import decode_flight_for_q3, hour_band
from q3_window_ops import (
    Q3_OUTPUT_TYPE,
    Q3AggregateFunction,
    Q3WindowFunction,
    group_key,
)

import logging

configure_logging()
logger = logging.getLogger(__name__)

AIRLINES: frozenset[str] = frozenset({"AA", "DL", "UA", "WN"})


# ── Config ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Q3DSConfig(FlinkRuntimeConfig):
    results_path_1d: str
    results_path_7d: str
    results_path_global: str
    watermark_delay_seconds: int
    sketch_alpha: float


def load_q3_ds_config() -> Q3DSConfig:
    cfg = load_config()
    flink_cfg = build_flink_runtime_config(cfg)

    # Consumer group dedicato: può girare insieme alle altre query.
    flink_dict = asdict(flink_cfg)
    flink_dict["kafka_consumer_group"] = (
        cfg["flink"].get("consumer_group_q3", cfg["flink"]["consumer_group"] + "-q3")
        + "-ds"
    )

    return Q3DSConfig(
        **flink_dict,
        results_path_1d=cfg["paths"]["q3_ds_results_path_1d"],
        results_path_7d=cfg["paths"]["q3_ds_results_path_7d"],
        results_path_global=cfg["paths"]["q3_ds_results_path_global"],
        watermark_delay_seconds=int(cfg["q3"]["watermark_delay_seconds"]),
        sketch_alpha=float(cfg["q3"].get("sketch_alpha", 0.01)),
    )


# ── Decodifica Kafka+Avro (identica a q3/job.py) ──────────────────────────────

class _FlightDecoder(MapFunction):
    """Recupera i byte Kafka e decodifica il record. None per i malformati.

    NON filtra sulla compagnia: anche il marker EOS deve attraversare
    l'assegnazione del watermark per drenare le finestre finali.
    """

    def map(self, value: str):
        try:
            return decode_flight_for_q3(value.encode('iso-8859-1'))
        except Exception:
            return None


class FlightTimestampAssigner:
    """event_time (ms) in tuple[0]."""
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return value[0]


def _is_q3_relevant(value) -> bool:
    """Voli delle 4 compagnie, non cancellati, non deviati, con DEP_DELAY."""
    _, airline, _, dep_delay, cancelled, diverted = value
    return (
        airline in AIRLINES
        and (cancelled or 0.0) < 0.5
        and (diverted or 0.0) < 0.5
        and dep_delay is not None
    )


# ── CSV sink (pattern di q1/job_datastream.py) ────────────────────────────────

class _CsvWriter(MapFunction):
    """Scrive ogni riga CSV in part-{subtask}-{run_ts} e la ri-emette (sink
    obbligatorio = .print()). Nomi con timestamp: evitano PermissionError sui
    bind mount Windows/Docker dove i file di run precedenti hanno UID diverso."""

    def __init__(self, base_path: str):
        self._base_path = base_path
        self._file = None

    def open(self, runtime_context):
        import os
        import time as _time
        os.makedirs(self._base_path, exist_ok=True)
        idx = runtime_context.get_index_of_this_subtask()
        run_ts = int(_time.time())
        self._file = open(
            os.path.join(self._base_path, f"part-{idx}-{run_ts}"),
            'w', encoding='utf-8',
        )

    def map(self, value):
        self._file.write(value + '\n')
        self._file.flush()
        return value

    def close(self):
        if self._file:
            self._file.close()


def _format_csv_row(row) -> str:
    """Riga di output Q3 -> stringa CSV (ordine: ts, airline, hour, count,
    min, p25, p50, p75, p90, max). ts = TIMESTAMP(3) -> datetime naive UTC."""
    ts = row[0]
    ts_str = ts.strftime('%Y-%m-%d %H:%M:%S') if hasattr(ts, "strftime") else str(ts)
    return (
        f"{ts_str},{row[1]},{row[2]},{row[3]},{row[4]},"
        f"{row[5]},{row[6]},{row[7]},{row[8]},{row[9]}"
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = load_q3_ds_config()

    logger.info("Q3-DS | Kafka: %s  topic: %s", cfg.kafka_bootstrap, cfg.kafka_topic)
    logger.info("Q3-DS | Consumer group: %s", cfg.kafka_consumer_group)
    logger.info("Q3-DS | Parallelism: %d", cfg.parallelism)
    logger.info("Q3-DS | Watermark delay: %d s", cfg.watermark_delay_seconds)
    logger.info("Q3-DS | DDSketch alpha: %g", cfg.sketch_alpha)

    env = create_stream_execution_environment(cfg)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(cfg.kafka_bootstrap)
        .set_topics(cfg.kafka_topic)
        .set_group_id(cfg.kafka_consumer_group)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema("ISO-8859-1"))
        .build()
    )

    raw_ds = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "KafkaSource[flights]",
    )

    decoded_ds = (
        raw_ds
        .map(_FlightDecoder(), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda v: v is not None)
    )

    # Watermark PRIMA del filtro compagnie (EOS drena le finestre finali).
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(cfg.watermark_delay_seconds))
        .with_timestamp_assigner(FlightTimestampAssigner())
    )
    decoded_ds = decoded_ds.assign_timestamps_and_watermarks(watermark_strategy)

    flights_ds = (
        decoded_ds
        .filter(_is_q3_relevant)
        .map(
            lambda v: (v[0], v[1], hour_band(v[2]), float(v[3])),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
    )

    keyed_stream = flights_ds.key_by(
        lambda v: group_key(v[1], v[2]),
        key_type=Types.STRING(),
    )

    # Tre finestre tumbling event-time, allineate all'inizio del dataset (offset).
    windows = [
        ("1d",     TumblingEventTimeWindows.of(Time.days(1)),                cfg.results_path_1d),
        ("7d",     TumblingEventTimeWindows.of(Time.days(7),   Time.days(6)), cfg.results_path_7d),
        ("global", TumblingEventTimeWindows.of(Time.days(365), Time.days(14)), cfg.results_path_global),
    ]

    for label, window_assigner, results_path in windows:
        result_ds = (
            keyed_stream
            .window(window_assigner)
            .aggregate(
                Q3AggregateFunction(cfg.sketch_alpha),
                window_function=Q3WindowFunction(),
                accumulator_type=Types.PICKLED_BYTE_ARRAY(),
                output_type=Q3_OUTPUT_TYPE,
            )
            .name(f"Q3DSWindow[{label}]")
        )

        (
            result_ds
            .map(_format_csv_row, output_type=Types.STRING())
            .map(_CsvWriter(results_path), output_type=Types.STRING())
            .print()
        )
        logger.info("Q3-DS [%s] | CSV sink → %s", label, results_path)

    logger.info("Q3-DS | Submitting job …")
    env.execute("Q3-DS")
    logger.info("Q3-DS | Job submitted successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()
