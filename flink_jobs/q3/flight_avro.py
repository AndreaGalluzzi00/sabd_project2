"""
Q3 – decoder puro-Python del wire format Confluent Avro (Flight).

Stessa tecnica di q1/job_datastream.py (SimpleStringSchema("ISO-8859-1") lato
KafkaSource + recupero dei byte originali con str.encode('iso-8859-1')), con
due differenze richieste da Q3:

  1. viene decodificato anche crs_dep_time (Q1 lo salta), da cui si ricava
     la fascia oraria di partenza programmata;
  2. NESSUN filtro sulle compagnie qui: il record va restituito anche per il
     marker end-of-stream (airline='__EOS__', event_time nel 2200), perché il
     job assegna timestamp e watermark PRIMA di filtrare. È il fix del gap
     noto di Q1-DS, dove l'EOS veniva scartato prima del watermark e l'ultima
     finestra si chiudeva solo con 'flink stop --drain'.

Wire format:  0x00 (magic byte) | 4 byte big-endian schema_id | Avro binario.
Ordine dei campi: schema/flight.avsc (it.uniroma2.sabd.Flight).
"""
from __future__ import annotations

import struct


def _avro_decode_varint(buf: bytes, pos: int):
    """Decodifica un varint zigzag. Ritorna (valore, nuova_posizione)."""
    b, shift = 0, 0
    while True:
        byte = buf[pos]; pos += 1
        b |= (byte & 0x7F) << shift
        shift += 7
        if not (byte & 0x80):
            break
    return (b >> 1) ^ -(b & 1), pos


def _avro_skip_varint(buf: bytes, pos: int) -> int:
    """Salta un varint senza decodificarlo; ritorna la nuova posizione."""
    while buf[pos] & 0x80:
        pos += 1
    return pos + 1


def _avro_decode_string(buf: bytes, pos: int):
    """Decodifica una stringa Avro (lunghezza + UTF-8). Ritorna (str, pos)."""
    n, pos = _avro_decode_varint(buf, pos)
    return buf[pos:pos + n].decode('utf-8'), pos + n


def _avro_decode_nullable_double(buf: bytes, pos: int):
    """Decodifica l'union ["null","double"]. Ritorna (float|None, pos)."""
    branch, pos = _avro_decode_varint(buf, pos)
    if branch == 0:          # ramo null
        return None, pos
    return struct.unpack_from('<d', buf, pos)[0], pos + 8


def decode_flight_for_q3(data: bytes):
    """
    Decodifica i campi del record Flight usati da Q3.

    Ritorna (event_time_ms, airline, crs_dep_time, dep_delay, cancelled,
    diverted) oppure None se il messaggio è malformato (magic byte errato o
    payload troppo corto).
    """
    if len(data) < 6 or data[0] != 0:
        return None
    pos = 5  # salta magic byte (1) + schema_id (4)

    event_time, pos = _avro_decode_varint(data, pos)             # event_time : long
    pos = _avro_skip_varint(data, pos)                           # year       : int
    pos = _avro_skip_varint(data, pos)                           # month      : int
    pos = _avro_skip_varint(data, pos)                           # day_of_month: int
    airline,      pos = _avro_decode_string(data, pos)           # airline    : string
    pos = _avro_skip_varint(data, pos)                           # origin_airport_id: int
    pos = _avro_skip_varint(data, pos)                           # dest_airport_id  : int
    crs_dep_time, pos = _avro_decode_varint(data, pos)           # crs_dep_time     : int
    dep_delay,    pos = _avro_decode_nullable_double(data, pos)  # dep_delay  : ["null","double"]
    _,            pos = _avro_decode_nullable_double(data, pos)  # arr_delay  (saltato)
    cancelled,    pos = _avro_decode_nullable_double(data, pos)  # cancelled  : ["null","double"]
    diverted,     pos = _avro_decode_nullable_double(data, pos)  # diverted   : ["null","double"]

    return event_time, airline, crs_dep_time, dep_delay, cancelled, diverted


def hour_band(crs_dep_time: int) -> int:
    """
    Fascia oraria di partenza programmata (0-23) da CRS_DEP_TIME (formato hhmm).

    Il dataset BTS codifica la mezzanotte come 2400: il modulo la riporta alla
    fascia 0, in modo coerente con la costruzione dell'event_time nel
    preprocessing (2400 → 00:00 del giorno stesso).
    """
    return (crs_dep_time // 100) % 24
