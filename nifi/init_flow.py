#!/usr/bin/env python3
"""
Configure Apache NiFi 1.16.3 via REST API at startup.

Flow built:
    ListenHTTP (port 9876, path=/flights)
        → ConvertRecord  (CSVReader → AvroRecordSetWriter Confluent)
            → PublishKafkaRecord_2_6  (topic=flights, Schema Registry)

Controller services created:
    ConfluentSchemaRegistry  → http://schema-registry:8081
    CSVReader                → uses schema from ConfluentSchemaRegistry
    AvroRecordSetWriter      → uses schema from ConfluentSchemaRegistry
                               (Confluent wire format: magic byte + schema ID)
"""
from __future__ import annotations

import os
import sys
import time
import logging

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

NIFI_BASE = os.getenv("NIFI_BASE_URL", "http://nifi:8080/nifi-api")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flights")
LISTEN_PORT = os.getenv("NIFI_LISTEN_PORT", "9876")
LISTEN_PATH = os.getenv("NIFI_LISTEN_PATH", "/flights")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _nifi(method: str, path: str, **kwargs) -> requests.Response:
    url = NIFI_BASE + path
    resp = requests.request(method, url, **kwargs)
    resp.raise_for_status()
    return resp


def _wait_nifi(max_retries: int = 60, interval: int = 5) -> None:
    for attempt in range(1, max_retries + 1):
        try:
            _nifi("GET", "/flow/status")
            logger.info("NiFi REST API ready.")
            return
        except Exception as exc:
            logger.warning("NiFi not ready (attempt %d/%d): %s", attempt, max_retries, exc)
            if attempt < max_retries:
                time.sleep(interval)
    raise RuntimeError("NiFi did not become ready in time.")


def _get_root_pg_id() -> str:
    data = _nifi("GET", "/flow/process-groups/root").json()
    return data["processGroupFlow"]["id"]


def _create_controller_service(pg_id: str, type_: str, name: str, properties: dict) -> str:
    body = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "type": type_,
            "properties": properties,
        },
    }
    data = _nifi("POST", f"/process-groups/{pg_id}/controller-services", json=body).json()
    return data["id"]


def _enable_controller_service(cs_id: str) -> None:
    # Get current revision
    data = _nifi("GET", f"/controller-services/{cs_id}").json()
    version = data["revision"]["version"]
    body = {
        "revision": {"version": version},
        "state": "ENABLED",
    }
    _nifi("PUT", f"/controller-services/{cs_id}/run-status", json=body)
    # Wait until enabled
    for _ in range(60):
        time.sleep(2)
        state = _nifi("GET", f"/controller-services/{cs_id}").json()
        if state["component"]["state"] == "ENABLED":
            return
    raise RuntimeError(f"Controller service {cs_id} did not enable in time.")


def _create_processor(pg_id: str, type_: str, name: str, properties: dict, position: dict) -> str:
    body = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "type": type_,
            "position": position,
            "config": {"properties": properties},
        },
    }
    data = _nifi("POST", f"/process-groups/{pg_id}/processors", json=body).json()
    return data["id"]


def _connect(pg_id: str, src_id: str, dst_id: str, relationships: list[str]) -> None:
    body = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": src_id, "groupId": pg_id, "type": "PROCESSOR"},
            "destination": {"id": dst_id, "groupId": pg_id, "type": "PROCESSOR"},
            "selectedRelationships": relationships,
        },
    }
    _nifi("POST", f"/process-groups/{pg_id}/connections", json=body)


def _auto_terminate(proc_id: str, relationships: list[str]) -> None:
    data = _nifi("GET", f"/processors/{proc_id}").json()
    version = data["revision"]["version"]
    config = data["component"]["config"]
    config["autoTerminatedRelationships"] = relationships
    body = {
        "revision": {"version": version},
        "component": {"id": proc_id, "config": config},
    }
    _nifi("PUT", f"/processors/{proc_id}", json=body)


def _start_processor(proc_id: str) -> None:
    data = _nifi("GET", f"/processors/{proc_id}").json()
    version = data["revision"]["version"]
    body = {
        "revision": {"version": version},
        "state": "RUNNING",
    }
    _nifi("PUT", f"/processors/{proc_id}/run-status", json=body)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    _wait_nifi()

    pg_id = _get_root_pg_id()
    logger.info("Root process group ID: %s", pg_id)

    # ── Controller services ───────────────────────────────────────────────────

    logger.info("Creating ConfluentSchemaRegistry …")
    sr_id = _create_controller_service(
        pg_id,
        "org.apache.nifi.confluent.schemaregistry.ConfluentSchemaRegistry",
        "ConfluentSchemaRegistry",
        {"url": SCHEMA_REGISTRY_URL},
    )

    logger.info("Creating CSVReader …")
    csv_reader_id = _create_controller_service(
        pg_id,
        "org.apache.nifi.csv.CSVReader",
        "CSVReader",
        {
            "schema-access-strategy": "schema-name",
            "schema-registry": sr_id,
            "schema-name": "flights-value",
            "Skip Header Line": "true",
        },
    )

    logger.info("Creating AvroRecordSetWriter (Confluent) …")
    avro_writer_id = _create_controller_service(
        pg_id,
        "org.apache.nifi.avro.AvroRecordSetWriter",
        "AvroRecordSetWriter",
        {
            "schema-access-strategy": "schema-name",
            "schema-registry": sr_id,
            "schema-name": "flights-value",
            "Schema Write Strategy": "confluent-encoded",
        },
    )

    for cs_id, label in [
        (sr_id, "ConfluentSchemaRegistry"),
        (csv_reader_id, "CSVReader"),
        (avro_writer_id, "AvroRecordSetWriter"),
    ]:
        logger.info("Enabling %s …", label)
        _enable_controller_service(cs_id)

    # ── Processors ────────────────────────────────────────────────────────────

    logger.info("Creating ListenHTTP processor …")
    listen_id = _create_processor(
        pg_id,
        "org.apache.nifi.processors.standard.ListenHTTP",
        "ListenHTTP",
        {
            "Listening Port": LISTEN_PORT,
            "Base Path": LISTEN_PATH.lstrip("/"),
        },
        {"x": 400, "y": 200},
    )

    logger.info("Creating PublishKafkaRecord processor …")
    publish_id = _create_processor(
        pg_id,
        "org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_6",
        "PublishKafkaRecord",
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "topic": KAFKA_TOPIC,
            "record-reader": csv_reader_id,
            "record-writer": avro_writer_id,
            "use-transactions": "false",
            "acks": "all",
        },
        {"x": 400, "y": 400},
    )

    # ── Connections ───────────────────────────────────────────────────────────

    logger.info("Connecting processors …")
    _connect(pg_id, listen_id, publish_id, ["success"])

    # Auto-terminate terminal relationships
    _auto_terminate(listen_id, ["dropped"])
    _auto_terminate(publish_id, ["success", "failure"])

    # ── Start processors ──────────────────────────────────────────────────────

    for proc_id, label in [
        (publish_id, "PublishKafkaRecord"),
        (listen_id, "ListenHTTP"),
    ]:
        logger.info("Starting %s …", label)
        _start_processor(proc_id)

    logger.info("NiFi flow configured and running.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.error("NiFi init failed: %s", exc)
        sys.exit(1)
