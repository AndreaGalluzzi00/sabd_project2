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


def _list_controller_services(pg_id: str) -> list[dict]:
    data = _nifi("GET", f"/flow/process-groups/{pg_id}/controller-services").json()
    return data.get("controllerServices", [])


def _list_processors(pg_id: str) -> list[dict]:
    data = _nifi("GET", f"/process-groups/{pg_id}/processors").json()
    return data.get("processors", [])


def _list_connections(pg_id: str) -> list[dict]:
    data = _nifi("GET", f"/process-groups/{pg_id}/connections").json()
    return data.get("connections", [])


def _find_component(
    entities: list[dict],
    name: str,
    type_: str | None = None,
) -> dict | None:
    matches = []
    for entity in entities:
        component = entity.get("component", {})
        if component.get("name") != name:
            continue
        if type_ is not None and component.get("type") != type_:
            continue
        matches.append(entity)

    if len(matches) > 1:
        type_label = f" of type {type_}" if type_ is not None else ""
        logger.warning(
            "Found %d existing NiFi component(s) named %s%s; reusing the first one.",
            len(matches),
            name,
            type_label,
        )

    return matches[0] if matches else None


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


def _ensure_controller_service(pg_id: str, type_: str, name: str, properties: dict) -> str:
    existing = _find_component(_list_controller_services(pg_id), name, type_)

    if existing is not None:
        service_id = existing["id"]
        logger.info("Reusing controller service %s (%s)", name, service_id)
        _update_controller_service(service_id, properties)
        return service_id

    logger.info("Creating controller service %s", name)
    return _create_controller_service(pg_id, type_, name, properties)


def _properties_match(existing: dict, desired: dict) -> bool:
    for key, value in desired.items():
        if str(existing.get(key)) != str(value):
            return False
    return True


def _disable_controller_service(cs_id: str) -> None:
    data = _nifi("GET", f"/controller-services/{cs_id}").json()
    state = data["component"]["state"]

    if state == "DISABLED":
        logger.info("Controller service %s already disabled.", cs_id)
        return

    version = data["revision"]["version"]
    body = {
        "revision": {"version": version},
        "state": "DISABLED",
    }
    _nifi("PUT", f"/controller-services/{cs_id}/run-status", json=body)

    for _ in range(60):
        time.sleep(2)
        state = _nifi("GET", f"/controller-services/{cs_id}").json()
        if state["component"]["state"] == "DISABLED":
            return

    raise RuntimeError(f"Controller service {cs_id} did not disable in time.")


def _update_controller_service(cs_id: str, properties: dict) -> None:
    data = _nifi("GET", f"/controller-services/{cs_id}").json()
    component = data["component"]
    current_properties = component.get("properties") or {}

    if _properties_match(current_properties, properties):
        logger.info("Controller service %s already has desired properties.", cs_id)
        return

    if component["state"] != "DISABLED":
        _disable_controller_service(cs_id)
        data = _nifi("GET", f"/controller-services/{cs_id}").json()
        component = data["component"]
        current_properties = component.get("properties") or {}

    updated_properties = dict(current_properties)
    updated_properties.update(properties)

    body = {
        "revision": {"version": data["revision"]["version"]},
        "component": {
            "id": cs_id,
            "properties": updated_properties,
        },
    }
    _nifi("PUT", f"/controller-services/{cs_id}", json=body)
    logger.info("Updated controller service %s properties.", cs_id)


def _enable_controller_service(cs_id: str) -> None:
    data = _nifi("GET", f"/controller-services/{cs_id}").json()
    state = data["component"]["state"]

    if state == "ENABLED":
        logger.info("Controller service %s already enabled.", cs_id)
        return

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


def _ensure_processor(
    pg_id: str,
    type_: str,
    name: str,
    properties: dict,
    position: dict,
) -> str:
    existing = _find_component(_list_processors(pg_id), name, type_)

    if existing is not None:
        proc_id = existing["id"]
        logger.info("Reusing processor %s (%s)", name, proc_id)
        _update_processor(proc_id, properties)
        return proc_id

    logger.info("Creating processor %s", name)
    return _create_processor(pg_id, type_, name, properties, position)


def _connection_exists(
    pg_id: str,
    src_id: str,
    dst_id: str,
    relationships: list[str],
) -> bool:
    expected = set(relationships)

    for entity in _list_connections(pg_id):
        component = entity.get("component", {})
        source = component.get("source", {})
        destination = component.get("destination", {})
        selected = set(component.get("selectedRelationships", []))

        if (
            source.get("id") == src_id
            and destination.get("id") == dst_id
            and expected.issubset(selected)
        ):
            return True

    return False


def _connect(pg_id: str, src_id: str, dst_id: str, relationships: list[str]) -> None:
    if _connection_exists(pg_id, src_id, dst_id, relationships):
        logger.info("Connection already exists; reusing it.")
        return

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
    state = data["component"].get("state")

    # NiFi returns 400 if you try to update config of a RUNNING processor.
    # If it's already running it was configured successfully in a previous init.
    if state == "RUNNING":
        logger.info("Processor %s is RUNNING; skipping auto-terminate update.", proc_id)
        return

    version = data["revision"]["version"]
    config = data["component"]["config"]
    existing = set(config.get("autoTerminatedRelationships", []))
    desired = set(relationships)

    if desired.issubset(existing):
        logger.info("Processor %s already has terminal relationships configured.", proc_id)
        return

    config["autoTerminatedRelationships"] = relationships
    body = {
        "revision": {"version": version},
        "component": {"id": proc_id, "config": config},
    }
    _nifi("PUT", f"/processors/{proc_id}", json=body)


def _stop_processor(proc_id: str) -> None:
    data = _nifi("GET", f"/processors/{proc_id}").json()
    state = data["component"].get("state")

    if state == "STOPPED":
        logger.info("Processor %s already stopped.", proc_id)
        return

    if state != "RUNNING":
        logger.info("Processor %s state is %s; no stop requested.", proc_id, state)
        return

    version = data["revision"]["version"]
    body = {
        "revision": {"version": version},
        "state": "STOPPED",
    }
    _nifi("PUT", f"/processors/{proc_id}/run-status", json=body)

    for _ in range(60):
        time.sleep(2)
        state = _nifi("GET", f"/processors/{proc_id}").json()
        if state["component"]["state"] == "STOPPED":
            return

    raise RuntimeError(f"Processor {proc_id} did not stop in time.")


def _update_processor(proc_id: str, properties: dict) -> None:
    data = _nifi("GET", f"/processors/{proc_id}").json()
    component = data["component"]
    config = component["config"]
    current_properties = config.get("properties") or {}

    if _properties_match(current_properties, properties):
        logger.info("Processor %s already has desired properties.", proc_id)
        return

    if component.get("state") == "RUNNING":
        _stop_processor(proc_id)
        data = _nifi("GET", f"/processors/{proc_id}").json()
        component = data["component"]
        config = component["config"]
        current_properties = config.get("properties") or {}

    updated_properties = dict(current_properties)
    updated_properties.update(properties)
    config["properties"] = updated_properties

    body = {
        "revision": {"version": data["revision"]["version"]},
        "component": {
            "id": proc_id,
            "config": config,
        },
    }
    _nifi("PUT", f"/processors/{proc_id}", json=body)
    logger.info("Updated processor %s properties.", proc_id)


def _start_processor(proc_id: str) -> None:
    data = _nifi("GET", f"/processors/{proc_id}").json()
    state = data["component"].get("state")

    if state == "RUNNING":
        logger.info("Processor %s already running.", proc_id)
        return

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

    # Existing containers may still hold the pre-Avro flow. Stop and disable the
    # known components before reconciling properties so old JSON writers/readers
    # cannot survive a code update.
    existing_processors = _list_processors(pg_id)
    for proc_type, proc_name in [
        ("org.apache.nifi.processors.standard.ListenHTTP", "ListenHTTP"),
        (
            "org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_6",
            "PublishKafkaRecord",
        ),
    ]:
        existing = _find_component(existing_processors, proc_name, proc_type)
        if existing is not None:
            logger.info("Stopping existing processor %s before reconciliation.", proc_name)
            _stop_processor(existing["id"])

    existing_services = _list_controller_services(pg_id)
    for service_type, service_name in [
        ("org.apache.nifi.avro.AvroRecordSetWriter", "AvroRecordSetWriter"),
        ("org.apache.nifi.csv.CSVReader", "CSVReader"),
        (
            "org.apache.nifi.confluent.schemaregistry.ConfluentSchemaRegistry",
            "ConfluentSchemaRegistry",
        ),
    ]:
        existing = _find_component(existing_services, service_name, service_type)
        if existing is not None:
            logger.info(
                "Disabling existing controller service %s before reconciliation.",
                service_name,
            )
            _disable_controller_service(existing["id"])

    # ── Controller services ───────────────────────────────────────────────────

    logger.info("Ensuring ConfluentSchemaRegistry ...")
    sr_id = _ensure_controller_service(
        pg_id,
        "org.apache.nifi.confluent.schemaregistry.ConfluentSchemaRegistry",
        "ConfluentSchemaRegistry",
        {"url": SCHEMA_REGISTRY_URL},
    )

    logger.info("Ensuring CSVReader ...")
    csv_reader_id = _ensure_controller_service(
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

    logger.info("Ensuring AvroRecordSetWriter (Confluent) ...")
    avro_writer_id = _ensure_controller_service(
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

    logger.info("Ensuring ListenHTTP processor ...")
    listen_id = _ensure_processor(
        pg_id,
        "org.apache.nifi.processors.standard.ListenHTTP",
        "ListenHTTP",
        {
            "Listening Port": LISTEN_PORT,
            "Base Path": LISTEN_PATH.lstrip("/"),
        },
        {"x": 400, "y": 200},
    )

    logger.info("Ensuring PublishKafkaRecord processor ...")
    publish_id = _ensure_processor(
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
