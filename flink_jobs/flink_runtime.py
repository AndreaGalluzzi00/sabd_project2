from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


@dataclass(frozen=True)
class FlinkRuntimeConfig:
    kafka_bootstrap: str
    kafka_topic: str
    kafka_consumer_group: str

    parallelism: int
    checkpoint_interval_ms: int


def build_flink_runtime_config(cfg: dict[str, Any]) -> FlinkRuntimeConfig:
    return FlinkRuntimeConfig(
        kafka_bootstrap=cfg["kafka"]["bootstrap_servers"],
        kafka_topic=cfg["kafka"]["topic"],
        kafka_consumer_group=cfg["flink"]["consumer_group"],
        parallelism=int(cfg["flink"]["parallelism"]),
        checkpoint_interval_ms=int(cfg["flink"]["checkpoint_interval_ms"]),
    )


def create_table_environment(
    runtime_cfg: FlinkRuntimeConfig,
) -> StreamTableEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(runtime_cfg.parallelism)
    env.enable_checkpointing(runtime_cfg.checkpoint_interval_ms)

    return StreamTableEnvironment.create(env)