from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyflink.common import RestartStrategies
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointCleanup
from pyflink.table import StreamTableEnvironment


@dataclass(frozen=True)
class FlinkRuntimeConfig:
    kafka_bootstrap: str
    kafka_topic: str
    kafka_consumer_group: str
    schema_registry_url: str
    schema_registry_subject: str

    parallelism: int
    checkpoint_interval_ms: int
    auto_watermark_interval_ms: int

    # ── Fault tolerance (checkpointing degli operatori con stato) ─────────────
    checkpointing_mode: str
    checkpoint_min_pause_ms: int
    checkpoint_timeout_ms: int
    checkpoint_max_concurrent: int
    checkpoint_tolerable_failures: int
    checkpoint_externalized: bool

    restart_attempts: int
    restart_delay_ms: int


def build_flink_runtime_config(cfg: dict[str, Any]) -> FlinkRuntimeConfig:
    schema_registry_cfg = cfg.get("schema_registry", {})
    flink_cfg = cfg["flink"]
    # I parametri di fault tolerance hanno default sicuri: gli esperimenti che
    # non li ridefiniscono (config/experiments/*.yml) restano validi.
    ckp = flink_cfg.get("checkpointing", {})
    restart = flink_cfg.get("restart", {})

    return FlinkRuntimeConfig(
        kafka_bootstrap=cfg["kafka"]["bootstrap_servers"],
        kafka_topic=cfg["kafka"]["topic"],
        kafka_consumer_group=flink_cfg["consumer_group"],
        schema_registry_url=str(
            schema_registry_cfg.get("url", "http://schema-registry:8081")
        ),
        schema_registry_subject=str(
            schema_registry_cfg.get("subject", "flights-value")
        ),
        parallelism=int(flink_cfg["parallelism"]),
        checkpoint_interval_ms=int(flink_cfg["checkpoint_interval_ms"]),
        auto_watermark_interval_ms=int(
            flink_cfg.get("auto_watermark_interval_ms", 200)
        ),
        checkpointing_mode=str(ckp.get("mode", "EXACTLY_ONCE")).upper(),
        checkpoint_min_pause_ms=int(ckp.get("min_pause_ms", 5000)),
        checkpoint_timeout_ms=int(ckp.get("timeout_ms", 60000)),
        checkpoint_max_concurrent=int(ckp.get("max_concurrent", 1)),
        checkpoint_tolerable_failures=int(ckp.get("tolerable_failures", 3)),
        checkpoint_externalized=bool(ckp.get("externalized", True)),
        restart_attempts=int(restart.get("attempts", 10)),
        restart_delay_ms=int(restart.get("delay_ms", 10000)),
    )


def _configure_fault_tolerance(
    env: StreamExecutionEnvironment,
    runtime_cfg: FlinkRuntimeConfig,
) -> None:
    """
    Applica il meccanismo di tolleranza ai guasti di Flink a TUTTI gli operatori
    con stato del job:

      * checkpoint periodici in modalita' EXACTLY_ONCE (barrier allineate), con
        snapshot coerente dello stato + offset Kafka su file:///opt/flink/checkpoints
        (state.checkpoints.dir, definito nelle FLINK_PROPERTIES del cluster);
      * checkpoint "externalized" (RETAIN_ON_CANCELLATION): l'ultimo checkpoint
        sopravvive a cancellazione/guasto ed e' riutilizzabile per il recovery;
      * restart strategy fixed-delay: a fronte del crash di un operatore o
        dell'intero TaskManager, il JobManager riavvia il job e ne ripristina lo
        stato dall'ultimo checkpoint completato. Senza restart strategy il job
        fallirebbe definitivamente invece di recuperare.
    """
    mode = (
        CheckpointingMode.EXACTLY_ONCE
        if runtime_cfg.checkpointing_mode == "EXACTLY_ONCE"
        else CheckpointingMode.AT_LEAST_ONCE
    )
    env.enable_checkpointing(runtime_cfg.checkpoint_interval_ms, mode)

    ckp = env.get_checkpoint_config()
    ckp.set_min_pause_between_checkpoints(runtime_cfg.checkpoint_min_pause_ms)
    ckp.set_checkpoint_timeout(runtime_cfg.checkpoint_timeout_ms)
    ckp.set_max_concurrent_checkpoints(runtime_cfg.checkpoint_max_concurrent)
    ckp.set_tolerable_checkpoint_failure_number(
        runtime_cfg.checkpoint_tolerable_failures
    )
    if runtime_cfg.checkpoint_externalized:
        ckp.set_externalized_checkpoint_cleanup(
            ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        )

    env.set_restart_strategy(
        RestartStrategies.fixed_delay_restart(
            runtime_cfg.restart_attempts,
            runtime_cfg.restart_delay_ms,
        )
    )


def create_stream_execution_environment(
    runtime_cfg: FlinkRuntimeConfig,
) -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(runtime_cfg.parallelism)

    _configure_fault_tolerance(env, runtime_cfg)

    # Periodic watermark emission interval. Smaller -> the watermark tracks the
    # data front more tightly (less "blind" event-time gap from batching), at the
    # cost of more watermark records flowing through the pipeline.
    env.get_config().set_auto_watermark_interval(
        runtime_cfg.auto_watermark_interval_ms
    )

    return env


def create_table_environment(
    runtime_cfg: FlinkRuntimeConfig,
) -> StreamTableEnvironment:
    return StreamTableEnvironment.create(
        create_stream_execution_environment(runtime_cfg)
    )
