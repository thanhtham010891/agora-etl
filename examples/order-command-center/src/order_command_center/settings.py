"""Environment-backed settings for the demo."""

from __future__ import annotations

import re
from dataclasses import dataclass
from os import getenv

_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class ProjectionSettings:
    """Identity and observable names for one independently deployed projection."""

    pipeline_id: str
    process_name: str
    consumer_group: str
    mapper_name: str


@dataclass(frozen=True, slots=True)
class DatabaseTables:
    """Validated table identifiers used by the example's operational code."""

    event_ledger: str
    current_state: str
    dead_letter_queue: str
    producer_runs: str
    replay_requests: str
    replay_audit: str
    schema_migrations: str


@dataclass(frozen=True, slots=True)
class Settings:
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_topic_partitions: int
    kafka_topic_replication_factor: int
    postgres_dsn: str
    redis_url: str
    redis_key_prefix: str
    redis_order_ttl_seconds: int
    projection_batch_size: int
    projection_flush_interval_ms: int
    projection_poll_timeout_ms: int
    projection_idle_polls: int
    projection_idle_log_interval_seconds: int
    projection_error_backoff_seconds: int
    projection_max_consecutive_errors: int
    metrics_host: str
    metrics_port: int | None
    metrics_auth_token: str | None
    postgres_projection: ProjectionSettings
    redis_projection: ProjectionSettings
    tables: DatabaseTables
    crash_marker_path: str
    dlq_inspection_limit: int
    verify_redis_mget_chunk_size: int
    verify_sample_order_limit: int


def load_settings() -> Settings:
    return Settings(
        kafka_bootstrap_servers=getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        kafka_topic=getenv("KAFKA_TOPIC", "commerce.orders.v1"),
        kafka_topic_partitions=_positive_int_env("KAFKA_TOPIC_PARTITIONS", 3),
        kafka_topic_replication_factor=_positive_int_env("KAFKA_TOPIC_REPLICATION_FACTOR", 1),
        postgres_dsn=getenv("POSTGRES_DSN", "postgresql://agora:agora@localhost:15432/agora_demo"),
        redis_url=getenv("REDIS_URL", "redis://localhost:16379/0"),
        redis_key_prefix=getenv("REDIS_KEY_PREFIX", "demo:orders"),
        redis_order_ttl_seconds=_positive_int_env("REDIS_ORDER_TTL_SECONDS", 604_800),
        projection_batch_size=_positive_int_env("PROJECTION_BATCH_SIZE", 500),
        projection_flush_interval_ms=_positive_int_env("PROJECTION_FLUSH_INTERVAL_MS", 250),
        projection_poll_timeout_ms=_positive_int_env("PROJECTION_POLL_TIMEOUT_MS", 250),
        projection_idle_polls=_positive_int_env("PROJECTION_IDLE_POLLS", 3),
        projection_idle_log_interval_seconds=_positive_int_env(
            "PROJECTION_IDLE_LOG_INTERVAL_SECONDS", 60
        ),
        projection_error_backoff_seconds=_positive_int_env("PROJECTION_ERROR_BACKOFF_SECONDS", 5),
        projection_max_consecutive_errors=_positive_int_env("PROJECTION_MAX_CONSECUTIVE_ERRORS", 5),
        metrics_host=getenv("METRICS_HOST", "0.0.0.0"),
        metrics_port=_optional_port_env("METRICS_PORT"),
        metrics_auth_token=getenv("METRICS_AUTH_TOKEN") or None,
        postgres_projection=_projection_settings(
            prefix="POSTGRES",
            defaults=ProjectionSettings(
                pipeline_id="order-postgres-projection",
                process_name="order-postgres-worker",
                consumer_group="order-postgres-v1",
                mapper_name="map-kafka-ledger-row",
            ),
        ),
        redis_projection=_projection_settings(
            prefix="REDIS",
            defaults=ProjectionSettings(
                pipeline_id="order-redis-projection",
                process_name="order-redis-worker",
                consumer_group="order-redis-v1",
                mapper_name="map-current-order-cache-row",
            ),
        ),
        tables=DatabaseTables(
            event_ledger=_identifier_env("EVENT_LEDGER_TABLE", "order_event_ledger"),
            current_state=_identifier_env("CURRENT_STATE_TABLE", "order_current_state"),
            dead_letter_queue=_identifier_env("DLQ_TABLE", "agora_dlq"),
            producer_runs=_identifier_env("PRODUCER_RUNS_TABLE", "order_producer_runs"),
            replay_requests=_identifier_env("REPLAY_REQUESTS_TABLE", "order_dlq_replay_requests"),
            replay_audit=_identifier_env("REPLAY_AUDIT_TABLE", "order_dlq_replay_audit"),
            schema_migrations=_identifier_env("SCHEMA_MIGRATIONS_TABLE", "agora_schema_migrations"),
        ),
        crash_marker_path=getenv("CRASH_MARKER_PATH", ".demo-state/postgres-flushed.marker"),
        dlq_inspection_limit=_positive_int_env("DLQ_INSPECTION_LIMIT", 100),
        verify_redis_mget_chunk_size=_positive_int_env("VERIFY_REDIS_MGET_CHUNK_SIZE", 500),
        verify_sample_order_limit=_positive_int_env("VERIFY_SAMPLE_ORDER_LIMIT", 5),
    )


def _projection_settings(*, prefix: str, defaults: ProjectionSettings) -> ProjectionSettings:
    return ProjectionSettings(
        pipeline_id=getenv(f"{prefix}_PIPELINE_ID", defaults.pipeline_id),
        process_name=getenv(f"{prefix}_PROCESS_NAME", defaults.process_name),
        consumer_group=getenv(f"{prefix}_GROUP", defaults.consumer_group),
        mapper_name=getenv(f"{prefix}_MAPPER_NAME", defaults.mapper_name),
    )


def _positive_int_env(name: str, default: int) -> int:
    value = int(getenv(name, str(default)))
    if value < 1:
        raise ValueError(f"{name} must be a positive integer.")
    return value


def _optional_port_env(name: str) -> int | None:
    raw_value = getenv(name)
    if raw_value in (None, ""):
        return None
    value = int(raw_value)
    if not 1 <= value <= 65_535:
        raise ValueError(f"{name} must be a TCP port between 1 and 65535.")
    return value


def _identifier_env(name: str, default: str) -> str:
    value = getenv(name, default)
    if not _SQL_IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} must be a simple SQL identifier.")
    return value
