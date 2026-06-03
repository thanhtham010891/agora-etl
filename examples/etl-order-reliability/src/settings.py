from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from agora_plugins.kafka import KafkaConfig
from agora_plugins.postgres import PostgresConfig
from pydantic import Field
from pydantic_settings import SettingsConfigDict

from agora.config import AgoraSettings


def _default_kafka_config() -> KafkaConfig:
    return KafkaConfig(bootstrap_servers="localhost:19092,localhost:19093,localhost:19094")


def _default_postgres_config() -> PostgresConfig:
    return PostgresConfig(database_url="postgresql://agora:agora@localhost:5432/agora_test")


class Settings(AgoraSettings):
    model_config = SettingsConfigDict(
        env_file=str(Path(__file__).resolve().parents[1] / "agora.env"),
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    kafka: KafkaConfig = Field(default_factory=_default_kafka_config)
    postgres: PostgresConfig = Field(default_factory=_default_postgres_config)

    redis_url: str = Field(default="redis://localhost:16379", alias="REDIS_URL")
    redis_dlq_prefix: str = Field(
        default="agora:dlq:orders-demo",
        alias="REDIS_DLQ_PREFIX",
    )
    redis_dedup_prefix: str = Field(
        default="agora:dedup:orders-demo:",
        alias="REDIS_DEDUP_PREFIX",
    )
    redis_checkpoint_prefix: str = Field(
        default="agora:checkpoint:orders-demo:",
        alias="REDIS_CHECKPOINT_PREFIX",
    )

    kafka_raw_topic: str = Field(default="demo.orders.raw", alias="KAFKA_RAW_TOPIC")
    kafka_clean_topic: str = Field(default="demo.orders.cleaned", alias="KAFKA_CLEAN_TOPIC")
    kafka_raw_group_id: str = Field(default="orders-normalizer", alias="KAFKA_RAW_GROUP_ID")
    kafka_clean_group_id: str = Field(default="orders-projector", alias="KAFKA_CLEAN_GROUP_ID")

    postgres_projection_table: str = Field(
        default="public.order_projection",
        alias="POSTGRES_PROJECTION_TABLE",
    )

    worker_batch_flush_interval_ms: int = Field(
        default=100,
        alias="WORKER_BATCH_FLUSH_INTERVAL_MS",
    )
    sample_records_per_run: int = Field(default=5_000, alias="SAMPLE_RECORDS_PER_RUN")
    sample_duplicate_every: int = Field(default=250, alias="SAMPLE_DUPLICATE_EVERY")
    sample_poison_every: int = Field(default=1_000, alias="SAMPLE_POISON_EVERY")
    sample_writer_batch_size: int = Field(default=500, alias="SAMPLE_WRITER_BATCH_SIZE")
    health_host: str = Field(default="127.0.0.1", alias="HEALTH_HOST")
    health_port: int = Field(default=8080, alias="HEALTH_PORT")
    health_auth_token: str | None = Field(default=None, alias="HEALTH_AUTH_TOKEN")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
