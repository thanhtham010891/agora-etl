from __future__ import annotations

from agora_plugins.kafka import KafkaSource
from agora_plugins.postgres import PostgresSink
from runtime_support import build_checkpoint_store, build_dlq_sink
from settings import get_settings
from transforms import deserialize_kafka_value, projection_row

from agora import DeliveryConfig, Pipeline, SinkFailurePolicy


async def build_pipeline():
    settings = get_settings()

    source = KafkaSource(
        topics=[settings.kafka_clean_topic],
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=settings.kafka_clean_group_id,
        deserializer=deserialize_kafka_value,
        enable_auto_commit=False,
        commit_every=100,
        max_poll_records=settings.kafka.consumer_max_poll_records,
        fetch_min_bytes=settings.kafka.consumer_fetch_min_bytes,
        fetch_max_wait_ms=settings.kafka.consumer_fetch_max_wait_ms,
        max_partition_fetch_bytes=settings.kafka.consumer_max_partition_fetch_bytes,
    )
    sink = PostgresSink(
        dsn=settings.postgres.database_url,
        table=settings.postgres_projection_table,
        row_mapper=projection_row,
        conflict_key="order_id",
        batch_size=settings.postgres.sink_batch_size,
        insert_mode="copy_merge",
        pool_size=settings.postgres.pool_size,
        max_rows_per_statement=settings.postgres.sink_max_rows_per_statement,
        max_parameters_per_statement=settings.postgres.sink_max_parameters_per_statement,
    )

    return Pipeline(source, id="orders_projection").build(
        sink,
        config=DeliveryConfig(
            dlq=build_dlq_sink(settings),
            checkpoint=build_checkpoint_store(settings),
            checkpoint_every=100,
            batch_size=100,
            batch_flush_interval_ms=settings.worker_batch_flush_interval_ms,
            sink_failure_policy=SinkFailurePolicy.FAIL_CLOSED,
        ),
    )
