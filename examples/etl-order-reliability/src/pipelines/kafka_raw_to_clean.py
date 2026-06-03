from __future__ import annotations

from agora_plugins.kafka import KafkaSink, KafkaSource
from agora_plugins.redis import RedisStore
from runtime_support import build_checkpoint_store, build_dlq_sink
from settings import get_settings
from transforms import deserialize_raw_kafka_value, normalize_order, serialize_kafka_value

from agora import DeliveryConfig, MapMiddleware, Pipeline, SinkFailurePolicy
from agora.middlewares.dedup import DedupMiddleware


async def build_pipeline():
    settings = get_settings()

    source = KafkaSource(
        topics=[settings.kafka_raw_topic],
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=settings.kafka_raw_group_id,
        deserializer=deserialize_raw_kafka_value,
        enable_auto_commit=False,
        commit_every=100,
        max_poll_records=settings.kafka.consumer_max_poll_records,
        fetch_min_bytes=settings.kafka.consumer_fetch_min_bytes,
        fetch_max_wait_ms=settings.kafka.consumer_fetch_max_wait_ms,
        max_partition_fetch_bytes=settings.kafka.consumer_max_partition_fetch_bytes,
    )
    sink = KafkaSink(
        topic=settings.kafka_clean_topic,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        serializer=serialize_kafka_value,
        key_fn=lambda event: event.order_id.encode("utf-8"),
        linger_ms=settings.kafka.producer_linger_ms,
        compression_type="gzip",
        enable_idempotence=True,
    )

    return (
        Pipeline(source, id="orders_normalize")
        .pipe(MapMiddleware(normalize_order, name="normalize_order"))
        .pipe(
            DedupMiddleware(
                key=lambda event: event.event_id,
                store=RedisStore(
                    url=settings.redis_url,
                    key_prefix=settings.redis_dedup_prefix,
                ),
                name="dedup_by_event_id",
            )
        )
        .build(
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
    )
