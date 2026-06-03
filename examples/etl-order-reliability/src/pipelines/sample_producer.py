from __future__ import annotations

from agora_plugins.kafka import KafkaSink
from sample_data import build_sample_order_batch
from settings import get_settings
from transforms import serialize_raw_kafka_value

from agora import DeliveryConfig, IterableSource, Pipeline, SinkFailurePolicy


async def build_pipeline():
    settings = get_settings()
    records = build_sample_order_batch(
        count=settings.sample_records_per_run,
        duplicate_every=settings.sample_duplicate_every,
        poison_every=settings.sample_poison_every,
    )

    source = IterableSource(records)
    sink = KafkaSink(
        topic=settings.kafka_raw_topic,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        serializer=serialize_raw_kafka_value,
        key_fn=lambda record: str(record["order_id"]).encode("utf-8"),
        linger_ms=settings.kafka.producer_linger_ms,
        compression_type="gzip",
        enable_idempotence=True,
    )

    return Pipeline(source, id="orders_sample_producer").build(
        sink,
        config=DeliveryConfig(
            batch_size=settings.sample_writer_batch_size,
            sink_failure_policy=SinkFailurePolicy.FAIL_CLOSED,
        ),
    )
