"""Kafka to Redis current-state serving projection."""

from __future__ import annotations

import argparse
import asyncio
from functools import partial

from agora_plugins.kafka import KafkaPoisonRecordPolicy, KafkaSource
from agora_plugins.postgres import PostgresDLQSink
from agora_plugins.redis import build_kafka_redis_sink, wrap_kafka_redis_deserializer

from agora import DeliveryConfig, MapMiddleware, Pipeline
from order_command_center.contracts import deserialize_event, redis_view
from order_command_center.pipelines.base import (
    ProjectionRunOptions,
    add_common_arguments,
    execute_projection,
    options_from_arguments,
)
from order_command_center.runtime import FlushBeforeAcknowledgeSink
from order_command_center.settings import load_settings


async def run(*, options: ProjectionRunOptions) -> int:
    """Build and run the Redis projection for one invocation."""

    settings = load_settings()
    batch_size = settings.projection_batch_size if options.forever else 1
    projection = settings.redis_projection

    async def build_pipeline() -> object:
        source = KafkaSource(
            topics=[settings.kafka_topic],
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=projection.consumer_group,
            deserializer=wrap_kafka_redis_deserializer(deserialize_event),
            enable_auto_commit=False,
            commit_every=settings.projection_batch_size,
            poll_timeout_ms=settings.projection_poll_timeout_ms,
            max_idle_polls=settings.projection_idle_polls,
            poison_record_policy=KafkaPoisonRecordPolicy.DLQ_AND_CONTINUE,
            poison_record_sink=PostgresDLQSink(
                settings.postgres_dsn,
                table=settings.tables.dead_letter_queue,
            ),
            poison_record_pipeline_id=projection.pipeline_id,
        )
        sink = build_kafka_redis_sink(
            url=settings.redis_url,
            key_fn=lambda row: str(row["redis_key"]),
            mode="set",
            ttl_seconds=settings.redis_order_ttl_seconds,
        )
        return (
            Pipeline(source, id=projection.pipeline_id)
            .pipe(
                MapMiddleware(
                    partial(redis_view, key_prefix=settings.redis_key_prefix),
                    name=projection.mapper_name,
                )
            )
            .build(
                FlushBeforeAcknowledgeSink(sink),
                config=DeliveryConfig(
                    batch_size=batch_size,
                    batch_flush_interval_ms=settings.projection_flush_interval_ms,
                ),
            )
        )

    return await execute_projection(
        settings=settings,
        projection=projection,
        options=options,
        build_pipeline=build_pipeline,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_common_arguments(parser)
    arguments = parser.parse_args()
    asyncio.run(run(options=options_from_arguments(arguments)))


if __name__ == "__main__":
    main()
