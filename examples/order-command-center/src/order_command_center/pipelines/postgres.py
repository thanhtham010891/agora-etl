"""Kafka to PostgreSQL durable event-ledger projection."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from agora_plugins.postgres import (
    KafkaPostgresDeliveryConfig,
    KafkaPostgresPoisonDLQConfig,
    build_kafka_postgres_sink,
    build_kafka_postgres_source,
)

from agora import DeliveryConfig, MapMiddleware, Pipeline
from order_command_center.contracts import deserialize_event, postgres_row
from order_command_center.pipelines.base import (
    ProjectionRunOptions,
    add_common_arguments,
    execute_projection,
    options_from_arguments,
)
from order_command_center.runtime import FailOnceAfterFlush, FlushBeforeAcknowledgeSink
from order_command_center.settings import load_settings


async def run(
    *,
    options: ProjectionRunOptions,
    fail_after_flush: bool = False,
    hard_crash_after_flush: bool = False,
) -> int:
    """Build and run the PostgreSQL projection for one invocation."""

    settings = load_settings()
    batch_size = settings.projection_batch_size if options.forever else 1
    projection = settings.postgres_projection

    async def build_pipeline() -> object:
        source = build_kafka_postgres_source(
            topics=[settings.kafka_topic],
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=projection.consumer_group,
            deserializer=deserialize_event,
            commit_every=settings.projection_batch_size,
            poll_timeout_ms=settings.projection_poll_timeout_ms,
            max_idle_polls=settings.projection_idle_polls,
            poison_dlq=KafkaPostgresPoisonDLQConfig(
                dsn=settings.postgres_dsn,
                table=settings.tables.dead_letter_queue,
                pipeline_id=projection.pipeline_id,
            ),
        )
        sink = build_kafka_postgres_sink(
            dsn=settings.postgres_dsn,
            table=settings.tables.event_ledger,
            row_mapper=lambda row: row,
            batch_size=batch_size,
            delivery=KafkaPostgresDeliveryConfig(metadata_field=None),
        )
        if fail_after_flush or hard_crash_after_flush:
            sink = FailOnceAfterFlush(
                sink,
                Path(settings.crash_marker_path),
                hard_exit=hard_crash_after_flush,
            )
        sink = FlushBeforeAcknowledgeSink(sink)
        return (
            Pipeline(source, id=projection.pipeline_id)
            .pipe(MapMiddleware(postgres_row, name=projection.mapper_name))
            .build(
                sink,
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
    parser.add_argument("--fail-after-flush", action="store_true")
    parser.add_argument(
        "--hard-crash-after-flush",
        action="store_true",
        help="Terminate the process after sink flush, before Kafka offset acknowledgement.",
    )
    arguments = parser.parse_args()
    asyncio.run(
        run(
            options=options_from_arguments(arguments),
            fail_after_flush=arguments.fail_after_flush,
            hard_crash_after_flush=arguments.hard_crash_after_flush,
        )
    )


if __name__ == "__main__":
    main()
