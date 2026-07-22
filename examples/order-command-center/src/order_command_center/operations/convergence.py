"""Inspect Kafka consumer-group convergence without relying on scrape timing."""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict, dataclass

from order_command_center.settings import load_settings


@dataclass(frozen=True, slots=True)
class PartitionLag:
    """One group's committed position relative to one Kafka partition end offset."""

    partition: int
    committed_offset: int | None
    end_offset: int
    lag: int


@dataclass(frozen=True, slots=True)
class ConsumerGroupConvergence:
    """Machine-readable lag snapshot for an isolated projection group."""

    consumer_group: str
    topic: str
    partitions: tuple[PartitionLag, ...]

    @property
    def lag(self) -> int:
        return sum(partition.lag for partition in self.partitions)


async def inspect_consumer_group(
    *,
    bootstrap_servers: str,
    topic: str,
    consumer_group: str,
) -> ConsumerGroupConvergence:
    """Read Kafka end offsets and one consumer group's committed offsets."""

    from aiokafka import AIOKafkaConsumer, TopicPartition
    from aiokafka.admin import AIOKafkaAdminClient

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap_servers)
    consumer_started = False
    try:
        topic_metadata = await admin.describe_topics([topic])
        partition_ids = {
            int(partition["partition"]) for partition in topic_metadata[0].get("partitions", [])
        }
        if not partition_ids:
            raise RuntimeError(f"Kafka topic {topic!r} has no visible partitions.")
        partitions = [TopicPartition(topic, partition) for partition in sorted(partition_ids)]
        await consumer.start()
        consumer_started = True
        end_offsets = await consumer.end_offsets(partitions)
        committed_offsets = await admin.list_consumer_group_offsets(
            consumer_group,
            partitions=partitions,
        )
    finally:
        await admin.close()
        if consumer_started:
            await consumer.stop()

    snapshots = tuple(
        PartitionLag(
            partition=partition.partition,
            committed_offset=_committed_offset(committed_offsets.get(partition)),
            end_offset=int(end_offsets[partition]),
            lag=max(
                int(end_offsets[partition])
                - (_committed_offset(committed_offsets.get(partition)) or 0),
                0,
            ),
        )
        for partition in partitions
    )
    return ConsumerGroupConvergence(
        consumer_group=consumer_group,
        topic=topic,
        partitions=snapshots,
    )


def _committed_offset(offset_metadata: object | None) -> int | None:
    """Normalize Kafka's ``-1`` no-commit sentinel to an absent offset."""

    if offset_metadata is None:
        return None
    offset = int(offset_metadata.offset)  # type: ignore[attr-defined]
    return offset if offset >= 0 else None


async def require_zero_lag(
    *,
    bootstrap_servers: str,
    topic: str,
    consumer_groups: tuple[str, ...],
) -> tuple[ConsumerGroupConvergence, ...]:
    """Raise with exact offsets unless every requested group has caught up."""

    snapshots = tuple(
        [
            await inspect_consumer_group(
                bootstrap_servers=bootstrap_servers,
                topic=topic,
                consumer_group=consumer_group,
            )
            for consumer_group in consumer_groups
        ]
    )
    lagging = [snapshot for snapshot in snapshots if snapshot.lag]
    if lagging:
        details = "; ".join(
            f"{snapshot.consumer_group} lag={snapshot.lag} "
            f"partitions={[asdict(partition) for partition in snapshot.partitions if partition.lag]}"
            for snapshot in lagging
        )
        raise RuntimeError(f"Kafka consumer groups have not converged: {details}")
    return snapshots


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group", action="append", dest="groups")
    parser.add_argument("--require-zero", action="store_true")
    arguments = parser.parse_args()
    settings = load_settings()
    groups = (
        tuple(arguments.groups)
        if arguments.groups
        else (
            settings.postgres_projection.consumer_group,
            settings.redis_projection.consumer_group,
        )
    )

    async def inspect() -> tuple[ConsumerGroupConvergence, ...]:
        if arguments.require_zero:
            return await require_zero_lag(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                topic=settings.kafka_topic,
                consumer_groups=groups,
            )
        return tuple(
            [
                await inspect_consumer_group(
                    bootstrap_servers=settings.kafka_bootstrap_servers,
                    topic=settings.kafka_topic,
                    consumer_group=group,
                )
                for group in groups
            ]
        )

    snapshots = asyncio.run(inspect())
    print(
        json.dumps([asdict(snapshot) | {"lag": snapshot.lag} for snapshot in snapshots], indent=2)
    )


if __name__ == "__main__":
    main()
