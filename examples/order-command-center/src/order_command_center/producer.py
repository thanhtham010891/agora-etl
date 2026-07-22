"""Create the Kafka topic and publish newly generated order lifecycle events."""

from __future__ import annotations

import asyncio
import contextlib
import json

from agora_plugins.kafka import KafkaSink

from order_command_center.contracts import generate_order_events
from order_command_center.settings import load_settings


async def ensure_topic(
    *,
    bootstrap_servers: str,
    topic: str,
    partitions: int,
    replication_factor: int,
) -> None:
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        with contextlib.suppress(TopicAlreadyExistsError):
            await admin.create_topics(
                [
                    NewTopic(
                        topic,
                        num_partitions=partitions,
                        replication_factor=replication_factor,
                    )
                ]
            )
    finally:
        await admin.close()


async def begin_producer_run(
    *,
    postgres_dsn: str,
    topic: str,
    producer_run_id: str,
    event_count: int,
    order_count: int,
    producer_runs_table: str,
) -> None:
    """Durably reserve a verification manifest before publishing to Kafka."""
    import psycopg

    async with await psycopg.AsyncConnection.connect(postgres_dsn) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                f"INSERT INTO {producer_runs_table} "
                "(producer_run_id, kafka_topic, expected_event_count, expected_order_count, publish_state) "
                "VALUES (%s, %s, %s, %s, 'publishing')",
                (producer_run_id, topic, event_count, order_count),
            )
        await connection.commit()


async def mark_producer_run(
    *,
    postgres_dsn: str,
    producer_run_id: str,
    state: str,
    failure_detail: str | None = None,
    producer_runs_table: str,
) -> None:
    """Advance a reserved manifest without inventing a second source of truth."""
    import psycopg

    async with await psycopg.AsyncConnection.connect(postgres_dsn) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                f"UPDATE {producer_runs_table} SET publish_state = %s, failure_detail = %s, "
                "updated_at = now() WHERE producer_run_id = %s",
                (state, failure_detail, producer_run_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"Producer manifest {producer_run_id!r} does not exist.")
        await connection.commit()


async def reconcile_producer_run(*, producer_run_id: str) -> None:
    """Finalize a run stranded after Kafka acknowledgement but before manifest update."""
    import psycopg

    settings = load_settings()
    async with await psycopg.AsyncConnection.connect(settings.postgres_dsn) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                "SELECT kafka_topic, expected_event_count, expected_order_count, publish_state "
                f"FROM {settings.tables.producer_runs} WHERE producer_run_id = %s",
                (producer_run_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise RuntimeError(f"Producer manifest {producer_run_id!r} was not found.")
            topic, expected_events, expected_orders, state = row
            if state != "publishing":
                raise RuntimeError(
                    f"Producer manifest {producer_run_id!r} is {state!r}, not a recoverable publishing run."
                )
            await cursor.execute(
                f"SELECT count(*), count(DISTINCT order_id) FROM {settings.tables.event_ledger} "
                "WHERE kafka_topic = %s AND producer_run_id = %s",
                (topic, producer_run_id),
            )
            observed_events, observed_orders = map(int, await cursor.fetchone())
            if observed_events != expected_events or observed_orders != expected_orders:
                raise RuntimeError(
                    "Cannot reconcile producer manifest before PostgreSQL has the complete Kafka run: "
                    f"events={observed_events}/{expected_events}, orders={observed_orders}/{expected_orders}."
                )
            await cursor.execute(
                f"UPDATE {settings.tables.producer_runs} SET publish_state = 'published', updated_at = now() "
                "WHERE producer_run_id = %s",
                (producer_run_id,),
            )
        await connection.commit()
    print(f"reconciled producer_run_id={producer_run_id}")


async def run(
    *,
    order_count: int = 100,
    delay_seconds: float = 0.0,
    progress_every: int = 100,
    flush_every: int = 100,
    seed: int | None = None,
    event_version: int = 1,
) -> int:
    if progress_every < 1:
        raise ValueError("progress_every must be at least 1")
    if flush_every < 1:
        raise ValueError("flush_every must be at least 1")
    if event_version not in {1, 2}:
        raise ValueError("event_version must be 1 or 2")
    settings = load_settings()
    events = generate_order_events(order_count=order_count, seed=seed, event_version=event_version)
    producer_run_id = str(events[0]["producer_run_id"])
    await begin_producer_run(
        postgres_dsn=settings.postgres_dsn,
        topic=settings.kafka_topic,
        producer_run_id=producer_run_id,
        event_count=len(events),
        order_count=order_count,
        producer_runs_table=settings.tables.producer_runs,
    )
    try:
        await ensure_topic(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_topic,
            partitions=settings.kafka_topic_partitions,
            replication_factor=settings.kafka_topic_replication_factor,
        )
        sink = KafkaSink(
            topic=settings.kafka_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            serializer=lambda row: json.dumps(row, sort_keys=True).encode(),
            key_fn=lambda row: str(row["order_id"]).encode(),
        )
        await sink.open()
        try:
            for position, event in enumerate(events, start=1):
                await sink.write(event)
                is_flush_boundary = position % flush_every == 0 or position == len(events)
                if is_flush_boundary:
                    # ``write`` preserves per-key enqueue order; acknowledgement is
                    # batched to avoid one Kafka round trip for every event.
                    await sink.flush()
                if is_flush_boundary and (
                    position == len(events) or position % progress_every == 0
                ):
                    print(
                        f"kafka published {position}/{len(events)} "
                        f"last_order={event['order_id']} status={event['status']}"
                    )
                if delay_seconds > 0:
                    await asyncio.sleep(delay_seconds)
        finally:
            await sink.close()
    except Exception as exc:
        with contextlib.suppress(Exception):
            await mark_producer_run(
                postgres_dsn=settings.postgres_dsn,
                producer_run_id=producer_run_id,
                state="failed",
                producer_runs_table=settings.tables.producer_runs,
                failure_detail=(
                    "Kafka publishing did not complete; the topic may contain a partial run. "
                    f"Last error: {type(exc).__name__}."
                ),
            )
        raise
    try:
        await mark_producer_run(
            postgres_dsn=settings.postgres_dsn,
            producer_run_id=producer_run_id,
            state="published",
            producer_runs_table=settings.tables.producer_runs,
        )
    except Exception as exc:
        raise RuntimeError(
            "Kafka publishing completed but the producer manifest remains in 'publishing'. "
            f"After PostgreSQL catches up, run `order-demo-reconcile {producer_run_id}`."
        ) from exc
    print(f"published={len(events)} topic={settings.kafka_topic} producer_run_id={producer_run_id}")
    return len(events)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--orders", type=int, default=3, help="Number of new orders to publish.")
    parser.add_argument("--delay-seconds", type=float, default=0.0)
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="Emit producer progress after this many Kafka records.",
    )
    parser.add_argument(
        "--flush-every",
        type=int,
        default=100,
        help="Wait for Kafka acknowledgements after this many records (default: 100).",
    )
    parser.add_argument("--seed", type=int, help="Deterministic event generation for tests only.")
    parser.add_argument(
        "--event-version",
        type=int,
        choices=(1, 2),
        default=1,
        help="Publish the V1 or V2 order-event contract.",
    )
    parser.add_argument(
        "--reconcile",
        metavar="PRODUCER_RUN_ID",
        help="Finalize a manifest left publishing after Kafka acknowledgement.",
    )
    args = parser.parse_args()
    if args.reconcile:
        asyncio.run(reconcile_producer_run(producer_run_id=args.reconcile))
        return
    asyncio.run(
        run(
            order_count=args.orders,
            delay_seconds=args.delay_seconds,
            progress_every=args.progress_every,
            flush_every=args.flush_every,
            seed=args.seed,
            event_version=args.event_version,
        )
    )


if __name__ == "__main__":
    main()
