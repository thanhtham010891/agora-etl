"""Verify the newest producer batch without scanning historical order data."""

from __future__ import annotations

import asyncio
import json
from time import monotonic

from order_command_center.settings import load_settings


class ProjectionNotConvergedError(RuntimeError):
    """A valid producer run whose independent projections are still catching up."""


class NoPublishedProducerRunError(RuntimeError):
    """Verification has no completed producer run to inspect."""


async def _validate_latest_run() -> dict[str, object]:
    import psycopg
    import redis.asyncio as redis

    settings = load_settings()
    async with (
        await psycopg.AsyncConnection.connect(settings.postgres_dsn) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            "SELECT producer_run_id, expected_event_count, expected_order_count "
            f"FROM {settings.tables.producer_runs} WHERE kafka_topic = %s AND publish_state = 'published' "
            "ORDER BY published_at DESC, producer_run_id DESC LIMIT 1",
            (settings.kafka_topic,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise NoPublishedProducerRunError(
                f"No completed producer batch is registered for topic {settings.kafka_topic!r}. "
                "Run `make producer` to publish a batch, then run `make verify` again."
            )
        producer_run_id, expected_event_count, expected_order_count = row
        await cursor.execute(
            "SELECT count(*), count(DISTINCT kafka_delivery_key), count(DISTINCT event_id) "
            f"FROM {settings.tables.event_ledger} "
            "WHERE kafka_topic = %s AND producer_run_id = %s",
            (settings.kafka_topic, producer_run_id),
        )
        run_delivery_rows, distinct_delivery_keys, logical_events = map(
            int, await cursor.fetchone()
        )
        if run_delivery_rows < expected_event_count:
            raise ProjectionNotConvergedError(
                "PostgreSQL ledger is catching up: "
                f"{run_delivery_rows}/{expected_event_count} deliveries persisted."
            )
        if run_delivery_rows > expected_event_count:
            raise RuntimeError(
                "PostgreSQL ledger has more rows than the completed producer manifest: "
                f"{run_delivery_rows}/{expected_event_count}."
            )
        if run_delivery_rows != distinct_delivery_keys:
            raise RuntimeError(
                "PostgreSQL ledger delivery-key uniqueness failed: "
                f"rows={run_delivery_rows}, distinct_delivery_keys={distinct_delivery_keys}."
            )
        if logical_events != expected_event_count:
            raise RuntimeError(
                "PostgreSQL ledger event-id uniqueness failed: "
                f"distinct_event_ids={logical_events}, expected={expected_event_count}."
            )

        await cursor.execute(
            f"SELECT count(*) FROM {settings.tables.current_state} "
            "WHERE kafka_topic = %s AND producer_run_id = %s ",
            (settings.kafka_topic, producer_run_id),
        )
        current_order_rows = int((await cursor.fetchone())[0])
        if current_order_rows < expected_order_count:
            raise ProjectionNotConvergedError(
                "PostgreSQL current-state projection is catching up: "
                f"{current_order_rows}/{expected_order_count} orders projected."
            )
        if current_order_rows > expected_order_count:
            raise RuntimeError(
                "PostgreSQL current-state has more rows than the completed producer manifest: "
                f"{current_order_rows}/{expected_order_count}."
            )

        await cursor.execute(
            f"SELECT order_id, event_id, status FROM {settings.tables.current_state} "
            "WHERE kafka_topic = %s AND producer_run_id = %s "
            "ORDER BY event_time DESC, kafka_delivery_key DESC",
            (settings.kafka_topic, producer_run_id),
        )
        expected_orders = [
            (str(order_id), str(event_id), str(status))
            for order_id, event_id, status in await cursor.fetchall()
        ]

    expected_event_count = int(expected_event_count)
    expected_order_count = int(expected_order_count)
    if len(expected_orders) != expected_order_count:
        raise RuntimeError(
            "PostgreSQL current-state count changed during verification: "
            f"{len(expected_orders)}/{expected_order_count}."
        )
    if not all(status == "packed" for _, _, status in expected_orders):
        raise RuntimeError(
            "PostgreSQL current-state is not converged to packed for the latest run."
        )

    client = redis.from_url(settings.redis_url, decode_responses=True)
    cache_misses = 0
    sample_orders: dict[str, dict[str, object]] = {}
    try:
        for offset in range(0, len(expected_orders), settings.verify_redis_mget_chunk_size):
            batch = expected_orders[offset : offset + settings.verify_redis_mget_chunk_size]
            keys = [f"{settings.redis_key_prefix}:current:{order_id}" for order_id, _, _ in batch]
            values = await client.mget(keys)
            for (order_id, expected_event_id, expected_status), value in zip(
                batch, values, strict=True
            ):
                if value is None:
                    cache_misses += 1
                    continue
                cache_row = json.loads(value)
                if (
                    cache_row.get("event_id") != expected_event_id
                    or cache_row.get("status") != expected_status
                ):
                    cache_misses += 1
                    continue
                if len(sample_orders) < settings.verify_sample_order_limit:
                    sample_orders[order_id] = cache_row
    finally:
        await client.aclose()

    if cache_misses:
        raise ProjectionNotConvergedError(
            "Redis cache is catching up or has stale values: "
            f"{cache_misses}/{expected_order_count} keys are not converged."
        )
    return {
        "producer_run_id": str(producer_run_id),
        "expected_delivery_rows": expected_event_count,
        "run_delivery_rows": run_delivery_rows,
        "orders_checked": expected_order_count,
        "redis_mget_chunk_size": settings.verify_redis_mget_chunk_size,
        "redis_cache_misses": cache_misses,
        "sample_current_orders": sample_orders,
        "result": "PASS: latest producer batch is ledgered once and Redis converged",
    }


async def run(*, wait_seconds: float = 0.0) -> None:
    deadline = monotonic() + wait_seconds
    delay_seconds = 0.25
    while True:
        try:
            payload = await _validate_latest_run()
        except ProjectionNotConvergedError as exc:
            if monotonic() >= deadline:
                raise TimeoutError(
                    f"Verification timed out after {wait_seconds:g}s: {exc} "
                    "Check `make status` and `make logs`; the producer batch remains durable "
                    "in Kafka and workers will continue from their committed offsets."
                ) from exc
            await asyncio.sleep(min(delay_seconds, max(0.0, deadline - monotonic())))
            delay_seconds = min(delay_seconds * 2, 2.0)
            continue
        print(json.dumps(payload, indent=2, sort_keys=True))
        return


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--wait-seconds",
        type=float,
        default=0.0,
        help="Wait for the background projections to converge before failing.",
    )
    args = parser.parse_args()
    try:
        asyncio.run(run(wait_seconds=args.wait_seconds))
    except NoPublishedProducerRunError as exc:
        parser.exit(2, f"verification precondition failed: {exc}\n")
    except ProjectionNotConvergedError as exc:
        parser.exit(1, f"verification pending: {exc}\n")
    except TimeoutError as exc:
        parser.exit(1, f"verification timed out: {exc}\n")


if __name__ == "__main__":
    main()
