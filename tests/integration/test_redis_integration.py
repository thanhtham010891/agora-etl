"""
tests/integration/test_redis_integration.py
============================================
Integration tests for Redis plugin (`agora_plugins.redis`).

Requires:
- ``AGORA_RUN_INTEGRATION=1`` environment variable
- Redis reachable at ``127.0.0.1:16379``
- ``agora-etl-redis`` plugin installed (`pip install "agora-etl-plugins[redis]"`)

Run with::

    AGORA_RUN_INTEGRATION=1 pytest tests/integration/test_redis_integration.py -v

Requirements: 2.17
"""

from __future__ import annotations

import asyncio
import json

import pytest

# Skip entire module when the Redis plugin is not installed.
agora_redis = pytest.importorskip("agora_plugins.redis")
aioredis = pytest.importorskip("redis.asyncio")

from agora import IterableSource, Pipeline  # noqa: E402
from agora.middlewares.dedup.middleware import DedupMiddleware  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_N_RECORDS = 8
_INTEGRATION_TIMEOUT_S = 20.0


def _make_records(n: int, suffix: str) -> list[dict]:
    """Return a list of simple dicts with a unique key per record."""
    return [{"id": i, "value": f"val_{i}_{suffix}"} for i in range(n)]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_sink_write_and_read(
    redis_url: str,
    unique_suffix: str,
) -> None:
    """Write records via RedisSink, read back and assert content.

    Requirements: 2.17
    """
    key_prefix = f"agora_test_{unique_suffix}"
    records = _make_records(_N_RECORDS, unique_suffix)

    # --- Write via RedisSink ---
    sink = agora_redis.RedisSink(
        url=redis_url,
        key_fn=lambda r: f"{key_prefix}:{r['id']}",
        serializer=lambda record: json.dumps(record),
    )
    summary = await asyncio.wait_for(
        Pipeline(IterableSource(records)).build(sink).run(),
        timeout=_INTEGRATION_TIMEOUT_S,
    )
    assert summary.records_written == _N_RECORDS

    # --- Read back directly via Redis client ---
    client = aioredis.from_url(redis_url)
    try:
        for record in records:
            raw = await client.get(f"{key_prefix}:{record['id']}")
            assert raw is not None, f"Key {key_prefix}:{record['id']} not found in Redis"
            stored = json.loads(raw)
            assert stored["id"] == record["id"]
            assert stored["value"] == record["value"]
    finally:
        # Cleanup test keys
        keys = [f"{key_prefix}:{i}" for i in range(_N_RECORDS)]
        if keys:
            await client.delete(*keys)
        await client.aclose()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_stream_source(
    redis_url: str,
    unique_suffix: str,
) -> None:
    """Produce to Redis stream, consume via RedisStreamSource, assert all records received.

    Requirements: 2.17
    """
    stream_key = f"agora_stream_{unique_suffix}"
    records = _make_records(_N_RECORDS, unique_suffix)

    client = aioredis.from_url(redis_url)
    group = f"agora_group_{unique_suffix}"
    consumer = f"agora_consumer_{unique_suffix}"

    # --- Consume via RedisStreamSource ---
    received: list[dict] = []

    class CollectSink:
        sink_name = "collect"

        async def open(self) -> None:
            pass

        async def write(self, record) -> None:
            received.append(record)

        async def flush(self) -> None:
            pass

        async def close(self) -> None:
            pass

    stream_source = agora_redis.RedisStreamSource(
        url=redis_url,
        stream=stream_key,
        group=group,
        consumer=consumer,
    )
    consumer_task = asyncio.create_task(
        asyncio.wait_for(
            (
                Pipeline(stream_source)
                .build(CollectSink())  # type: ignore[arg-type]
                .run(max_records=_N_RECORDS)
            ),
            timeout=_INTEGRATION_TIMEOUT_S,
        )
    )

    await asyncio.sleep(0.5)

    # --- Produce to Redis stream via RedisSink (stream mode) ---
    stream_sink = agora_redis.RedisSink(
        url=redis_url,
        key_fn=lambda _: stream_key,
        serializer=lambda record: record,
        mode="xadd",
    )
    produce_summary = await asyncio.wait_for(
        Pipeline(IterableSource(records)).build(stream_sink).run(),
        timeout=_INTEGRATION_TIMEOUT_S,
    )
    assert produce_summary.records_written == _N_RECORDS

    await consumer_task

    assert len(received) == _N_RECORDS
    received_ids = {r["id"] for r in received}
    expected_ids = {str(r["id"]) for r in records}
    assert received_ids == expected_ids
    await client.delete(stream_key)
    await client.aclose()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_dedup_store(
    redis_url: str,
    unique_suffix: str,
) -> None:
    """Run pipeline with DedupMiddleware + RedisStore, assert duplicates are dropped.

    Requirements: 2.17
    """
    # Build a record set with intentional duplicates:
    # records 0..4 appear once, records 0..1 appear again (2 duplicates)
    base_records = _make_records(5, unique_suffix)
    duplicate_records = _make_records(2, unique_suffix)  # ids 0 and 1 repeated
    all_records = base_records + duplicate_records  # 7 total, 2 duplicates

    received: list[dict] = []

    class CollectSink:
        sink_name = "collect"

        async def open(self) -> None:
            pass

        async def write(self, record) -> None:
            received.append(record)

        async def flush(self) -> None:
            pass

        async def close(self) -> None:
            pass

    redis_store = agora_redis.RedisStore(
        url=redis_url,
        key_prefix=f"dedup_{unique_suffix}",
        ttl_seconds=60,
    )

    dedup = DedupMiddleware(
        key=lambda r: f"{r['id']}_{unique_suffix}",
        store=redis_store,
    )

    summary = await asyncio.wait_for(
        (
            Pipeline(IterableSource(all_records))
            .pipe(dedup)
            .build(CollectSink())  # type: ignore[arg-type]
            .run()
        ),
        timeout=_INTEGRATION_TIMEOUT_S,
    )

    # 5 unique records should pass through; 2 duplicates should be dropped
    assert summary.records_consumed == 7
    assert summary.records_dropped == 2
    assert len(received) == 5

    # Verify the received records are the unique ones (ids 0-4)
    received_ids = sorted(r["id"] for r in received)
    assert received_ids == [0, 1, 2, 3, 4]
