"""Recovery contracts exercised against the Compose Kafka/PostgreSQL/Redis stack."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import pytest
from agora_plugins.kafka import KafkaSink
from order_command_center import dlq, migrate, producer
from order_command_center.contracts import generate_order_events
from order_command_center.operations import convergence
from order_command_center.pipelines import postgres, redis
from order_command_center.pipelines.base import ProjectionRunOptions
from order_command_center.settings import Settings, load_settings

pytestmark = pytest.mark.integration

if os.getenv("AGORA_RUN_INTEGRATION") != "1":
    pytest.skip(
        "set AGORA_RUN_INTEGRATION=1 after `make up` to run Docker recovery tests",
        allow_module_level=True,
    )


@dataclass(frozen=True, slots=True)
class IsolatedBatch:
    topic: str
    producer_run_id: str
    event_count: int


def _one_shot_options(event_count: int) -> ProjectionRunOptions:
    return ProjectionRunOptions(
        max_records=event_count,
        forever=False,
        emit_report=False,
        metrics_host=None,
        metrics_port=None,
        metrics_auth_token=None,
    )


async def _publish_isolated_batch(
    monkeypatch: pytest.MonkeyPatch,
    *,
    marker_path: Path,
) -> IsolatedBatch:
    suffix = uuid4().hex
    topic = f"commerce-recovery-{suffix}"
    monkeypatch.setenv("KAFKA_TOPIC", topic)
    monkeypatch.setenv("POSTGRES_GROUP", f"postgres-recovery-{suffix}")
    monkeypatch.setenv("REDIS_GROUP", f"redis-recovery-{suffix}")
    monkeypatch.setenv("CRASH_MARKER_PATH", str(marker_path))
    monkeypatch.setenv("PROJECTION_ERROR_BACKOFF_SECONDS", "1")
    monkeypatch.delenv("METRICS_PORT", raising=False)

    await migrate.run()
    event_count = await producer.run(order_count=1, flush_every=1)
    settings = load_settings()
    producer_run_id = await _latest_producer_run_id(settings, topic)
    return IsolatedBatch(topic=topic, producer_run_id=producer_run_id, event_count=event_count)


async def _latest_producer_run_id(settings: Settings, topic: str) -> str:
    import psycopg

    async with (
        await psycopg.AsyncConnection.connect(settings.postgres_dsn) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            f"SELECT producer_run_id FROM {settings.tables.producer_runs} "
            "WHERE kafka_topic = %s ORDER BY published_at DESC LIMIT 1",
            (topic,),
        )
        row = await cursor.fetchone()
    assert row is not None
    return str(row[0])


async def _ledger_delivery_stats(settings: Settings, batch: IsolatedBatch) -> tuple[int, int]:
    import psycopg

    async with (
        await psycopg.AsyncConnection.connect(settings.postgres_dsn) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            f"SELECT count(*), count(DISTINCT kafka_delivery_key) "
            f"FROM {settings.tables.event_ledger} "
            "WHERE kafka_topic = %s AND producer_run_id = %s",
            (batch.topic, batch.producer_run_id),
        )
        rows, delivery_keys = await cursor.fetchone()
    return int(rows), int(delivery_keys)


@pytest.mark.asyncio
async def test_postgres_replays_after_sink_flush_before_kafka_acknowledgement(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    batch = await _publish_isolated_batch(monkeypatch, marker_path=tmp_path / "flushed.marker")
    options = _one_shot_options(batch.event_count)

    exit_code = await _hard_crash_postgres_worker(
        marker_path=Path(load_settings().crash_marker_path),
        event_count=batch.event_count,
    )

    assert Path(load_settings().crash_marker_path).is_file()
    assert exit_code == 75
    assert await postgres.run(options=options) == batch.event_count

    rows, delivery_keys = await _ledger_delivery_stats(load_settings(), batch)
    assert (rows, delivery_keys) == (batch.event_count, batch.event_count)


@pytest.mark.asyncio
async def test_redis_projection_can_be_rebuilt_from_a_fresh_consumer_group(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    batch = await _publish_isolated_batch(monkeypatch, marker_path=tmp_path / "unused.marker")
    options = _one_shot_options(batch.event_count)

    assert await postgres.run(options=options) == batch.event_count
    assert await redis.run(options=options) == batch.event_count

    settings = load_settings()
    order_rows = await _expected_current_orders(settings, batch)
    keys = [f"{settings.redis_key_prefix}:current:{order_id}" for order_id, _, _ in order_rows]
    await _delete_cache_keys(settings, keys)

    monkeypatch.setenv("REDIS_GROUP", f"redis-rebuild-{uuid4().hex}")
    assert await redis.run(options=options) == batch.event_count
    assert await _cache_matches_current_state(load_settings(), order_rows)


@pytest.mark.asyncio
async def test_worker_restart_converges_consumer_groups_and_projections(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    batch = await _publish_isolated_batch(monkeypatch, marker_path=tmp_path / "flushed.marker")
    options = _one_shot_options(batch.event_count)

    assert (
        await _hard_crash_postgres_worker(
            marker_path=Path(load_settings().crash_marker_path),
            event_count=batch.event_count,
        )
        == 75
    )
    assert await postgres.run(options=options) == batch.event_count
    assert await redis.run(options=options) == batch.event_count

    settings = load_settings()
    snapshots = await convergence.require_zero_lag(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=batch.topic,
        consumer_groups=(
            settings.postgres_projection.consumer_group,
            settings.redis_projection.consumer_group,
        ),
    )
    assert [snapshot.lag for snapshot in snapshots] == [0, 0]
    rows, delivery_keys = await _ledger_delivery_stats(settings, batch)
    assert (rows, delivery_keys) == (batch.event_count, batch.event_count)
    assert await _cache_matches_current_state(
        settings,
        await _expected_current_orders(settings, batch),
    )


@pytest.mark.asyncio
async def test_mixed_v1_v2_events_converge_through_both_projections(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    v1_batch = await _publish_isolated_batch(monkeypatch, marker_path=tmp_path / "unused.marker")
    v2_event_count = await producer.run(order_count=1, flush_every=1, event_version=2)
    options = _one_shot_options(v1_batch.event_count + v2_event_count)

    assert await postgres.run(options=options) == v1_batch.event_count + v2_event_count
    assert await redis.run(options=options) == v1_batch.event_count + v2_event_count

    settings = load_settings()
    assert await _ledger_contract_versions(settings, v1_batch.topic) == {
        (1, "standard", v1_batch.event_count),
        (2, "delivery", v2_event_count),
    }
    assert await _cache_contract_versions(settings, v1_batch.topic) == {
        (1, "standard"),
        (2, "delivery"),
    }


@pytest.mark.asyncio
async def test_dlq_corrected_replay_is_audited_and_converges(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    suffix = uuid4().hex
    topic = f"commerce-dlq-replay-{suffix}"
    monkeypatch.setenv("KAFKA_TOPIC", topic)
    monkeypatch.setenv("POSTGRES_GROUP", f"postgres-dlq-{suffix}")
    monkeypatch.setenv("REDIS_GROUP", f"redis-dlq-{suffix}")
    monkeypatch.delenv("METRICS_PORT", raising=False)
    await migrate.run()
    settings = load_settings()

    await _publish_raw_kafka_value(settings, b'{"event_version": 999}')
    assert await postgres.run(options=_one_shot_options(1)) == 0
    dlq_record_id = await _poison_record_id(settings, topic)

    corrected_event = generate_order_events(order_count=1)[0]
    payload_file = tmp_path / "corrected-event.json"
    payload_file.write_text(json.dumps(corrected_event), encoding="utf-8")
    preview = await dlq.preview_replay(
        dlq_record_id=dlq_record_id,
        payload_file=payload_file,
        change_ticket="CHG-1234",
        reason="restored the missing order fields from the source of record",
    )
    result = await dlq.execute_replay(preview)

    assert result.state == "published"
    assert await postgres.run(options=_one_shot_options(1)) == 1
    assert await redis.run(options=_one_shot_options(1)) == 1
    inspected = await dlq.show_record(dlq_record_id)
    assert [entry["event_type"] for entry in inspected["audit"]] == ["requested", "published"]
    assert await _audit_rejects_mutation(settings, result.replay_id)

    ledger_event_id, cache_event_id = await _replayed_event_ids(
        settings,
        producer_run_id=result.producer_run_id,
        order_id=str(corrected_event["order_id"]),
    )
    assert ledger_event_id == str(corrected_event["event_id"])
    assert cache_event_id == str(corrected_event["event_id"])


@pytest.mark.asyncio
async def test_dlq_reconcile_completes_only_a_ledger_proven_publishing_request(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    suffix = uuid4().hex
    topic = f"commerce-dlq-reconcile-{suffix}"
    monkeypatch.setenv("KAFKA_TOPIC", topic)
    monkeypatch.setenv("POSTGRES_GROUP", f"postgres-reconcile-{suffix}")
    monkeypatch.setenv("REDIS_GROUP", f"redis-reconcile-{suffix}")
    monkeypatch.delenv("METRICS_PORT", raising=False)
    await migrate.run()
    settings = load_settings()

    await _publish_raw_kafka_value(settings, b'{"event_version": 999}')
    assert await postgres.run(options=_one_shot_options(1)) == 0
    dlq_record_id = await _poison_record_id(settings, topic)
    corrected_event = generate_order_events(order_count=1)[0]
    payload_file = tmp_path / "corrected-event.json"
    payload_file.write_text(json.dumps(corrected_event), encoding="utf-8")
    preview = await dlq.preview_replay(
        dlq_record_id=dlq_record_id,
        payload_file=payload_file,
        change_ticket="CHG-5678",
        reason="recovered the original event from the source of record",
    )

    completion = dlq._complete_replay_request

    async def interrupted_completion(**_: object) -> None:
        raise ConnectionError("simulated state-update interruption after Kafka publish")

    monkeypatch.setattr(dlq, "_complete_replay_request", interrupted_completion)
    with pytest.raises(ConnectionError, match="state-update interruption"):
        await dlq.execute_replay(preview)
    monkeypatch.setattr(dlq, "_complete_replay_request", completion)

    publishing_replay_id = (await dlq.show_record(dlq_record_id))["replays"][0]["replay_id"]
    assert isinstance(publishing_replay_id, str)
    with pytest.raises(RuntimeError, match="exactly one corrected Kafka delivery"):
        await dlq.reconcile_replay(publishing_replay_id)

    assert await postgres.run(options=_one_shot_options(1)) == 1
    assert await redis.run(options=_one_shot_options(1)) == 1
    reconciled = await dlq.reconcile_replay(publishing_replay_id)
    assert reconciled.state == "published"
    assert (await dlq.reconcile_replay(publishing_replay_id)).state == "published"
    inspected = await dlq.show_record(dlq_record_id)
    assert [entry["event_type"] for entry in inspected["audit"]] == ["requested", "reconciled"]


async def _hard_crash_postgres_worker(*, marker_path: Path, event_count: int) -> int:
    """Run the worker in a child process so no Kafka cleanup can commit its offset."""

    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "order_command_center.pipelines.postgres",
        "--max-records",
        str(event_count),
        "--hard-crash-after-flush",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=os.environ.copy(),
    )
    for _ in range(200):
        if marker_path.is_file():
            break
        if process.returncode is not None:
            output = (await process.communicate())[0].decode()
            pytest.fail(f"worker exited before the crash marker: {output}")
        await asyncio.sleep(0.05)
    else:
        process.kill()
        output = (await process.communicate())[0].decode()
        pytest.fail(f"worker did not reach the crash boundary: {output}")
    return await asyncio.wait_for(process.wait(), timeout=10)


async def _publish_raw_kafka_value(settings: Settings, value: bytes) -> None:
    sink = KafkaSink(
        topic=settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        serializer=lambda row: row,
        key_fn=lambda _: b"poison-order",
    )
    await sink.open()
    try:
        await sink.write(value)
        await sink.flush()
    finally:
        await sink.close()


async def _poison_record_id(settings: Settings, topic: str) -> int:
    import psycopg

    async with (
        await psycopg.AsyncConnection.connect(settings.postgres_dsn) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            f"SELECT id FROM {settings.tables.dead_letter_queue} "
            "WHERE pipeline_id = %s AND checkpoint ->> 'topic' = %s "
            "ORDER BY id DESC LIMIT 1",
            (settings.postgres_projection.pipeline_id, topic),
        )
        row = await cursor.fetchone()
    assert row is not None
    return int(row[0])


async def _replayed_event_ids(
    settings: Settings,
    *,
    producer_run_id: str,
    order_id: str,
) -> tuple[str, str]:
    import psycopg
    import redis.asyncio as redis

    async with (
        await psycopg.AsyncConnection.connect(settings.postgres_dsn) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            f"SELECT event_id FROM {settings.tables.event_ledger} "
            "WHERE kafka_topic = %s AND producer_run_id = %s",
            (settings.kafka_topic, producer_run_id),
        )
        row = await cursor.fetchone()
    assert row is not None
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        cached = await client.get(f"{settings.redis_key_prefix}:current:{order_id}")
    finally:
        await client.aclose()
    assert cached is not None
    return str(row[0]), str(json.loads(cached)["event_id"])


async def _audit_rejects_mutation(settings: Settings, replay_id: str) -> bool:
    import psycopg

    async with (
        await psycopg.AsyncConnection.connect(settings.postgres_dsn) as connection,
        connection.cursor() as cursor,
    ):
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
            await cursor.execute(
                f"UPDATE {settings.tables.replay_audit} SET event_type = 'failed' "
                "WHERE replay_id = %s",
                (replay_id,),
            )
    return True


async def _ledger_contract_versions(settings: Settings, topic: str) -> set[tuple[int, str, int]]:
    import psycopg

    async with (
        await psycopg.AsyncConnection.connect(settings.postgres_dsn) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            f"SELECT event_version, fulfillment_channel, count(*) "
            f"FROM {settings.tables.event_ledger} WHERE kafka_topic = %s "
            "GROUP BY event_version, fulfillment_channel",
            (topic,),
        )
        rows = await cursor.fetchall()
    return {(int(version), str(channel), int(count)) for version, channel, count in rows}


async def _cache_contract_versions(settings: Settings, topic: str) -> set[tuple[int, str]]:
    import psycopg
    import redis.asyncio as redis

    async with (
        await psycopg.AsyncConnection.connect(settings.postgres_dsn) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            f"SELECT order_id, event_version, fulfillment_channel "
            f"FROM {settings.tables.current_state} WHERE kafka_topic = %s",
            (topic,),
        )
        rows = await cursor.fetchall()
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        values = await client.mget(
            [f"{settings.redis_key_prefix}:current:{order_id}" for order_id, _, _ in rows]
        )
    finally:
        await client.aclose()
    assert all(value is not None for value in values)
    return {
        (int(payload["event_version"]), str(payload["fulfillment_channel"]))
        for payload in (json.loads(value) for value in values if value is not None)
    }


async def _expected_current_orders(
    settings: Settings,
    batch: IsolatedBatch,
) -> list[tuple[str, str, str]]:
    import psycopg

    async with (
        await psycopg.AsyncConnection.connect(settings.postgres_dsn) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            f"SELECT order_id, event_id, status FROM {settings.tables.current_state} "
            "WHERE kafka_topic = %s AND producer_run_id = %s",
            (batch.topic, batch.producer_run_id),
        )
        rows = await cursor.fetchall()
    return [(str(order_id), str(event_id), str(status)) for order_id, event_id, status in rows]


async def _delete_cache_keys(settings: Settings, keys: list[str]) -> None:
    import redis.asyncio as redis

    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        await client.delete(*keys)
        assert await client.mget(keys) == [None] * len(keys)
    finally:
        await client.aclose()


async def _cache_matches_current_state(
    settings: Settings,
    expected_orders: list[tuple[str, str, str]],
) -> bool:
    import json

    import redis.asyncio as redis

    keys = [f"{settings.redis_key_prefix}:current:{order_id}" for order_id, _, _ in expected_orders]
    client = redis.from_url(settings.redis_url, decode_responses=True)
    try:
        values = await client.mget(keys)
    finally:
        await client.aclose()
    return all(
        value is not None
        and json.loads(value).get("event_id") == event_id
        and json.loads(value).get("status") == status
        for (_, event_id, status), value in zip(expected_orders, values, strict=True)
    )
