"""
tests/integration/test_kafka_integration.py
============================================
Integration tests for Kafka plugin (`agora_plugins.kafka`).

Requires:
- ``AGORA_RUN_INTEGRATION=1`` environment variable
- Kafka broker reachable at ``127.0.0.1:9092``
- ``agora-etl-kafka`` plugin installed (`pip install "agora-etl-plugins[kafka]"`)

Run with::

    AGORA_RUN_INTEGRATION=1 pytest tests/integration/test_kafka_integration.py -v

Requirements: 2.16
"""

from __future__ import annotations

import json

import pytest

# Skip entire module when the Kafka plugin is not installed.
agora_kafka = pytest.importorskip("agora_plugins.kafka")

from agora import IterableSource, Pipeline  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_N_RECORDS = 10


def _make_records(n: int, suffix: str) -> list[dict]:
    """Return a list of simple JSON-serialisable dicts."""
    return [{"id": i, "value": f"record_{i}_{suffix}"} for i in range(n)]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_kafka_produce_consume_roundtrip(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    """Produce N records via KafkaSink, consume via KafkaSource, assert all received in order.

    Requirements: 2.16
    """
    topic = f"agora_test_{unique_suffix}"
    records = _make_records(_N_RECORDS, unique_suffix)

    # --- Produce ---
    sink = agora_kafka.KafkaSink(
        bootstrap_servers=kafka_bootstrap,
        topic=topic,
        serializer=lambda r: json.dumps(r).encode(),
    )
    source_produce = IterableSource(records)
    summary = await Pipeline(source_produce).build(sink).run()
    assert summary.records_written == _N_RECORDS

    # --- Consume ---
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

    consumer_source = agora_kafka.KafkaSource(
        bootstrap_servers=kafka_bootstrap,
        topics=[topic],
        group_id=f"agora_test_group_{unique_suffix}",
        deserializer=lambda b: json.loads(b.decode()),
        auto_offset_reset="earliest",
    )
    await (
        Pipeline(consumer_source)
        .build(CollectSink())  # type: ignore[arg-type]
        .run(max_records=_N_RECORDS)
    )

    assert len(received) == _N_RECORDS
    for i, record in enumerate(received):
        assert record["id"] == i
        assert record["value"] == f"record_{i}_{unique_suffix}"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_kafka_pipeline_end_to_end(
    kafka_bootstrap: str,
    unique_suffix: str,
) -> None:
    """Full pipeline KafkaSource → transform → KafkaSink.

    Produces records to an input topic, runs a transform pipeline that reads
    from the input topic and writes transformed records to an output topic,
    then consumes the output topic and asserts record count and content.

    Requirements: 2.16
    """
    input_topic = f"agora_e2e_in_{unique_suffix}"
    output_topic = f"agora_e2e_out_{unique_suffix}"
    records = _make_records(_N_RECORDS, unique_suffix)

    # --- Step 1: seed the input topic ---
    seed_sink = agora_kafka.KafkaSink(
        bootstrap_servers=kafka_bootstrap,
        topic=input_topic,
        serializer=lambda r: json.dumps(r).encode(),
    )
    await Pipeline(IterableSource(records)).build(seed_sink).run()

    # --- Step 2: transform pipeline (KafkaSource → add "processed" flag → KafkaSink) ---
    from agora import MapMiddleware

    transform_source = agora_kafka.KafkaSource(
        bootstrap_servers=kafka_bootstrap,
        topics=[input_topic],
        group_id=f"agora_e2e_transform_{unique_suffix}",
        deserializer=lambda b: json.loads(b.decode()),
        auto_offset_reset="earliest",
    )
    transform_sink = agora_kafka.KafkaSink(
        bootstrap_servers=kafka_bootstrap,
        topic=output_topic,
        serializer=lambda r: json.dumps(r).encode(),
    )
    transform_summary = await (
        Pipeline(transform_source)
        .pipe(MapMiddleware(lambda r: {**r, "processed": True}, name="add_processed"))
        .build(transform_sink)
        .run(max_records=_N_RECORDS)
    )
    assert transform_summary.records_written == _N_RECORDS

    # --- Step 3: consume output topic and verify ---
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

    output_source = agora_kafka.KafkaSource(
        bootstrap_servers=kafka_bootstrap,
        topics=[output_topic],
        group_id=f"agora_e2e_verify_{unique_suffix}",
        deserializer=lambda b: json.loads(b.decode()),
        auto_offset_reset="earliest",
    )
    await (
        Pipeline(output_source)
        .build(CollectSink())  # type: ignore[arg-type]
        .run(max_records=_N_RECORDS)
    )

    assert len(received) == _N_RECORDS
    for record in received:
        assert record.get("processed") is True
        assert "id" in record
        assert "value" in record
