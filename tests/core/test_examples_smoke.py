"""
tests/core/test_examples_smoke.py
===================================
Smoke tests for the official example packs (P0-07).

Verifies that:
- Example code from docs/guides/examples.md uses valid import paths
- The file-etl example pipeline runs end-to-end with an IterableSource substitute
- The normalise/parse/normalise_row transform functions behave correctly
- run_sync() works in the file-etl example shape (P0-01 integration)
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from agora import DeliveryConfig, IterableSource, MapMiddleware, Pipeline
from agora.sinks.io.stdout import StdoutSink

# ======================================================================
# file-etl example functions (copied from docs/guides/examples.md)
# ======================================================================


def _file_etl_normalise(record: dict) -> dict:
    return {k: v.strip() for k, v in record.items()}


# ======================================================================
# postgres-incremental example function
# ======================================================================


def _postgres_normalise(record: dict) -> dict:
    return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in record.items()}


# ======================================================================
# kafka-consumer example function
# ======================================================================


def _kafka_parse(record: dict) -> dict:
    import json

    raw = record.get("value", b"")
    if isinstance(raw, (bytes, bytearray)):
        return json.loads(raw.decode())
    return record


# ======================================================================
# Unit tests — transform functions
# ======================================================================


def test_file_etl_normalise_strips_whitespace() -> None:
    assert _file_etl_normalise({"name": " alice ", "city": "NY "}) == {
        "name": "alice",
        "city": "NY",
    }


def test_file_etl_normalise_empty_record() -> None:
    assert _file_etl_normalise({}) == {}


def test_postgres_normalise_converts_datetime() -> None:
    dt = datetime(2026, 6, 4, 10, 0, 0, tzinfo=UTC)
    result = _postgres_normalise({"id": 1, "updated_at": dt})
    assert result["updated_at"] == dt.isoformat()
    assert result["id"] == 1


def test_postgres_normalise_passthrough_non_datetime() -> None:
    record = {"id": 42, "name": "alice"}
    assert _postgres_normalise(record) == record


def test_kafka_parse_bytes_payload() -> None:
    import json

    payload = json.dumps({"id": "a", "value": 1}).encode()
    result = _kafka_parse({"value": payload})
    assert result == {"id": "a", "value": 1}


def test_kafka_parse_dict_passthrough() -> None:
    record = {"id": "b", "value": 2}
    assert _kafka_parse(record) == record


# ======================================================================
# Integration — file-etl example runs end to end
# ======================================================================


@pytest.mark.asyncio
async def test_file_etl_example_runs_end_to_end() -> None:
    """The file-etl example from examples.md runs correctly with IterableSource."""
    records = [
        {"id": "1", "name": " alice ", "city": "HCM "},
        {"id": "2", "name": " bob   ", "city": " HN "},
        {"id": "3", "name": " carol ", "city": "DN  "},
    ]

    sink = StdoutSink()
    summary = await (
        Pipeline(IterableSource(records), id="file_etl_smoke")
        .pipe(MapMiddleware(_file_etl_normalise, name="normalise"))
        .build(sink, config=DeliveryConfig(batch_size=10))
        .run()
    )

    assert summary.records_written == 3
    assert summary.records_errored == 0


def test_file_etl_example_run_sync() -> None:
    """run_sync() works for the file-etl example shape (no asyncio.run() needed)."""
    records = [{"id": str(i), "value": f"  val{i}  "} for i in range(5)]

    sink = StdoutSink()
    summary = (
        Pipeline(IterableSource(records), id="file_etl_sync_smoke")
        .pipe(MapMiddleware(_file_etl_normalise, name="normalise"))
        .build(sink, config=DeliveryConfig(batch_size=10))
        .run_sync()
    )

    assert summary.records_written == 5


@pytest.mark.asyncio
async def test_postgres_example_transform_runs_in_pipeline() -> None:
    """The postgres normalise function works inside a real pipeline."""
    dt = datetime(2026, 6, 4, 10, 0, 0, tzinfo=UTC)
    records = [
        {"id": 1, "updated_at": dt, "name": "alice"},
        {"id": 2, "updated_at": dt, "name": "bob"},
    ]

    sink = StdoutSink()
    summary = await (
        Pipeline(IterableSource(records), id="postgres_smoke")
        .pipe(MapMiddleware(_postgres_normalise, name="normalise"))
        .build(sink, config=DeliveryConfig(batch_size=10))
        .run()
    )

    assert summary.records_written == 2


@pytest.mark.asyncio
async def test_kafka_example_transform_runs_in_pipeline() -> None:
    """The kafka parse function works inside a real pipeline."""
    import json

    records = [{"value": json.dumps({"id": str(i), "score": i * 10}).encode()} for i in range(4)]

    sink = StdoutSink()
    summary = await (
        Pipeline(IterableSource(records), id="kafka_smoke")
        .pipe(MapMiddleware(_kafka_parse, name="parse"))
        .build(sink, config=DeliveryConfig(batch_size=10))
        .run()
    )

    assert summary.records_written == 4


# ======================================================================
# Import path validation — examples.md API surface
# ======================================================================


def test_example_imports_are_valid() -> None:
    """All import paths used in examples.md must be importable."""
    import importlib

    paths = [
        "agora",
        "agora.sources.file.csv",
        "agora.sinks.io.stdout",
    ]
    for path in paths:
        try:
            importlib.import_module(path)
        except ImportError as exc:
            pytest.fail(f"Example import path {path!r} is not importable: {exc}")


def test_example_api_surface_exists() -> None:
    """Key classes used in examples.md must exist on the agora package."""
    import agora

    for name in ["Pipeline", "MapMiddleware", "DeliveryConfig", "IterableSource"]:
        assert hasattr(agora, name), f"agora.{name} not found"
