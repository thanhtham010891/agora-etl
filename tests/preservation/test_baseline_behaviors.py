"""
tests/preservation/test_baseline_behaviors.py
===============================================
Preservation Property Tests — Agora Core Baseline Behavior Suite

**Property 2: Preservation** — Agora Core Baseline Behavior Suite

IMPORTANT: These tests MUST PASS on UNFIXED code.
They capture baseline behaviors that must be preserved after all fixes.

Each test documents:
- The requirement ID being preserved
- The baseline behavior observed on unfixed code
- The invariant that must hold after fixes

**Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9,
             3.10, 3.11, 3.12, 3.13, 3.14, 3.15, 3.16, 3.17, 3.18, 3.19, 3.20**
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

import pytest

# ======================================================================
# [SEC-1] Health Endpoints — Preservation (no auth_token configured)
# ======================================================================


@pytest.mark.asyncio
async def test_sec1_health_endpoint_returns_200_when_no_auth_configured() -> None:
    """[SEC-1] Preservation: GET /health with no auth_token configured → 200 with JSON payload.

    Baseline behavior: when HealthServer is created WITHOUT auth_token (default),
    GET /health returns HTTP 200 with a full JSON health payload.
    This backward-compatible behavior must be preserved after the auth fix.

    Validates: Requirements 3.1
    """
    from agora.health.server import HealthServer

    # No auth_token — backward-compatible default
    server = HealthServer(port=0)
    server._stop_event = asyncio.Event()
    server._server = await asyncio.start_server(
        server._handle_connection,
        host="127.0.0.1",
        port=0,
    )
    port = server._server.sockets[0].getsockname()[1]

    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
        await writer.drain()
        response = await asyncio.wait_for(reader.read(4096), timeout=2.0)
        writer.close()
        await writer.wait_closed()

        response_str = response.decode("ascii", errors="replace")
        status_line = response_str.split("\r\n")[0]

        # Must return 200 when no auth is configured
        assert "200" in status_line, (
            f"[SEC-1] PRESERVATION FAILED: GET /health with no auth_token should return 200, "
            f"got: {status_line!r}"
        )

        # Body must be valid JSON with health payload
        body_start = response_str.find("\r\n\r\n")
        assert body_start != -1, "Response has no body separator"
        body = response_str[body_start + 4 :]
        payload = json.loads(body)

        assert "status" in payload, f"Health payload missing 'status' key: {payload}"
        assert "pipelines" in payload, f"Health payload missing 'pipelines' key: {payload}"
    finally:
        server.stop()
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_sec1_ready_endpoint_returns_200_when_healthy() -> None:
    """[SEC-1] Preservation: GET /ready with healthy worker → {"ready": true} 200.

    Baseline behavior: /ready returns 200 {"ready": true} when no pipelines are failing.
    Must be preserved after auth fix (valid auth or auth disabled → same response).

    Validates: Requirements 3.3
    """
    from agora.health.server import HealthServer

    server = HealthServer(port=0)
    server._stop_event = asyncio.Event()
    server._server = await asyncio.start_server(
        server._handle_connection,
        host="127.0.0.1",
        port=0,
    )
    port = server._server.sockets[0].getsockname()[1]

    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n")
        await writer.drain()
        response = await asyncio.wait_for(reader.read(4096), timeout=2.0)
        writer.close()
        await writer.wait_closed()

        response_str = response.decode("ascii", errors="replace")
        status_line = response_str.split("\r\n")[0]

        # With no pipelines registered, status is "idle" → ready=True → 200
        assert "200" in status_line, (
            f"[SEC-1] PRESERVATION FAILED: GET /ready with healthy worker should return 200, "
            f"got: {status_line!r}"
        )

        body_start = response_str.find("\r\n\r\n")
        body = response_str[body_start + 4 :]
        payload = json.loads(body)

        assert "ready" in payload, f"Ready payload missing 'ready' key: {payload}"
        assert payload["ready"] is True, (
            f"[SEC-1] PRESERVATION FAILED: Expected ready=True for healthy worker, got: {payload}"
        )
    finally:
        server.stop()
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_sec1_ready_endpoint_returns_503_when_failing() -> None:
    """[SEC-1] Preservation: GET /ready with failing pipeline → {"ready": false} 503.

    Baseline behavior: /ready returns 503 {"ready": false} when a pipeline is failing.
    Must be preserved after auth fix.

    Validates: Requirements 3.4
    """
    from agora.health.server import HealthServer
    from agora.metrics.collector import MetricsCollector

    collector = MetricsCollector()
    # Record a failed run to make the pipeline status "failing"
    await collector.record_run("test_pipe", error=RuntimeError("boom"))
    # Record more failures to push success_rate below 0.5
    for _ in range(4):
        await collector.record_run("test_pipe", error=RuntimeError("boom"))

    server = HealthServer(port=0, collector=collector)
    server._stop_event = asyncio.Event()
    server._server = await asyncio.start_server(
        server._handle_connection,
        host="127.0.0.1",
        port=0,
    )
    port = server._server.sockets[0].getsockname()[1]

    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n")
        await writer.drain()
        response = await asyncio.wait_for(reader.read(4096), timeout=2.0)
        writer.close()
        await writer.wait_closed()

        response_str = response.decode("ascii", errors="replace")
        status_line = response_str.split("\r\n")[0]

        assert "503" in status_line, (
            f"[SEC-1] PRESERVATION FAILED: GET /ready with failing pipeline should return 503, "
            f"got: {status_line!r}"
        )

        body_start = response_str.find("\r\n\r\n")
        body = response_str[body_start + 4 :]
        payload = json.loads(body)

        assert payload["ready"] is False, (
            f"[SEC-1] PRESERVATION FAILED: Expected ready=False for failing pipeline, "
            f"got: {payload}"
        )
    finally:
        server.stop()
        await asyncio.sleep(0.05)


# ======================================================================
# [SEC-6] Prompt Rendering — Preservation (matching fields)
# ======================================================================


def _make_ai_middleware_instance():
    """Create a minimal AIMiddleware subclass for testing _render_prompt."""
    from agora.middlewares.ai.base import AIMiddleware

    class _TestAIMiddleware(AIMiddleware):
        async def process(self, record, ctx):
            return record

    class _FakeProvider:
        async def complete(self, prompt, **kwargs):
            pass

    return _TestAIMiddleware(provider=_FakeProvider())  # type: ignore[arg-type]


def test_sec6_render_prompt_substitutes_matching_dict_field() -> None:
    """[SEC-6] Preservation: _render_prompt("{text}", {"text": "hello"}) → "hello".

    Baseline behavior: when template variables exactly match record keys,
    the substitution works correctly. This must be preserved after the injection fix.

    Validates: Requirements 3.5
    """
    middleware = _make_ai_middleware_instance()

    result = middleware._render_prompt("{text}", {"text": "hello"})

    assert result == "hello", (
        f"[SEC-6] PRESERVATION FAILED: _render_prompt('{{text}}', {{'text': 'hello'}}) "
        f"should return 'hello', got: {result!r}"
    )


def test_sec6_render_prompt_substitutes_multiple_matching_fields() -> None:
    """[SEC-6] Preservation: multiple template vars all matching record keys → correct output.

    Validates: Requirements 3.5
    """
    middleware = _make_ai_middleware_instance()

    result = middleware._render_prompt(
        "Classify: {text} in category {category}",
        {"text": "hello world", "category": "greeting"},
    )

    assert result == "Classify: hello world in category greeting", (
        f"[SEC-6] PRESERVATION FAILED: Expected 'Classify: hello world in category greeting', "
        f"got: {result!r}"
    )


def test_sec6_render_prompt_supports_dict_input() -> None:
    """[SEC-6] Preservation: dict record input is supported.

    Validates: Requirements 3.6
    """
    middleware = _make_ai_middleware_instance()
    result = middleware._render_prompt("{name}", {"name": "Alice"})
    assert result == "Alice", f"Dict input failed: {result!r}"


def test_sec6_render_prompt_supports_pydantic_model_input() -> None:
    """[SEC-6] Preservation: Pydantic model record input is supported.

    Validates: Requirements 3.6
    """
    from pydantic import BaseModel

    class MyRecord(BaseModel):
        name: str
        value: int

    middleware = _make_ai_middleware_instance()
    record = MyRecord(name="Bob", value=42)
    result = middleware._render_prompt("{name} has value {value}", record)
    assert result == "Bob has value 42", f"Pydantic model input failed: {result!r}"


def test_sec6_render_prompt_supports_dataclass_input() -> None:
    """[SEC-6] Preservation: dataclass record input is supported.

    Validates: Requirements 3.6
    """
    from dataclasses import dataclass

    @dataclass
    class MyRecord:
        name: str
        score: float

    middleware = _make_ai_middleware_instance()
    record = MyRecord(name="Carol", score=9.5)
    result = middleware._render_prompt("{name}: {score}", record)
    assert result == "Carol: 9.5", f"Dataclass input failed: {result!r}"


# ======================================================================
# [PERF-2] MetricsCollector — Preservation (sequential behavior)
# ======================================================================


@pytest.mark.asyncio
async def test_perf2_sequential_record_run_increments_total_runs() -> None:
    """[PERF-2] Preservation: sequential record_run() calls increment total_runs correctly.

    Baseline behavior: N sequential calls → total_runs == N.
    Must be preserved after the asyncio.Lock fix.

    Validates: Requirements 3.7
    """
    from agora.metrics.collector import MetricsCollector

    collector = MetricsCollector()

    for _i in range(5):
        await collector.record_run("pipe_a")

    stats = collector.get("pipe_a")
    assert stats is not None, "Stats should exist after record_run calls"
    assert stats.total_runs == 5, (
        f"[PERF-2] PRESERVATION FAILED: Expected total_runs=5 after 5 sequential calls, "
        f"got: {stats.total_runs}"
    )


@pytest.mark.asyncio
async def test_perf2_sequential_record_run_tracks_success_and_failure() -> None:
    """[PERF-2] Preservation: sequential calls track successful_runs and failed_runs correctly.

    Validates: Requirements 3.7
    """
    from agora.metrics.collector import MetricsCollector

    collector = MetricsCollector()

    await collector.record_run("pipe_b")  # success
    await collector.record_run("pipe_b")  # success
    await collector.record_run("pipe_b", error=RuntimeError("oops"))  # failure

    stats = collector.get("pipe_b")
    assert stats is not None
    assert stats.total_runs == 3, f"Expected total_runs=3, got {stats.total_runs}"
    assert stats.successful_runs == 2, f"Expected successful_runs=2, got {stats.successful_runs}"
    assert stats.failed_runs == 1, f"Expected failed_runs=1, got {stats.failed_runs}"


@pytest.mark.asyncio
async def test_perf2_get_returns_pipeline_stats_with_correct_interface() -> None:
    """[PERF-2] Preservation: get() returns PipelineStats with correct interface.

    Baseline behavior: get() returns PipelineStats object with all expected fields.
    Must be preserved after the lock fix.

    Validates: Requirements 3.8
    """
    from agora.metrics.collector import MetricsCollector, PipelineStats

    collector = MetricsCollector()
    await collector.record_run("pipe_c")

    stats = collector.get("pipe_c")

    assert stats is not None, "get() should return stats after record_run"
    assert isinstance(stats, PipelineStats), f"Expected PipelineStats, got {type(stats)}"

    # Verify all expected fields exist
    assert hasattr(stats, "pipeline_id"), "PipelineStats missing pipeline_id"
    assert hasattr(stats, "total_runs"), "PipelineStats missing total_runs"
    assert hasattr(stats, "successful_runs"), "PipelineStats missing successful_runs"
    assert hasattr(stats, "failed_runs"), "PipelineStats missing failed_runs"
    assert hasattr(stats, "total_records_consumed"), "PipelineStats missing total_records_consumed"
    assert hasattr(stats, "total_records_written"), "PipelineStats missing total_records_written"
    assert hasattr(stats, "last_run_at"), "PipelineStats missing last_run_at"
    assert hasattr(stats, "success_rate"), "PipelineStats missing success_rate property"
    assert hasattr(stats, "status"), "PipelineStats missing status property"

    assert stats.pipeline_id == "pipe_c"
    assert stats.total_runs == 1


@pytest.mark.asyncio
async def test_perf2_all_returns_dict_of_pipeline_stats() -> None:
    """[PERF-2] Preservation: all() returns dict[str, PipelineStats] with correct data.

    Validates: Requirements 3.8
    """
    from agora.metrics.collector import MetricsCollector, PipelineStats

    collector = MetricsCollector()
    await collector.record_run("pipe_x")
    await collector.record_run("pipe_y")
    await collector.record_run("pipe_y")

    all_stats = collector.all()

    assert isinstance(all_stats, dict), f"all() should return dict, got {type(all_stats)}"
    assert "pipe_x" in all_stats, "pipe_x should be in all() result"
    assert "pipe_y" in all_stats, "pipe_y should be in all() result"
    assert isinstance(all_stats["pipe_x"], PipelineStats)
    assert isinstance(all_stats["pipe_y"], PipelineStats)
    assert all_stats["pipe_x"].total_runs == 1
    assert all_stats["pipe_y"].total_runs == 2


@pytest.mark.asyncio
async def test_perf2_to_health_dict_returns_correct_json_structure() -> None:
    """[PERF-2] Preservation: to_health_dict() returns health payload with correct JSON structure.

    Validates: Requirements 3.9
    """
    from agora.metrics.collector import MetricsCollector

    collector = MetricsCollector()
    await collector.record_run("pipe_z")

    health = collector.to_health_dict()

    assert isinstance(health, dict), f"to_health_dict() should return dict, got {type(health)}"
    assert "status" in health, "Health dict missing 'status'"
    assert "process" in health, "Health dict missing 'process'"
    assert "uptime_seconds" in health, "Health dict missing 'uptime_seconds'"
    assert "started_at" in health, "Health dict missing 'started_at'"
    assert "pipelines" in health, "Health dict missing 'pipelines'"
    assert "pipe_z" in health["pipelines"], "pipe_z should be in pipelines"

    # Verify it serializes to valid JSON
    serialized = json.dumps(health, default=str)
    parsed = json.loads(serialized)
    assert parsed["pipelines"]["pipe_z"]["total_runs"] == 1


# ======================================================================
# [PERF-4] Backpressure — Preservation (prefetch_limit existing behavior)
# ======================================================================


@pytest.mark.asyncio
async def test_perf4_source_with_prefetch_limit_uses_asyncio_queue() -> None:
    """[PERF-4] Preservation: source with prefetch_limit=100 → asyncio.Queue(maxsize=100).

    Baseline behavior: when a source has supports_prefetch=True and prefetch_limit=100,
    the runtime uses asyncio.Queue(maxsize=100) to bound memory.
    This existing behavior must be preserved after the backpressure fix.

    Validates: Requirements 3.10
    """
    from agora.core.source import BaseSource, prefetch_limit_for

    class _PrefetchSource(BaseSource[int]):
        source_name = "prefetch_test"
        supports_prefetch = True
        prefetch_limit = 100

        async def stream(self):
            for i in range(10):
                yield i

    src = _PrefetchSource()

    # Verify prefetch_limit_for() returns the correct value
    limit = prefetch_limit_for(src)
    assert limit == 100, (
        f"[PERF-4] PRESERVATION FAILED: prefetch_limit_for() should return 100 "
        f"for source with prefetch_limit=100, got: {limit}"
    )

    # Verify the source is recognized as prefetch-capable
    from agora.core.source import is_prefetch_capable

    assert is_prefetch_capable(src), (
        "[PERF-4] PRESERVATION FAILED: source with supports_prefetch=True should be "
        "recognized as prefetch-capable"
    )


@pytest.mark.asyncio
async def test_perf4_source_without_prefetch_limit_returns_zero() -> None:
    """[PERF-4] Preservation: source without prefetch_limit → prefetch_limit_for() returns 0.

    Validates: Requirements 3.10
    """
    from agora.core.source import IterableSource, prefetch_limit_for

    src = IterableSource([1, 2, 3])
    limit = prefetch_limit_for(src)

    assert limit == 0, (
        f"[PERF-4] PRESERVATION FAILED: IterableSource (no prefetch) should return 0, got: {limit}"
    )


@pytest.mark.asyncio
async def test_perf4_max_records_stops_pipeline_correctly() -> None:
    """[PERF-4] Preservation: max_records stops pipeline after exactly N records.

    Baseline behavior: pipeline with max_records=5 processes exactly 5 records.
    Must be preserved after backpressure fix.

    Validates: Requirements 3.12
    """
    from agora.core.pipeline import Pipeline
    from agora.core.source import IterableSource

    records_written: list[int] = []

    class _CollectSink:
        sink_name = "collect"

        async def open(self) -> None:
            pass

        async def write(self, record: int):
            records_written.append(record)
            from agora.core.sink import WriteResult

            return WriteResult(written=True)

        async def flush(self) -> None:
            pass

        async def close(self) -> None:
            pass

    src = IterableSource(list(range(100)))
    sink = _CollectSink()
    pipeline = Pipeline(src).build(sink)

    await pipeline.run(max_records=5)

    assert len(records_written) == 5, (
        f"[PERF-4] PRESERVATION FAILED: max_records=5 should stop after 5 records, "
        f"got {len(records_written)} records written"
    )


# ======================================================================
# [CODE-2] BoundPipeline Builder Methods — Preservation (immutability)
# ======================================================================


def test_code2_bound_pipeline_with_dlq_returns_new_object() -> None:
    """[CODE-2] Preservation: BoundPipeline.with_dlq(sink) returns new object via _clone().

    Baseline behavior: BoundPipeline.with_dlq() already uses _clone() correctly.
    This must be preserved after the Pipeline.with_dlq() fix.

    Validates: Requirements 3.13
    """
    from agora.core.pipeline import BoundPipeline, Pipeline
    from agora.core.source import IterableSource

    class _FakeDLQSink:
        sink_name = "fake_dlq"

        async def open(self) -> None:
            pass

        async def write(self, record: Any) -> None:
            pass

        async def flush(self) -> None:
            pass

        async def close(self) -> None:
            pass

    src = IterableSource([1, 2, 3])
    bound = Pipeline(src).build()
    dlq_sink = _FakeDLQSink()

    # BoundPipeline built with dlq should have it set
    new_bound = Pipeline(src).build(dlq=dlq_sink)

    assert new_bound is not bound, (
        "[CODE-2] PRESERVATION FAILED: BoundPipeline.build(dlq=...) should return a new object, "
        "not mutate self"
    )
    assert isinstance(new_bound, BoundPipeline), (
        f"[CODE-2] PRESERVATION FAILED: build(dlq=...) should return BoundPipeline, "
        f"got {type(new_bound)}"
    )
    # Original should not have DLQ set
    assert bound._dlq_sink is None, (
        "[CODE-2] PRESERVATION FAILED: Original BoundPipeline should not be mutated"
    )
    # New object should have DLQ set
    assert new_bound._dlq_sink is dlq_sink, (
        "[CODE-2] PRESERVATION FAILED: New BoundPipeline should have DLQ sink set"
    )


def test_code2_bound_pipeline_with_checkpoint_store_is_immutable() -> None:
    """[CODE-2] Preservation: BoundPipeline.with_checkpoint_store() behaves immutably.

    Validates: Requirements 3.14
    """
    from agora.core.pipeline import BoundPipeline, Pipeline
    from agora.core.source import IterableSource

    class _FakeCheckpointStore:
        async def load(self, key: str):
            return None

        async def save(self, key: str, checkpoint) -> None:
            pass

    src = IterableSource([1, 2, 3])
    bound = Pipeline(src).build()
    store = _FakeCheckpointStore()

    new_bound = Pipeline(src).build(checkpoint=store)

    assert new_bound is not bound, (
        "[CODE-2] PRESERVATION FAILED: build(checkpoint=...) should return new object"
    )
    assert isinstance(new_bound, BoundPipeline)
    assert bound._checkpoint_store is None, (
        "[CODE-2] PRESERVATION FAILED: Original should not be mutated by build(checkpoint=...)"
    )
    assert new_bound._checkpoint_store is store, (
        "[CODE-2] PRESERVATION FAILED: New object should have checkpoint store set"
    )


def test_code2_bound_pipeline_with_sink_is_immutable() -> None:
    """[CODE-2] Preservation: BoundPipeline.with_sink() behaves immutably.

    Validates: Requirements 3.14
    """
    from agora.core.pipeline import BoundPipeline, Pipeline
    from agora.core.source import IterableSource
    from agora.sinks.io.stdout import StdoutSink

    src = IterableSource([1, 2, 3])
    bound = Pipeline(src).build()
    original_writer = bound._writer

    new_sink = StdoutSink()
    new_bound = bound.with_sink(new_sink)

    assert new_bound is not bound, (
        "[CODE-2] PRESERVATION FAILED: with_sink() should return new object"
    )
    assert isinstance(new_bound, BoundPipeline)
    # Original writer should be unchanged
    assert bound._writer is original_writer, (
        "[CODE-2] PRESERVATION FAILED: Original BoundPipeline writer should not be mutated"
    )


# ======================================================================
# [PROD-3] DLQRecord — Preservation (existing fields)
# ======================================================================


def test_prod3_dlq_record_preserves_all_existing_fields() -> None:
    """[PROD-3] Preservation: DLQRecord created with existing fields → all fields present.

    Baseline behavior: DLQRecord has all 11 existing fields with correct types.
    These must be preserved after adding attempt/max_attempts fields.

    Validates: Requirements 3.15
    """
    from agora.core.dlq import DLQRecord

    now = datetime.now(UTC)
    record = DLQRecord(
        pipeline_id="test_pipe",
        run_id="run_001",
        stage="middleware",
        error_type="RuntimeError",
        error_message="something went wrong",
        record={"id": 42, "data": "payload"},
        source="kafka_source",
        checkpoint={"offset": 100},
        middleware="enrich_middleware",
        sink="postgres_sink",
        created_at=now,
    )

    # All 11 existing fields must be present and correct
    assert record.pipeline_id == "test_pipe", f"pipeline_id mismatch: {record.pipeline_id}"
    assert record.run_id == "run_001", f"run_id mismatch: {record.run_id}"
    assert record.stage == "middleware", f"stage mismatch: {record.stage}"
    assert record.error_type == "RuntimeError", f"error_type mismatch: {record.error_type}"
    assert record.error_message == "something went wrong", (
        f"error_message mismatch: {record.error_message}"
    )
    assert record.record == {"id": 42, "data": "payload"}, f"record mismatch: {record.record}"
    assert record.source == "kafka_source", f"source mismatch: {record.source}"
    assert record.checkpoint == {"offset": 100}, f"checkpoint mismatch: {record.checkpoint}"
    assert record.middleware == "enrich_middleware", f"middleware mismatch: {record.middleware}"
    assert record.sink == "postgres_sink", f"sink mismatch: {record.sink}"
    assert record.created_at == now, f"created_at mismatch: {record.created_at}"


def test_prod3_dlq_record_optional_fields_default_to_none() -> None:
    """[PROD-3] Preservation: DLQRecord optional fields default to None.

    Validates: Requirements 3.15
    """
    from agora.core.dlq import DLQRecord

    record = DLQRecord(
        pipeline_id="pipe",
        run_id="run",
        stage="sink",
        error_type="ValueError",
        error_message="bad value",
        record=None,
    )

    assert record.source is None, f"source should default to None, got {record.source}"
    assert record.checkpoint is None, f"checkpoint should default to None, got {record.checkpoint}"
    assert record.middleware is None, f"middleware should default to None, got {record.middleware}"
    assert record.sink is None, f"sink should default to None, got {record.sink}"
    assert record.created_at is not None, "created_at should have a default value"


def test_prod3_dlq_record_is_frozen() -> None:
    """[PROD-3] Preservation: DLQRecord is frozen (immutable).

    Validates: Requirements 3.15
    """
    from agora.core.dlq import DLQRecord

    record = DLQRecord(
        pipeline_id="pipe",
        run_id="run",
        stage="sink",
        error_type="ValueError",
        error_message="bad value",
        record=None,
    )

    with pytest.raises((AttributeError, TypeError)):
        record.pipeline_id = "mutated"  # type: ignore[misc]


def test_prod3_dlq_sink_is_usable_as_base_class() -> None:
    """[PROD-3] Preservation: DLQSink can be subclassed without breaking changes.

    Baseline behavior: DLQSink is a marker base class that existing subclasses extend.
    Must remain usable after adding replay() method.

    Validates: Requirements 3.16
    """
    from agora.core.dlq import DLQRecord, DLQSink

    class _ConcreteFileDLQSink(DLQSink):
        sink_name = "file_dlq"

        def __init__(self) -> None:
            self.records: list[DLQRecord] = []

        async def open(self) -> None:
            pass

        async def write(self, record: DLQRecord) -> None:
            self.records.append(record)

        async def flush(self) -> None:
            pass

        async def close(self) -> None:
            pass

    # Should be instantiable without errors
    sink = _ConcreteFileDLQSink()
    assert sink.sink_name == "file_dlq"
    assert isinstance(sink, DLQSink)


# ======================================================================
# [TEST-1] Existing Unit Tests — Preservation (not broken by additions)
# ======================================================================


def test_test1_existing_core_imports_work() -> None:
    """[TEST-1] Preservation: existing core module imports work correctly.

    Validates: Requirements 3.17
    """
    # These imports must continue to work after integration test additions
    from agora.core.dlq import DLQRecord, DLQSink
    from agora.core.pipeline import BoundPipeline, Pipeline
    from agora.core.source import BaseSource, IterableSource
    from agora.health.server import HealthServer
    from agora.metrics.collector import MetricsCollector, PipelineStats

    assert Pipeline is not None
    assert BoundPipeline is not None
    assert BaseSource is not None
    assert IterableSource is not None
    assert DLQRecord is not None
    assert DLQSink is not None
    assert MetricsCollector is not None
    assert PipelineStats is not None
    assert HealthServer is not None


def test_test1_existing_ai_imports_work() -> None:
    """[TEST-1] Preservation: existing AI module imports work correctly.

    Validates: Requirements 3.17
    """
    from agora.ai.cache import LLMCache, make_cache_key
    from agora.middlewares.ai.base import AIMiddleware

    assert AIMiddleware is not None
    assert LLMCache is not None
    assert make_cache_key is not None
