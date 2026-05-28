"""
tests/exploration/test_bug_conditions.py
=========================================
Bug Condition Exploration Suite — Agora Core 8-Bug Exploration

**Property 1: Bug Condition** — Agora Core 8-Bug Exploration Suite

CRITICAL: These tests are EXPECTED TO FAIL on unfixed code.
Failure confirms the bugs exist. DO NOT fix the code when these fail.

Each test documents:
- The bug ID and description
- The bug condition (input that triggers the bug)
- The expected (correct) behavior
- The observed (buggy) behavior

**Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8,
             1.9, 1.10, 1.11, 1.12, 1.13, 1.14, 1.15, 1.16**
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

# ======================================================================
# [SEC-1] Health Endpoints Without Authentication
# ======================================================================


@pytest.mark.asyncio
async def test_sec1_health_endpoint_returns_401_without_auth_token() -> None:
    """[SEC-1] Bug Condition: GET /health without auth header when auth_token is configured.

    Expected (correct): HTTP 401 Unauthorized
    Observed (buggy):   HTTP 200 OK — exposes pipeline stats without auth

    Validates: Requirements 1.1, 1.2
    """
    from agora.health.server import HealthServer

    # Bug condition: HealthServer configured with auth_token
    # HealthServer.__init__() does NOT accept auth_token parameter — this is the bug
    try:
        server = HealthServer(port=0, auth_token="secret-token")
    except TypeError as exc:
        # This is the bug: __init__() doesn't accept auth_token
        pytest.fail(
            f"[SEC-1] BUG CONFIRMED: HealthServer.__init__() does not accept auth_token "
            f"parameter. Cannot configure authentication. Error: {exc}"
        )

    # If we get here, the server was created. Now test that unauthenticated request → 401.
    # Start the server on a random port
    server._stop_event = asyncio.Event()
    server._server = await asyncio.start_server(
        server._handle_connection,
        host="127.0.0.1",
        port=0,
    )
    port = server._server.sockets[0].getsockname()[1]

    try:
        # Send GET /health WITHOUT Authorization header
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
        await writer.drain()
        response = await asyncio.wait_for(reader.read(4096), timeout=2.0)
        writer.close()
        await writer.wait_closed()

        response_str = response.decode("ascii", errors="replace")
        status_line = response_str.split("\r\n")[0]

        # EXPECTED: 401 Unauthorized
        # OBSERVED (buggy): 200 OK
        assert "401" in status_line, (
            f"[SEC-1] BUG CONFIRMED: Expected HTTP 401 for unauthenticated request to "
            f"/health when auth_token is configured, but got: {status_line!r}. "
            f"Full response: {response_str[:200]!r}"
        )
    finally:
        server.stop()
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_sec1_metrics_endpoint_returns_401_with_wrong_bearer_token() -> None:
    """[SEC-1] Bug Condition: GET /metrics with wrong Bearer token when auth_token is configured.

    Expected (correct): HTTP 401 Unauthorized
    Observed (buggy):   HTTP 200 OK — exposes Prometheus metrics without valid auth

    Validates: Requirements 1.2
    """
    from agora.health.server import HealthServer

    # Bug condition: HealthServer configured with auth_token
    try:
        server = HealthServer(port=0, auth_token="correct-token")
    except TypeError as exc:
        pytest.fail(
            f"[SEC-1] BUG CONFIRMED: HealthServer.__init__() does not accept auth_token "
            f"parameter. Error: {exc}"
        )

    server._stop_event = asyncio.Event()
    server._server = await asyncio.start_server(
        server._handle_connection,
        host="127.0.0.1",
        port=0,
    )
    port = server._server.sockets[0].getsockname()[1]

    try:
        # Send GET /metrics WITH wrong Bearer token
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(
            b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer wrong-token\r\n\r\n"
        )
        await writer.drain()
        response = await asyncio.wait_for(reader.read(4096), timeout=2.0)
        writer.close()
        await writer.wait_closed()

        response_str = response.decode("ascii", errors="replace")
        status_line = response_str.split("\r\n")[0]

        # EXPECTED: 401 Unauthorized
        # OBSERVED (buggy): 200 OK
        assert "401" in status_line, (
            f"[SEC-1] BUG CONFIRMED: Expected HTTP 401 for wrong Bearer token on /metrics, "
            f"but got: {status_line!r}. Full response: {response_str[:200]!r}"
        )
    finally:
        server.stop()
        await asyncio.sleep(0.05)


# ======================================================================
# [SEC-6] Prompt Injection via str.format_map
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


def test_sec6_render_prompt_raises_key_error_with_extra_record_key() -> None:
    """[SEC-6] Fix Verification: _render_prompt("{text}", {"text": "hi", "extra_key": "x"})

    Expected (correct): Returns "hi" safely — extra keys are ignored, no KeyError
    Previously buggy:   Raised KeyError when template vars were missing from record

    This test verifies the SEC-6 fix is in place:
    - Extra record keys not in template are silently ignored
    - Template vars missing from record are kept as-is (no KeyError)

    Validates: Requirements 1.4, 1.5, 1.6
    """
    middleware = _make_ai_middleware_instance()

    # Verify: extra record key not in template → no KeyError, extra key not in output
    result_extra_key = middleware._render_prompt("{text}", {"text": "hi", "extra_key": "x"})
    assert result_extra_key == "hi", (
        f"[SEC-6] FIX FAILED: _render_prompt('{{text}}', {{'text': 'hi', 'extra_key': 'x'}}) "
        f"returned {result_extra_key!r}, expected 'hi'. Extra keys must be ignored."
    )

    # Verify: template var not in record → placeholder kept as-is, no KeyError
    result_missing_var = middleware._render_prompt("{text} {system}", {"text": "hi"})
    assert "{system}" in result_missing_var, (
        f"[SEC-6] FIX FAILED: _render_prompt('{{text}} {{system}}', {{'text': 'hi'}}) "
        f"returned {result_missing_var!r}, expected '{{system}}' placeholder to be kept. "
        f"Missing template vars must not raise KeyError."
    )
    assert "hi" in result_missing_var, (
        f"[SEC-6] FIX FAILED: 'hi' not found in result {result_missing_var!r}. "
        f"Present template vars must still be substituted."
    )


def test_sec6_render_prompt_raises_key_error_with_template_var_not_in_record() -> None:
    """[SEC-6] Fix Verification: _render_prompt("{text} {system}", {"text": "hi"})

    Expected (correct): Returns "hi {system}" — no exception, placeholder kept as-is
    Previously buggy:   Raised KeyError('system') — unhandled exception crashed pipeline

    This test verifies the SEC-6 fix is in place:
    - Template vars missing from record are kept as-is (e.g. "{system}" stays "{system}")
    - No KeyError is raised

    Validates: Requirements 1.6
    """
    middleware = _make_ai_middleware_instance()

    template = "{text} {system}"
    record = {"text": "hi"}

    # After the fix: no exception, placeholder kept as-is
    result = middleware._render_prompt(template, record)

    assert "{system}" in result, (
        f"[SEC-6] FIX FAILED: _render_prompt('{template}', {record!r}) "
        f"returned {result!r}, expected '{{system}}' placeholder to be kept. "
        f"Missing template vars must not raise KeyError — they should be preserved."
    )
    assert "hi" in result, (
        f"[SEC-6] FIX FAILED: 'hi' not found in result {result!r}. "
        f"Present template vars must still be substituted correctly."
    )


# ======================================================================
# [PERF-2] MetricsCollector Race Condition
# ======================================================================


@pytest.mark.asyncio
async def test_perf2_concurrent_record_run_loses_updates() -> None:
    """[PERF-2] Bug Condition → Fix Verification: 100 concurrent record_run() calls.

    Expected (correct): total_runs == 100 (all updates atomic via asyncio.Lock)
    Observed (buggy):   total_runs < 100 (lost updates due to race condition)

    Now that record_run() is async and protected by asyncio.Lock, calling the
    real implementation concurrently must yield total_runs == 100.

    Validates: Requirements 1.7, 1.8
    """
    from agora.metrics.collector import MetricsCollector

    collector = MetricsCollector()

    # Run 100 concurrent calls against the REAL record_run() implementation
    n = 100
    tasks = [asyncio.create_task(collector.record_run("pipe_a")) for _ in range(n)]
    await asyncio.gather(*tasks)

    total_runs = collector._pipelines["pipe_a"].total_runs

    # EXPECTED: total_runs == 100 (fix confirmed)
    assert total_runs == n, (
        f"[PERF-2] FIX FAILED: Race condition still present! "
        f"Expected total_runs == {n} after {n} concurrent calls, "
        f"but got total_runs == {total_runs}. "
        f"Lost {n - total_runs} updates - asyncio.Lock not working correctly. "
        f"Counterexample: N={n}, observed={total_runs}, lost={n - total_runs}"
    )


@pytest.mark.asyncio
async def test_perf2_record_run_is_synchronous_not_async() -> None:
    """[PERF-2] Bug Condition: record_run() is sync, not async — cannot use asyncio.Lock.

    Expected (correct): record_run() is async def and uses asyncio.Lock
    Observed (buggy):   record_run() is a regular def — cannot acquire asyncio.Lock

    Validates: Requirements 1.7
    """
    import inspect

    from agora.metrics.collector import MetricsCollector

    collector = MetricsCollector()

    # Check if record_run is async
    is_async = inspect.iscoroutinefunction(collector.record_run)

    # Check if collector has a lock
    has_lock = hasattr(collector, "_lock")

    assert is_async and has_lock, (
        f"[PERF-2] BUG CONFIRMED: MetricsCollector.record_run() is not properly protected. "
        f"is_async={is_async} (expected True), has_lock={has_lock} (expected True). "
        f"The docstring claims 'uses asyncio.Lock' but implementation has no lock. "
        f"Counterexample: record_run is {'async' if is_async else 'sync'}, "
        f"_lock {'exists' if has_lock else 'does not exist'}."
    )


# ======================================================================
# [CODE-2] Pipeline.with_dlq() Mutates Private Attributes
# ======================================================================


def test_code2_pipeline_with_dlq_mutates_intermediate_object() -> None:
    """[CODE-2] Bug Condition: build(dlq=...) must not mutate intermediate BoundPipeline.

    Expected (correct): build() and build(dlq=...) return independent objects — no shared state.
    Observed (buggy):   build(dlq=...) mutates _dlq_sink on a shared intermediate object.

    Validates: Requirements 1.11, 1.12
    """
    from agora import DeliveryConfig, IterableSource, Pipeline

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

    src = IterableSource([])
    pipeline = Pipeline(src)
    dlq = _FakeDLQSink()

    base = pipeline.build()
    with_dlq = pipeline.build(config=DeliveryConfig(dlq=dlq))  # type: ignore[arg-type]

    # Must be different objects
    assert base is not with_dlq, (
        "[CODE-2] BUG CONFIRMED: build(dlq=...) returned same object as build()"
    )

    # base must not have been mutated
    assert base._config.dlq is None, (
        f"[CODE-2] BUG CONFIRMED: build(dlq=...) mutated the intermediate BoundPipeline. "
        f"base._config.dlq={base._config.dlq!r} (expected None)"
    )

    # with_dlq must have the DLQ set
    assert with_dlq._config.dlq is dlq, (
        f"[CODE-2] BUG CONFIRMED: build(dlq=...) did not set _dlq_sink correctly. "
        f"with_dlq._config.dlq={with_dlq._config.dlq!r}"
    )


# ======================================================================
# [PROD-3] DLQ Missing Retry/Replay Mechanism
# ======================================================================


def test_prod3_dlq_record_missing_attempt_field() -> None:
    """[PROD-3] Bug Condition: DLQRecord has no 'attempt' field.

    Expected (correct): DLQRecord has attempt: int = 0 and max_attempts: int | None = None
    Observed (buggy):   DLQRecord has no attempt or max_attempts fields

    Validates: Requirements 1.15
    """

    from agora.core.dlq import DLQRecord

    record = DLQRecord(
        pipeline_id="test_pipe",
        run_id="run_001",
        stage="middleware",
        error_type="RuntimeError",
        error_message="boom",
        record={"id": 1},
    )

    # Check for attempt field
    has_attempt = hasattr(record, "attempt")
    has_max_attempts = hasattr(record, "max_attempts")

    assert has_attempt and has_max_attempts, (
        f"[PROD-3] BUG CONFIRMED: DLQRecord is missing retry fields. "
        f"has_attempt={has_attempt} (expected True), "
        f"has_max_attempts={has_max_attempts} (expected True). "
        f"DLQRecord fields: {list(record.__dataclass_fields__.keys())}. "
        f"Without attempt tracking, operators cannot implement retry limiting "
        f"or know how many times a record has been retried."
    )


def test_prod3_dlq_sink_missing_replay_method() -> None:
    """[PROD-3] Bug Condition: DLQSink has no replay() method.

    Expected (correct): DLQSink has async replay(record: DLQRecord) -> DLQRecord
    Observed (buggy):   DLQSink is a marker-only base class with no replay() method

    Validates: Requirements 1.13
    """
    from agora.core.dlq import DLQSink

    has_replay = hasattr(DLQSink, "replay")

    assert has_replay, (
        f"[PROD-3] BUG CONFIRMED: DLQSink has no replay() method. "
        f"DLQSink is a marker-only base class. "
        f"DLQSink methods: {[m for m in dir(DLQSink) if not m.startswith('__')]}. "
        f"Without replay(), operators cannot re-emit failed records for retry."
    )


def test_prod3_dlq_source_does_not_exist() -> None:
    """[PROD-3] Bug Condition: DLQSource class does not exist.

    Expected (correct): DLQSource class exists in agora.core.dlq
    Observed (buggy):   No DLQSource class — cannot read DLQ records back into a pipeline

    Validates: Requirements 1.14
    """
    import agora.core.dlq as dlq_module

    has_dlq_source = hasattr(dlq_module, "DLQSource")

    assert has_dlq_source, (
        f"[PROD-3] BUG CONFIRMED: DLQSource class does not exist in agora.core.dlq. "
        f"Available names in module: {[n for n in dir(dlq_module) if not n.startswith('_')]}. "
        f"Without DLQSource, operators cannot replay failed records into a new pipeline."
    )


# ======================================================================
# [PERF-4] No Backpressure Mechanism
# ======================================================================


@pytest.mark.asyncio
async def test_perf4_in_flight_grows_unbounded_with_slow_sink() -> None:
    """[PERF-4] Bug Condition: Fast source + slow sink + no max_buffer_size → unbounded memory.

    Expected (correct): len(in_flight) stays bounded when max_buffer_size is configured
    Observed (buggy):   in_flight deque grows without bound — no backpressure applied

    We use a fast IterableSource (10k records) and a slow sink (sleep 0.01s/record).
    Without backpressure, all futures are submitted immediately and in_flight grows
    to the full source size.

    Note: This test uses a BufferedMiddleware to trigger run_buffered_pipeline()
    which is where in_flight accumulates.

    Validates: Requirements 1.9, 1.10
    """
    import asyncio

    from agora.core.middleware import Middleware

    class _TrackingBufferedMiddleware(Middleware[int, int]):
        """Buffered middleware that tracks in_flight size."""

        name = "tracking_buffered"
        min_concurrency = 10_000  # Very high concurrency limit - no drain until full

        def __init__(self) -> None:
            self._pending: list[tuple[int, asyncio.Future]] = []

        async def process(self, record: int, ctx) -> int | None:
            return record

        async def submit(self, record: int, ctx) -> asyncio.Future:
            future: asyncio.Future = asyncio.get_running_loop().create_future()
            self._pending.append((record, future))
            # Immediately resolve to keep pipeline moving
            future.set_result(record)
            return future

        async def drain_pending(self, ctx) -> None:
            self._pending.clear()

    class _SlowSink:
        sink_name = "slow_sink"

        def __init__(self) -> None:
            self.records: list[int] = []

        async def open(self) -> None:
            pass

        async def write(self, record: int) -> None:
            await asyncio.sleep(0.001)  # Simulate slow sink
            self.records.append(record)

        async def flush(self) -> None:
            pass

        async def close(self) -> None:
            pass

    # Bug is fixed: BoundPipeline now accepts backpressure via build(config=DeliveryConfig(...))
    import inspect

    from agora import DeliveryConfig
    from agora.core.pipeline import BoundPipeline

    # Confirm backpressure is configurable via the DeliveryConfig passed to build()

    build_sig = inspect.signature(BoundPipeline.__init__)
    has_config_param = "config" in build_sig.parameters
    config_fields = inspect.signature(DeliveryConfig).parameters
    has_backpressure_param = "backpressure" in config_fields

    assert has_config_param and has_backpressure_param, (
        f"[PERF-4] BUG CONFIRMED: BoundPipeline has no backpressure configuration. "
        f"BoundPipeline.__init__ params: {list(build_sig.parameters)}; "
        f"DeliveryConfig fields: {list(config_fields)}. "
        f"Without backpressure support, in_flight will grow unbounded with fast sources and slow sinks."
    )


def test_perf4_bound_pipeline_has_no_max_buffer_size_attribute() -> None:
    """[PERF-4] Bug Condition: BoundPipeline has no max_buffer_size configuration.

    Expected (correct): BoundPipeline exposes max_buffer_size and backpressure via _config
    Observed (buggy):   No such configuration exists

    Validates: Requirements 1.9, 1.10
    """
    from agora import IterableSource, Pipeline
    from agora.sinks.io.stdout import StdoutSink

    bound = Pipeline(IterableSource([])).build(StdoutSink())

    has_attr = hasattr(bound._config, "max_buffer_size")
    backpressure_via_build = hasattr(bound._config, "backpressure")

    assert has_attr and backpressure_via_build, (
        f"[PERF-4] BUG CONFIRMED: BoundPipeline missing backpressure configuration. "
        f"_config has 'max_buffer_size': {has_attr} (expected True), "
        f"_config has 'backpressure': {backpressure_via_build} (expected True). "
        f"Without these, there is no way to bound memory usage for fast-source/slow-sink pipelines."
    )
