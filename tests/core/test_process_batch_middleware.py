"""
tests/core/test_process_batch_middleware.py
============================================
Tests for ProcessBatchMiddleware and ProcessPoolRunner.

Coverage:
- Constructor validation
- Pool runner: success, timeout, worker exception
- Middleware: transforms batch correctly
- Middleware: batch-only contract is enforced
- Middleware: metrics are not double-counted on the batch lane
- Middleware: worker failure → BatchProcessResult with failure set
- Middleware: on_stop cleans up pool
- Middleware: used before on_start raises
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from agora.core.runtime._process import (
    ProcessBatchError,
    ProcessPoolRunner,
    ProcessPoolUnavailableError,
)
from agora.core.runtime._process_codec import ArrowBatchCodec, BatchCodecError, PythonObjectCodec
from agora.middlewares.process import ArrowProcessBatchMiddleware, ProcessBatchMiddleware

requires_process_pool = pytest.mark.requires_process_pool

# ======================================================================
# Helpers
# ======================================================================


def _double(batch: list[int]) -> list[int]:
    return [x * 2 for x in batch]


def _identity(batch: list[Any]) -> list[Any]:
    return list(batch)


def _raise_value_error(batch: list[Any]) -> list[Any]:
    raise ValueError("worker exploded")


def _drop_one(batch: list[Any]) -> list[Any]:
    return batch[:-1]  # returns wrong length on purpose


def _slow_batch(batch: list[Any]) -> list[Any]:
    import time

    time.sleep(10)
    return batch


def _timeout_then_double(batch: list[int]) -> list[int]:
    import time

    if batch and batch[0] < 0:
        time.sleep(2.0)
    return [x * 2 for x in batch]


def _sleepy_double(batch: list[int]) -> list[int]:
    import time

    if batch and batch[0] == 1:
        time.sleep(1.5)
    else:
        time.sleep(0.2)
    return [x * 2 for x in batch]


def _timeout_then_invalidate_sibling(batch: list[int]) -> list[int]:
    import time

    if batch and batch[0] in {-1, 0}:
        time.sleep(3.0)
    else:
        time.sleep(0.2)
    return [x * 2 for x in batch]


def _arrow_double(batch: Any) -> Any:
    import pyarrow.compute as pc

    idx = batch.schema.get_field_index("value")
    doubled = pc.multiply(batch.column(idx), 2)
    return batch.set_column(idx, "value", doubled)


def _arrow_drop_row(batch: Any) -> Any:
    return batch.slice(0, max(0, batch.num_rows - 1))


def _arrow_timeout_then_double(batch: Any) -> Any:
    import time

    if batch.num_rows and batch.column(0)[0].as_py() < 0:
        time.sleep(4.0)
    return _arrow_double(batch)


def _arrow_sleepy_double(batch: Any) -> Any:
    import time

    if batch.num_rows and batch.column(0)[0].as_py() < 0:
        time.sleep(1.5)
    else:
        time.sleep(0.2)
    return _arrow_double(batch)


class _EnvelopeCodec:
    name = "envelope"

    def batch_size(self, batch: Any) -> int:
        return len(batch["items"])

    def encode_for_worker(self, batch: Any) -> Any:
        return {"items": list(batch["items"])}

    def decode_in_worker(self, payload: Any) -> list[int]:
        return list(payload["items"])

    def encode_from_worker(self, batch: Any) -> Any:
        return {"items": list(batch)}

    def decode_from_worker(self, payload: Any, *, expected_rows: int) -> list[int]:
        result = list(payload["items"])
        if len(result) != expected_rows:
            raise BatchCodecError("custom codec length mismatch")
        return result


def _make_ctx() -> MagicMock:
    ctx = MagicMock()
    m_metrics = MagicMock()
    m_metrics.records_in = 0
    m_metrics.records_out = 0
    m_metrics.records_dropped = 0
    m_metrics.total_time_ms = 0.0
    ctx.metrics.middleware.return_value = m_metrics
    ctx.log = MagicMock()
    ctx.trace_span = MagicMock()
    ctx.trace_span.__enter__ = MagicMock(return_value=None)
    ctx.trace_span.__exit__ = MagicMock(return_value=False)
    return ctx


# ======================================================================
# Constructor validation
# ======================================================================


def test_constructor_rejects_zero_max_workers() -> None:
    with pytest.raises(ValueError, match="max_workers"):
        ProcessBatchMiddleware(fn=_identity, max_workers=0)


def test_constructor_rejects_negative_max_workers() -> None:
    with pytest.raises(ValueError, match="max_workers"):
        ProcessBatchMiddleware(fn=_identity, max_workers=-1)


def test_constructor_rejects_zero_max_in_flight() -> None:
    with pytest.raises(ValueError, match="max_in_flight_batches"):
        ProcessBatchMiddleware(fn=_identity, max_in_flight_batches=0)


def test_constructor_rejects_zero_timeout() -> None:
    with pytest.raises(ValueError, match="timeout_s"):
        ProcessBatchMiddleware(fn=_identity, timeout_s=0.0)


def test_constructor_rejects_negative_timeout() -> None:
    with pytest.raises(ValueError, match="timeout_s"):
        ProcessBatchMiddleware(fn=_identity, timeout_s=-1.0)


def test_constructor_accepts_valid_params() -> None:
    mw = ProcessBatchMiddleware(
        fn=_identity,
        max_workers=2,
        ordered=True,
        timeout_s=30.0,
        max_in_flight_batches=4,
        name="test_mw",
    )
    assert mw.name == "test_mw"


# ======================================================================
# Codec unit tests
# ======================================================================


def test_python_object_codec_rejects_non_list_input() -> None:
    codec = PythonObjectCodec()
    with pytest.raises(BatchCodecError, match="expected batch input to be list"):
        codec.encode_for_worker((1, 2, 3))


def test_python_object_codec_rejects_non_sequence_worker_result() -> None:
    codec = PythonObjectCodec()
    with pytest.raises(BatchCodecError, match="non-sequence result"):
        codec.decode_from_worker(123, expected_rows=1)


def test_arrow_batch_codec_rejects_non_record_batch() -> None:
    pytest.importorskip("pyarrow")
    codec = ArrowBatchCodec()
    with pytest.raises(BatchCodecError, match=r"pyarrow.RecordBatch"):
        codec.encode_for_worker([{"id": 1}])


def test_arrow_batch_codec_roundtrip_uses_ipc_bytes() -> None:
    pa = pytest.importorskip("pyarrow")
    codec = ArrowBatchCodec()
    batch = pa.RecordBatch.from_pylist([{"id": 1, "value": 2}, {"id": 2, "value": 3}])
    payload = codec.encode_for_worker(batch)
    assert isinstance(payload, bytes)
    result = codec.decode_from_worker(payload, expected_rows=2)
    assert isinstance(result, pa.RecordBatch)
    assert result.num_rows == 2


def test_arrow_batch_codec_rejects_row_count_mismatch() -> None:
    pa = pytest.importorskip("pyarrow")
    codec = ArrowBatchCodec()
    batch = pa.RecordBatch.from_pylist([{"id": 1, "value": 2}])
    payload = codec.encode_for_worker(batch)
    with pytest.raises(BatchCodecError, match="row count must match"):
        codec.decode_from_worker(payload, expected_rows=2)


# ======================================================================
# ProcessPoolRunner unit tests
# ======================================================================


@pytest.mark.asyncio
@requires_process_pool
async def test_runner_returns_batch_result() -> None:
    runner = ProcessPoolRunner(max_workers=1, middleware_name="test")
    codec = PythonObjectCodec()
    runner.open()
    try:
        result = await runner.submit(
            _double,
            [1, 2, 3],
            batch_index=0,
            timeout_s=None,
            codec=codec,
        )
        assert result == [2, 4, 6]
    finally:
        runner.close()


@pytest.mark.asyncio
@requires_process_pool
async def test_runner_raises_process_batch_error_on_worker_exception() -> None:
    runner = ProcessPoolRunner(max_workers=1, middleware_name="test")
    codec = PythonObjectCodec()
    runner.open()
    try:
        with pytest.raises(ProcessBatchError) as exc_info:
            await runner.submit(
                _raise_value_error,
                [1, 2],
                batch_index=0,
                timeout_s=None,
                codec=codec,
            )
        assert "worker exploded" in str(exc_info.value)
        assert exc_info.value.timed_out is False
        assert exc_info.value.batch_index == 0
    finally:
        runner.close()


@pytest.mark.asyncio
@requires_process_pool
async def test_runner_raises_process_batch_error_on_timeout() -> None:
    runner = ProcessPoolRunner(max_workers=1, middleware_name="test")
    codec = PythonObjectCodec()
    runner.open()
    try:
        with pytest.raises(ProcessBatchError) as exc_info:
            await runner.submit(
                _slow_batch,
                [1],
                batch_index=0,
                timeout_s=0.1,
                codec=codec,
            )
        assert exc_info.value.timed_out is True
    finally:
        runner.close(wait=False)


@pytest.mark.asyncio
@requires_process_pool
async def test_runner_drain_reports_pending_work_after_timeout() -> None:
    runner = ProcessPoolRunner(max_workers=1, middleware_name="test")
    codec = PythonObjectCodec()
    runner.open()
    try:
        with pytest.raises(ProcessBatchError):
            await runner.submit(
                _slow_batch,
                [1],
                batch_index=0,
                timeout_s=0.1,
                codec=codec,
            )
        assert await runner.drain(timeout_s=0.01) is False
    finally:
        runner.close(wait=False, force=True)


@pytest.mark.asyncio
@requires_process_pool
async def test_runner_raises_when_closed() -> None:
    runner = ProcessPoolRunner(max_workers=1, middleware_name="test")
    codec = PythonObjectCodec()
    runner.open()
    runner.close()
    with pytest.raises(RuntimeError, match="already closed"):
        await runner.submit(_identity, [1], batch_index=0, timeout_s=None, codec=codec)


@pytest.mark.asyncio
@requires_process_pool
async def test_runner_uses_codec_worker_wrapper() -> None:
    runner = ProcessPoolRunner(max_workers=1, middleware_name="test")
    runner.open()
    try:
        result = await runner.submit(
            _double,
            {"items": [1, 2, 3]},
            batch_index=0,
            timeout_s=None,
            codec=_EnvelopeCodec(),
        )
        assert result == [2, 4, 6]
    finally:
        runner.close()


def test_runner_open_surfaces_process_pool_unavailable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boom(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise PermissionError("Operation not permitted")

    monkeypatch.setattr("agora.core.runtime._process.ProcessPoolExecutor", _boom)

    runner = ProcessPoolRunner(max_workers=1, middleware_name="test")

    with pytest.raises(ProcessPoolUnavailableError, match="Process pool unavailable"):
        runner.open()


# ======================================================================
# ProcessBatchMiddleware integration tests
# ======================================================================


@pytest.mark.asyncio
@requires_process_pool
async def test_middleware_transforms_batch() -> None:
    mw: ProcessBatchMiddleware[int, int] = ProcessBatchMiddleware(
        fn=_double, max_workers=1, name="doubler"
    )
    ctx = _make_ctx()
    await mw.on_start(ctx)
    try:
        result = await mw.process_batch([1, 2, 3], ctx)
        assert result == [2, 4, 6]
    finally:
        await mw.on_stop(ctx)


@pytest.mark.asyncio
@requires_process_pool
async def test_middleware_multiple_batches_ordered() -> None:
    mw: ProcessBatchMiddleware[int, int] = ProcessBatchMiddleware(
        fn=_double, max_workers=2, max_in_flight_batches=4, name="doubler"
    )
    ctx = _make_ctx()
    await mw.on_start(ctx)
    try:
        r1 = await mw.process_batch([1, 2], ctx)
        r2 = await mw.process_batch([3, 4], ctx)
        r3 = await mw.process_batch([5], ctx)
        assert r1 == [2, 4]
        assert r2 == [6, 8]
        assert r3 == [10]
    finally:
        await mw.on_stop(ctx)


@pytest.mark.asyncio
async def test_middleware_raises_before_on_start() -> None:
    mw: ProcessBatchMiddleware[int, int] = ProcessBatchMiddleware(fn=_double)
    ctx = _make_ctx()
    with pytest.raises(RuntimeError, match="before on_start"):
        await mw.process_batch([1, 2], ctx)


@pytest.mark.asyncio
@requires_process_pool
async def test_middleware_worker_exception_propagates() -> None:
    mw: ProcessBatchMiddleware[Any, Any] = ProcessBatchMiddleware(
        fn=_raise_value_error, max_workers=1, name="failing"
    )
    ctx = _make_ctx()
    await mw.on_start(ctx)
    try:
        with pytest.raises(ProcessBatchError) as exc_info:
            await mw.process_batch([1, 2], ctx)
        assert "worker exploded" in str(exc_info.value)
    finally:
        await mw.on_stop(ctx)


@pytest.mark.asyncio
async def test_middleware_rejects_per_record_execution() -> None:
    mw: ProcessBatchMiddleware[int, int] = ProcessBatchMiddleware(fn=_double, max_workers=1)
    ctx = _make_ctx()
    with pytest.raises(RuntimeError, match="batch-capable source"):
        await mw.process(1, ctx)


@pytest.mark.asyncio
@requires_process_pool
async def test_middleware_result_length_mismatch_raises() -> None:
    mw: ProcessBatchMiddleware[Any, Any] = ProcessBatchMiddleware(
        fn=_drop_one, max_workers=1, name="bad_length"
    )
    ctx = _make_ctx()
    await mw.on_start(ctx)
    try:
        with pytest.raises(RuntimeError, match="lengths must match"):
            await mw.process_batch([1, 2, 3], ctx)
    finally:
        await mw.on_stop(ctx)


@pytest.mark.asyncio
@requires_process_pool
async def test_middleware_on_stop_is_idempotent() -> None:
    mw: ProcessBatchMiddleware[int, int] = ProcessBatchMiddleware(fn=_double, max_workers=1)
    ctx = _make_ctx()
    await mw.on_start(ctx)
    await mw.on_stop(ctx)
    # Second stop should not raise
    await mw.on_stop(ctx)


@pytest.mark.asyncio
async def test_middleware_rejects_unordered_pipelined_mode_for_now() -> None:
    mw: ProcessBatchMiddleware[int, int] = ProcessBatchMiddleware(
        fn=_double,
        max_workers=2,
        ordered=False,
        max_in_flight_batches=2,
        name="unordered_not_supported",
    )
    ctx = _make_ctx()
    with pytest.raises(NotImplementedError, match="ordered=True"):
        await mw.on_start(ctx)


@pytest.mark.asyncio
@requires_process_pool
async def test_middleware_apply_in_batch_returns_failure_on_worker_error() -> None:
    """apply_in_batch (called by MiddlewareChain) must return BatchProcessResult on failure."""
    from agora.core.batch import BatchProcessResult

    mw: ProcessBatchMiddleware[Any, Any] = ProcessBatchMiddleware(
        fn=_raise_value_error, max_workers=1, name="failing"
    )
    ctx = _make_ctx()
    await mw.on_start(ctx)
    try:
        chain_stub = MagicMock()
        result = await mw.apply_in_batch([1, 2], ctx, chain_stub, idx=0)
        assert isinstance(result, BatchProcessResult)
        assert result.failure is not None
        assert result.ok is False
        assert "worker exploded" in str(result.failure.exception)
    finally:
        await mw.on_stop(ctx)


@pytest.mark.asyncio
@requires_process_pool
async def test_middleware_metrics_not_double_counted_in_apply_in_batch() -> None:
    mw: ProcessBatchMiddleware[int, int] = ProcessBatchMiddleware(
        fn=_double, max_workers=1, name="metrics"
    )
    ctx = _make_ctx()
    await mw.on_start(ctx)
    try:
        result = await mw.apply_in_batch([1, 2, 3], ctx, MagicMock(), idx=0)
        assert result == [2, 4, 6]
        metrics = ctx.metrics.middleware.return_value
        assert metrics.records_in == 3
        assert metrics.records_out == 3
        assert metrics.records_dropped == 0
        assert ctx.trace_span.call_count == 1
    finally:
        await mw.on_stop(ctx)


@pytest.mark.asyncio
@requires_process_pool
async def test_middleware_recycles_pool_after_timeout_and_next_batch_succeeds() -> None:
    mw: ProcessBatchMiddleware[int, int] = ProcessBatchMiddleware(
        fn=_timeout_then_double,
        max_workers=1,
        timeout_s=1.5,
        name="timeout_recycle",
    )
    ctx = _make_ctx()
    await mw.on_start(ctx)
    original_runner = mw._runner
    try:
        with pytest.raises(ProcessBatchError) as exc_info:
            await mw.process_batch([-1], ctx)
        assert exc_info.value.timed_out is True
        assert mw._runner is not None
        assert mw._runner is not original_runner

        result = await mw.process_batch([2, 3], ctx)
        assert result == [4, 6]
    finally:
        await mw.on_stop(ctx)


@pytest.mark.asyncio
@requires_process_pool
async def test_process_batch_submit_batch_returns_concurrent_tasks() -> None:
    mw: ProcessBatchMiddleware[int, int] = ProcessBatchMiddleware(
        fn=_sleepy_double,
        max_workers=2,
        max_in_flight_batches=2,
        name="submit_batch",
    )
    ctx = _make_ctx()
    await mw.on_start(ctx)
    try:
        # Pre-warm the process pool so the concurrency assertion is not
        # sensitive to first-worker startup variance on slower hosts.
        assert await mw.process_batch([0], ctx) == [0]
        slow = await mw.submit_batch([1], ctx)
        fast = await mw.submit_batch([2], ctx)
        done, pending = await asyncio.wait({slow, fast}, timeout=1.2)
        assert fast in done
        assert slow in pending
        assert fast.result() == [4]
        await slow
    finally:
        await mw.on_stop(ctx)


@pytest.mark.asyncio
@requires_process_pool
async def test_timeout_invalidates_unresolved_sibling_batch_in_same_generation() -> None:
    mw: ProcessBatchMiddleware[int, int] = ProcessBatchMiddleware(
        fn=_timeout_then_invalidate_sibling,
        max_workers=2,
        max_in_flight_batches=2,
        timeout_s=1.5,
        name="generation_timeout",
    )
    ctx = _make_ctx()
    await mw.on_start(ctx)
    try:
        timed_out = await mw.submit_batch([-1], ctx)
        await asyncio.sleep(1.1)
        invalidated = await mw.submit_batch([0], ctx)

        with pytest.raises(ProcessBatchError) as timeout_exc:
            await timed_out
        assert timeout_exc.value.timed_out is True
        assert timeout_exc.value.invalidated is False

        with pytest.raises(ProcessBatchError) as invalidated_exc:
            await invalidated
        assert invalidated_exc.value.invalidated is True
        assert invalidated_exc.value.timed_out is False

        result = await mw.process_batch([2], ctx)
        assert result == [4]
    finally:
        await mw.on_stop(ctx)


# ======================================================================
# Lifecycle tests
# ======================================================================


@pytest.mark.asyncio
@requires_process_pool
async def test_middleware_repeated_runs_do_not_leak_resources() -> None:
    """on_start / on_stop cycles must not leak process pool resources."""
    mw: ProcessBatchMiddleware[int, int] = ProcessBatchMiddleware(
        fn=_double, max_workers=1, name="leak_test"
    )
    ctx = _make_ctx()

    for _ in range(3):
        await mw.on_start(ctx)
        result = await mw.process_batch([1, 2], ctx)
        assert result == [2, 4]
        await mw.on_stop(ctx)
        # After stop, runner must be None — no dangling pool reference.
        assert mw._runner is None


@pytest.mark.asyncio
async def test_middleware_on_stop_without_on_start_is_safe() -> None:
    """on_stop called before on_start must not raise."""
    mw: ProcessBatchMiddleware[int, int] = ProcessBatchMiddleware(fn=_double, max_workers=1)
    ctx = _make_ctx()
    await mw.on_stop(ctx)  # should not raise


@pytest.mark.asyncio
@requires_process_pool
async def test_middleware_clean_shutdown_after_successful_run() -> None:
    """Pool shuts down cleanly after a normal pipeline run."""
    mw: ProcessBatchMiddleware[int, int] = ProcessBatchMiddleware(
        fn=_double, max_workers=1, name="clean_shutdown"
    )
    ctx = _make_ctx()
    await mw.on_start(ctx)
    await mw.process_batch([10, 20, 30], ctx)
    await mw.on_stop(ctx)
    # After stop, submitting must raise because runner is gone.
    with pytest.raises(RuntimeError, match="before on_start"):
        await mw.process_batch([1], ctx)


@pytest.mark.asyncio
@requires_process_pool
async def test_middleware_pool_closed_after_worker_failure() -> None:
    """Pool must close cleanly even when the last batch raised a worker error."""
    mw: ProcessBatchMiddleware[Any, Any] = ProcessBatchMiddleware(
        fn=_raise_value_error, max_workers=1, name="fail_then_stop"
    )
    ctx = _make_ctx()
    await mw.on_start(ctx)

    with pytest.raises(ProcessBatchError):
        await mw.process_batch([1], ctx)

    # on_stop must not raise despite the prior failure.
    await mw.on_stop(ctx)
    assert mw._runner is None


@pytest.mark.asyncio
async def test_middleware_on_start_surfaces_process_pool_unavailable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boom(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise PermissionError("Operation not permitted")

    monkeypatch.setattr("agora.core.runtime._process.ProcessPoolExecutor", _boom)

    mw: ProcessBatchMiddleware[int, int] = ProcessBatchMiddleware(
        fn=_double, max_workers=1, name="unavailable_pool"
    )
    ctx = _make_ctx()

    with pytest.raises(ProcessPoolUnavailableError, match="unavailable_pool"):
        await mw.on_start(ctx)


# ======================================================================
# ArrowProcessBatchMiddleware tests
# ======================================================================


@pytest.mark.asyncio
@requires_process_pool
async def test_arrow_process_middleware_transforms_record_batch() -> None:
    pa = pytest.importorskip("pyarrow")
    mw = ArrowProcessBatchMiddleware(fn=_arrow_double, max_workers=1, name="arrow_doubler")
    ctx = _make_ctx()
    batch = pa.RecordBatch.from_pylist([{"id": 1, "value": 2}, {"id": 2, "value": 3}])
    await mw.on_start(ctx)
    try:
        result = await mw.process_arrow_batch(batch, ctx)
        assert isinstance(result, pa.RecordBatch)
        assert result.column(result.schema.get_field_index("value")).to_pylist() == [4, 6]
    finally:
        await mw.on_stop(ctx)


@pytest.mark.asyncio
@requires_process_pool
async def test_arrow_process_middleware_rejects_row_count_change() -> None:
    pa = pytest.importorskip("pyarrow")
    mw = ArrowProcessBatchMiddleware(fn=_arrow_drop_row, max_workers=1, name="arrow_bad_rows")
    ctx = _make_ctx()
    batch = pa.RecordBatch.from_pylist([{"id": 1, "value": 2}, {"id": 2, "value": 3}])
    await mw.on_start(ctx)
    try:
        with pytest.raises(RuntimeError, match="row count must match"):
            await mw.process_arrow_batch(batch, ctx)
    finally:
        await mw.on_stop(ctx)


@pytest.mark.asyncio
@requires_process_pool
async def test_arrow_process_middleware_recycles_pool_after_timeout() -> None:
    pa = pytest.importorskip("pyarrow")
    mw = ArrowProcessBatchMiddleware(
        fn=_arrow_timeout_then_double,
        max_workers=1,
        timeout_s=2.0,
        name="arrow_timeout",
    )
    ctx = _make_ctx()
    timeout_batch = pa.RecordBatch.from_pylist([{"id": -1, "value": 2}])
    ok_batch = pa.RecordBatch.from_pylist([{"id": 1, "value": 3}])
    await mw.on_start(ctx)
    original_runner = mw._runner
    try:
        with pytest.raises(ProcessBatchError) as exc_info:
            await mw.process_arrow_batch(timeout_batch, ctx)
        assert exc_info.value.timed_out is True
        assert mw._runner is not None
        assert mw._runner is not original_runner

        result = await mw.process_arrow_batch(ok_batch, ctx)
        assert result.column(result.schema.get_field_index("value")).to_pylist() == [6]
    finally:
        await mw.on_stop(ctx)


# ======================================================================
# Error-path tests
# ======================================================================


@pytest.mark.asyncio
@requires_process_pool
async def test_process_batch_error_includes_middleware_name() -> None:
    mw: ProcessBatchMiddleware[Any, Any] = ProcessBatchMiddleware(
        fn=_raise_value_error, max_workers=1, name="named_mw"
    )
    ctx = _make_ctx()
    await mw.on_start(ctx)
    try:
        with pytest.raises(Exception) as exc_info:
            await mw.process_batch([1], ctx)
        assert "named_mw" in str(exc_info.value)
    finally:
        await mw.on_stop(ctx)


@pytest.mark.asyncio
@requires_process_pool
async def test_process_batch_error_includes_batch_index() -> None:
    from agora.core.runtime._process import ProcessBatchError

    mw: ProcessBatchMiddleware[Any, Any] = ProcessBatchMiddleware(
        fn=_raise_value_error, max_workers=1, name="idx_test"
    )
    ctx = _make_ctx()
    await mw.on_start(ctx)
    try:
        # First batch: index 0
        with pytest.raises(ProcessBatchError) as exc_info:
            await mw.process_batch([1], ctx)
        assert exc_info.value.batch_index == 0
    finally:
        await mw.on_stop(ctx)


@pytest.mark.asyncio
@requires_process_pool
async def test_timeout_error_includes_timed_out_flag() -> None:
    from agora.core.runtime._process import ProcessBatchError

    mw: ProcessBatchMiddleware[Any, Any] = ProcessBatchMiddleware(
        fn=_slow_batch, max_workers=1, name="timeout_test", timeout_s=0.1
    )
    ctx = _make_ctx()
    await mw.on_start(ctx)
    try:
        with pytest.raises(ProcessBatchError) as exc_info:
            await mw.process_batch([1], ctx)
        assert exc_info.value.timed_out is True
        assert "timeout_test" in str(exc_info.value)
    finally:
        await mw.on_stop(ctx)


@pytest.mark.asyncio
@requires_process_pool
async def test_non_pickleable_fn_fails_at_submission() -> None:
    """A lambda (non-pickleable) must fail clearly, not silently hang."""
    from agora.core.runtime._process import ProcessBatchError

    mw: ProcessBatchMiddleware[Any, Any] = ProcessBatchMiddleware(
        fn=lambda batch: batch,  # non-pickleable
        max_workers=1,
        name="unpickleable",
    )
    ctx = _make_ctx()
    await mw.on_start(ctx)
    try:
        with pytest.raises(ProcessBatchError):
            await mw.process_batch([1, 2], ctx)
    finally:
        await mw.on_stop(ctx)
