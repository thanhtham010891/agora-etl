"""
tests/core/test_backpressure.py
================================
Property-Based Tests — Backpressure in run_buffered_pipeline()

**Property 7: Bug Condition — Backpressure Bounds Memory**

For any pipeline with max_buffer_size=N configured and source emitting faster
than sink consumes, fixed pipeline SHALL suspend source iteration when the
number of pending records exceeds N, ensuring memory usage is bounded.

**Property 8: Preservation — Backpressure Does Not Affect Balanced Throughput**

For any pipeline where source and sink have equivalent throughput, fixed
pipeline SHALL process records with the same throughput as original, without
introducing unnecessary latency.

**Validates: Requirements 2.9, 2.10, 3.10, 3.11, 3.12**
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from agora import InMemoryCheckpointStore, IterableSource, Pipeline
from agora.core.middleware import Middleware
from agora.core.sink import WriteResult
from agora.core.source import BaseSource
from agora.core.types import Backpressure

# ======================================================================
# Helpers — instrumented buffered middleware and sinks
# ======================================================================


class _InFlightTrackingMiddleware(Middleware[int, int]):
    """Buffered middleware that tracks the maximum observed in_flight depth.

    Records are submitted as futures that resolve immediately, so the
    in_flight deque depth is determined purely by the backpressure logic.
    """

    name = "in_flight_tracking"

    def __init__(self, min_concurrency: int = 10_000) -> None:
        # Very high concurrency limit so the existing concurrency-based drain
        # does NOT trigger — only the max_buffer_size backpressure should drain.
        self.min_concurrency = min_concurrency
        self._pending: list[tuple[int, asyncio.Future[int]]] = []

    async def process(self, record: int, ctx: Any) -> int | None:
        return record

    async def submit(self, record: int, ctx: Any) -> asyncio.Future[int]:
        future: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        self._pending.append((record, future))
        # Resolve immediately so the future is ready when drained
        future.set_result(record)
        return future

    async def drain_pending(self, ctx: Any) -> None:
        for _record, future in self._pending:
            if not future.done():
                future.set_result(_record)
        self._pending.clear()


class _SlowSink:
    """Sink that introduces a configurable delay per write to simulate slow consumers."""

    sink_name = "slow_sink"

    def __init__(self, delay: float = 0.0) -> None:
        self.delay = delay
        self.records: list[int] = []

    async def open(self) -> None:
        pass

    async def write(self, record: int) -> WriteResult:
        if self.delay > 0:
            await asyncio.sleep(self.delay)
        self.records.append(record)
        return WriteResult(written=True)

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


class _CollectSink:
    """Simple collecting sink with no delay."""

    sink_name = "collect_sink"

    def __init__(self) -> None:
        self.records: list[int] = []

    async def open(self) -> None:
        pass

    async def write(self, record: int) -> WriteResult:
        self.records.append(record)
        return WriteResult(written=True)

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


class _FastBatchCollectSink:
    sink_name = "fast_batch_collect"

    def __init__(self) -> None:
        self.records: list[int] = []
        self.batches: list[list[int]] = []

    async def open(self) -> None:
        pass

    async def write(self, record: int) -> WriteResult:
        self.records.append(record)
        return WriteResult(written=True)

    async def write_batch(self, records: list[int]) -> list[WriteResult]:
        batch = list(records)
        self.batches.append(batch)
        self.records.extend(batch)
        return [WriteResult(written=True) for _ in batch]

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


class _SlowBatchCollectSink:
    sink_name = "slow_batch_collect"

    def __init__(self, delay: float) -> None:
        self.delay = delay
        self.records: list[int] = []
        self.batches: list[list[int]] = []

    async def open(self) -> None:
        pass

    async def write(self, record: int) -> WriteResult:
        self.records.append(record)
        return WriteResult(written=True)

    async def write_batch(self, records: list[int]) -> list[WriteResult]:
        await asyncio.sleep(self.delay)
        batch = list(records)
        self.batches.append(batch)
        self.records.extend(batch)
        return [WriteResult(written=True) for _ in batch]

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


class _CheckpointedSequenceSource(BaseSource[int]):
    source_name = "checkpointed_sequence"
    supports_checkpoint = True

    def __init__(self, count: int) -> None:
        self._count = count
        self._last_index = -1

    def current_checkpoint(self) -> dict[str, int] | None:
        if self._last_index < 0:
            return None
        return {"index": self._last_index}

    async def stream(self):
        for index in range(self._count):
            self._last_index = index
            yield index


class _SlowCheckpointStore(InMemoryCheckpointStore):
    def __init__(self, delay: float) -> None:
        super().__init__()
        self.delay = delay

    async def save(self, key: str, checkpoint) -> None:
        await asyncio.sleep(self.delay)
        await super().save(key, checkpoint)


# ======================================================================
# Property 7: Bug Condition — Backpressure Bounds Memory
# ======================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "num_records, max_buffer_size",
    [
        (10, 1),
        (10, 3),
        (10, 5),
        (50, 5),
        (50, 10),
        (100, 10),
        (100, 25),
        (200, 50),
    ],
)
async def test_backpressure_in_flight_never_exceeds_max_buffer_size(
    num_records: int,
    max_buffer_size: int,
) -> None:
    """Property 7: With max_buffer_size=N, len(in_flight) never exceeds N.

    For any combination of source record count and max_buffer_size, the
    backpressure drain loop in run_buffered_pipeline() must ensure that
    in_flight never grows beyond max_buffer_size.

    We instrument the runtime by patching ExecutionCoordinator.resolve_buffered_record
    to observe in_flight depth at each drain point.

    **Validates: Requirements 2.9, 2.10**
    """
    from agora.core import runtime as runtime_module

    max_in_flight_observed = 0
    original_resolve = runtime_module.ExecutionCoordinator.resolve_buffered_record

    async def _patched_resolve(self, state, future, split_index, buffered_name, source_record):
        # Capture the current in_flight depth via the coordinator's context.
        # We can't directly access in_flight here, but we can track via the
        # buffered_stage_max_in_flight metric which is updated before each drain.
        nonlocal max_in_flight_observed
        max_in_flight_observed = max(
            max_in_flight_observed,
            state.ctx.metrics.runtime.buffered_stage_max_in_flight,
        )
        return await original_resolve(
            self, state, future, split_index, buffered_name, source_record
        )

    runtime_module.ExecutionCoordinator.resolve_buffered_record = _patched_resolve  # type: ignore[method-assign]

    try:
        sink = _CollectSink()
        middleware = _InFlightTrackingMiddleware(min_concurrency=10_000)
        pipeline = (
            Pipeline(IterableSource(list(range(num_records))))
            .pipe(middleware)
            .build(sink, max_buffer_size=max_buffer_size)  # type: ignore[arg-type]
        )

        summary = await pipeline.run()

        # All records must be processed
        assert summary.records_consumed == num_records, (
            f"Expected {num_records} records consumed, got {summary.records_consumed}"
        )
        assert summary.records_written == num_records, (
            f"Expected {num_records} records written, got {summary.records_written}"
        )

        # The max in_flight observed must never exceed max_buffer_size
        assert max_in_flight_observed <= max_buffer_size, (
            f"[PERF-4] BACKPRESSURE FAILED: max_buffer_size={max_buffer_size}, "
            f"but observed max in_flight={max_in_flight_observed}. "
            f"Backpressure drain loop did not bound in_flight correctly. "
            f"num_records={num_records}"
        )
    finally:
        runtime_module.ExecutionCoordinator.resolve_buffered_record = original_resolve  # type: ignore[method-assign]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "num_records, max_buffer_size",
    [
        (20, 2),
        (20, 5),
        (50, 10),
        (100, 20),
    ],
)
async def test_backpressure_all_records_processed_with_slow_sink(
    num_records: int,
    max_buffer_size: int,
) -> None:
    """Property 7: With backpressure enabled and slow sink, all records are still processed.

    Backpressure must not drop records — it only slows down source iteration.
    Every record emitted by the source must eventually be written to the sink.

    **Validates: Requirements 2.9, 2.10**
    """
    sink = _CollectSink()
    middleware = _InFlightTrackingMiddleware(min_concurrency=10_000)
    pipeline = (
        Pipeline(IterableSource(list(range(num_records))))
        .pipe(middleware)
        .build(sink, max_buffer_size=max_buffer_size)  # type: ignore[arg-type]
    )

    summary = await pipeline.run()

    assert summary.records_consumed == num_records, (
        f"[PERF-4] BACKPRESSURE DROPPED RECORDS: Expected {num_records} consumed, "
        f"got {summary.records_consumed}. Backpressure must not drop records."
    )
    assert summary.records_written == num_records, (
        f"[PERF-4] BACKPRESSURE DROPPED RECORDS: Expected {num_records} written, "
        f"got {summary.records_written}. Backpressure must not drop records."
    )
    assert len(sink.records) == num_records, (
        f"[PERF-4] SINK MISSING RECORDS: Expected {num_records} in sink, got {len(sink.records)}."
    )
    # Records must be in order (no reordering from backpressure)
    assert sink.records == list(range(num_records)), (
        f"[PERF-4] RECORD ORDER VIOLATED: Records were reordered by backpressure. "
        f"Expected {list(range(num_records))}, got {sink.records}"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "max_buffer_size",
    [1, 2, 5, 10],
)
async def test_backpressure_runtime_metric_respects_bound(max_buffer_size: int) -> None:
    """Property 7: buffered_stage_max_in_flight metric never exceeds max_buffer_size.

    The runtime metric buffered_stage_max_in_flight is updated before each drain.
    With backpressure enabled, this metric must never exceed max_buffer_size.

    **Validates: Requirements 2.9, 2.10**
    """
    num_records = max_buffer_size * 5  # Ensure multiple drain cycles
    sink = _CollectSink()
    middleware = _InFlightTrackingMiddleware(min_concurrency=10_000)
    pipeline = (
        Pipeline(IterableSource(list(range(num_records))))
        .pipe(middleware)
        .build(sink, max_buffer_size=max_buffer_size)  # type: ignore[arg-type]
    )

    summary = await pipeline.run()

    assert summary.records_written == num_records
    assert summary.runtime.buffered_stage_max_in_flight <= max_buffer_size, (
        f"[PERF-4] METRIC VIOLATION: buffered_stage_max_in_flight="
        f"{summary.runtime.buffered_stage_max_in_flight} exceeds "
        f"max_buffer_size={max_buffer_size}. "
        f"Backpressure must bound in_flight to max_buffer_size."
    )


# ======================================================================
# Property 8: Preservation — Backpressure Does Not Affect Balanced Throughput
# ======================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "num_records",
    [5, 10, 20, 50],
)
async def test_no_backpressure_pipeline_still_works(num_records: int) -> None:
    """Property 8: Without max_buffer_size, buffered pipeline works correctly (no regression).

    Pipelines without max_buffer_size configured must continue to work exactly
    as before — no records dropped, no errors, correct order.

    **Validates: Requirements 3.10, 3.11**
    """
    sink = _CollectSink()
    middleware = _InFlightTrackingMiddleware(min_concurrency=10_000)
    pipeline = (
        Pipeline(IterableSource(list(range(num_records)))).pipe(middleware).build(sink)  # type: ignore[arg-type]
        # No max_buffer_size — backpressure disabled
    )

    summary = await pipeline.run()

    assert summary.records_consumed == num_records, (
        f"[PERF-4] REGRESSION: Without backpressure, expected {num_records} consumed, "
        f"got {summary.records_consumed}"
    )
    assert summary.records_written == num_records, (
        f"[PERF-4] REGRESSION: Without backpressure, expected {num_records} written, "
        f"got {summary.records_written}"
    )
    assert len(sink.records) == num_records
    assert sink.records == list(range(num_records)), (
        "[PERF-4] REGRESSION: Record order violated without backpressure."
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "num_records, max_records",
    [
        (100, 5),
        (100, 10),
        (100, 25),
        (50, 3),
        (50, 50),
    ],
)
async def test_max_records_stops_correctly_with_backpressure(
    num_records: int,
    max_records: int,
) -> None:
    """Property 8: max_records still stops correctly with backpressure enabled.

    When both max_records and max_buffer_size are configured, the pipeline
    must stop after exactly max_records records, regardless of backpressure.

    **Validates: Requirements 3.12**
    """
    max_buffer_size = max(1, max_records // 2)
    sink = _CollectSink()
    middleware = _InFlightTrackingMiddleware(min_concurrency=10_000)
    pipeline = (
        Pipeline(IterableSource(list(range(num_records))))
        .pipe(middleware)
        .build(sink, max_buffer_size=max_buffer_size)  # type: ignore[arg-type]
    )

    summary = await pipeline.run(max_records=max_records)

    assert summary.records_consumed == max_records, (
        f"[PERF-4] MAX_RECORDS BROKEN: With backpressure, expected {max_records} consumed, "
        f"got {summary.records_consumed}. max_records must still stop the pipeline correctly."
    )
    assert summary.records_written == max_records, (
        f"[PERF-4] MAX_RECORDS BROKEN: With backpressure, expected {max_records} written, "
        f"got {summary.records_written}."
    )


@pytest.mark.asyncio
async def test_backpressure_with_max_buffer_size_one() -> None:
    """Property 7: max_buffer_size=1 forces fully sequential processing.

    With max_buffer_size=1, each record must be fully resolved before the
    next is submitted. This is the strictest backpressure setting.

    **Validates: Requirements 2.9, 2.10**
    """
    num_records = 10
    sink = _CollectSink()
    middleware = _InFlightTrackingMiddleware(min_concurrency=10_000)
    pipeline = (
        Pipeline(IterableSource(list(range(num_records))))
        .pipe(middleware)
        .build(sink, max_buffer_size=1)  # type: ignore[arg-type]
    )

    summary = await pipeline.run()

    assert summary.records_written == num_records
    assert summary.runtime.buffered_stage_max_in_flight <= 1, (
        f"[PERF-4] max_buffer_size=1 violated: "
        f"buffered_stage_max_in_flight={summary.runtime.buffered_stage_max_in_flight}"
    )
    assert sink.records == list(range(num_records))


@pytest.mark.asyncio
async def test_adaptive_backpressure_scales_up_when_writer_and_checkpoint_are_fast() -> None:
    sink = _FastBatchCollectSink()

    summary = await (
        Pipeline(IterableSource(list(range(18))))
        .pipe(_InFlightTrackingMiddleware(min_concurrency=2))
        .build(
            sink,  # type: ignore[arg-type]
            batch_size=2,
            backpressure=Backpressure.adaptive(
                max_buffer_size=5,
                writer_slow_ms=100.0,
                checkpoint_slow_ms=100.0,
            ),
        )
        .run()
    )

    assert summary.records_written == 18
    assert summary.runtime.adaptive_backpressure_enabled is True
    assert summary.runtime.adaptive_backpressure_scale_up_count >= 1
    assert summary.runtime.adaptive_backpressure_scale_down_count == 0
    assert summary.runtime.buffered_stage_limit > 2
    assert summary.runtime.buffered_stage_max_in_flight >= 3
    assert sink.records == list(range(18))


@pytest.mark.asyncio
async def test_adaptive_backpressure_scales_down_when_checkpoint_persistence_is_slow() -> None:
    summary = await (
        Pipeline(_CheckpointedSequenceSource(12))
        .pipe(_InFlightTrackingMiddleware(min_concurrency=4))
        .build(
            _FastBatchCollectSink(),  # type: ignore[arg-type]
            batch_size=2,
            checkpoint=_SlowCheckpointStore(delay=0.01),
            backpressure=Backpressure.adaptive(
                max_buffer_size=6,
                writer_slow_ms=100.0,
                checkpoint_slow_ms=1.0,
            ),
        )
        .run()
    )

    assert summary.records_written == 12
    assert summary.runtime.adaptive_backpressure_enabled is True
    assert summary.runtime.adaptive_backpressure_scale_down_count >= 1
    assert summary.runtime.buffered_stage_limit < 4
    assert summary.runtime.checkpoint_save_count >= 1


@pytest.mark.asyncio
async def test_adaptive_backpressure_scales_down_when_writer_flush_is_slow() -> None:
    summary = await (
        Pipeline(IterableSource(list(range(12))))
        .pipe(_InFlightTrackingMiddleware(min_concurrency=4))
        .build(
            _SlowBatchCollectSink(delay=0.01),  # type: ignore[arg-type]
            batch_size=2,
            backpressure=Backpressure.adaptive(
                max_buffer_size=6,
                writer_slow_ms=1.0,
                checkpoint_slow_ms=100.0,
            ),
        )
        .run()
    )

    assert summary.records_written == 12
    assert summary.runtime.adaptive_backpressure_enabled is True
    assert summary.runtime.adaptive_backpressure_scale_down_count >= 1
    assert summary.runtime.buffered_stage_limit < 4
    assert summary.runtime.writer_flush_count >= 1
