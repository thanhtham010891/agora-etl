"""
tests/preservation/test_runtime_guarantees.py
==============================================
Preservation tests for the runtime guarantees declared in
``docs/guides/runtime-guarantees.md``.

Each test maps to one declared guarantee. If a test fails, the public
contract is broken — fix the code, not the test.

These tests must pass on every release in the ``0.1.x`` line and beyond.
Removing a guarantee requires a major-version bump and a changelog entry.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import pytest

from agora import (
    DataPlane,
    DeliveryConfig,
    InMemoryCheckpointStore,
    IterableSource,
    MapMiddleware,
    Pipeline,
    ProcessBatchMiddleware,
    SinkFailurePolicy,
)
from agora.core.checkpoint import Checkpoint, is_checkpoint_capable
from agora.core.data_plane import SourceDataPlaneSpec
from agora.core.middleware import Middleware
from agora.core.source import BaseSource, SourceRecordError
from agora.core.types import Backpressure, CheckpointFailurePolicy, DLQFailurePolicy
from agora.core.writer import WriteResult

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from agora.core.dlq import DLQRecord

pytestmark = pytest.mark.contract

# ======================================================================
# Test fixtures
# ======================================================================


class _CollectSink:
    sink_name = "collect"

    def __init__(self) -> None:
        self.records: list[Any] = []
        self.write_calls = 0

    async def open(self) -> None:
        return None

    async def write(self, record: Any) -> None:
        self.write_calls += 1
        self.records.append(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _RaisingSink:
    sink_name = "raising"

    def __init__(self, exc: Exception | None = None) -> None:
        self._exc = exc or RuntimeError("sink boom")
        self.write_calls = 0

    async def open(self) -> None:
        return None

    async def write(self, record: Any) -> None:
        self.write_calls += 1
        raise self._exc

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _DLQCollectSink:
    sink_name = "dlq"

    def __init__(self) -> None:
        self.records: list[DLQRecord] = []

    async def open(self) -> None:
        return None

    async def write(self, record: DLQRecord) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _RecordingCheckpointStore(InMemoryCheckpointStore):
    """Checkpoint store that records persisted values for cadence assertions."""

    def __init__(self) -> None:
        super().__init__()
        self.saved_values: list[Any] = []

    async def save(self, key: str, checkpoint: Checkpoint) -> None:
        self.saved_values.append(checkpoint.value)
        await super().save(key, checkpoint)


class _FailingCheckpointStore(InMemoryCheckpointStore):
    """Checkpoint store that always fails save() and counts attempts."""

    def __init__(self, exc: Exception | None = None) -> None:
        super().__init__()
        self._exc = exc or RuntimeError("checkpoint broke")
        self.save_calls = 0

    async def save(self, key: str, checkpoint: Checkpoint) -> None:
        del key, checkpoint
        self.save_calls += 1
        raise self._exc


class _CheckpointedSequenceSource(BaseSource[int]):
    """A minimal checkpointable source used to verify checkpoint advancement."""

    source_name = "checkpointed_sequence"
    supports_checkpoint = True

    def __init__(self, records: list[int]) -> None:
        self._records = records
        self._resume_index = -1
        self._last_index = -1

    async def prepare_resume(self, checkpoint: Any) -> None:
        if checkpoint is None:
            self._resume_index = -1
            return
        self._resume_index = int(checkpoint.value["index"])

    def current_checkpoint(self) -> dict[str, int] | None:
        if self._last_index < 0:
            return None
        return {"index": self._last_index}

    async def stream(self) -> Any:
        for index, record in enumerate(self._records):
            if index <= self._resume_index:
                continue
            self._last_index = index
            yield record


class _AckTrackingSource(IterableSource[int]):
    """Source fixture that exposes delivery_success_callback()."""

    source_name = "ack_tracking"

    def __init__(self, records: list[int], target: list[int]) -> None:
        super().__init__(records)
        self._target = target
        self._current: int | None = None

    def delivery_success_callback(self):
        current = self._current
        if current is None:
            return None

        async def _ack() -> None:
            self._target.append(current)

        return _ack

    async def stream(self):
        for record in self._records:
            self._current = record
            yield record


class _RaisingMiddleware(Middleware[int, int]):
    name = "raising_middleware"

    def __init__(self, fail_on: int) -> None:
        self._fail_on = fail_on

    async def process(self, record: int, ctx: Any) -> int | None:
        del ctx
        if record == self._fail_on:
            raise ValueError(f"middleware boom on {record}")
        return record


class _BufferedPassThroughMiddleware(Middleware[int, int]):
    """A buffered middleware that releases records out of arrival order.

    The runtime must still commit them in source order.
    """

    name = "buffered_passthrough"

    def __init__(self, batch_size: int = 4) -> None:
        self.min_concurrency = batch_size
        self._batch_size = batch_size
        self._pending: list[tuple[int, asyncio.Future[int | None]]] = []

    async def process(self, record: int, ctx: Any) -> int | None:
        del ctx
        return record

    async def submit(self, record: int, ctx: Any) -> asyncio.Future[int | None]:
        del ctx
        future: asyncio.Future[int | None] = asyncio.get_running_loop().create_future()
        self._pending.append((record, future))
        if len(self._pending) >= self._batch_size:
            await self._flush_pending()
        return future

    async def drain_pending(self, ctx: Any) -> None:
        del ctx
        await self._flush_pending()

    async def _flush_pending(self) -> None:
        batch, self._pending = self._pending, []
        # Resolve in REVERSE order — this proves the runtime reorders, not the middleware.
        for record, future in reversed(batch):
            if not future.done():
                future.set_result(record)


class _DelayedBufferedPassThroughMiddleware(Middleware[int, int]):
    """Buffered middleware that resolves records independently and out of order."""

    name = "delayed_buffered_passthrough"

    def __init__(
        self,
        *,
        delays: dict[int, float] | None = None,
        min_concurrency: int = 4,
    ) -> None:
        self.min_concurrency = min_concurrency
        self._delays = delays or {}

    async def process(self, record: int, ctx: Any) -> int | None:
        del ctx
        return record

    async def submit(self, record: int, ctx: Any) -> asyncio.Future[int | None]:
        del ctx
        future: asyncio.Future[int | None] = asyncio.get_running_loop().create_future()
        resolve_task: asyncio.Task[None] | None = None

        async def _resolve() -> None:
            delay = self._delays.get(record, 0.0)
            if delay > 0:
                await asyncio.sleep(delay)
            future.set_result(record)

        resolve_task = asyncio.create_task(_resolve())
        future.add_done_callback(lambda _: resolve_task)
        return future

    async def drain_pending(self, ctx: Any) -> None:
        del ctx


class _FailingSource(BaseSource[int]):
    source_name = "failing_source"
    supports_checkpoint = True

    def __init__(self) -> None:
        self._last_index = -1

    def current_checkpoint(self) -> dict[str, int] | None:
        if self._last_index < 0:
            return None
        return {"index": self._last_index}

    async def stream(self):
        self._last_index = 0
        yield 10
        raise RuntimeError("source broke")


class _FailingRecordSource(BaseSource[int]):
    source_name = "failing_record_source"
    supports_checkpoint = True

    def __init__(self) -> None:
        self._last_index = -1

    def current_checkpoint(self) -> dict[str, int] | None:
        if self._last_index < 0:
            return None
        return {"index": self._last_index}

    async def stream(self):
        self._last_index = 0
        yield 10
        self._last_index = 1
        raise SourceRecordError(
            ValueError("bad row"),
            record={"id": 2, "raw": "broken"},
            checkpoint=self.current_checkpoint(),
            source=self.source_name,
        )


class _BlockingBufferedMiddleware(Middleware[int, int]):
    name = "blocking_buffered"

    def __init__(self, expected_records: int) -> None:
        self.min_concurrency = expected_records
        self._expected_records = expected_records
        self._started = 0
        self.all_started = asyncio.Event()
        self.cancelled: list[int] = []
        self.stopped = False

    async def process(self, record: int, ctx: Any) -> int | None:
        del ctx
        return record

    async def submit(self, record: int, ctx: Any) -> asyncio.Task[int]:
        del ctx
        self._started += 1
        if self._started >= self._expected_records:
            self.all_started.set()

        async def _resolve() -> int:
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                self.cancelled.append(record)
                raise

        return asyncio.create_task(_resolve())

    async def drain_pending(self, ctx: Any) -> None:
        del ctx

    async def on_stop(self, ctx: Any) -> None:
        del ctx
        self.stopped = True


class _PrefetchSequenceSource(BaseSource[int]):
    source_name = "prefetch_sequence"
    supports_prefetch = True
    prefetch_limit = 2

    def __init__(self, records: list[int]) -> None:
        self._records = records

    async def stream(self):
        for record in self._records:
            yield record


class _FailingPrefetchSource(BaseSource[int]):
    source_name = "failing_prefetch"
    supports_prefetch = True
    prefetch_limit = 2

    async def stream(self):
        yield 10
        yield 20
        raise RuntimeError("prefetch source broke")


class _OutOfOrderBufferedMiddleware(Middleware[int, int]):
    """Buffered middleware whose later items resolve before the head item.

    The runtime must not commit later completed work ahead of earlier records,
    even during cancellation.
    """

    name = "out_of_order_buffered"

    def __init__(self) -> None:
        self.min_concurrency = 3
        self.all_started = asyncio.Event()
        self.cancelled: list[int] = []
        self._started = 0

    async def process(self, record: int, ctx: Any) -> int | None:
        del ctx
        return record

    async def submit(self, record: int, ctx: Any) -> asyncio.Task[int]:
        del ctx
        self._started += 1
        if self._started >= 3:
            self.all_started.set()

        async def _resolve() -> int:
            try:
                if record == 1:
                    await asyncio.Future()
                return record * 10
            except asyncio.CancelledError:
                self.cancelled.append(record)
                raise

        return asyncio.create_task(_resolve())

    async def drain_pending(self, ctx: Any) -> None:
        del ctx


# ProcessBatchMiddleware worker functions must stay module-level so they remain
# pickleable under spawn-based multiprocessing.
def _process_batch_double_values(
    batch: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [{**row, "value": row["value"] * 2} for row in batch]


def _process_batch_timeout_generation_then_double(
    batch: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    import time

    if batch and batch[0]["id"] in {"timeout", "stale"}:
        time.sleep(3.0)
    else:
        time.sleep(0.2)
    return _process_batch_double_values(batch)


def _process_batch_very_slow_double_values(
    batch: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    import time

    time.sleep(5.0)
    return _process_batch_double_values(batch)


class _ProcessBatchSource(BaseSource[dict[str, Any]]):
    """Checkpointable batch source used for process-batch preservation tests."""

    source_name = "process_batch_preservation_source"
    supports_checkpoint = True

    def __init__(
        self,
        batches: list[list[dict[str, Any]]],
        *,
        delays: list[float] | None = None,
    ) -> None:
        self._batches = batches
        self._delays = delays or [0.0] * len(batches)
        self._next_batch_index = 0
        self._last_batch_index = -1

    def current_checkpoint(self) -> dict[str, int] | None:
        if self._last_batch_index < 0:
            return None
        return {"batch_index": self._last_batch_index}

    async def prepare_resume(self, checkpoint: Any) -> None:
        if checkpoint is None:
            self._next_batch_index = 0
            self._last_batch_index = -1
            return
        next_batch_index = int(checkpoint.value["batch_index"]) + 1
        self._next_batch_index = next_batch_index
        self._last_batch_index = next_batch_index - 1

    def data_plane_spec(self) -> SourceDataPlaneSpec:
        return SourceDataPlaneSpec(
            source_name=self.source_name,
            emitted_plane=DataPlane.PYTHON_BATCHES,
            supports_batch_emit=True,
            emits_arrow_batches=False,
        )

    async def stream(self) -> Any:
        async for batch in self.stream_batches():
            for record in batch:
                yield record

    async def stream_batches(self) -> Any:  # type: ignore[override]
        while self._next_batch_index < len(self._batches):
            delay = self._delays[self._next_batch_index]
            if delay > 0:
                await asyncio.sleep(delay)
            self._last_batch_index = self._next_batch_index
            yield self._batches[self._next_batch_index]
            self._next_batch_index += 1


# ======================================================================
# [GUARANTEE-S01] Checkpoint restore happens before source open
# ======================================================================


@pytest.mark.asyncio
async def test_gs01_checkpoint_restore_happens_before_source_open() -> None:
    """[GUARANTEE-S01] Checkpoint restore happens before source.open().

    Validates: docs/guides/runtime-guarantees.md — "Checkpoint restore happens
    before source open"
    """

    class _ResumeOrderingSource(_CheckpointedSequenceSource):
        source_name = "resume_ordering"

        def __init__(self, records: list[int], events: list[str]) -> None:
            super().__init__(records)
            self._events = events
            self.prepared_checkpoint: Any = None

        async def prepare_resume(self, checkpoint: Any) -> None:
            self.prepared_checkpoint = None if checkpoint is None else checkpoint.value
            self._events.append("prepare_resume")
            await super().prepare_resume(checkpoint)

        async def open(self) -> None:
            self._events.append("source.open")
            assert self.prepared_checkpoint == {"index": 1}, (
                "[GUARANTEE-S01] prepare_resume must receive the stored checkpoint "
                "before source.open()"
            )
            assert self._resume_index == 1, (
                "[GUARANTEE-S01] resume cursor must be restored before source.open()"
            )

    store = InMemoryCheckpointStore()
    await store.save(
        "resume_ordering",
        Checkpoint(
            pipeline_id="resume_ordering",
            run_id="seed",
            source="resume_ordering",
            value={"index": 1},
        ),
    )

    events: list[str] = []
    sink = _CollectSink()

    summary = await (
        Pipeline(_ResumeOrderingSource([10, 20, 30, 40], events))
        .build(sink, config=DeliveryConfig(checkpoint=store))  # type: ignore[arg-type]
        .run()
    )

    assert events == ["prepare_resume", "source.open"]
    assert sink.records == [30, 40], (
        "[GUARANTEE-S01] restored checkpoint must apply before streaming starts"
    )
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"index": 3}


# ======================================================================
# [GUARANTEE-S02] Middleware startup is ordered and rollback-safe
# ======================================================================


@pytest.mark.asyncio
async def test_gs02_middleware_startup_is_ordered_and_rollback_safe() -> None:
    """[GUARANTEE-S02] Middleware startup runs in registration order and rolls
    back safely on failure before sink/source startup begins.

    Validates: docs/guides/runtime-guarantees.md — "Middleware startup is
    ordered and rollback-safe"
    """

    events: list[str] = []

    class _TrackingSource(BaseSource[int]):
        source_name = "startup_tracking_source"

        async def open(self) -> None:
            events.append("source.open")

        async def stream(self):
            events.append("source.stream")
            yield 1

    class _TrackingSink:
        sink_name = "tracking_sink"

        async def open(self) -> None:
            events.append("sink.open")

        async def write(self, record: int) -> None:
            events.append(f"sink.write:{record}")

        async def flush(self) -> None:
            events.append("sink.flush")

        async def close(self) -> None:
            events.append("sink.close")

    class _TrackingMiddleware(Middleware[int, int]):
        name = "tracking_start"

        async def on_start(self, ctx: Any) -> None:
            del ctx
            events.append("tracking.start")

        async def on_stop(self, ctx: Any) -> None:
            del ctx
            events.append("tracking.stop")

        async def process(self, record: int, ctx: Any) -> int | None:
            del ctx
            return record

    class _FailingStartMiddleware(Middleware[int, int]):
        name = "failing_start"

        async def on_start(self, ctx: Any) -> None:
            del ctx
            events.append("failing.start")
            raise RuntimeError("middleware start broke")

        async def on_stop(self, ctx: Any) -> None:
            del ctx
            events.append("failing.stop")

        async def process(self, record: int, ctx: Any) -> int | None:
            del ctx
            return record

    with pytest.raises(RuntimeError, match="middleware start broke"):
        await (
            Pipeline(_TrackingSource())
            .pipe(_TrackingMiddleware())
            .pipe(_FailingStartMiddleware())
            .build(_TrackingSink())  # type: ignore[arg-type]
            .run()
        )

    assert events == [
        "tracking.start",
        "failing.start",
        "failing.stop",
        "tracking.stop",
    ], "[GUARANTEE-S02] startup failure must roll back started middlewares in reverse order"


# ======================================================================
# [GUARANTEE-S03] Writer/DLQ open is all-or-nothing
# ======================================================================


@pytest.mark.asyncio
async def test_gs03_writer_and_dlq_open_is_all_or_nothing_before_streaming() -> None:
    """[GUARANTEE-S03] Writer/DLQ open failure rolls back opened sinks and
    prevents source consumption entirely.

    Validates: docs/guides/runtime-guarantees.md — "Writer/DLQ open is
    all-or-nothing at the run boundary"
    """

    events: list[str] = []

    class _TrackingSource(BaseSource[int]):
        source_name = "open_boundary_source"

        async def open(self) -> None:
            events.append("source.open")

        async def stream(self):
            events.append("source.stream")
            yield 1

    class _WriterSink:
        sink_name = "writer"

        async def open(self) -> None:
            events.append("writer.open")

        async def write(self, record: int) -> None:
            events.append(f"writer.write:{record}")

        async def flush(self) -> None:
            events.append("writer.flush")

        async def close(self) -> None:
            events.append("writer.close")

    class _FailingDLQSink:
        sink_name = "dlq"

        async def open(self) -> None:
            events.append("dlq.open")
            raise RuntimeError("dlq open broke")

        async def write(self, record: Any) -> None:
            events.append(f"dlq.write:{record}")

        async def flush(self) -> None:
            events.append("dlq.flush")

        async def close(self) -> None:
            events.append("dlq.close")

    with pytest.raises(RuntimeError, match="dlq open broke"):
        await (
            Pipeline(_TrackingSource())
            .build(
                _WriterSink(),
                config=DeliveryConfig(dlq=_FailingDLQSink()),
            )  # type: ignore[arg-type]
            .run()
        )

    assert events == [
        "writer.open",
        "dlq.open",
        "dlq.close",
        "writer.close",
    ], "[GUARANTEE-S03] open failure must roll back sinks before source streaming begins"


# ======================================================================
# [GUARANTEE-P01] Middleware ordering and short-circuit semantics
# ======================================================================


@pytest.mark.asyncio
async def test_gp01_middleware_executes_left_to_right_with_drop_and_raise_short_circuit() -> None:
    """[GUARANTEE-P01] Middleware runs left-to-right and short-circuits on
    drop/raise per record.

    Validates: docs/guides/runtime-guarantees.md — "Middleware execution order
    is left to right"
    """

    class _TracingMiddleware(Middleware[int, int]):
        def __init__(
            self,
            name: str,
            events: list[str],
            *,
            transform: Callable[[int], int],
            drop_on: set[int] | None = None,
            raise_on: set[int] | None = None,
        ) -> None:
            self.name = name
            self._events = events
            self._transform = transform
            self._drop_on = drop_on or set()
            self._raise_on = raise_on or set()

        async def process(self, record: int, ctx: Any) -> int | None:
            del ctx
            self._events.append(f"{self.name}.in:{record}")
            if record in self._drop_on:
                self._events.append(f"{self.name}.drop:{record}")
                return None
            if record in self._raise_on:
                self._events.append(f"{self.name}.raise:{record}")
                raise ValueError(f"{self.name} boom on {record}")
            result = self._transform(record)
            self._events.append(f"{self.name}.out:{result}")
            return result

        async def on_error(self, record: int, exc: Exception, ctx: Any) -> None:
            del ctx
            self._events.append(f"{self.name}.on_error:{record}:{type(exc).__name__}")

    events: list[str] = []
    sink = _CollectSink()
    dlq = _DLQCollectSink()

    summary = await (
        Pipeline(IterableSource([1, 2, 3]))
        .pipe(_TracingMiddleware("stage_one", events, transform=lambda value: value * 10))
        .pipe(
            _TracingMiddleware(
                "stage_two",
                events,
                transform=lambda value: value + 5,
                drop_on={20},
                raise_on={30},
            )
        )
        .pipe(_TracingMiddleware("stage_three", events, transform=lambda value: value - 1))
        .build(sink, config=DeliveryConfig(dlq=dlq))  # type: ignore[arg-type]
        .run()
    )

    assert events == [
        "stage_one.in:1",
        "stage_one.out:10",
        "stage_two.in:10",
        "stage_two.out:15",
        "stage_three.in:15",
        "stage_three.out:14",
        "stage_one.in:2",
        "stage_one.out:20",
        "stage_two.in:20",
        "stage_two.drop:20",
        "stage_one.in:3",
        "stage_one.out:30",
        "stage_two.in:30",
        "stage_two.raise:30",
        "stage_two.on_error:3:ValueError",
    ], "[GUARANTEE-P01] middleware must execute left-to-right and short-circuit per record"
    assert sink.records == [14]
    assert summary.records_written == 1
    assert summary.records_dropped == 1
    assert summary.records_errored == 1
    assert len(dlq.records) == 1
    assert dlq.records[0].stage == "middleware"
    assert dlq.records[0].middleware == "stage_two"


# ======================================================================
# [GUARANTEE-01] Source order — linear mode
# ======================================================================


@pytest.mark.asyncio
async def test_g01_linear_mode_commits_in_source_order() -> None:
    """[GUARANTEE-01] Linear pipeline commits records to the sink in source order.

    Validates: docs/guides/runtime-guarantees.md — "Source order"
    """
    sink = _CollectSink()
    summary = await (
        Pipeline(IterableSource([1, 2, 3, 4, 5]))
        .pipe(MapMiddleware(lambda r: r * 10))
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [10, 20, 30, 40, 50], (
        "[GUARANTEE-01] linear pipeline must commit in source order"
    )
    assert summary.records_written == 5


# ======================================================================
# [GUARANTEE-02] Source order — buffered mode
# ======================================================================


@pytest.mark.asyncio
async def test_g02_buffered_mode_commits_in_source_order_despite_reordering() -> None:
    """[GUARANTEE-02] Buffered pipeline commits in source order even when the
    buffered stage resolves futures out of order.

    Validates: docs/guides/runtime-guarantees.md — "Source order" (buffered case)
    """
    sink = _CollectSink()
    await (
        Pipeline(IterableSource([1, 2, 3, 4, 5, 6, 7, 8]))
        .pipe(_BufferedPassThroughMiddleware(batch_size=4))
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [1, 2, 3, 4, 5, 6, 7, 8], (
        "[GUARANTEE-02] buffered pipeline must commit in source order"
    )


# ======================================================================
# [GUARANTEE-03] Sink fail-closed by default
# ======================================================================


@pytest.mark.asyncio
async def test_g03_sink_fail_closed_propagates_when_no_dlq() -> None:
    """[GUARANTEE-03] FAIL_CLOSED (default) without DLQ propagates the sink
    error and stops the run.

    Validates: docs/guides/runtime-guarantees.md — "Sink fail-closed by default"
    """
    sink_exc = RuntimeError("sink boom")
    with pytest.raises(RuntimeError, match="sink boom"):
        await (
            Pipeline(IterableSource([1, 2, 3]))
            .build(_RaisingSink(sink_exc))  # type: ignore[arg-type]
            .run()
        )


# ======================================================================
# [GUARANTEE-D02] Sink failure policy controls record terminality
# ======================================================================


@pytest.mark.asyncio
async def test_gd02_sink_failure_policy_controls_record_terminality() -> None:
    """[GUARANTEE-D02] Sink failure policy determines whether an unrouted sink
    error is terminal, while a DLQ-routed failure remains non-terminal even
    under FAIL_CLOSED.

    Validates: docs/guides/runtime-guarantees.md — "Sink failure policy
    controls whether a failed record is terminal"
    """

    fail_closed_store = InMemoryCheckpointStore()
    with pytest.raises(RuntimeError, match="sink fail closed"):
        await (
            Pipeline(_CheckpointedSequenceSource([1, 2]))
            .build(
                _RaisingSink(RuntimeError("sink fail closed")),
                config=DeliveryConfig(checkpoint=fail_closed_store),
            )  # type: ignore[arg-type]
            .run()
        )

    fail_closed_checkpoint = await fail_closed_store.load("checkpointed_sequence")
    assert fail_closed_checkpoint is None, (
        "[GUARANTEE-D02] FAIL_CLOSED without DLQ must stop before any checkpoint advance"
    )

    routed_store = InMemoryCheckpointStore()
    routed_dlq = _DLQCollectSink()
    routed_summary = await (
        Pipeline(_CheckpointedSequenceSource([1, 2]))
        .build(
            _RaisingSink(RuntimeError("sink routed to dlq")),
            config=DeliveryConfig(checkpoint=routed_store, dlq=routed_dlq),
        )  # type: ignore[arg-type]
        .run()
    )

    assert routed_summary.records_written == 0
    assert routed_summary.records_errored == 2
    assert len(routed_dlq.records) == 2
    assert routed_summary.last_checkpoint is not None
    assert routed_summary.last_checkpoint.value == {"index": 1}, (
        "[GUARANTEE-D02] DLQ-routed sink failures must remain non-terminal and "
        "advance the checkpoint under FAIL_CLOSED"
    )

    log_continue_store = InMemoryCheckpointStore()
    log_continue_summary = await (
        Pipeline(_CheckpointedSequenceSource([1, 2]))
        .build(
            _RaisingSink(RuntimeError("sink log and continue")),
            config=DeliveryConfig(
                checkpoint=log_continue_store,
                sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE,
            ),
        )  # type: ignore[arg-type]
        .run()
    )

    assert log_continue_summary.records_written == 0
    assert log_continue_summary.records_errored == 2
    assert log_continue_summary.last_checkpoint is not None
    assert log_continue_summary.last_checkpoint.value == {"index": 1}, (
        "[GUARANTEE-D02] LOG_AND_CONTINUE must treat unrouted sink failures as handled"
    )


# ======================================================================
# [GUARANTEE-04] Checkpoint advances on success
# ======================================================================


@pytest.mark.asyncio
async def test_g04_checkpoint_advances_on_successful_write() -> None:
    """[GUARANTEE-04] Checkpoint advances through records the sink wrote successfully.

    Validates: docs/guides/runtime-guarantees.md — "Checkpoint advancement"
    """
    store = InMemoryCheckpointStore()
    sink = _CollectSink()

    summary = await (
        Pipeline(_CheckpointedSequenceSource([10, 20, 30]))
        .build(sink, config=DeliveryConfig(checkpoint=store))  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [10, 20, 30]
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"index": 2}, (
        "[GUARANTEE-04] checkpoint must advance to the last successfully written record"
    )


# ======================================================================
# [GUARANTEE-C03] Checkpoint save cadence is explicit
# ======================================================================


@pytest.mark.asyncio
async def test_gc03_checkpoint_every_controls_save_cadence_for_row_and_batch_lanes() -> None:
    """[GUARANTEE-C03] Checkpoint persistence happens only when the configured
    record cadence is reached, and batch lanes still count records rather than
    batches.

    Validates: docs/guides/runtime-guarantees.md — "Checkpoint save cadence is explicit"
    """
    row_store = _RecordingCheckpointStore()
    row_sink = _CollectSink()

    row_summary = await (
        Pipeline(_CheckpointedSequenceSource([10, 20, 30, 40, 50]))
        .build(
            row_sink,
            config=DeliveryConfig(checkpoint=row_store, checkpoint_every=2),
        )  # type: ignore[arg-type]
        .run()
    )

    assert row_sink.records == [10, 20, 30, 40, 50]
    assert row_store.saved_values == [{"index": 1}, {"index": 3}], (
        "[GUARANTEE-C03] row-lane checkpoint saves must happen only when "
        "checkpoint_every is reached"
    )
    assert row_summary.runtime.checkpoint_save_count == 2
    assert row_summary.last_checkpoint is not None
    assert row_summary.last_checkpoint.value == {"index": 3}

    batch_store = _RecordingCheckpointStore()
    batch_sink = _CollectSink()

    batch_summary = await (
        Pipeline(_BatchSource([[1, 2], [3], [4, 5]]))
        .build(
            batch_sink,
            config=DeliveryConfig(checkpoint=batch_store, checkpoint_every=3),
        )  # type: ignore[arg-type]
        .run()
    )

    assert batch_sink.records == [1, 2, 3, 4, 5]
    assert batch_store.saved_values == [{"batch_index": 1}], (
        "[GUARANTEE-C03] batch-lane checkpoint cadence must count records, not batches"
    )
    assert batch_summary.runtime.checkpoint_save_count == 1
    assert batch_summary.last_checkpoint is not None
    assert batch_summary.last_checkpoint.value == {"batch_index": 1}


# ======================================================================
# [GUARANTEE-05] Checkpoint advances when middleware drops to DLQ
# ======================================================================


@pytest.mark.asyncio
async def test_g05_checkpoint_advances_when_middleware_dlqs_record() -> None:
    """[GUARANTEE-05] A record routed to the DLQ from a middleware failure still
    advances the checkpoint — it was handled.

    Validates: docs/guides/runtime-guarantees.md — "Checkpoint advancement" table
    """
    store = InMemoryCheckpointStore()
    sink = _CollectSink()
    dlq = _DLQCollectSink()

    summary = await (
        Pipeline(_CheckpointedSequenceSource([1, 2, 3]))
        .pipe(_RaisingMiddleware(fail_on=2))
        .build(sink, config=DeliveryConfig(checkpoint=store, dlq=dlq))  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [1, 3], "non-failing records must still write"
    assert len(dlq.records) == 1, "[GUARANTEE-05] failing record must land in DLQ"
    assert dlq.records[0].stage == "middleware"
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"index": 2}, (
        "[GUARANTEE-05] checkpoint must advance past a DLQ-routed record"
    )


# ======================================================================
# [GUARANTEE-06] DLQ routing on middleware failure
# ======================================================================


@pytest.mark.asyncio
async def test_g06_middleware_failure_routes_to_dlq_with_correct_stage() -> None:
    """[GUARANTEE-06] When a middleware raises, the failed record is written to
    the DLQ with stage="middleware" and the middleware name attached.

    Validates: docs/guides/runtime-guarantees.md — "DLQ routing on middleware failure"
    """
    sink = _CollectSink()
    dlq = _DLQCollectSink()

    await (
        Pipeline(IterableSource([1, 2, 3]))
        .pipe(_RaisingMiddleware(fail_on=2))
        .build(sink, config=DeliveryConfig(dlq=dlq))  # type: ignore[arg-type]
        .run()
    )

    assert len(dlq.records) == 1
    record = dlq.records[0]
    assert record.stage == "middleware", (
        "[GUARANTEE-06] DLQRecord.stage must be 'middleware' for middleware failures"
    )
    assert record.middleware == "raising_middleware", (
        "[GUARANTEE-06] middleware name must be on the DLQRecord"
    )
    assert record.error_type == "ValueError"


# ======================================================================
# [GUARANTEE-07] No DLQ + middleware failure -> pipeline continues, error counted
# ======================================================================


@pytest.mark.asyncio
async def test_g07_middleware_failure_without_dlq_continues_pipeline() -> None:
    """[GUARANTEE-07] Middleware failure with no DLQ counts the error in
    records_errored and continues the pipeline.

    Validates: docs/guides/runtime-guarantees.md — "DLQ routing on middleware failure"
    """
    sink = _CollectSink()

    summary = await (
        Pipeline(IterableSource([1, 2, 3]))
        .pipe(_RaisingMiddleware(fail_on=2))
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [1, 3]
    assert summary.records_errored == 1


# ======================================================================
# [GUARANTEE-08] Sink fail-closed advances checkpoint when DLQ catches the record
# ======================================================================


@pytest.mark.asyncio
async def test_g08_sink_failure_with_dlq_advances_checkpoint() -> None:
    """[GUARANTEE-08] When the sink raises but the record is routed to the DLQ,
    the checkpoint still advances and the run does not abort.

    Validates: docs/guides/runtime-guarantees.md — "Checkpoint advancement" (DLQ row)
    """
    store = InMemoryCheckpointStore()
    dlq = _DLQCollectSink()

    summary = await (
        Pipeline(_CheckpointedSequenceSource([1, 2]))
        .build(_RaisingSink(), config=DeliveryConfig(checkpoint=store, dlq=dlq))  # type: ignore[arg-type]
        .run()
    )

    assert len(dlq.records) == 2
    assert all(r.stage == "sink_write" for r in dlq.records)
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"index": 1}, (
        "[GUARANTEE-08] checkpoint must advance when a sink failure is captured by the DLQ"
    )


# ======================================================================
# [GUARANTEE-09] is_checkpoint_capable reflects the supports_checkpoint flag
# ======================================================================


def test_g09_is_checkpoint_capable_requires_explicit_opt_in() -> None:
    """[GUARANTEE-09] A source is treated as checkpointable only when it sets
    supports_checkpoint=True. The runtime relies on this flag (not protocol shape).

    Validates: docs/guides/recovery-matrix.md — "A source is checkpointable when..."
    """
    iterable: IterableSource[int] = IterableSource([1, 2, 3])
    assert is_checkpoint_capable(iterable) is False, (
        "[GUARANTEE-09] IterableSource must not be reported as checkpoint-capable"
    )

    checkpointed = _CheckpointedSequenceSource([1])
    assert is_checkpoint_capable(checkpointed) is True


# ======================================================================
# [GUARANTEE-10] DLQFailurePolicy.RAISE propagates DLQ write errors
# ======================================================================


@pytest.mark.asyncio
async def test_g10_dlq_failure_policy_raise_propagates_error() -> None:
    """[GUARANTEE-10] DLQFailurePolicy.RAISE makes a DLQ write failure terminal.

    Validates: docs/guides/runtime-guarantees.md — "DLQ failure policy is honored"
    """

    class _BrokenDLQ:
        sink_name = "broken_dlq"

        async def open(self) -> None:
            return None

        async def write(self, record: DLQRecord) -> None:
            raise RuntimeError("dlq boom")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    with pytest.raises(RuntimeError, match="dlq boom"):
        await (
            Pipeline(IterableSource([1]))
            .pipe(_RaisingMiddleware(fail_on=1))
            .build(
                _CollectSink(),  # type: ignore[arg-type]
                config=DeliveryConfig(
                    dlq=_BrokenDLQ(),  # type: ignore[arg-type]
                    dlq_failure_policy=DLQFailurePolicy.RAISE,
                ),
            )
            .run()
        )


# ======================================================================
# [GUARANTEE-11] DLQFailurePolicy.LOG_ONLY swallows DLQ write errors
# ======================================================================


@pytest.mark.asyncio
async def test_g11_dlq_failure_policy_log_only_continues_pipeline() -> None:
    """[GUARANTEE-11] DLQFailurePolicy.LOG_ONLY (default) treats DLQ write failures
    as non-fatal — original errors are already counted.

    Validates: docs/guides/runtime-guarantees.md — "DLQ failure policy is honored"
    """

    class _BrokenDLQ:
        sink_name = "broken_dlq"

        async def open(self) -> None:
            return None

        async def write(self, record: DLQRecord) -> None:
            raise RuntimeError("dlq boom")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    sink = _CollectSink()
    summary = await (
        Pipeline(IterableSource([1, 2, 3]))
        .pipe(_RaisingMiddleware(fail_on=2))
        .build(sink, config=DeliveryConfig(dlq=_BrokenDLQ()))  # type: ignore[arg-type]  # default DLQFailurePolicy.LOG_ONLY
        .run()
    )

    assert sink.records == [1, 3]
    assert summary.records_errored == 1


@pytest.mark.asyncio
async def test_g11b_middleware_dlq_failure_with_checkpoint_aborts_before_later_checkpoint_advance() -> (
    None
):
    """[GUARANTEE-11b] With checkpointing enabled, a middleware failure that
    also fails DLQ routing must stop before later records can advance the
    checkpoint.
    """

    class _BrokenDLQ:
        sink_name = "broken_dlq"

        async def open(self) -> None:
            return None

        async def write(self, record: DLQRecord) -> None:
            del record
            raise RuntimeError("dlq boom")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    store = InMemoryCheckpointStore()
    sink = _CollectSink()

    with pytest.raises(ValueError, match="middleware boom on 2"):
        await (
            Pipeline(
                _CheckpointedSequenceSource([1, 2, 3]),
                id="middleware_dlq_failure_checkpoint_contract",
            )
            .pipe(_RaisingMiddleware(fail_on=2))
            .build(
                sink,
                config=DeliveryConfig(
                    checkpoint=store,
                    dlq=_BrokenDLQ(),  # type: ignore[arg-type]
                ),
            )
            .run()
        )

    checkpoint = await store.load("middleware_dlq_failure_checkpoint_contract")
    assert sink.records == [1], (
        "[GUARANTEE-11b] later records must not write after an unrouted "
        "middleware failure when checkpointing is enabled"
    )
    assert checkpoint is not None
    assert checkpoint.value == {"index": 0}, (
        "[GUARANTEE-11b] checkpoint must remain at the last handled record "
        "before the unrouted middleware failure"
    )


# ======================================================================
# [GUARANTEE-12] Sink LOG_AND_CONTINUE advances checkpoint without DLQ
# ======================================================================


@pytest.mark.asyncio
async def test_g12_sink_log_and_continue_advances_checkpoint() -> None:
    """[GUARANTEE-12] SinkFailurePolicy.LOG_AND_CONTINUE advances the checkpoint
    over a failed write — by policy, the record is considered handled.

    Validates: docs/guides/runtime-guarantees.md — "Checkpoint advancement" table
    """
    store = InMemoryCheckpointStore()

    summary = await (
        Pipeline(_CheckpointedSequenceSource([1, 2]))
        .build(
            _RaisingSink(),  # type: ignore[arg-type]
            config=DeliveryConfig(
                checkpoint=store,
                sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE,
            ),
        )
        .run()
    )

    assert summary.records_errored == 2
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"index": 1}, (
        "[GUARANTEE-12] LOG_AND_CONTINUE must still advance the checkpoint"
    )


# ======================================================================
# [GUARANTEE-C05] Checkpoint save failure policy is honored
# ======================================================================


@pytest.mark.asyncio
async def test_gc05a_checkpoint_save_failure_is_fail_closed_by_default() -> None:
    """[GUARANTEE-C05a] Checkpoint save failures abort the run under the default
    fail-closed policy.

    Validates: docs/guides/runtime-guarantees.md — "Checkpoint save failure
    policy is honored"
    """

    store = _FailingCheckpointStore()
    sink = _CollectSink()

    with pytest.raises(RuntimeError, match="checkpoint broke"):
        await (
            Pipeline(_CheckpointedSequenceSource([1, 2]))
            .build(
                sink,
                config=DeliveryConfig(checkpoint=store),
            )  # type: ignore[arg-type]
            .run()
        )

    assert sink.records == [1], (
        "[GUARANTEE-C05a] the written record may reach the sink, but the run must "
        "stop on checkpoint save failure"
    )
    assert store.save_calls == 1


@pytest.mark.asyncio
async def test_gc05b_checkpoint_save_failure_log_and_continue_avoids_retry_storm() -> None:
    """[GUARANTEE-C05b] Under LOG_AND_CONTINUE, save failures are logged and the
    pipeline keeps moving without retrying the same slot endlessly.

    Validates: docs/guides/runtime-guarantees.md — "Checkpoint save failure
    policy is honored"
    """

    store = _FailingCheckpointStore()
    sink = _CollectSink()

    summary = await (
        Pipeline(_CheckpointedSequenceSource([1, 2, 3]))
        .build(
            sink,
            config=DeliveryConfig(
                checkpoint=store,
                checkpoint_failure_policy=CheckpointFailurePolicy.LOG_AND_CONTINUE,
            ),
        )  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [1, 2, 3]
    assert store.save_calls == 3, (
        "[GUARANTEE-C05b] each handled record should trigger at most one failed "
        "save attempt under LOG_AND_CONTINUE"
    )
    assert summary.runtime.checkpoint_save_count == 0
    assert summary.runtime.checkpoint_failure_count == 3


# ======================================================================
# [GUARANTEE-13] Source delivery hook runs after successful sink write
# ======================================================================


@pytest.mark.asyncio
async def test_g13_source_delivery_hook_runs_after_successful_write() -> None:
    """[GUARANTEE-13] delivery_success_callback() runs after a successful sink write."""

    acknowledged: list[int] = []
    sink = _CollectSink()

    summary = await (
        Pipeline(_AckTrackingSource([1, 2, 3], acknowledged))
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [1, 2, 3]
    assert summary.records_written == 3
    assert acknowledged == [1, 2, 3]


# ======================================================================
# [GUARANTEE-14] Source delivery hook runs after DLQ-routed sink failure
# ======================================================================


@pytest.mark.asyncio
async def test_g14_dlq_routed_sink_failure_acknowledges_source_delivery() -> None:
    """[GUARANTEE-14] A DLQ-routed sink failure still acknowledges source delivery."""

    acknowledged: list[int] = []
    dlq = _DLQCollectSink()

    summary = await (
        Pipeline(_AckTrackingSource([1, 2], acknowledged))
        .build(_RaisingSink(), config=DeliveryConfig(dlq=dlq))  # type: ignore[arg-type]
        .run()
    )

    assert summary.records_written == 0
    assert summary.records_errored == 2
    assert len(dlq.records) == 2
    assert acknowledged == [1, 2]


# ======================================================================
# [GUARANTEE-15] LOG_AND_CONTINUE without DLQ does not acknowledge sink failure
# ======================================================================


@pytest.mark.asyncio
async def test_g15_sink_log_and_continue_without_dlq_does_not_acknowledge_source_delivery() -> None:
    """[GUARANTEE-15] LOG_AND_CONTINUE without DLQ does not fire source delivery hooks.

    This must remain true even on the batched writer path.
    """

    acknowledged: list[int] = []

    summary = await (
        Pipeline(_AckTrackingSource([1, 2], acknowledged))
        .build(
            _FailingBatchSink(),  # type: ignore[arg-type]
            config=DeliveryConfig(
                batch_size=2, sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE
            ),
        )
        .run()
    )

    assert summary.records_written == 0
    assert summary.records_errored == 2
    assert acknowledged == []


# ======================================================================
# [GUARANTEE-16] Source stream failures route to DLQ but preserve original error
# ======================================================================


@pytest.mark.asyncio
async def test_g16_source_stream_failure_routes_to_dlq_and_reraises_original_error() -> None:
    """[GUARANTEE-16] source_stream failures write a structured DLQ record and re-raise."""

    store = InMemoryCheckpointStore()
    dlq = _DLQCollectSink()
    sink = _CollectSink()

    with pytest.raises(RuntimeError, match="source broke"):
        await (
            Pipeline(_FailingSource())
            .build(sink, config=DeliveryConfig(checkpoint=store, dlq=dlq))  # type: ignore[arg-type]
            .run()
        )

    assert sink.records == [10]
    assert len(dlq.records) == 1
    dlq_record = dlq.records[0]
    assert dlq_record.stage == "source_stream"
    assert dlq_record.source == "failing_source"
    assert dlq_record.checkpoint == {"index": 0}
    assert dlq_record.record is None
    assert dlq_record.original_record is None
    assert dlq_record.processed_record is None


# ======================================================================
# [GUARANTEE-17] Source record failures route raw record to DLQ and re-raise
# ======================================================================


@pytest.mark.asyncio
async def test_g17_source_record_failure_routes_raw_record_to_dlq_and_reraises() -> None:
    """[GUARANTEE-17] source_record failures preserve the raw record in the DLQ."""

    dlq = _DLQCollectSink()
    sink = _CollectSink()

    with pytest.raises(ValueError, match="bad row"):
        await (
            Pipeline(_FailingRecordSource())
            .build(sink, config=DeliveryConfig(dlq=dlq))  # type: ignore[arg-type]
            .run()
        )

    assert sink.records == [10]
    assert len(dlq.records) == 1
    dlq_record = dlq.records[0]
    assert dlq_record.stage == "source_record"
    assert dlq_record.source == "failing_record_source"
    assert dlq_record.checkpoint == {"index": 1}
    assert dlq_record.record == {"id": 2, "raw": "broken"}
    assert dlq_record.original_record == {"id": 2, "raw": "broken"}
    assert dlq_record.processed_record is None


# ======================================================================
# [GUARANTEE-18] Cancellation preserves the original terminal reason
# ======================================================================


@pytest.mark.asyncio
async def test_g18_cancellation_preserves_cancelled_error_despite_cleanup_failure() -> None:
    """[GUARANTEE-18] cleanup failures must not mask cancellation."""

    middleware = _BlockingBufferedMiddleware(expected_records=4)
    events: list[str] = []

    class _FailingCloseSink:
        sink_name = "failing_close"

        async def open(self) -> None:
            events.append("sink.open")

        async def write(self, record: int) -> None:
            events.append(f"sink.write:{record}")

        async def flush(self) -> None:
            events.append("sink.flush")

        async def close(self) -> None:
            events.append("sink.close")
            raise RuntimeError("close broke")

    task = asyncio.create_task(
        Pipeline(IterableSource([1, 2, 3, 4]))
        .pipe(middleware)
        .build(_FailingCloseSink())  # type: ignore[arg-type]
        .run()
    )

    await asyncio.wait_for(middleware.all_started.wait(), timeout=1.0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert sorted(middleware.cancelled) == [1, 2, 3, 4]
    assert middleware.stopped is True
    assert events == ["sink.open", "sink.flush", "sink.close"]


# ======================================================================
# [GUARANTEE-H02] Shutdown order is stable
# ======================================================================


@pytest.mark.asyncio
async def test_gh02_shutdown_order_is_stable_across_source_middleware_sinks_and_checkpoint() -> (
    None
):
    """[GUARANTEE-H02] Shutdown order stays source -> middleware reverse order
    -> DLQ -> writer -> checkpoint store.

    Validates: docs/guides/runtime-guarantees.md — "Shutdown order is stable"
    """

    events: list[str] = []

    class _CheckpointingSource(BaseSource[int]):
        source_name = "shutdown_order_source"
        supports_checkpoint = True

        def __init__(self) -> None:
            self._last_index = -1

        async def open(self) -> None:
            events.append("source.open")

        async def close(self) -> None:
            events.append("source.close")

        def current_checkpoint(self) -> dict[str, int] | None:
            if self._last_index < 0:
                return None
            return {"index": self._last_index}

        async def stream(self):
            self._last_index = 0
            yield 1

    class _TrackingMiddleware(Middleware[int, int]):
        def __init__(self, name: str) -> None:
            self.name = name

        async def on_stop(self, ctx: Any) -> None:
            del ctx
            events.append(f"{self.name}.stop")

        async def process(self, record: int, ctx: Any) -> int | None:
            del ctx
            return record

    class _TrackingWriterSink:
        sink_name = "writer"

        async def open(self) -> None:
            events.append("writer.open")

        async def write(self, record: int) -> None:
            events.append(f"writer.write:{record}")

        async def flush(self) -> None:
            events.append("writer.flush")

        async def close(self) -> None:
            events.append("writer.close")

    class _TrackingDLQSink:
        sink_name = "dlq"

        async def open(self) -> None:
            events.append("dlq.open")

        async def write(self, record: Any) -> None:
            events.append(f"dlq.write:{record}")

        async def flush(self) -> None:
            events.append("dlq.flush")

        async def close(self) -> None:
            events.append("dlq.close")

    class _TrackingCheckpointStore(InMemoryCheckpointStore):
        async def close(self) -> None:
            events.append("checkpoint.close")
            await super().close()

    store = _TrackingCheckpointStore()

    summary = await (
        Pipeline(_CheckpointingSource())
        .pipe(_TrackingMiddleware("middleware.one"))
        .pipe(_TrackingMiddleware("middleware.two"))
        .build(
            _TrackingWriterSink(),
            config=DeliveryConfig(checkpoint=store, dlq=_TrackingDLQSink()),
        )  # type: ignore[arg-type]
        .run()
    )

    assert summary.records_written == 1

    expected_shutdown_order = [
        "source.close",
        "middleware.two.stop",
        "middleware.one.stop",
        "dlq.flush",
        "dlq.close",
        "writer.flush",
        "writer.close",
        "checkpoint.close",
    ]
    assert all(event in events for event in expected_shutdown_order)
    shutdown_positions = {event: events.index(event) for event in expected_shutdown_order}
    assert [shutdown_positions[event] for event in expected_shutdown_order] == sorted(
        shutdown_positions.values()
    ), "[GUARANTEE-H02] shutdown steps must preserve the documented order"


# ======================================================================
# [GUARANTEE-19] Prefetch preserves order and does not duplicate records
# ======================================================================


@pytest.mark.asyncio
async def test_g19_prefetch_preserves_order_without_duplicates() -> None:
    """[GUARANTEE-19] Prefetch changes throughput behavior, not record order."""

    sink = _CollectSink()

    summary = await (
        Pipeline(_PrefetchSequenceSource([1, 2, 3, 4, 5]))
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [1, 2, 3, 4, 5]
    assert summary.records_written == 5
    assert summary.runtime.source_prefetch_enabled is True
    assert summary.runtime.source_prefetch_limit == 2


# ======================================================================
# [GUARANTEE-20] Prefetch surfaces source failures after prior buffered records
# ======================================================================


@pytest.mark.asyncio
async def test_g20_prefetch_surfaces_source_failure_without_silent_eof() -> None:
    """[GUARANTEE-20] Prefetch must not swallow source failures into a clean EOF."""

    sink = _CollectSink()

    with pytest.raises(RuntimeError, match="prefetch source broke"):
        await Pipeline(_FailingPrefetchSource()).build(sink).run()  # type: ignore[arg-type]

    assert sink.records == [10, 20]


# ======================================================================
# [GUARANTEE-21] Parquet Arrow batch producer does not duplicate rows
# ======================================================================


@pytest.mark.asyncio
async def test_g21_parquet_arrow_batches_do_not_duplicate_rows_under_bounded_queue(
    tmp_path: Path,
) -> None:
    """[GUARANTEE-21] Built-in Parquet Arrow batch streaming must not duplicate rows."""

    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    from agora.sources.file.parquet import ParquetSource

    path = tmp_path / "records.parquet"
    rows = [{"id": idx, "value": idx * 10} for idx in range(10)]
    pq.write_table(pa.Table.from_pylist(rows), path)

    source = ParquetSource(
        path=path, row_mapper=lambda row: row, batch_size=2, use_arrow_batches=True
    )
    source.prefetch_limit = 1

    seen_ids: list[int] = []
    async for batch in source.stream_batches():
        seen_ids.extend(row["id"] for row in batch.to_pylist())

    assert seen_ids == list(range(10))
    assert len(seen_ids) == 10


# ======================================================================
# Batch-path preservation tests (0.2.0)
# These verify that the 0.1.8 guarantees hold in the batch execution lane.
# ======================================================================


class _BatchSource(BaseSource[int]):
    """Minimal batch-capable source for preservation tests."""

    source_name = "batch_preservation_source"
    supports_batch_emit: bool = True
    supports_checkpoint: bool = True

    def __init__(self, batches: list[list[int]]) -> None:
        self._batches = batches
        self._last_batch_index = -1

    def current_checkpoint(self) -> dict[str, int] | None:
        if self._last_batch_index < 0:
            return None
        return {"batch_index": self._last_batch_index}

    async def prepare_resume(self, checkpoint: Any) -> None:
        return None

    def data_plane_spec(self) -> SourceDataPlaneSpec:
        return SourceDataPlaneSpec(
            source_name=self.source_name,
            emitted_plane=DataPlane.PYTHON_BATCHES,
            supports_batch_emit=True,
            emits_arrow_batches=False,
        )

    async def stream_batches(self) -> Any:  # type: ignore[override]
        for i, batch in enumerate(self._batches):
            self._last_batch_index = i
            yield batch

    async def stream(self) -> Any:
        for batch in self._batches:
            for record in batch:
                yield record


class _ArrowBatchSource(BaseSource[dict[str, Any]]):
    """Checkpointable Arrow-emitting source for preservation tests."""

    source_name = "arrow_preservation_source"
    supports_checkpoint = True

    def __init__(self, batches: list[list[dict[str, Any]]]) -> None:
        self._batches = batches
        self._last_batch_index = -1

    def current_checkpoint(self) -> dict[str, int] | None:
        if self._last_batch_index < 0:
            return None
        return {"batch_index": self._last_batch_index}

    async def prepare_resume(self, checkpoint: Any) -> None:
        del checkpoint

    def data_plane_spec(self) -> SourceDataPlaneSpec:
        return SourceDataPlaneSpec(
            source_name=self.source_name,
            emitted_plane=DataPlane.ARROW_BATCHES,
            supports_batch_emit=True,
            emits_arrow_batches=True,
        )

    async def stream_batches(self) -> Any:  # type: ignore[override]
        pa = pytest.importorskip("pyarrow")
        for index, batch in enumerate(self._batches):
            self._last_batch_index = index
            yield pa.RecordBatch.from_pylist(batch)

    async def stream(self) -> Any:
        for batch in self._batches:
            for record in batch:
                yield record


class _ArrowCollectSink:
    sink_name = "arrow_collect"

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    async def open(self) -> None:
        return None

    async def write_arrow_batch(self, batch: Any) -> None:
        self.records.extend(batch.to_pylist())

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _FailingBatchSink:
    sink_name = "failing_batch"

    async def open(self) -> None:
        return None

    async def write(self, record: Any) -> None:
        raise AssertionError(f"single-record write should not be used: {record!r}")

    async def write_batch(self, records: list[Any]) -> None:
        del records
        raise RuntimeError("batch sink boom")

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _FailingArrowSink(_ArrowCollectSink):
    async def write_arrow_batch(self, batch: Any) -> None:
        del batch
        raise RuntimeError("arrow sink boom")


# ======================================================================
# [GUARANTEE-B01] Source order in batch mode
# ======================================================================


@pytest.mark.asyncio
async def test_gb01_batch_mode_commits_in_source_order() -> None:
    """[GUARANTEE-B01] Batch pipeline commits records in source order.

    Validates: docs/guides/runtime-guarantees.md — "Source order" (batch lane)
    """
    sink = _CollectSink()
    source = _BatchSource([[10, 20], [30, 40], [50]])

    await (
        Pipeline(source)
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [10, 20, 30, 40, 50], (
        "[GUARANTEE-B01] batch pipeline must commit in source order"
    )


# ======================================================================
# [GUARANTEE-B02] Checkpoint advances after each batch is durably written
# ======================================================================


@pytest.mark.asyncio
async def test_gb02_batch_checkpoint_advances_after_write() -> None:
    """[GUARANTEE-B02] Checkpoint advances once per batch, after the batch
    is durably written — not before.

    Validates: docs/guides/runtime-guarantees.md — "Checkpoint advancement"
    """
    store = InMemoryCheckpointStore()
    sink = _CollectSink()
    source = _BatchSource([[1, 2], [3, 4]])

    summary = await (
        Pipeline(source)
        .build(sink, config=DeliveryConfig(checkpoint=store))  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [1, 2, 3, 4]
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"batch_index": 1}, (
        "[GUARANTEE-B02] checkpoint must reflect the last successfully written batch"
    )


# ======================================================================
# [GUARANTEE-B03] Sink fail-closed in batch mode
# ======================================================================


@pytest.mark.asyncio
async def test_gb03_batch_sink_fail_closed_aborts_run() -> None:
    """[GUARANTEE-B03] FAIL_CLOSED (default) in batch mode propagates the
    sink error and stops the run.

    Validates: docs/guides/runtime-guarantees.md — "Sink fail-closed by default"
    """

    class _RaisingSink:
        sink_name = "raising"

        async def open(self) -> None:
            return None

        async def write(self, record: Any) -> None:
            raise RuntimeError("sink boom")

        async def write_batch(self, records: list[Any]) -> None:
            raise RuntimeError("sink boom")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    source = _BatchSource([[1, 2, 3]])

    with pytest.raises(RuntimeError, match="sink boom"):
        await (
            Pipeline(source)
            .build(_RaisingSink())  # type: ignore[arg-type]
            .run()
        )


# ======================================================================
# [GUARANTEE-B04] DLQ routing on batch failure (Option A)
# ======================================================================


@pytest.mark.asyncio
async def test_gb04_batch_failure_routes_entire_batch_to_dlq() -> None:
    """[GUARANTEE-B04] When BatchMiddleware raises, the entire batch is
    routed to the DLQ with stage='batch_middleware'.

    Validates: docs/guides/runtime-guarantees.md — "DLQ routing" (batch lane)
    """
    from agora import BatchMiddleware
    from agora.core.context import PipelineContext  # noqa: TC001

    class _AlwaysRaises(BatchMiddleware[int, int]):
        name = "always_raises"

        async def process_batch(self, records: list[int], ctx: PipelineContext) -> list[int | None]:
            raise ValueError("batch boom")

    sink = _CollectSink()
    dlq = _DLQCollectSink()
    source = _BatchSource([[1, 2, 3]])

    summary = await (
        Pipeline(source)
        .pipe(_AlwaysRaises())
        .build(sink, config=DeliveryConfig(dlq=dlq))  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == []
    assert len(dlq.records) == 3, "[GUARANTEE-B04] all 3 records must be in DLQ"
    assert all(r.stage == "batch_middleware" for r in dlq.records)
    assert summary.records_errored == 3


@pytest.mark.asyncio
async def test_gb04_batch_middleware_dlq_failure_does_not_advance_checkpoint() -> None:
    """[GUARANTEE-B04] If batch middleware failure cannot be routed to DLQ
    under FAIL_CLOSED, the checkpoint must not advance.
    """
    from agora import BatchMiddleware
    from agora.core.context import PipelineContext  # noqa: TC001

    class _AlwaysRaises(BatchMiddleware[int, int]):
        name = "always_raises"

        async def process_batch(self, records: list[int], ctx: PipelineContext) -> list[int | None]:
            raise ValueError("batch boom")

    class _BrokenDLQ:
        sink_name = "broken_dlq"

        async def open(self) -> None:
            return None

        async def write(self, record: Any) -> None:
            del record
            raise RuntimeError("dlq unavailable")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    store = InMemoryCheckpointStore()

    summary = await (
        Pipeline(_BatchSource([[1, 2, 3]]), id="batch_dlq_failure_contract")
        .pipe(_AlwaysRaises())
        .build(
            _CollectSink(),
            config=DeliveryConfig(
                checkpoint=store,
                checkpoint_every=1,
                dlq=_BrokenDLQ(),  # type: ignore[arg-type]
            ),
        )  # type: ignore[arg-type]
        .run()
    )

    assert summary.records_errored == 3
    assert summary.runtime.dlq_failure_count == 3
    assert await store.load("batch_dlq_failure_contract") is None


@pytest.mark.asyncio
async def test_gb04_row_middleware_dlq_failure_does_not_advance_checkpoint() -> None:
    """[GUARANTEE-B04] Row middleware failures are not checkpointed when the
    configured DLQ sink fails while checkpointing is enabled.
    """

    class _AlwaysRaises(Middleware[int, int]):
        name = "always_raises"

        async def process(self, record: int, ctx: Any) -> int | None:
            del record, ctx
            raise ValueError("middleware boom")

    class _BrokenDLQ:
        sink_name = "broken_dlq"

        async def open(self) -> None:
            return None

        async def write(self, record: Any) -> None:
            del record
            raise RuntimeError("dlq unavailable")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    store = InMemoryCheckpointStore()

    with pytest.raises(ValueError, match="middleware boom"):
        await (
            Pipeline(_CheckpointedSequenceSource([1]), id="row_dlq_failure_contract")
            .pipe(_AlwaysRaises())
            .build(
                _CollectSink(),
                config=DeliveryConfig(
                    checkpoint=store,
                    checkpoint_every=1,
                    dlq=_BrokenDLQ(),  # type: ignore[arg-type]
                ),
            )  # type: ignore[arg-type]
            .run()
        )

    assert await store.load("row_dlq_failure_contract") is None


# ======================================================================
# [GUARANTEE-D03] Batch writes preserve per-record outcome handling
# ======================================================================


@pytest.mark.asyncio
async def test_gd03_batch_write_preserves_per_record_outcomes_and_checkpointing() -> None:
    """[GUARANTEE-D03] A mixed-success batch write still resolves outcomes per
    record: successful records commit, failed records route to DLQ, and the
    checkpoint advances through the handled tail.

    Validates: docs/guides/runtime-guarantees.md — "Batch writes preserve
    per-record outcome handling"
    """

    class _PartiallyFailingBatchSink:
        sink_name = "partial_batch_sink"

        def __init__(self) -> None:
            self.records: list[int] = []
            self.batches: list[list[int]] = []

        async def open(self) -> None:
            return None

        async def write(self, record: int) -> WriteResult:
            raise AssertionError(f"single-record path should not be used: {record!r}")

        async def write_batch(self, records: list[int]) -> list[WriteResult]:
            batch = list(records)
            self.batches.append(batch)
            self.records.extend([batch[0], batch[2]])
            return [
                WriteResult(written=True),
                WriteResult(written=False, errors=[RuntimeError("second record broke")]),
                WriteResult(written=True),
            ]

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    store = InMemoryCheckpointStore()
    dlq = _DLQCollectSink()
    sink = _PartiallyFailingBatchSink()

    summary = await (
        Pipeline(_CheckpointedSequenceSource([10, 20, 30]))
        .build(
            sink,
            config=DeliveryConfig(
                checkpoint=store,
                checkpoint_every=1,
                dlq=dlq,
                batch_size=3,
            ),
        )  # type: ignore[arg-type]
        .run()
    )

    assert sink.batches == [[10, 20, 30]]
    assert sink.records == [10, 30]
    assert summary.records_written == 2
    assert summary.records_errored == 1
    assert len(dlq.records) == 1
    assert dlq.records[0].record == 20
    assert dlq.records[0].stage == "sink_write"
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"index": 2}, (
        "[GUARANTEE-D03] handled outcomes across one batch must still allow the "
        "checkpoint to advance through the successful tail record"
    )


@pytest.mark.asyncio
async def test_gd03b_batch_sink_dlq_partial_failure_does_not_checkpoint_past_first_unrouted_record() -> (
    None
):
    """[GUARANTEE-D03b] The whole-batch exception helper must stop
    checkpointing and hook delivery at the first unrouted record under
    FAIL_CLOSED.
    """
    from types import SimpleNamespace

    from agora.core.runtime._delivery import RecordDeliveryError, RunState, make_checkpoint_state
    from agora.core.runtime._delivery_batching import flush_batch_outcomes

    class _Metrics:
        def __init__(self) -> None:
            self.records_errored = 0

    class _Ctx:
        def __init__(self) -> None:
            self.metrics = _Metrics()

    state = RunState(
        ctx=_Ctx(),  # type: ignore[arg-type]
        checkpoint_state=make_checkpoint_state(),
        pending_writes=[],
    )
    persisted: list[tuple[int, int]] = []
    hook_calls: list[str] = []
    attempted_records: list[int] = []

    async def _write_to_dlq(**kwargs: Any) -> bool:
        record = kwargs["record"]
        assert isinstance(record, int)
        attempted_records.append(record)
        return record != 20

    def _prepare_checkpoint(ctx: Any, checkpoint_state: Any, checkpoint_value: Any) -> Any:
        del ctx
        checkpoint_state.increment()
        return SimpleNamespace(value=checkpoint_value)

    async def _persist_checkpoint(
        ctx: Any,
        checkpoint_state: Any,
        checkpoint: Any,
        *,
        batch_size: int = 1,
    ) -> None:
        del ctx
        persisted.append((checkpoint.value, batch_size))
        checkpoint_state.mark_saved(checkpoint.value)

    async def _hook(name: str) -> None:
        hook_calls.append(name)

    with pytest.raises(RecordDeliveryError, match="sink boom"):
        await flush_batch_outcomes(
            state=state,
            exc=RuntimeError("sink boom"),
            processed_list=["p10", "p20", "p30"],
            raw_list=[10, 20, 30],
            checkpoint_list=[0, 1, 2],
            on_success_list=[
                lambda: _hook("first"),
                lambda: _hook("second"),
                lambda: _hook("third"),
            ],
            sink_failure_policy=SinkFailurePolicy.FAIL_CLOSED,
            write_to_dlq=_write_to_dlq,
            prepare_checkpoint=_prepare_checkpoint,
            persist_checkpoint=_persist_checkpoint,
        )

    assert attempted_records == [10, 20], (
        "[GUARANTEE-D03b] later records must not be processed after the first "
        "unrouted batch item under FAIL_CLOSED"
    )
    assert persisted == [(0, 1)], (
        "[GUARANTEE-D03b] checkpoint must stop at the last handled record before "
        "the first unrouted batch item"
    )
    assert hook_calls == ["first"], (
        "[GUARANTEE-D03b] success hooks for later items must not run after the "
        "first unrouted batch item"
    )


# ======================================================================
# [GUARANTEE-P08] Process-isolated batch middleware keeps the same commit contract
# ======================================================================


@pytest.mark.asyncio
@pytest.mark.requires_process_pool
async def test_gp08a_process_batch_sink_failure_does_not_advance_checkpoint() -> None:
    """[GUARANTEE-P08a] Process-isolated batch work is not committed until the
    downstream sink write succeeds in the main runtime.

    Validates: docs/guides/runtime-guarantees.md — "Process-isolated batch
    middleware keeps the same commit contract"
    """

    store = InMemoryCheckpointStore()

    with pytest.raises(RuntimeError, match="batch sink boom"):
        await (
            Pipeline(
                _ProcessBatchSource(
                    [
                        [{"id": "a", "value": 1}],
                        [{"id": "b", "value": 2}],
                    ]
                ),
                id="process_batch_sink_failure_contract",
            )
            .pipe(
                ProcessBatchMiddleware(
                    fn=_process_batch_double_values,
                    max_workers=1,
                    name="process_contract",
                )
            )
            .build(
                _FailingBatchSink(),  # type: ignore[arg-type]
                config=DeliveryConfig(
                    batch_size=10,
                    checkpoint=store,
                    checkpoint_every=1,
                ),
            )
            .run()
        )

    checkpoint = await store.load("process_batch_sink_failure_contract")
    assert checkpoint is None, (
        "[GUARANTEE-P08a] checkpoint must not advance before downstream write succeeds"
    )


@pytest.mark.asyncio
@pytest.mark.requires_process_pool
async def test_gp08b_process_batch_timeout_invalidates_stale_batches_and_preserves_ordered_commit() -> (
    None
):
    """[GUARANTEE-P08b] Timed-out process batches fail the whole batch, stale
    sibling results from the recycled pool do not commit, and later batches can
    continue in source order once handled.

    Validates: docs/guides/runtime-guarantees.md — "Process-isolated batch
    middleware keeps the same commit contract"
    """

    sink = _CollectSink()
    dlq = _DLQCollectSink()
    store = InMemoryCheckpointStore()

    summary = await (
        Pipeline(
            _ProcessBatchSource(
                [
                    [{"id": "timeout", "value": 1}],
                    [{"id": "stale", "value": 2}],
                    [{"id": "ok", "value": 3}],
                ],
                delays=[0.0, 1.1, 0.0],
            ),
            id="process_batch_timeout_contract",
        )
        .pipe(
            ProcessBatchMiddleware(
                fn=_process_batch_timeout_generation_then_double,
                max_workers=2,
                max_in_flight_batches=2,
                timeout_s=1.5,
                name="process_timeout_contract",
            )
        )
        .build(
            sink,
            config=DeliveryConfig(
                batch_size=10,
                checkpoint=store,
                checkpoint_every=1,
                dlq=dlq,
                sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE,
            ),
        )  # type: ignore[arg-type]
        .run()
    )

    assert [record.record["id"] for record in dlq.records] == ["timeout", "stale"], (
        "[GUARANTEE-P08b] timed-out or stale sibling batches must fail as whole batches"
    )
    assert [record["id"] for record in sink.records] == ["ok"], (
        "[GUARANTEE-P08b] later batches must not commit ahead of earlier failed generations"
    )
    assert [record["value"] for record in sink.records] == [6]
    assert summary.records_written == 1
    assert summary.records_errored == 2
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"batch_index": 2}, (
        "[GUARANTEE-P08b] checkpoint must advance only through handled batch outcomes"
    )


@pytest.mark.asyncio
@pytest.mark.requires_process_pool
async def test_gp08c_process_batch_cancellation_aborts_inflight_work_promptly() -> None:
    """[GUARANTEE-P08c] Cancelling the pipeline aborts the active process-pool
    generation instead of waiting indefinitely for worker completion.

    Validates: docs/guides/runtime-guarantees.md — "Process-isolated batch
    middleware keeps the same commit contract"
    """

    task = asyncio.create_task(
        Pipeline(
            _ProcessBatchSource(
                [
                    [{"id": "slow-1", "value": 1}],
                    [{"id": "slow-2", "value": 2}],
                    [{"id": "slow-3", "value": 3}],
                ]
            ),
            id="process_batch_cancel_contract",
        )
        .pipe(
            ProcessBatchMiddleware(
                fn=_process_batch_very_slow_double_values,
                max_workers=2,
                max_in_flight_batches=2,
                timeout_s=30.0,
                name="process_cancel_contract",
            )
        )
        .build(_CollectSink(), config=DeliveryConfig(batch_size=10))  # type: ignore[arg-type]
        .run()
    )

    await asyncio.sleep(0.2)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=3.0)


# ======================================================================
# [GUARANTEE-H03] Buffered shutdown does not break ordering
# ======================================================================


@pytest.mark.asyncio
async def test_gh03_buffered_shutdown_does_not_commit_later_ready_results_ahead_of_blocked_head() -> (
    None
):
    """[GUARANTEE-H03] During buffered cancellation, later ready results do not
    commit ahead of an earlier blocked record.

    Validates: docs/guides/runtime-guarantees.md — "Buffered work does not break
    ordering during shutdown"
    """

    middleware = _OutOfOrderBufferedMiddleware()
    sink = _CollectSink()

    task = asyncio.create_task(
        Pipeline(IterableSource([1, 2, 3]), id="buffered_shutdown_order_contract")
        .pipe(middleware)
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    await asyncio.wait_for(middleware.all_started.wait(), timeout=1.0)
    await asyncio.sleep(0.1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert sink.records == [], (
        "[GUARANTEE-H03] buffered shutdown must not flush later completed results "
        "ahead of the first blocked record"
    )
    assert 1 in middleware.cancelled


# ======================================================================
# [GUARANTEE-BP01] Backpressure does not relax semantics
# ======================================================================


@pytest.mark.asyncio
async def test_gbp01_adaptive_backpressure_preserves_ordering_dlq_and_checkpoint_semantics() -> (
    None
):
    """[GUARANTEE-BP01] Adaptive backpressure changes throughput only; it does
    not relax source-order, DLQ routing, or checkpoint gating semantics.

    Validates: docs/guides/runtime-guarantees.md — "Backpressure and buffering"
    """

    class _SlowCheckpointStore(_RecordingCheckpointStore):
        async def save(self, key: str, checkpoint: Checkpoint) -> None:
            await asyncio.sleep(0.01)
            await super().save(key, checkpoint)

    class _FailOnThreeSink:
        sink_name = "fail_on_three"

        def __init__(self) -> None:
            self.records: list[int] = []

        async def open(self) -> None:
            return None

        async def write(self, record: int) -> None:
            if record == 3:
                raise RuntimeError("record 3 broke")
            self.records.append(record)

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    store = _SlowCheckpointStore()
    dlq = _DLQCollectSink()
    sink = _FailOnThreeSink()

    summary = await (
        Pipeline(_CheckpointedSequenceSource(list(range(1, 13))))
        .pipe(
            _DelayedBufferedPassThroughMiddleware(
                delays={
                    1: 0.03,
                    2: 0.02,
                    3: 0.01,
                },
                min_concurrency=4,
            )
        )
        .build(
            sink,
            config=DeliveryConfig(
                checkpoint=store,
                dlq=dlq,
                backpressure=Backpressure.adaptive(
                    max_buffer_size=6,
                    writer_slow_ms=100.0,
                    checkpoint_slow_ms=1.0,
                ),
            ),
        )  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12], (
        "[GUARANTEE-BP01] adaptive backpressure must not let later records "
        "commit out of source order"
    )
    assert summary.records_written == 11
    assert summary.records_errored == 1
    assert len(dlq.records) == 1
    assert dlq.records[0].record == 3
    assert dlq.records[0].stage == "sink_write"
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"index": 11}, (
        "[GUARANTEE-BP01] handled outcomes under adaptive backpressure must still "
        "gate checkpoint advancement correctly"
    )
    assert summary.runtime.adaptive_backpressure_enabled is True
    assert summary.runtime.adaptive_backpressure_scale_down_count >= 1
    assert summary.runtime.buffered_stage_limit < 4


# ======================================================================
# Arrow-path preservation tests (0.3.0)
# These verify that the public runtime guarantees still hold on Arrow chains.
# ======================================================================


@pytest.mark.asyncio
async def test_ga01_arrow_chain_commits_in_source_order() -> None:
    """[GUARANTEE-A01] Arrow chain preserves source order at the sink boundary."""

    from agora import ArrowMapMiddleware

    source = _ArrowBatchSource(
        [
            [{"id": 1}, {"id": 2}],
            [{"id": 3}, {"id": 4}],
        ]
    )
    sink = _ArrowCollectSink()

    summary = await (
        Pipeline(source)
        .pipe(ArrowMapMiddleware(lambda batch: batch))
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}]
    assert summary.runtime.execution_lane == "batch"
    assert summary.runtime.arrow_chain_active is True


@pytest.mark.asyncio
async def test_ga02_arrow_sink_failure_with_dlq_advances_checkpoint() -> None:
    """[GUARANTEE-A02] Arrow sink failure routed to DLQ still advances checkpoint."""

    from agora import ArrowMapMiddleware

    store = InMemoryCheckpointStore()
    dlq = _DLQCollectSink()
    source = _ArrowBatchSource([[{"id": 1}, {"id": 2}]])

    summary = await (
        Pipeline(source)
        .pipe(ArrowMapMiddleware(lambda batch: batch))
        .build(
            _FailingArrowSink(),  # type: ignore[arg-type]
            config=DeliveryConfig(checkpoint=store, dlq=dlq),
        )
        .run()
    )

    assert summary.records_written == 0
    assert summary.records_errored == 2
    assert len(dlq.records) == 2
    assert all(record.stage == "sink_write" for record in dlq.records)
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"batch_index": 0}


@pytest.mark.asyncio
async def test_ga03_arrow_middleware_failure_routes_to_dlq_and_advances_checkpoint() -> None:
    """[GUARANTEE-A03] Arrow middleware failure DLQs the raw records and advances checkpoint."""

    from agora import ArrowBatchMiddleware

    class _FailingArrowMiddleware(ArrowBatchMiddleware):
        name = "failing_arrow"

        async def process_arrow_batch(self, batch: Any, ctx: Any) -> Any:
            del batch, ctx
            raise ValueError("arrow transform boom")

    store = InMemoryCheckpointStore()
    dlq = _DLQCollectSink()
    source = _ArrowBatchSource([[{"id": 1}, {"id": 2}]])

    summary = await (
        Pipeline(source)
        .pipe(_FailingArrowMiddleware())
        .build(_ArrowCollectSink(), config=DeliveryConfig(checkpoint=store, dlq=dlq))  # type: ignore[arg-type]
        .run()
    )

    assert summary.records_written == 0
    assert summary.records_errored == 2
    assert len(dlq.records) == 2
    assert [record.record for record in dlq.records] == [{"id": 1}, {"id": 2}]
    assert all(record.stage == "batch_middleware" for record in dlq.records)
    assert all(record.middleware == "failing_arrow" for record in dlq.records)
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"batch_index": 0}
