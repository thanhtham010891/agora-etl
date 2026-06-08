"""
tests/preservation/test_runtime_guarantees.py
==============================================
Preservation tests for the runtime guarantees declared in
``packages/agora/docs/guides/runtime-guarantees.md``.

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
    SinkFailurePolicy,
)
from agora.core.checkpoint import is_checkpoint_capable
from agora.core.data_plane import SourceDataPlaneSpec
from agora.core.middleware import Middleware
from agora.core.source import BaseSource, SourceRecordError
from agora.core.types import DLQFailurePolicy

if TYPE_CHECKING:
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
