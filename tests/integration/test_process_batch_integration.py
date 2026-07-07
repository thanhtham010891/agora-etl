"""
tests/integration/test_process_batch_integration.py
=====================================================
End-to-end integration tests for ProcessBatchMiddleware on the batch lane.

Covers:
- Full pipeline: batch source -> process middleware -> sink -> checkpoint
- Checkpoint does not advance until sink write succeeds
- Worker failure routes entire batch to DLQ, checkpoint advances past it
- Pipeline works with multiple batches sequentially
- Sink failure after successful process transform prevents checkpoint advance
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from agora import (
    DataPlane,
    DeliveryConfig,
    Pipeline,
    SinkFailurePolicy,
    SourceDataPlaneSpec,
)
from agora.core.checkpoint import InMemoryCheckpointStore
from agora.core.source import BaseSource
from agora.middlewares.process import ArrowProcessBatchMiddleware, ProcessBatchMiddleware

pytestmark = [pytest.mark.integration, pytest.mark.requires_process_pool]

# ======================================================================
# Module-level transform functions (must be pickleable)
# ======================================================================


def _double_values(batch: list[dict]) -> list[dict]:
    return [{**r, "value": r["value"] * 2} for r in batch]


def _raise_on_batch(batch: list[dict]) -> list[dict]:
    raise ValueError("deliberate worker failure")


def _timeout_first_batch_then_double(batch: list[dict]) -> list[dict]:
    import time

    if batch and batch[0]["value"] < 0:
        time.sleep(2.0)
    return [{**r, "value": r["value"] * 2} for r in batch]


def _sleepy_double_values(batch: list[dict]) -> list[dict]:
    import time

    if batch and batch[0]["id"] == "slow":
        time.sleep(1.5)
    else:
        time.sleep(0.2)
    return [{**r, "value": r["value"] * 2} for r in batch]


def _timeout_generation_then_double(batch: list[dict]) -> list[dict]:
    import time

    if batch and batch[0]["id"] in {"timeout", "stale"}:
        time.sleep(3.0)
    else:
        time.sleep(0.2)
    return [{**r, "value": r["value"] * 2} for r in batch]


def _arrow_double_values(batch: Any) -> Any:
    import pyarrow.compute as pc

    idx = batch.schema.get_field_index("value")
    doubled = pc.multiply(batch.column(idx), 2)
    return batch.set_column(idx, "value", doubled)


def _arrow_timeout_first_batch_then_double(batch: Any) -> Any:
    import time

    if batch.num_rows and batch.column(batch.schema.get_field_index("value"))[0].as_py() < 0:
        time.sleep(4.0)
    return _arrow_double_values(batch)


def _arrow_sleepy_double_values(batch: Any) -> Any:
    import time

    if batch.num_rows and batch.column(batch.schema.get_field_index("id"))[0].as_py() == "slow":
        time.sleep(1.5)
    else:
        time.sleep(0.2)
    return _arrow_double_values(batch)


def _arrow_timeout_generation_then_double(batch: Any) -> Any:
    import time

    if batch.num_rows:
        first_id = batch.column(batch.schema.get_field_index("id"))[0].as_py()
        if first_id in {"timeout", "stale"}:
            time.sleep(4.0)
        else:
            time.sleep(0.2)
    return _arrow_double_values(batch)


def _very_slow_double_values(batch: list[dict]) -> list[dict]:
    import time

    time.sleep(5.0)
    return [{**r, "value": r["value"] * 2} for r in batch]


# ======================================================================
# Test fixtures
# ======================================================================


class _BatchSource(BaseSource[dict]):
    """Simple batch source that emits pre-defined batches."""

    source_name = "test_batch_source"
    supports_checkpoint = True

    def __init__(self, batches: list[list[dict]], *, delays: list[float] | None = None) -> None:
        self._batches = batches
        self._delays = delays or [0.0] * len(batches)
        self._idx = 0

    def data_plane_spec(self) -> SourceDataPlaneSpec:
        return SourceDataPlaneSpec(
            source_name=self.source_name,
            emitted_plane=DataPlane.PYTHON_BATCHES,
            supports_batch_emit=True,
            emits_arrow_batches=False,
        )

    def current_checkpoint(self) -> dict[str, int]:
        return {"batch_index": self._idx}

    async def prepare_resume(self, checkpoint: Any) -> None:
        if checkpoint and isinstance(checkpoint.value, dict):
            self._idx = int(checkpoint.value.get("batch_index", 0))

    async def stream(self):  # type: ignore[override]
        async for batch in self.stream_batches():
            for record in batch:
                yield record

    async def stream_batches(self):  # type: ignore[override]
        while self._idx < len(self._batches):
            delay = self._delays[self._idx] if self._idx < len(self._delays) else 0.0
            if delay > 0:
                await asyncio.sleep(delay)
            batch = self._batches[self._idx]
            self._idx += 1
            yield batch


class _ArrowBatchSource(BaseSource[Any]):
    """Simple Arrow-emitting batch source for process-arrow integration tests."""

    source_name = "test_arrow_batch_source"
    supports_checkpoint = True

    def __init__(
        self,
        batches: list[list[dict[str, Any]]],
        *,
        delays: list[float] | None = None,
    ) -> None:
        self._batches = batches
        self._delays = delays or [0.0] * len(batches)
        self._idx = 0

    def data_plane_spec(self) -> SourceDataPlaneSpec:
        return SourceDataPlaneSpec(
            source_name=self.source_name,
            emitted_plane=DataPlane.ARROW_BATCHES,
            supports_batch_emit=True,
            emits_arrow_batches=True,
        )

    def current_checkpoint(self) -> dict[str, int]:
        return {"batch_index": self._idx}

    async def prepare_resume(self, checkpoint: Any) -> None:
        if checkpoint and isinstance(checkpoint.value, dict):
            self._idx = int(checkpoint.value.get("batch_index", 0))

    async def stream(self):
        async for batch in self.stream_batches():
            for row in batch.to_pylist():
                yield row

    async def stream_batches(self):
        pa = pytest.importorskip("pyarrow")
        while self._idx < len(self._batches):
            delay = self._delays[self._idx] if self._idx < len(self._delays) else 0.0
            if delay > 0:
                await asyncio.sleep(delay)
            rows = self._batches[self._idx]
            self._idx += 1
            yield pa.RecordBatch.from_pylist(rows)


class _CollectSink:
    sink_name = "collect"

    def __init__(self) -> None:
        self.records: list[Any] = []
        self.batches: list[list[Any]] = []

    async def open(self) -> None:
        pass

    async def write(self, record: Any) -> None:
        self.records.append(record)

    async def write_batch(self, records: list[Any]) -> None:
        self.batches.append(list(records))
        self.records.extend(records)

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


class _FailingSink:
    sink_name = "failing"

    async def open(self) -> None:
        pass

    async def write(self, record: Any) -> None:
        raise OSError("sink unavailable")

    async def write_batch(self, records: list[Any]) -> None:
        raise OSError("sink unavailable")

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


class _DLQSink:
    sink_name = "dlq"

    def __init__(self) -> None:
        self.records: list[Any] = []

    async def open(self) -> None:
        pass

    async def write(self, record: Any) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


class _ArrowCollectSink:
    sink_name = "arrow_collect"

    def __init__(self) -> None:
        self.batches: list[Any] = []

    async def open(self) -> None:
        pass

    async def write(self, record: Any) -> None:
        raise AssertionError("ArrowCollectSink should receive arrow batches, not records")

    async def write_arrow_batch(self, batch: Any) -> None:
        self.batches.append(batch)

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


# ======================================================================
# Tests
# ======================================================================


@pytest.mark.asyncio
async def test_process_middleware_transforms_batches_end_to_end() -> None:
    batches = [
        [{"id": "a", "value": 1}, {"id": "b", "value": 2}],
        [{"id": "c", "value": 3}],
    ]
    source = _BatchSource(batches)
    sink = _CollectSink()
    checkpoint_store = InMemoryCheckpointStore()

    summary = await (
        Pipeline(source, id="test_pipeline")
        .pipe(ProcessBatchMiddleware(fn=_double_values, max_workers=1, name="doubler"))
        .build(
            sink,
            config=DeliveryConfig(batch_size=10, checkpoint=checkpoint_store),
        )
        .run()
    )

    assert summary.records_written == 3
    values = [r["value"] for r in sink.records]
    assert values == [2, 4, 6]


@pytest.mark.asyncio
async def test_process_middleware_checkpoint_advances_after_write() -> None:
    batches = [
        [{"id": "a", "value": 1}],
        [{"id": "b", "value": 2}],
    ]
    source = _BatchSource(batches)
    sink = _CollectSink()
    checkpoint_store = InMemoryCheckpointStore()

    await (
        Pipeline(source, id="checkpoint_test")
        .pipe(ProcessBatchMiddleware(fn=_double_values, max_workers=1, name="doubler"))
        .build(
            sink,
            config=DeliveryConfig(
                batch_size=1,
                checkpoint=checkpoint_store,
                checkpoint_every=1,
            ),
        )
        .run()
    )

    checkpoint = await checkpoint_store.load("checkpoint_test")
    assert checkpoint is not None
    assert checkpoint.value == {"batch_index": 2}
    assert len(sink.records) == 2


@pytest.mark.asyncio
async def test_process_middleware_worker_failure_routes_to_dlq() -> None:
    batches = [
        [{"id": "a", "value": 1}, {"id": "b", "value": 2}],
    ]
    source = _BatchSource(batches)
    sink = _CollectSink()
    dlq = _DLQSink()
    checkpoint_store = InMemoryCheckpointStore()

    summary = await (
        Pipeline(source, id="dlq_test")
        .pipe(ProcessBatchMiddleware(fn=_raise_on_batch, max_workers=1, name="failing"))
        .build(
            sink,
            config=DeliveryConfig(
                batch_size=10,
                checkpoint=checkpoint_store,
                dlq=dlq,
                sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE,
            ),
        )
        .run()
    )

    # No records written to main sink — all failed
    assert len(sink.records) == 0
    # Both records routed to DLQ
    assert len(dlq.records) == 2
    assert summary.records_errored == 2


@pytest.mark.asyncio
async def test_process_middleware_multiple_batches_all_written() -> None:
    batches = [
        [{"id": str(i), "value": i} for i in range(5)],
        [{"id": str(i), "value": i} for i in range(5, 10)],
        [{"id": str(i), "value": i} for i in range(10, 13)],
    ]
    source = _BatchSource(batches)
    sink = _CollectSink()

    summary = await (
        Pipeline(source, id="multi_batch_test")
        .pipe(ProcessBatchMiddleware(fn=_double_values, max_workers=2, name="doubler"))
        .build(sink, config=DeliveryConfig(batch_size=20))
        .run()
    )

    assert summary.records_written == 13
    assert len(sink.records) == 13
    # Values are doubled
    for original_batches_flat, record in zip(
        [r for batch in batches for r in batch], sink.records, strict=True
    ):
        assert record["value"] == original_batches_flat["value"] * 2


@pytest.mark.asyncio
async def test_process_middleware_sink_failure_does_not_advance_checkpoint() -> None:
    batches = [
        [{"id": "a", "value": 1}],
        [{"id": "b", "value": 2}],
    ]
    source = _BatchSource(batches)
    sink = _FailingSink()
    checkpoint_store = InMemoryCheckpointStore()

    with pytest.raises(OSError, match="sink unavailable"):
        await (
            Pipeline(source, id="sink_failure_test")
            .pipe(ProcessBatchMiddleware(fn=_double_values, max_workers=1, name="doubler"))
            .build(
                sink,
                config=DeliveryConfig(
                    batch_size=1,
                    checkpoint=checkpoint_store,
                    checkpoint_every=1,
                ),
            )
            .run()
        )

    checkpoint = await checkpoint_store.load("sink_failure_test")
    assert checkpoint is None


@pytest.mark.asyncio
async def test_process_middleware_timeout_recycles_pool_and_later_batches_continue() -> None:
    batches = [
        [{"id": "timeout", "value": -1}],
        [{"id": "ok-1", "value": 2}, {"id": "ok-2", "value": 3}],
    ]
    source = _BatchSource(batches)
    sink = _CollectSink()
    dlq = _DLQSink()
    checkpoint_store = InMemoryCheckpointStore()

    summary = await (
        Pipeline(source, id="timeout_recovery_test")
        .pipe(
            ProcessBatchMiddleware(
                fn=_timeout_first_batch_then_double,
                max_workers=1,
                timeout_s=1.0,
                name="timeout_recycle",
            )
        )
        .build(
            sink,
            config=DeliveryConfig(
                batch_size=10,
                checkpoint=checkpoint_store,
                checkpoint_every=1,
                dlq=dlq,
                sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE,
            ),
        )
        .run()
    )

    assert len(dlq.records) == 1
    assert [r["value"] for r in sink.records] == [4, 6]
    assert summary.records_written == 2
    assert summary.records_errored == 1

    checkpoint = await checkpoint_store.load("timeout_recovery_test")
    assert checkpoint is not None
    assert checkpoint.value == {"batch_index": 2}


@pytest.mark.asyncio
async def test_process_middleware_pipelines_batches_in_order() -> None:
    batches = [
        [{"id": "slow", "value": 1}],
        [{"id": "fast-1", "value": 2}],
        [{"id": "fast-2", "value": 3}],
    ]
    source = _BatchSource(batches)
    sink = _CollectSink()

    summary = await (
        Pipeline(source, id="pipelined_order_test")
        .pipe(
            ProcessBatchMiddleware(
                fn=_sleepy_double_values,
                max_workers=2,
                max_in_flight_batches=2,
                name="pipelined_doubler",
            )
        )
        .build(sink, config=DeliveryConfig(batch_size=10))
        .run()
    )

    assert [record["id"] for record in sink.records] == ["slow", "fast-1", "fast-2"]
    assert [record["value"] for record in sink.records] == [2, 4, 6]
    assert summary.runtime.process_batch_stage_max_in_flight >= 2


@pytest.mark.asyncio
async def test_process_middleware_timeout_invalidates_unresolved_inflight_batches() -> None:
    batches = [
        [{"id": "timeout", "value": 1}],
        [{"id": "stale", "value": 2}],
        [{"id": "ok", "value": 3}],
    ]
    source = _BatchSource(batches, delays=[0.0, 1.1, 0.0])
    sink = _CollectSink()
    dlq = _DLQSink()
    checkpoint_store = InMemoryCheckpointStore()

    summary = await (
        Pipeline(source, id="timeout_generation_test")
        .pipe(
            ProcessBatchMiddleware(
                fn=_timeout_generation_then_double,
                max_workers=2,
                max_in_flight_batches=2,
                timeout_s=1.5,
                name="generation_timeout",
            )
        )
        .build(
            sink,
            config=DeliveryConfig(
                batch_size=10,
                checkpoint=checkpoint_store,
                checkpoint_every=1,
                dlq=dlq,
                sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE,
            ),
        )
        .run()
    )

    assert [record.record["id"] for record in dlq.records] == ["timeout", "stale"]
    assert [record["id"] for record in sink.records] == ["ok"]
    assert [record["value"] for record in sink.records] == [6]
    assert summary.records_written == 1
    assert summary.records_errored == 2

    checkpoint = await checkpoint_store.load("timeout_generation_test")
    assert checkpoint is not None
    assert checkpoint.value == {"batch_index": 3}


@pytest.mark.asyncio
async def test_arrow_process_middleware_transforms_batches_end_to_end() -> None:
    pytest.importorskip("pyarrow")

    batches = [
        [{"id": "a", "value": 1}, {"id": "b", "value": 2}],
        [{"id": "c", "value": 3}],
    ]
    source = _ArrowBatchSource(batches, delays=[0.0, 0.8, 0.0])
    sink = _ArrowCollectSink()
    checkpoint_store = InMemoryCheckpointStore()

    summary = await (
        Pipeline(source, id="arrow_process_pipeline")
        .pipe(
            ArrowProcessBatchMiddleware(
                fn=_arrow_double_values, max_workers=1, name="arrow_doubler"
            )
        )
        .build(
            sink,
            config=DeliveryConfig(batch_size=10, checkpoint=checkpoint_store),
        )
        .run()
    )

    assert len(sink.batches) == 2
    assert summary.records_written == 3
    assert [value for batch in sink.batches for value in batch.column("value").to_pylist()] == [
        2,
        4,
        6,
    ]


@pytest.mark.asyncio
async def test_arrow_process_middleware_timeout_recycles_pool_and_later_batches_continue() -> None:
    pytest.importorskip("pyarrow")

    batches = [
        [{"id": "timeout", "value": -1}],
        [{"id": "ok-1", "value": 2}, {"id": "ok-2", "value": 3}],
    ]
    source = _ArrowBatchSource(batches, delays=[0.0, 0.3, 0.0])
    sink = _ArrowCollectSink()
    checkpoint_store = InMemoryCheckpointStore()

    summary = await (
        Pipeline(source, id="arrow_timeout_recovery_test")
        .pipe(
            ArrowProcessBatchMiddleware(
                fn=_arrow_timeout_first_batch_then_double,
                max_workers=1,
                timeout_s=2.0,
                name="arrow_timeout_recycle",
            )
        )
        .build(
            sink,
            config=DeliveryConfig(
                batch_size=10,
                checkpoint=checkpoint_store,
                sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE,
            ),
        )
        .run()
    )

    assert len(sink.batches) == 1
    assert sink.batches[0].column("value").to_pylist() == [4, 6]
    assert summary.records_written == 2
    assert summary.records_errored == 1

    checkpoint = await checkpoint_store.load("arrow_timeout_recovery_test")
    assert checkpoint is not None
    assert checkpoint.value == {"batch_index": 2}


@pytest.mark.asyncio
async def test_arrow_process_middleware_pipelines_batches_in_order() -> None:
    pytest.importorskip("pyarrow")

    batches = [
        [{"id": "slow", "value": 1}],
        [{"id": "fast-1", "value": 2}],
        [{"id": "fast-2", "value": 3}],
    ]
    source = _ArrowBatchSource(batches)
    sink = _ArrowCollectSink()

    summary = await (
        Pipeline(source, id="arrow_pipelined_order_test")
        .pipe(
            ArrowProcessBatchMiddleware(
                fn=_arrow_sleepy_double_values,
                max_workers=2,
                max_in_flight_batches=2,
                name="arrow_pipelined_doubler",
            )
        )
        .build(sink, config=DeliveryConfig(batch_size=10))
        .run()
    )

    assert [value for batch in sink.batches for value in batch.column("id").to_pylist()] == [
        "slow",
        "fast-1",
        "fast-2",
    ]
    assert [value for batch in sink.batches for value in batch.column("value").to_pylist()] == [
        2,
        4,
        6,
    ]
    assert summary.runtime.process_batch_stage_max_in_flight >= 2


@pytest.mark.asyncio
async def test_arrow_process_middleware_timeout_invalidates_unresolved_inflight_batches() -> None:
    pytest.importorskip("pyarrow")

    batches = [
        [{"id": "timeout", "value": 1}],
        [{"id": "stale", "value": 2}],
        [{"id": "ok", "value": 3}],
    ]
    source = _ArrowBatchSource(batches, delays=[0.0, 0.3, 0.0])
    sink = _ArrowCollectSink()
    checkpoint_store = InMemoryCheckpointStore()

    summary = await (
        Pipeline(source, id="arrow_timeout_generation_test")
        .pipe(
            ArrowProcessBatchMiddleware(
                fn=_arrow_timeout_generation_then_double,
                max_workers=2,
                max_in_flight_batches=2,
                timeout_s=2.0,
                name="arrow_generation_timeout",
            )
        )
        .build(
            sink,
            config=DeliveryConfig(
                batch_size=10,
                checkpoint=checkpoint_store,
                sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE,
            ),
        )
        .run()
    )

    assert len(sink.batches) == 1
    assert sink.batches[0].column("id").to_pylist() == ["ok"]
    assert sink.batches[0].column("value").to_pylist() == [6]
    assert summary.records_written == 1
    assert summary.records_errored == 2

    checkpoint = await checkpoint_store.load("arrow_timeout_generation_test")
    assert checkpoint is not None
    assert checkpoint.value == {"batch_index": 3}


@pytest.mark.asyncio
async def test_process_middleware_cancellation_aborts_inflight_batches_promptly() -> None:
    batches = [
        [{"id": "slow-1", "value": 1}],
        [{"id": "slow-2", "value": 2}],
        [{"id": "slow-3", "value": 3}],
    ]
    source = _BatchSource(batches)
    sink = _CollectSink()

    task = asyncio.create_task(
        Pipeline(source, id="process_cancel_test")
        .pipe(
            ProcessBatchMiddleware(
                fn=_very_slow_double_values,
                max_workers=2,
                max_in_flight_batches=2,
                timeout_s=30.0,
                name="cancel_generation",
            )
        )
        .build(sink, config=DeliveryConfig(batch_size=10))
        .run()
    )

    await asyncio.sleep(0.2)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=3.0)
