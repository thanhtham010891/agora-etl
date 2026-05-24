from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from agora import InMemoryCheckpointStore, IterableSource, Pipeline, SQLiteCheckpointStore
from agora.core.middleware import Middleware
from agora.core.source import BaseSource
from agora.core.types import CheckpointFailurePolicy

if TYPE_CHECKING:
    from pathlib import Path


class _CollectSink:
    sink_name = "collect"

    def __init__(self) -> None:
        self.records: list[int] = []

    async def open(self) -> None:
        return None

    async def write(self, record: int) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _CheckpointedSequenceSource(BaseSource[int]):
    source_name = "checkpointed_sequence"
    supports_checkpoint = True

    def __init__(self, records: list[int], fail_after_index: int | None = None) -> None:
        self._records = records
        self._fail_after_index = fail_after_index
        self._resume_index = -1
        self._last_index = -1

    async def prepare_resume(self, checkpoint) -> None:
        if checkpoint is None:
            self._resume_index = -1
            return
        self._resume_index = int(checkpoint.value["index"])

    def current_checkpoint(self) -> dict[str, int] | None:
        if self._last_index < 0:
            return None
        return {"index": self._last_index}

    async def stream(self):
        for index, record in enumerate(self._records):
            if index <= self._resume_index:
                continue
            if self._fail_after_index is not None and index == self._fail_after_index:
                raise RuntimeError("source boom")
            self._last_index = index
            yield record


class _BufferedPassThroughMiddleware(Middleware[int, int]):
    name = "buffered_passthrough"

    def __init__(self, batch_size: int = 2) -> None:
        self.min_concurrency = batch_size
        self._batch_size = batch_size
        self._pending: list[tuple[int, asyncio.Future[int | None]]] = []

    async def process(self, record: int, ctx) -> int | None:
        del ctx
        return record

    async def submit(self, record: int, ctx) -> asyncio.Future[int | None]:
        del ctx
        future: asyncio.Future[int | None] = asyncio.get_running_loop().create_future()
        self._pending.append((record, future))
        if len(self._pending) >= self._batch_size:
            await self._flush_pending()
        return future

    async def drain_pending(self, ctx) -> None:
        del ctx
        await self._flush_pending()

    async def _flush_pending(self) -> None:
        batch, self._pending = self._pending, []
        for record, future in batch:
            if not future.done():
                future.set_result(record)


@pytest.mark.asyncio
async def test_pipeline_checkpoint_store_resumes_from_last_saved_position() -> None:
    store = InMemoryCheckpointStore()
    first_sink = _CollectSink()

    first_summary = await (
        Pipeline(_CheckpointedSequenceSource([10, 20, 30, 40]))
        .build(first_sink, checkpoint=store)  # type: ignore[arg-type]
        .run(max_records=2)
    )

    assert first_sink.records == [10, 20]
    assert first_summary.last_checkpoint is not None
    assert first_summary.last_checkpoint.value == {"index": 1}
    assert first_summary.runtime.checkpoint_enabled is True
    assert first_summary.runtime.checkpoint_save_count == 2

    second_sink = _CollectSink()
    second_summary = await (
        Pipeline(_CheckpointedSequenceSource([10, 20, 30, 40]))
        .build(second_sink, checkpoint=store)  # type: ignore[arg-type]
        .run()
    )

    assert second_sink.records == [30, 40]
    assert second_summary.last_checkpoint is not None
    assert second_summary.last_checkpoint.value == {"index": 3}


@pytest.mark.asyncio
async def test_pipeline_batch_writer_preserves_checkpoint_resume_semantics() -> None:
    store = InMemoryCheckpointStore()
    first_sink = _CollectSink()

    first_summary = await (
        Pipeline(_CheckpointedSequenceSource([10, 20, 30, 40]))
        .build(first_sink, checkpoint=store, batch_size=2)  # type: ignore[arg-type]
        .run(max_records=3)
    )

    assert first_sink.records == [10, 20, 30]
    assert first_summary.last_checkpoint is not None
    assert first_summary.last_checkpoint.value == {"index": 2}

    second_sink = _CollectSink()
    second_summary = await (
        Pipeline(_CheckpointedSequenceSource([10, 20, 30, 40]))
        .build(second_sink, checkpoint=store, batch_size=2)  # type: ignore[arg-type]
        .run()
    )

    assert second_sink.records == [40]
    assert second_summary.last_checkpoint is not None
    assert second_summary.last_checkpoint.value == {"index": 3}


@pytest.mark.asyncio
async def test_sqlite_checkpoint_store_persists_resume_state(tmp_path: Path) -> None:
    store = SQLiteCheckpointStore(path=tmp_path / "checkpoint.db")
    first_sink = _CollectSink()

    await (
        Pipeline(_CheckpointedSequenceSource([1, 2, 3]))
        .build(first_sink, checkpoint=store)  # type: ignore[arg-type]
        .run(max_records=2)
    )
    await store.close()

    resumed_store = SQLiteCheckpointStore(path=tmp_path / "checkpoint.db")
    second_sink = _CollectSink()
    try:
        summary = await (
            Pipeline(_CheckpointedSequenceSource([1, 2, 3]))
            .build(second_sink, checkpoint=resumed_store)  # type: ignore[arg-type]
            .run()
        )
    finally:
        await resumed_store.close()

    assert first_sink.records == [1, 2]
    assert second_sink.records == [3]
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"index": 2}


class _FailingCheckpointStore:
    async def load(self, key: str):
        return None

    async def save(self, key: str, checkpoint) -> None:
        raise RuntimeError("checkpoint broke")

    async def close(self) -> None:
        return None


class _CountingCheckpointStore(InMemoryCheckpointStore):
    def __init__(self) -> None:
        super().__init__()
        self.save_calls = 0
        self.saved_values: list[object] = []

    async def save(self, key: str, checkpoint) -> None:
        self.save_calls += 1
        self.saved_values.append(checkpoint.value)
        await super().save(key, checkpoint)


class _TrackingCheckpointStore(InMemoryCheckpointStore):
    def __init__(self) -> None:
        super().__init__()
        self.load_calls = 0
        self.save_calls = 0

    async def load(self, key: str):
        self.load_calls += 1
        return await super().load(key)

    async def save(self, key: str, checkpoint) -> None:
        self.save_calls += 1
        await super().save(key, checkpoint)


@pytest.mark.asyncio
async def test_pipeline_can_log_and_continue_on_checkpoint_save_failure() -> None:
    sink = _CollectSink()

    summary = await (
        Pipeline(_CheckpointedSequenceSource([1, 2]))
        .build(
            sink,  # type: ignore[arg-type]
            checkpoint=_FailingCheckpointStore(),  # type: ignore[arg-type]
            checkpoint_failure_policy=CheckpointFailurePolicy.LOG_AND_CONTINUE,
        )
        .run()
    )

    assert sink.records == [1, 2]
    assert summary.runtime.checkpoint_enabled is True
    assert summary.runtime.checkpoint_save_count == 0
    assert summary.runtime.checkpoint_failure_count == 2


@pytest.mark.asyncio
async def test_pipeline_batch_writer_coalesces_checkpoint_saves_per_flush() -> None:
    store = _CountingCheckpointStore()

    summary = await (
        Pipeline(_CheckpointedSequenceSource([10, 20, 30]))
        .build(
            _CollectSink(),  # type: ignore[arg-type]
            checkpoint=store,
            batch_size=2,
        )
        .run()
    )

    assert summary.records_written == 3
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"index": 2}
    assert store.save_calls == 2
    assert store.saved_values == [{"index": 1}, {"index": 2}]
    assert summary.runtime.checkpoint_save_count == 2
    assert summary.runtime.checkpoint_save_max_batch_size == 2
    assert summary.runtime.checkpoint_save_time_ms >= 0.0


@pytest.mark.asyncio
async def test_pipeline_fails_closed_on_checkpoint_save_failure_by_default() -> None:
    with pytest.raises(RuntimeError, match="checkpoint broke"):
        await (
            Pipeline(_CheckpointedSequenceSource([1]))
            .build(
                _CollectSink(),  # type: ignore[arg-type]
                checkpoint=_FailingCheckpointStore(),  # type: ignore[arg-type]
            )
            .run()
        )


@pytest.mark.asyncio
async def test_non_checkpointable_source_does_not_enable_or_load_checkpointing() -> None:
    store = _TrackingCheckpointStore()
    sink = _CollectSink()

    summary = await (
        Pipeline(IterableSource([1, 2, 3]))
        .build(sink, checkpoint=store)  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [1, 2, 3]
    assert summary.runtime.checkpoint_enabled is False
    assert summary.last_checkpoint is None
    assert store.load_calls == 0
    assert store.save_calls == 0


class _ClosableCheckpointStore(InMemoryCheckpointStore):
    def __init__(self) -> None:
        super().__init__()
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1
        await super().close()


@pytest.mark.asyncio
async def test_pipeline_closes_checkpoint_store_after_run() -> None:
    store = _ClosableCheckpointStore()

    await (
        Pipeline(_CheckpointedSequenceSource([1, 2]))
        .build(_CollectSink(), checkpoint=store)  # type: ignore[arg-type]
        .run()
    )

    assert store.close_calls == 1


@pytest.mark.asyncio
async def test_batch_sink_fail_closed_advances_checkpoint_only_through_last_successful_batch() -> (
    None
):
    store = InMemoryCheckpointStore()

    class _FailOnSecondBatchSink:
        sink_name = "fail_on_second_batch"

        def __init__(self) -> None:
            self.batches: list[list[int]] = []

        async def open(self) -> None:
            return None

        async def write(self, record: int) -> None:
            raise AssertionError("single-record path should not be used")

        async def write_batch(self, records: list[int]) -> None:
            self.batches.append(list(records))
            if len(self.batches) == 2:
                raise RuntimeError("second batch broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    with pytest.raises(RuntimeError, match="second batch broke"):
        await (
            Pipeline(_CheckpointedSequenceSource([10, 20, 30, 40]))
            .build(_FailOnSecondBatchSink(), checkpoint=store, batch_size=2)  # type: ignore[arg-type]
            .run()
        )

    resumed_sink = _CollectSink()
    summary = await (
        Pipeline(_CheckpointedSequenceSource([10, 20, 30, 40]))
        .build(resumed_sink, checkpoint=store, batch_size=2)  # type: ignore[arg-type]
        .run()
    )

    assert resumed_sink.records == [30, 40]
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"index": 3}


@pytest.mark.asyncio
async def test_buffered_fail_closed_write_stops_pending_delivery_and_preserves_resume_point() -> (
    None
):
    store = InMemoryCheckpointStore()
    acknowledged: list[int] = []

    class _AckTrackingSource(_CheckpointedSequenceSource):
        def __init__(self, records: list[int], target: list[int]) -> None:
            super().__init__(records)
            self._target = target
            self._last_delivered: int | None = None

        def delivery_success_callback(self):
            record = self._last_delivered
            if record is None:
                return None

            async def _ack() -> None:
                self._target.append(record)

            return _ack

        async def stream(self):
            async for record in super().stream():
                self._last_delivered = record
                yield record

    class _FailOnSecondWriteSink:
        sink_name = "fail_on_second_write"

        def __init__(self) -> None:
            self.records: list[int] = []

        async def open(self) -> None:
            return None

        async def write(self, record: int) -> None:
            self.records.append(record)
            if len(self.records) == 2:
                raise RuntimeError("second write broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    failing_sink = _FailOnSecondWriteSink()

    with pytest.raises(RuntimeError, match="second write broke"):
        await (
            Pipeline(_AckTrackingSource([10, 20, 30, 40], acknowledged))
            .pipe(_BufferedPassThroughMiddleware(batch_size=4))
            .build(failing_sink, checkpoint=store)  # type: ignore[arg-type]
            .run()
        )

    assert failing_sink.records == [10, 20]
    assert acknowledged == [10]

    resumed_sink = _CollectSink()
    resumed_acknowledged: list[int] = []
    resumed_summary = await (
        Pipeline(_AckTrackingSource([10, 20, 30, 40], resumed_acknowledged))
        .pipe(_BufferedPassThroughMiddleware(batch_size=4))
        .build(resumed_sink, checkpoint=store)  # type: ignore[arg-type]
        .run()
    )

    assert resumed_sink.records == [20, 30, 40]
    assert resumed_acknowledged == [20, 30, 40]
    assert resumed_summary.last_checkpoint is not None
    assert resumed_summary.last_checkpoint.value == {"index": 3}
