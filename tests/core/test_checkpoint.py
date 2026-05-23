from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from agora import InMemoryCheckpointStore, Pipeline, SQLiteCheckpointStore
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
