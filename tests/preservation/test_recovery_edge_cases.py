"""
tests/preservation/test_recovery_edge_cases.py
==============================================
Recovery edge-case coverage for the contracts declared in
``packages/agora/docs/guides/recovery-matrix.md``.

These tests cover failure boundaries that are easy to break silently:

- corrupted checkpoint payload on load
- ``prepare_resume`` raising
- checkpoint load failure under both failure policies
- checkpoint save failure under both failure policies
- non-checkpointable source paired with a checkpoint store

Some of these protect specific fixes from earlier releases (the ``0.1.7``
``TypeError`` on corrupted checkpoints, the ``LOG_AND_CONTINUE`` ``mark_saved``
fix). They must stay passing so those fixes cannot regress silently.
"""

from __future__ import annotations

from typing import Any

import pytest

from agora import (
    DeliveryConfig,
    InMemoryCheckpointStore,
    IterableSource,
    Pipeline,
    SQLiteCheckpointStore,
)
from agora.core.checkpoint import (
    BackendCheckpointStore,
    Checkpoint,
    is_checkpoint_capable,
)
from agora.core.source import BaseSource
from agora.core.types import CheckpointFailurePolicy
from agora.state.backend import MemoryBackend

# ======================================================================
# Test fixtures
# ======================================================================


class _CollectSink:
    sink_name = "collect"

    def __init__(self) -> None:
        self.records: list[Any] = []

    async def open(self) -> None:
        return None

    async def write(self, record: Any) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _CheckpointedSequenceSource(BaseSource[int]):
    source_name = "checkpointed_sequence"
    supports_checkpoint = True

    def __init__(self, records: list[int]) -> None:
        self._records = records
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
            self._last_index = index
            yield record


class _PrepareResumeRaisingSource(_CheckpointedSequenceSource):
    """Source whose prepare_resume raises — used to verify load-path failure handling."""

    source_name = "prepare_resume_raising"

    def __init__(self, records: list[int], exc: Exception) -> None:
        super().__init__(records)
        self._exc = exc

    async def prepare_resume(self, checkpoint) -> None:
        raise self._exc


class _LoadRaisingStore:
    """Checkpoint store whose load() raises."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc
        self.load_calls = 0
        self.save_calls = 0

    async def load(self, key: str):
        self.load_calls += 1
        raise self._exc

    async def save(self, key: str, checkpoint) -> None:
        self.save_calls += 1

    async def close(self) -> None:
        return None


class _SaveRaisingStore(InMemoryCheckpointStore):
    """Checkpoint store whose save() raises every time."""

    def __init__(self, exc: Exception | None = None) -> None:
        super().__init__()
        self._exc = exc or RuntimeError("save broke")
        self.save_calls = 0

    async def save(self, key: str, checkpoint) -> None:
        self.save_calls += 1
        raise self._exc


# ======================================================================
# [RECOVERY-01] Corrupted checkpoint payload raises a descriptive TypeError
# ======================================================================


@pytest.mark.asyncio
async def test_r01_load_corrupted_checkpoint_raises_descriptive_typeerror() -> None:
    """[RECOVERY-01] BackendCheckpointStore.load() must raise TypeError with a
    descriptive message when the stored payload is missing required fields.

    Protects the 0.1.7 fix that replaced a bare KeyError with a typed error.

    Validates: docs/guides/recovery-matrix.md — "Checkpoint failure handling"
    """
    backend = MemoryBackend()
    store = BackendCheckpointStore(backend, namespace="checkpoint")
    backend.set("checkpoint:corrupted", {"value": {"index": 1}})

    with pytest.raises(TypeError, match="missing required fields"):
        await store.load("corrupted")


@pytest.mark.asyncio
async def test_r01b_load_non_dict_payload_raises_descriptive_typeerror() -> None:
    """[RECOVERY-01b] A non-dict checkpoint payload must surface a typed error
    rather than a silent crash deeper in the runtime.

    Validates: docs/guides/recovery-matrix.md — "Checkpoint failure handling"
    """
    backend = MemoryBackend()
    store = BackendCheckpointStore(backend, namespace="checkpoint")
    backend.set("checkpoint:bad", "not-a-dict")

    with pytest.raises(TypeError, match="expected dict"):
        await store.load("bad")


# ======================================================================
# [RECOVERY-02] prepare_resume failure stops the run under FAIL_CLOSED
# ======================================================================


@pytest.mark.asyncio
async def test_r02_prepare_resume_failure_aborts_run_under_fail_closed() -> None:
    """[RECOVERY-02] When prepare_resume() raises, the default FAIL_CLOSED
    policy must propagate the error rather than silently start from scratch.

    Validates: docs/guides/recovery-matrix.md — "Checkpoint failure handling"
    """
    store = InMemoryCheckpointStore()
    # Seed a checkpoint so prepare_resume is actually called with non-None.
    await store.save(
        "prepare_resume_raising",
        Checkpoint(
            pipeline_id="prepare_resume_raising",
            run_id="seed",
            source="prepare_resume_raising",
            value={"index": 0},
        ),
    )

    source = _PrepareResumeRaisingSource([1, 2, 3], RuntimeError("resume boom"))

    with pytest.raises(RuntimeError, match="resume boom"):
        await Pipeline(source).build(_CollectSink(), config=DeliveryConfig(checkpoint=store)).run()


# ======================================================================
# [RECOVERY-03] prepare_resume failure under LOG_AND_CONTINUE -> fresh start
# ======================================================================


@pytest.mark.asyncio
async def test_r03_prepare_resume_failure_log_and_continue_starts_from_scratch() -> None:
    """[RECOVERY-03] Under LOG_AND_CONTINUE, a prepare_resume() failure is
    logged and the pipeline starts as if no checkpoint existed.

    Validates: docs/guides/recovery-matrix.md — "Checkpoint failure handling"
    """
    store = InMemoryCheckpointStore()
    await store.save(
        "prepare_resume_raising",
        Checkpoint(
            pipeline_id="prepare_resume_raising",
            run_id="seed",
            source="prepare_resume_raising",
            value={"index": 0},
        ),
    )

    sink = _CollectSink()
    source = _PrepareResumeRaisingSource([1, 2, 3], RuntimeError("resume boom"))

    summary = await (
        Pipeline(source)
        .build(
            sink,
            config=DeliveryConfig(
                checkpoint=store,
                checkpoint_failure_policy=CheckpointFailurePolicy.LOG_AND_CONTINUE,
            ),
        )
        .run()
    )

    assert sink.records == [1, 2, 3], (
        "[RECOVERY-03] LOG_AND_CONTINUE must let the pipeline run from the start"
    )
    assert summary.runtime.checkpoint_failure_count >= 1


# ======================================================================
# [RECOVERY-04] Checkpoint load failure under FAIL_CLOSED
# ======================================================================


@pytest.mark.asyncio
async def test_r04_checkpoint_load_failure_aborts_run_under_fail_closed() -> None:
    """[RECOVERY-04] A failure in CheckpointStore.load() must abort the run
    under the default FAIL_CLOSED policy.

    Validates: docs/guides/recovery-matrix.md — "Checkpoint failure handling"
    """
    store = _LoadRaisingStore(RuntimeError("load broke"))
    source = _CheckpointedSequenceSource([1, 2, 3])

    with pytest.raises(RuntimeError, match="load broke"):
        await Pipeline(source).build(_CollectSink(), config=DeliveryConfig(checkpoint=store)).run()


# ======================================================================
# [RECOVERY-05] Checkpoint load failure under LOG_AND_CONTINUE
# ======================================================================


@pytest.mark.asyncio
async def test_r05_checkpoint_load_failure_log_and_continue_runs_from_start() -> None:
    """[RECOVERY-05] Under LOG_AND_CONTINUE, a failed load() lets the pipeline
    run from the beginning of the source.

    Validates: docs/guides/recovery-matrix.md — "Checkpoint failure handling"
    """
    store = _LoadRaisingStore(RuntimeError("load broke"))
    sink = _CollectSink()

    summary = await (
        Pipeline(_CheckpointedSequenceSource([1, 2, 3]))
        .build(
            sink,
            config=DeliveryConfig(
                checkpoint=store,
                checkpoint_failure_policy=CheckpointFailurePolicy.LOG_AND_CONTINUE,
            ),
        )
        .run()
    )

    assert sink.records == [1, 2, 3]
    assert summary.runtime.checkpoint_failure_count >= 1


# ======================================================================
# [RECOVERY-06] Save failure under LOG_AND_CONTINUE does not retry-storm
# ======================================================================


@pytest.mark.asyncio
async def test_r06_save_failure_log_and_continue_does_not_retry_per_record() -> None:
    """[RECOVERY-06] Under LOG_AND_CONTINUE, a checkpoint save failure must
    advance ``mark_saved`` internally so the runtime does not re-attempt the
    failing save on every subsequent record.

    Protects the 0.1.7 fix where LOG_AND_CONTINUE failed to call mark_saved()
    and produced a retry storm.

    Validates: docs/guides/recovery-matrix.md — "Checkpoint failure handling"
    """
    store = _SaveRaisingStore()
    sink = _CollectSink()

    summary = await (
        Pipeline(_CheckpointedSequenceSource([1, 2, 3, 4, 5]))
        .build(
            sink,
            config=DeliveryConfig(
                checkpoint=store,
                checkpoint_failure_policy=CheckpointFailurePolicy.LOG_AND_CONTINUE,
            ),
        )
        .run()
    )

    # Each record produces one save attempt — not more (no retry storm).
    assert sink.records == [1, 2, 3, 4, 5]
    assert store.save_calls == 5, (
        "[RECOVERY-06] save must be called once per record under LOG_AND_CONTINUE, "
        f"got {store.save_calls}"
    )
    assert summary.runtime.checkpoint_failure_count == 5


# ======================================================================
# [RECOVERY-07] Non-checkpointable source paired with store -> warning, no save
# ======================================================================


@pytest.mark.asyncio
async def test_r07_non_checkpointable_source_with_store_runs_without_checkpointing() -> None:
    """[RECOVERY-07] When a checkpoint store is wired with a source that does
    not declare checkpoint support, the run logs a warning and proceeds without
    checkpointing — it must not raise.

    Validates: docs/guides/recovery-matrix.md — IterableSource note
    """
    store = InMemoryCheckpointStore()
    sink = _CollectSink()

    source = IterableSource([1, 2, 3])
    assert is_checkpoint_capable(source) is False

    summary = await Pipeline(source).build(sink, config=DeliveryConfig(checkpoint=store)).run()

    assert sink.records == [1, 2, 3]
    assert summary.runtime.checkpoint_enabled is False
    assert summary.last_checkpoint is None


# ======================================================================
# [RECOVERY-08] Round-trip resume: second run starts from saved position
# ======================================================================


@pytest.mark.asyncio
async def test_r08_second_run_resumes_from_saved_position() -> None:
    """[RECOVERY-08] After a partial first run, a second run with the same store
    and source resumes from the last saved checkpoint — not from the start.

    Validates: docs/guides/recovery-matrix.md — "What 'resume' promises"
    """
    store = InMemoryCheckpointStore()

    first_sink = _CollectSink()
    await (
        Pipeline(_CheckpointedSequenceSource([10, 20, 30, 40]))
        .build(first_sink, config=DeliveryConfig(checkpoint=store))
        .run(max_records=2)
    )
    assert first_sink.records == [10, 20]

    # Second run uses the same store key (defaults to source/pipeline id).
    second_sink = _CollectSink()
    summary = await (
        Pipeline(_CheckpointedSequenceSource([10, 20, 30, 40]))
        .build(second_sink, config=DeliveryConfig(checkpoint=store))
        .run()
    )

    assert second_sink.records == [30, 40], (
        "[RECOVERY-08] resumed run must skip already-checkpointed records"
    )
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"index": 3}


# ======================================================================
# [RECOVERY-09] SQLite checkpoint store survives close/reopen
# ======================================================================


@pytest.mark.asyncio
async def test_r09_sqlite_checkpoint_survives_store_close_and_reopen(tmp_path) -> None:
    """[RECOVERY-09] SQLiteCheckpointStore persists across explicit close/reopen
    of the store object — this is the basis for cross-process resume.

    Validates: docs/guides/recovery-matrix.md — Built-in sources persistence
    """
    db_path = tmp_path / "checkpoint.db"

    store = SQLiteCheckpointStore(path=str(db_path))
    await (
        Pipeline(_CheckpointedSequenceSource([1, 2, 3, 4]))
        .build(_CollectSink(), config=DeliveryConfig(checkpoint=store))
        .run(max_records=2)
    )
    await store.close()

    # Re-open and confirm the saved checkpoint is still there.
    reopened = SQLiteCheckpointStore(path=str(db_path))
    sink = _CollectSink()
    summary = await (
        Pipeline(_CheckpointedSequenceSource([1, 2, 3, 4])).build(sink, config=DeliveryConfig(checkpoint=reopened)).run()
    )

    assert sink.records == [3, 4], "[RECOVERY-09] SQLite checkpoint must survive store close/reopen"
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"index": 3}
