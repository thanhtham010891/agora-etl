from __future__ import annotations

import asyncio
import sqlite3
import threading
from contextlib import nullcontext
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from agora import (
    DeliveryConfig,
    IterableSource,
    Pipeline,
)
from agora.core.checkpoint import InMemoryCheckpointStore
from agora.core.dlq import DLQRecord, SQLiteDLQSink, SQLiteDLQSource
from agora.core.middleware import Middleware
from agora.core.runtime._delivery import DeliveryEngine, RunState, make_checkpoint_state
from agora.core.runtime._writer_transport import WriterTransport
from agora.core.source import BaseSource, SourceRecordError
from agora.core.types import CheckpointFailurePolicy, DLQFailurePolicy, SinkFailurePolicy
from agora.core.writer import WriteResult


class _CollectDLQSink:
    sink_name = "collect_dlq"

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


class _CollectSink:
    sink_name = "collect"

    def __init__(self) -> None:
        self.records: list[dict] = []

    async def open(self) -> None:
        return None

    async def write(self, record: dict) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _AckTrackingSource(BaseSource[dict]):
    source_name = "ack_tracking"

    def __init__(self, records: list[dict], target: list[dict]) -> None:
        self._records = records
        self._target = target
        self._current: dict | None = None

    def delivery_success_callback(self):
        record = self._current
        if record is None:
            return None

        async def _ack() -> None:
            self._target.append(record)

        return _ack

    async def stream(self):
        for record in self._records:
            self._current = record
            yield record


class _BoomMiddleware(Middleware[dict, dict]):
    name = "boom"

    async def process(self, record: dict, ctx):
        raise RuntimeError("middleware blew up")


@pytest.mark.asyncio
async def test_pipeline_routes_middleware_errors_to_dlq() -> None:
    sink = _CollectSink()
    dlq = _CollectDLQSink()
    pipeline = (
        Pipeline(IterableSource([{"id": 1}]))
        .pipe(_BoomMiddleware())
        .build(sink, config=DeliveryConfig(dlq=dlq))  # type: ignore[arg-type]
    )

    summary = await pipeline.run()

    assert sink.records == []
    assert summary.records_dropped == 0
    assert summary.records_errored == 1
    assert len(dlq.records) == 1
    dlq_record = dlq.records[0]
    assert dlq_record.stage == "middleware"
    assert dlq_record.error_type == "RuntimeError"
    assert dlq_record.error_message == "middleware blew up"
    assert dlq_record.record == {"id": 1}


@pytest.mark.asyncio
async def test_dlq_redactor_scrubs_payload_before_sink_write() -> None:
    sink = _CollectSink()
    dlq = _CollectDLQSink()

    def _redact(value):
        if isinstance(value, dict):
            return {
                key: "[REDACTED]" if key in {"api_key", "token"} else _redact(item)
                for key, item in value.items()
            }
        if isinstance(value, str):
            return value.replace("secret-token", "[REDACTED]")
        return value

    pipeline = (
        Pipeline(IterableSource([{"id": 1, "api_key": "secret-token"}]))
        .pipe(_BoomMiddleware())
        .build(
            sink,
            config=DeliveryConfig(
                dlq=dlq,
                dlq_redactor=_redact,
            ),
        )  # type: ignore[arg-type]
    )

    await pipeline.run()

    record = dlq.records[0]
    assert record.record == {"id": 1, "api_key": "[REDACTED]"}
    assert record.original_record == {"id": 1, "api_key": "[REDACTED]"}
    assert "secret-token" not in str(record)


@pytest.mark.asyncio
async def test_pipeline_routes_sink_write_errors_to_dlq_without_dropping_run() -> None:
    dlq = _CollectDLQSink()

    class _BoomSink:
        sink_name = "boom_sink"

        async def open(self) -> None:
            return None

        async def write(self, record: dict) -> None:
            raise RuntimeError("sink broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    summary = await (
        Pipeline(IterableSource([{"id": 1}]))
        .build(_BoomSink(), config=DeliveryConfig(dlq=dlq))  # type: ignore[arg-type]
        .run()
    )

    assert summary.records_written == 0
    assert summary.records_errored == 1
    assert len(dlq.records) == 1
    dlq_record = dlq.records[0]
    assert dlq_record.stage == "sink_write"
    assert dlq_record.error_type == "RuntimeError"
    assert dlq_record.error_message == "sink broke"
    assert dlq_record.record == {"id": 1}
    assert dlq_record.original_record == {"id": 1}
    assert dlq_record.processed_record == {"id": 1}


@pytest.mark.asyncio
async def test_dlq_routed_sink_failure_still_acknowledges_source_delivery() -> None:
    acknowledged: list[dict] = []
    dlq = _CollectDLQSink()

    class _BoomSink:
        sink_name = "boom_sink"

        async def open(self) -> None:
            return None

        async def write(self, record: dict) -> None:
            del record
            raise RuntimeError("sink broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    summary = await (
        Pipeline(_AckTrackingSource([{"id": 1}], acknowledged))
        .build(_BoomSink(), config=DeliveryConfig(dlq=dlq))  # type: ignore[arg-type]
        .run()
    )

    assert summary.records_errored == 1
    assert len(dlq.records) == 1
    assert acknowledged == [{"id": 1}]


@pytest.mark.asyncio
async def test_pipeline_fails_closed_on_sink_write_error_without_dlq() -> None:
    class _BoomSink:
        sink_name = "boom_sink"

        async def open(self) -> None:
            return None

        async def write(self, record: dict) -> None:
            raise RuntimeError("sink broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    with pytest.raises(RuntimeError, match="sink broke"):
        await (
            Pipeline(IterableSource([{"id": 1}]))
            .build(_BoomSink())  # type: ignore[arg-type]
            .run()
        )


@pytest.mark.asyncio
async def test_pipeline_can_log_and_continue_on_sink_write_error_without_dlq() -> None:
    class _BoomSink:
        sink_name = "boom_sink"

        async def open(self) -> None:
            return None

        async def write(self, record: dict) -> None:
            raise RuntimeError("sink broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    summary = await (
        Pipeline(IterableSource([{"id": 1}]))
        .build(
            _BoomSink(),
            config=DeliveryConfig(sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE),
        )  # type: ignore[arg-type]
        .run()
    )

    assert summary.records_written == 0
    assert summary.records_errored == 1


@pytest.mark.asyncio
async def test_batch_log_and_continue_sink_error_without_dlq_does_not_acknowledge_source() -> None:
    acknowledged: list[dict] = []

    class _BoomBatchSink:
        sink_name = "boom_batch_sink"

        async def open(self) -> None:
            return None

        async def write(self, record: dict) -> None:
            raise AssertionError(f"single-record write should not be used: {record}")

        async def write_batch(self, records: list[dict]) -> None:
            del records
            raise RuntimeError("batch sink broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    summary = await (
        Pipeline(_AckTrackingSource([{"id": 1}, {"id": 2}], acknowledged))
        .build(
            _BoomBatchSink(),
            config=DeliveryConfig(
                batch_size=2, sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE
            ),
        )  # type: ignore[arg-type]
        .run()
    )

    assert summary.records_written == 0
    assert summary.records_errored == 2
    assert acknowledged == []


@pytest.mark.asyncio
async def test_direct_flush_log_and_continue_transport_error_without_dlq_does_not_acknowledge() -> (
    None
):
    acknowledged: list[str] = []

    class _ExplodingWriter:
        async def open(self) -> None:
            return None

        async def write(self, record) -> None:
            del record
            raise AssertionError("single-record path should not be used")

        async def write_batch(self, records):
            del records
            raise RuntimeError("transport batch broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    ctx = SimpleNamespace(
        pipeline_id="orders",
        run_id="run-1",
        metrics=SimpleNamespace(
            records_written=0,
            records_errored=0,
            records_dropped=0,
            runtime=SimpleNamespace(
                dlq_failure_count=0,
                checkpoint_failure_count=0,
                checkpoint_save_time_ms=0.0,
                checkpoint_save_count=0,
                checkpoint_save_max_batch_size=0,
            ),
            last_checkpoint=None,
        ),
        log=SimpleNamespace(
            exception=lambda *args, **kwargs: None,
            error=lambda *args, **kwargs: None,
            warning=lambda *args, **kwargs: None,
        ),
        trace_span=lambda *args, **kwargs: nullcontext(),
        pop_success_hooks=lambda *records: [],
    )

    engine = DeliveryEngine(
        transport=WriterTransport(writer=_ExplodingWriter()),  # type: ignore[arg-type]
        source_name="ack_tracking",
        current_checkpoint=lambda: {"id": 2},
        dlq_sink=None,
        dlq_failure_policy=DLQFailurePolicy.LOG_ONLY,
        dlq_redactor=None,
        sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE,
        checkpoint_store=None,
        checkpoint_failure_policy=CheckpointFailurePolicy.FAIL_CLOSED,
        checkpoint_key="orders",
        checkpoint_every=1,
    )
    state = RunState(
        ctx=ctx,
        checkpoint_state=make_checkpoint_state(),
        pending_writes=[],
    )

    async def _ack1() -> None:
        acknowledged.append("a")

    async def _ack2() -> None:
        acknowledged.append("b")

    await engine.flush_batch_direct(
        state,
        processed_list=[{"id": 1}, {"id": 2}],
        raw_list=[{"id": 1}, {"id": 2}],
        checkpoint_list=[{"id": 1}, {"id": 2}],
        on_success_list=[_ack1, _ack2],
    )

    assert ctx.metrics.records_written == 0
    assert ctx.metrics.records_errored == 2
    assert acknowledged == []


@pytest.mark.asyncio
async def test_direct_flush_partial_failure_routes_failed_records_to_dlq() -> None:
    """A direct-flush batch with a mid-batch failure routes only the failed
    record to the DLQ, acknowledges the rest, and advances the checkpoint."""
    dlq = _CollectDLQSink()
    acknowledged: list[str] = []

    class _PartialWriter:
        async def open(self) -> None:
            return None

        async def write(self, record) -> WriteResult:
            del record
            raise AssertionError("single-record path should not be used")

        async def write_batch(self, records):
            # Second record fails, the other two succeed.
            return [
                WriteResult(written=True, errors=[])
                if record["id"] != 2
                else WriteResult(written=False, errors=[RuntimeError("row 2 broke")])
                for record in records
            ]

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    ctx = SimpleNamespace(
        pipeline_id="orders",
        run_id="run-1",
        metrics=SimpleNamespace(
            records_written=0,
            records_errored=0,
            records_dropped=0,
            runtime=SimpleNamespace(
                dlq_failure_count=0,
                checkpoint_failure_count=0,
                checkpoint_save_time_ms=0.0,
                checkpoint_save_count=0,
                checkpoint_save_max_batch_size=0,
                writer_flush_count=0,
                writer_flush_time_ms=0.0,
                writer_flush_max_batch_size=0,
            ),
            last_checkpoint=None,
        ),
        log=SimpleNamespace(
            exception=lambda *args, **kwargs: None,
            error=lambda *args, **kwargs: None,
            warning=lambda *args, **kwargs: None,
        ),
        trace_span=lambda *args, **kwargs: nullcontext(),
        pop_success_hooks=lambda *records: [],
    )

    engine = DeliveryEngine(
        transport=WriterTransport(writer=_PartialWriter()),  # type: ignore[arg-type]
        source_name="orders",
        current_checkpoint=lambda: {"id": 3},
        dlq_sink=dlq,  # type: ignore[arg-type]
        dlq_failure_policy=DLQFailurePolicy.LOG_ONLY,
        dlq_redactor=None,
        sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE,
        checkpoint_store=None,
        checkpoint_failure_policy=CheckpointFailurePolicy.FAIL_CLOSED,
        checkpoint_key="orders",
        checkpoint_every=1,
    )
    state = RunState(
        ctx=ctx,
        checkpoint_state=make_checkpoint_state(),
        pending_writes=[],
    )

    async def _ack(label: str):
        async def _inner() -> None:
            acknowledged.append(label)

        return _inner

    await engine.flush_batch_direct(
        state,
        processed_list=[{"id": 1}, {"id": 2}, {"id": 3}],
        raw_list=[{"id": 1}, {"id": 2}, {"id": 3}],
        checkpoint_list=[{"id": 1}, {"id": 2}, {"id": 3}],
        on_success_list=[await _ack("a"), await _ack("b"), await _ack("c")],
    )

    assert ctx.metrics.records_written == 2
    assert ctx.metrics.records_errored == 1
    # The failed row is acknowledged because it was successfully routed to the DLQ.
    assert acknowledged == ["a", "b", "c"]
    assert len(dlq.records) == 1
    assert dlq.records[0].record == {"id": 2}
    assert dlq.records[0].processed_record == {"id": 2}
    assert dlq.records[0].stage == "sink_write"


@pytest.mark.asyncio
async def test_fail_closed_sink_error_does_not_advance_checkpoint_without_dlq() -> None:
    store = InMemoryCheckpointStore()

    class _CheckpointedIterableSource(BaseSource[int]):
        source_name = "checkpointed_iterable"
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

    class _BoomOnSecondSink:
        sink_name = "boom_second"

        def __init__(self) -> None:
            self._writes = 0

        async def open(self) -> None:
            return None

        async def write(self, record: int) -> None:
            self._writes += 1
            if self._writes == 2:
                raise RuntimeError("second write broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    with pytest.raises(RuntimeError, match="second write broke"):
        await (
            Pipeline(_CheckpointedIterableSource([1, 2, 3]))
            .build(_BoomOnSecondSink(), config=DeliveryConfig(checkpoint=store))  # type: ignore[arg-type]
            .run()
        )

    resumed_sink = _CollectSink()
    summary = await (
        Pipeline(_CheckpointedIterableSource([1, 2, 3]))
        .build(resumed_sink, config=DeliveryConfig(checkpoint=store))  # type: ignore[arg-type]
        .run()
    )

    assert resumed_sink.records == [2, 3]
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"index": 2}


@pytest.mark.asyncio
async def test_pipeline_routes_batched_sink_write_errors_to_dlq_per_record() -> None:
    dlq = _CollectDLQSink()

    class _BoomBatchSink:
        sink_name = "boom_batch_sink"

        async def open(self) -> None:
            return None

        async def write(self, record: dict) -> None:
            raise AssertionError("single-record path should not be used")

        async def write_batch(self, records: list[dict]) -> None:
            raise RuntimeError("batch sink broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    summary = await (
        Pipeline(IterableSource([{"id": 1}, {"id": 2}, {"id": 3}]))
        .build(_BoomBatchSink(), config=DeliveryConfig(dlq=dlq, batch_size=3))  # type: ignore[arg-type]
        .run()
    )

    assert summary.records_written == 0
    assert summary.records_errored == 3
    assert len(dlq.records) == 3
    assert [record.record for record in dlq.records] == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert [record.original_record for record in dlq.records] == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert [record.processed_record for record in dlq.records] == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert all(record.stage == "sink_write" for record in dlq.records)


@pytest.mark.asyncio
async def test_pipeline_routes_sink_write_errors_with_both_original_and_processed_records() -> None:
    dlq = _CollectDLQSink()

    class _AppendHistoryMiddleware(Middleware[dict, dict]):
        name = "append_history"

        async def process(self, record: dict, ctx):
            del ctx
            return {
                **record,
                "history": [*record.get("history", []), "normalized"],
            }

    class _BoomSink:
        sink_name = "boom_sink"

        async def open(self) -> None:
            return None

        async def write(self, record: dict) -> None:
            del record
            raise RuntimeError("sink broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    summary = await (
        Pipeline(IterableSource([{"id": 1}]))
        .pipe(_AppendHistoryMiddleware())
        .build(_BoomSink(), config=DeliveryConfig(dlq=dlq))  # type: ignore[arg-type]
        .run()
    )

    assert summary.records_errored == 1
    assert len(dlq.records) == 1
    dlq_record = dlq.records[0]
    assert dlq_record.original_record == {"id": 1}
    assert dlq_record.processed_record == {"id": 1, "history": ["normalized"]}
    assert dlq_record.replay_payload() == {"id": 1}


@pytest.mark.asyncio
async def test_pipeline_routes_source_failures_to_dlq_and_reraises() -> None:
    store = InMemoryCheckpointStore()
    dlq = _CollectDLQSink()

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

    sink = _CollectSink()
    pipeline = (
        Pipeline(_FailingSource()).build(sink, config=DeliveryConfig(checkpoint=store, dlq=dlq))  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="source broke"):
        await pipeline.run()

    assert sink.records == [10]
    assert len(dlq.records) == 1
    dlq_record = dlq.records[0]
    assert dlq_record.stage == "source_stream"
    assert dlq_record.source == "failing_source"
    assert dlq_record.checkpoint == {"index": 0}
    assert dlq_record.record is None
    assert dlq_record.original_record is None
    assert dlq_record.processed_record is None


@pytest.mark.asyncio
async def test_pipeline_routes_source_record_failures_to_dlq_with_raw_record() -> None:
    dlq = _CollectDLQSink()

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

    sink = _CollectSink()
    pipeline = (
        Pipeline(_FailingRecordSource()).build(sink, config=DeliveryConfig(dlq=dlq))  # type: ignore[arg-type]
    )

    with pytest.raises(ValueError, match="bad row"):
        await pipeline.run()

    assert sink.records == [10]
    assert len(dlq.records) == 1
    dlq_record = dlq.records[0]
    assert dlq_record.stage == "source_record"
    assert dlq_record.source == "failing_record_source"
    assert dlq_record.checkpoint == {"index": 1}
    assert dlq_record.record == {"id": 2, "raw": "broken"}
    assert dlq_record.original_record == {"id": 2, "raw": "broken"}
    assert dlq_record.processed_record is None


@pytest.mark.asyncio
async def test_sqlite_dlq_acknowledge_uses_storage_identity_for_duplicate_metadata(
    tmp_path,
) -> None:
    path = tmp_path / "dlq.db"
    created_at = datetime(2026, 5, 22, 12, 0, tzinfo=UTC)
    sink = SQLiteDLQSink(path)
    source = SQLiteDLQSource(path, pipeline_id="orders")

    first = DLQRecord(
        pipeline_id="orders",
        run_id="run-1",
        stage="sink_write",
        error_type="RuntimeError",
        error_message="boom",
        record={"id": 1},
        created_at=created_at,
    )
    second = DLQRecord(
        pipeline_id="orders",
        run_id="run-1",
        stage="sink_write",
        error_type="RuntimeError",
        error_message="boom",
        record={"id": 2},
        created_at=created_at,
    )

    await sink.open()
    try:
        await sink.write(first)
        await sink.write(second)

        await source.open()
        try:
            records = [record async for record in source.stream()]
        finally:
            await source.close()

        assert [record.record for record in records] == [{"id": 1}, {"id": 2}]

        replayed_second = await sink.replay(records[1])
        await sink.acknowledge(replayed_second)

        await source.open()
        try:
            remaining = [record async for record in source.stream()]
        finally:
            await source.close()

        assert [record.record for record in remaining] == [{"id": 1}]
        assert remaining[0].attempt == 0
    finally:
        await sink.close()


@pytest.mark.asyncio
async def test_sqlite_dlq_open_upgrades_legacy_schema_with_retry_columns(tmp_path) -> None:
    path = tmp_path / "legacy-dlq.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE dlq_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                stage TEXT NOT NULL,
                error_type TEXT NOT NULL,
                error_message TEXT NOT NULL,
                record TEXT,
                source TEXT,
                checkpoint TEXT,
                middleware TEXT,
                sink TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

    sink = SQLiteDLQSink(path)
    source = SQLiteDLQSource(path, pipeline_id="orders")
    record = DLQRecord(
        pipeline_id="orders",
        run_id="run-1",
        stage="sink_write",
        error_type="RuntimeError",
        error_message="boom",
        record={"id": 1},
    )

    await sink.open()
    try:
        await sink.write(record)

        await source.open()
        try:
            records = [item async for item in source.stream()]
        finally:
            await source.close()

        assert [item.record for item in records] == [{"id": 1}]
        assert records[0].attempt == 0
        assert records[0].max_attempts is None

        check_conn = sqlite3.connect(path)
        try:
            columns = {
                row[1] for row in check_conn.execute("PRAGMA table_info(dlq_records)").fetchall()
            }
        finally:
            check_conn.close()

        assert {
            "original_record",
            "processed_record",
            "details",
            "attempt",
            "max_attempts",
        } <= columns
    finally:
        await sink.close()


@pytest.mark.asyncio
async def test_sqlite_dlq_handles_to_thread_calls_across_different_threads(
    tmp_path, monkeypatch
) -> None:
    path = tmp_path / "dlq-threaded.db"

    async def _fresh_thread_to_thread(func, /, *args, **kwargs):
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        def _runner() -> None:
            try:
                result = func(*args, **kwargs)
            except Exception as exc:
                loop.call_soon_threadsafe(future.set_exception, exc)
            else:
                loop.call_soon_threadsafe(future.set_result, result)

        thread = threading.Thread(target=_runner)
        thread.start()
        try:
            return await future
        finally:
            thread.join()

    monkeypatch.setattr(asyncio, "to_thread", _fresh_thread_to_thread)

    sink = SQLiteDLQSink(path)
    source = SQLiteDLQSource(path, pipeline_id="orders")
    record = DLQRecord(
        pipeline_id="orders",
        run_id="run-1",
        stage="sink_write",
        error_type="RuntimeError",
        error_message="boom",
        record={"id": 1},
    )

    await sink.open()
    try:
        await sink.write(record)

        await source.open()
        try:
            records = [item async for item in source.stream()]
        finally:
            await source.close()

        assert [item.record for item in records] == [{"id": 1}]
        replayed = await sink.replay(records[0])
        assert replayed.attempt == 1
        await sink.acknowledge(replayed)
    finally:
        await sink.close()


@pytest.mark.asyncio
async def test_pipeline_logs_and_continues_when_dlq_write_fails_by_default() -> None:
    class _FailingDLQSink:
        sink_name = "failing_dlq"

        async def open(self) -> None:
            return None

        async def write(self, record: DLQRecord) -> None:
            raise RuntimeError("dlq broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    summary = await (
        Pipeline(IterableSource([{"id": 1}]))
        .pipe(_BoomMiddleware())
        .build(_CollectSink(), config=DeliveryConfig(dlq=_FailingDLQSink()))  # type: ignore[arg-type]
        .run()
    )

    assert summary.records_dropped == 0
    assert summary.records_errored == 1
    assert summary.runtime.dlq_failure_count == 1


@pytest.mark.asyncio
async def test_pipeline_can_raise_when_dlq_write_fails() -> None:
    class _FailingDLQSink:
        sink_name = "failing_dlq"

        async def open(self) -> None:
            return None

        async def write(self, record: DLQRecord) -> None:
            raise RuntimeError("dlq broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    with pytest.raises(RuntimeError, match="dlq broke"):
        await (
            Pipeline(IterableSource([{"id": 1}]))
            .pipe(_BoomMiddleware())
            .build(
                _CollectSink(),  # type: ignore[arg-type]
                config=DeliveryConfig(
                    dlq=_FailingDLQSink(),
                    dlq_failure_policy=DLQFailurePolicy.RAISE,
                ),
            )
            .run()
        )


@pytest.mark.asyncio
async def test_fail_closed_sink_error_with_raising_dlq_does_not_advance_checkpoint() -> None:
    store = InMemoryCheckpointStore()

    class _CheckpointedIterableSource(BaseSource[int]):
        source_name = "checkpointed_iterable"
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

    class _BoomOnSecondSink:
        sink_name = "boom_second"

        def __init__(self) -> None:
            self._writes = 0

        async def open(self) -> None:
            return None

        async def write(self, record: int) -> None:
            self._writes += 1
            if self._writes == 2:
                raise RuntimeError("second write broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    class _FailingDLQSink:
        sink_name = "failing_dlq"

        async def open(self) -> None:
            return None

        async def write(self, record: DLQRecord) -> None:
            del record
            raise RuntimeError("dlq broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    with pytest.raises(RuntimeError, match="dlq broke"):
        await (
            Pipeline(_CheckpointedIterableSource([1, 2, 3]))
            .build(
                _BoomOnSecondSink(),  # type: ignore[arg-type]
                config=DeliveryConfig(
                    checkpoint=store,
                    dlq=_FailingDLQSink(),
                    dlq_failure_policy=DLQFailurePolicy.RAISE,
                ),
            )
            .run()
        )

    resumed_sink = _CollectSink()
    summary = await (
        Pipeline(_CheckpointedIterableSource([1, 2, 3]))
        .build(resumed_sink, config=DeliveryConfig(checkpoint=store))  # type: ignore[arg-type]
        .run()
    )

    assert resumed_sink.records == [2, 3]
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"index": 2}
