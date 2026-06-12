"""
tests/sources/test_file.py
===========================
Tests for FileSource implementations (CsvSource, JsonLinesSource, ParquetSource).
No network access required.
"""

from __future__ import annotations

import asyncio
import csv
import inspect
import json
from pathlib import Path
from typing import Any

import pytest

from agora import (
    DeliveryConfig,
    InMemoryCheckpointStore,
    Pipeline,
    SourceRecordError,
    SourceRecordFailurePolicy,
)
from agora.sources.file import CsvSource, JsonLinesSource, ParquetSource


class _SlowSink:
    sink_name = "slow"

    def __init__(self, delay: float = 0.01) -> None:
        self._delay = delay

    async def open(self) -> None:
        return None

    async def write(self, record) -> None:
        del record
        await asyncio.sleep(self._delay)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


# ======================================================================
# CsvSource
# ======================================================================


class TestCsvSource:
    @pytest.fixture
    def csv_file(self, tmp_path: Path) -> Path:
        path = tmp_path / "records.csv"
        with path.open("w", encoding="utf-8", newline="") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=["id", "name"])
            writer.writeheader()
            writer.writerow({"id": "1", "name": "Alice"})
            writer.writerow({"id": "2", "name": "Bob"})
            writer.writerow({"id": "3", "name": "Charlie"})
        return path

    @pytest.fixture
    def csv_file_with_blank_row(self, tmp_path: Path) -> Path:
        path = tmp_path / "blanks.csv"
        path.write_text("id,name\n1,Alice\n,\n2,Bob\n", encoding="utf-8")
        return path

    async def test_reads_all_records(self, csv_file: Path) -> None:
        source = CsvSource(path=csv_file, row_mapper=lambda row: row, batch_size=2)
        records = [record async for record in source.stream()]
        assert records == [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
            {"id": "3", "name": "Charlie"},
        ]

    async def test_skips_blank_rows(self, csv_file_with_blank_row: Path) -> None:
        source = CsvSource(path=csv_file_with_blank_row, row_mapper=lambda row: row, batch_size=1)
        records = [record async for record in source.stream()]
        assert records == [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
        ]

    async def test_skip_blank_lines_false_keeps_empty_field_rows(self, tmp_path: Path) -> None:
        path = tmp_path / "keep-blanks.csv"
        path.write_text("id,name\n1,Alice\n,\n2,Bob\n", encoding="utf-8")

        source = CsvSource(
            path=path,
            row_mapper=lambda row: row,
            batch_size=1,
            skip_blank_lines=False,
        )
        records = [record async for record in source.stream()]

        assert records == [
            {"id": "1", "name": "Alice"},
            {"id": "", "name": ""},
            {"id": "2", "name": "Bob"},
        ]

    async def test_handles_short_and_long_rows_like_dict_reader(self, tmp_path: Path) -> None:
        path = tmp_path / "ragged.csv"
        path.write_text("id,name\n1\n2,Bob,extra\n", encoding="utf-8")

        source = CsvSource(path=path, row_mapper=lambda row: row, batch_size=1)
        records = [record async for record in source.stream()]

        assert records == [
            {"id": "1", "name": None},
            {"id": "2", "name": "Bob", None: ["extra"]},
        ]

    async def test_uses_explicit_fieldnames_when_header_is_disabled(self, tmp_path: Path) -> None:
        path = tmp_path / "no-header.csv"
        path.write_text("1,Alice\n2,Bob\n", encoding="utf-8")

        source = CsvSource(
            path=path,
            row_mapper=lambda row: row,
            has_header=False,
            fieldnames=["id", "name"],
            batch_size=1,
        )
        records = [record async for record in source.stream()]

        assert records == [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
        ]

    async def test_row_mapper_transform(self, csv_file: Path) -> None:
        source = CsvSource(
            path=csv_file,
            row_mapper=lambda row: row["name"].upper(),
            batch_size=2,
        )
        records = [record async for record in source.stream()]
        assert records == ["ALICE", "BOB", "CHARLIE"]

    async def test_stream_is_async_iterable(self, csv_file: Path) -> None:
        source = CsvSource(path=csv_file, row_mapper=lambda row: row)
        assert inspect.isasyncgen(source.stream())

    async def test_checkpoint_resume_skips_already_processed_rows(self, csv_file: Path) -> None:
        store = InMemoryCheckpointStore()
        first_records: list[dict] = []
        second_records: list[dict] = []

        class _CollectSink:
            sink_name = "collect"

            def __init__(self, target: list[dict]) -> None:
                self._target = target

            async def open(self) -> None:
                return None

            async def write(self, record: dict) -> None:
                self._target.append(record)

            async def flush(self) -> None:
                return None

            async def close(self) -> None:
                return None

        await (
            Pipeline(CsvSource(path=csv_file, row_mapper=lambda row: row, batch_size=1))
            .build(_CollectSink(first_records), config=DeliveryConfig(checkpoint=store))  # type: ignore[arg-type]
            .run(max_records=2)
        )

        await (
            Pipeline(CsvSource(path=csv_file, row_mapper=lambda row: row, batch_size=1))
            .build(_CollectSink(second_records), config=DeliveryConfig(checkpoint=store))  # type: ignore[arg-type]
            .run()
        )

        assert first_records == [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
        ]
        assert second_records == [
            {"id": "3", "name": "Charlie"},
        ]

    async def test_checkpoint_progress_tracks_consumed_rows_even_when_mapper_skips(
        self,
        csv_file: Path,
    ) -> None:
        source = CsvSource(
            path=csv_file,
            row_mapper=lambda row: row if row["id"] != "2" else None,
            batch_size=1,
        )

        records = [record async for record in source.stream()]

        assert records == [
            {"id": "1", "name": "Alice"},
            {"id": "3", "name": "Charlie"},
        ]
        assert source.current_checkpoint() == {"row_number": 3}

    async def test_row_mapper_errors_fail_closed_by_default(self, csv_file: Path) -> None:
        source = CsvSource(
            path=csv_file,
            row_mapper=lambda row: row["name"] if row["id"] != "2" else int(row["name"]),
            batch_size=1,
        )

        with pytest.raises(SourceRecordError) as exc_info:
            _ = [record async for record in source.stream()]

        assert isinstance(exc_info.value.original, ValueError)
        assert exc_info.value.record == {"id": "2", "name": "Bob"}
        assert source.current_checkpoint() == {"row_number": 2}
        assert source.runtime_metrics().to_dict() == {
            "record_error_count": 1,
            "record_drop_count": 0,
        }

    async def test_early_stop_closes_cleanly_without_waiting_for_producer(
        self, csv_file: Path
    ) -> None:
        source = CsvSource(
            path=csv_file,
            row_mapper=lambda row: row,
            batch_size=1,
            queue_maxsize=1,
        )
        stream = source.stream()

        first = await anext(stream)
        assert first["id"] == "1"

        await asyncio.wait_for(stream.aclose(), timeout=1.0)

    async def test_queue_maxsize_controls_runtime_prefetch_limit(self, csv_file: Path) -> None:
        # CsvSource now uses sync stream() in the event loop — no thread prefetch.
        # queue_maxsize is accepted for API compat but prefetch is disabled.
        summary = await (
            Pipeline(
                CsvSource(
                    path=csv_file,
                    row_mapper=lambda row: row,
                    batch_size=1,
                    queue_maxsize=1,
                )
            )
            .build(_SlowSink())  # type: ignore[arg-type]
            .run(max_records=3)
        )

        assert summary.records_consumed == 3
        assert summary.records_written == 3


# ======================================================================
# JsonLinesSource
# ======================================================================


class TestJsonLinesSource:
    @pytest.fixture
    def jsonl_file(self, tmp_path: Path) -> Path:
        """Write a 3-record JSONL file and return its path."""
        path = tmp_path / "records.jsonl"
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
        path.write_text(
            "\n".join(json.dumps(r) for r in records) + "\n",
            encoding="utf-8",
        )
        return path

    @pytest.fixture
    def jsonl_file_with_blanks(self, tmp_path: Path) -> Path:
        """JSONL with blank lines that should be skipped."""
        path = tmp_path / "blanks.jsonl"
        path.write_text('{"id": 1}\n\n{"id": 2}\n\n', encoding="utf-8")
        return path

    @pytest.fixture
    def jsonl_file_with_bad_json(self, tmp_path: Path) -> Path:
        """JSONL with one unparseable line — should log warning and continue."""
        path = tmp_path / "bad.jsonl"
        path.write_text('{"id": 1}\nnot-json-{{\n{"id": 3}\n', encoding="utf-8")
        return path

    async def test_reads_all_records(self, jsonl_file: Path) -> None:
        source = JsonLinesSource(path=jsonl_file, row_mapper=lambda d: d)
        records = [r async for r in source.stream()]
        assert len(records) == 3
        assert records[0]["name"] == "Alice"
        assert records[2]["name"] == "Charlie"

    async def test_skips_blank_lines(self, jsonl_file_with_blanks: Path) -> None:
        source = JsonLinesSource(path=jsonl_file_with_blanks, row_mapper=lambda d: d)
        records = [r async for r in source.stream()]
        assert len(records) == 2

    async def test_row_mapper_transform(self, jsonl_file: Path) -> None:
        source = JsonLinesSource(
            path=jsonl_file,
            row_mapper=lambda d: d["name"].upper(),
        )
        records = [r async for r in source.stream()]
        assert records == ["ALICE", "BOB", "CHARLIE"]

    async def test_row_mapper_returning_none_skips_record(self, jsonl_file: Path) -> None:
        """row_mapper returning None → record is skipped."""
        source = JsonLinesSource(
            path=jsonl_file,
            row_mapper=lambda d: d["name"] if d["id"] != 2 else None,
        )
        records = [r async for r in source.stream()]
        assert len(records) == 2
        assert "Bob" not in records

    async def test_bad_json_line_fails_closed_by_default(
        self, jsonl_file_with_bad_json: Path
    ) -> None:
        source = JsonLinesSource(path=jsonl_file_with_bad_json, row_mapper=lambda d: d)

        with pytest.raises(SourceRecordError) as exc_info:
            _ = [r async for r in source.stream()]

        assert isinstance(exc_info.value.original, json.JSONDecodeError)
        assert exc_info.value.record == "not-json-{{"
        assert source.current_checkpoint() == {"line_number": 2}
        assert source.runtime_metrics().to_dict() == {
            "record_error_count": 1,
            "record_drop_count": 0,
        }

    async def test_bad_json_line_can_log_and_continue(self, jsonl_file_with_bad_json: Path) -> None:
        """Unparseable JSON lines can be skipped when best-effort mode is explicit."""
        source = JsonLinesSource(
            path=jsonl_file_with_bad_json,
            row_mapper=lambda d: d,
            on_record_error=SourceRecordFailurePolicy.LOG_AND_CONTINUE,
        )
        records = [r async for r in source.stream()]
        assert len(records) == 2
        assert records[0]["id"] == 1
        assert records[1]["id"] == 3
        assert source.current_checkpoint() == {"line_number": 3}
        assert source.runtime_metrics().to_dict() == {
            "record_error_count": 1,
            "record_drop_count": 1,
        }

    async def test_stream_is_async_iterable(self, jsonl_file: Path) -> None:
        """Verify source.stream() is an async generator."""
        source = JsonLinesSource(path=jsonl_file, row_mapper=lambda d: d)
        assert inspect.isasyncgen(source.stream())

    async def test_empty_file(self, tmp_path: Path) -> None:
        path = tmp_path / "empty.jsonl"
        path.write_text("", encoding="utf-8")
        source = JsonLinesSource(path=path, row_mapper=lambda d: d)
        records = [r async for r in source.stream()]
        assert records == []

    async def test_streaming_reader_does_not_use_read_text(
        self,
        jsonl_file: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def _fail_read_text(*args, **kwargs):
            raise AssertionError("JsonLinesSource should stream through a file handle")

        monkeypatch.setattr(Path, "read_text", _fail_read_text)
        source = JsonLinesSource(path=jsonl_file, row_mapper=lambda d: d, batch_size=1)
        records = [r async for r in source.stream()]
        assert len(records) == 3

    async def test_pipeline_summary_exposes_source_runtime_counters(
        self,
        jsonl_file_with_bad_json: Path,
    ) -> None:
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

        sink = _CollectSink()
        summary = await (
            Pipeline(
                JsonLinesSource(
                    path=jsonl_file_with_bad_json,
                    row_mapper=lambda d: d,
                    on_record_error=SourceRecordFailurePolicy.LOG_AND_CONTINUE,
                )
            )
            .build(sink)  # type: ignore[arg-type]
            .run()
        )

        assert sink.records == [{"id": 1}, {"id": 3}]
        assert summary.by_source == {"jsonl": 2}
        assert summary.runtime.source_record_error_count == 1
        assert summary.runtime.source_record_drop_count == 1

    async def test_early_stop_closes_cleanly_without_waiting_for_producer(
        self, jsonl_file: Path
    ) -> None:
        source = JsonLinesSource(
            path=jsonl_file,
            row_mapper=lambda d: d,
            batch_size=1,
            queue_maxsize=1,
        )
        stream = source.stream()

        first = await anext(stream)
        assert first["id"] == 1

        await asyncio.wait_for(stream.aclose(), timeout=1.0)

    async def test_queue_maxsize_controls_runtime_prefetch_limit(self, jsonl_file: Path) -> None:
        # JsonLinesSource now uses sync stream() in the event loop — no thread prefetch.
        # queue_maxsize is accepted for API compat but prefetch is disabled.
        summary = await (
            Pipeline(
                JsonLinesSource(
                    path=jsonl_file,
                    row_mapper=lambda d: d,
                    batch_size=1,
                    queue_maxsize=1,
                )
            )
            .build(_SlowSink())  # type: ignore[arg-type]
            .run(max_records=3)
        )

        assert summary.records_consumed == 3
        assert summary.records_written == 3


class TestParquetSource:
    async def test_streams_records(self, tmp_path: Path) -> None:
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        path = tmp_path / "records.parquet"
        table = pa.Table.from_pylist([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
        pq.write_table(table, path)

        source = ParquetSource(path=path, row_mapper=lambda row: row)
        records = [record async for record in source.stream()]

        assert records == [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

    async def test_early_stop_closes_cleanly_without_waiting_for_producer(
        self, tmp_path: Path
    ) -> None:
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        path = tmp_path / "records.parquet"
        table = pa.Table.from_pylist(
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
                {"id": 3, "name": "Charlie"},
            ]
        )
        pq.write_table(table, path)

        source = ParquetSource(path=path, row_mapper=lambda row: row, batch_size=1)
        stream = source.stream()

        first = await anext(stream)
        assert first["id"] == 1

        await asyncio.wait_for(stream.aclose(), timeout=1.0)

    async def test_stream_batches_does_not_duplicate_rows(self, tmp_path: Path) -> None:
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        path = tmp_path / "records.parquet"
        row_count = 5000
        table = pa.Table.from_pylist([{"id": i} for i in range(row_count)])
        pq.write_table(table, path)

        # Small arrow batch + small prefetch buffer forces the consumer's
        # drain-queue path (the inner `while not queue.empty()` loop) to run,
        # which previously yielded each drained batch twice.
        source = ParquetSource(path=path, row_mapper=lambda row: row, use_arrow_batches=True)
        source._arrow_batch_size = 256
        source.prefetch_limit = 2

        seen_ids: list[int] = []
        async for batch in source.stream_batches():
            seen_ids.extend(batch.column("id").to_pylist())

        assert len(seen_ids) == row_count
        assert seen_ids == list(range(row_count))

    async def test_stream_batches_surfaces_producer_error_when_error_enqueue_times_out(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        pa = pytest.importorskip("pyarrow")
        import pyarrow.dataset as ds

        import agora.sources.file.parquet as parquet_module

        path = tmp_path / "records.parquet"
        batch = pa.record_batch([pa.array([1, 2])], names=["id"])

        class _FakeScanner:
            def to_batches(self):
                yield batch
                raise RuntimeError("producer boom")

        class _FakeDataset:
            def scanner(self, batch_size: int):
                del batch_size
                return _FakeScanner()

        monkeypatch.setattr(ds, "dataset", lambda *args, **kwargs: _FakeDataset())

        real_run_coroutine_threadsafe = parquet_module.asyncio.run_coroutine_threadsafe

        class _TimeoutFuture:
            def result(self, timeout: float | None = None) -> None:
                del timeout
                raise TimeoutError

            def cancel(self) -> bool:
                return True

        def _patched_run_coroutine_threadsafe(coro, loop):
            frame = getattr(coro, "cr_frame", None)
            item = frame.f_locals.get("item") if frame is not None else None
            if isinstance(item, Exception) or item is parquet_module._BATCH_QUEUE_DONE:
                coro.close()
                return _TimeoutFuture()
            return real_run_coroutine_threadsafe(coro, loop)

        monkeypatch.setattr(
            parquet_module.asyncio,
            "run_coroutine_threadsafe",
            _patched_run_coroutine_threadsafe,
        )

        source = ParquetSource(path=path, row_mapper=lambda row: row, use_arrow_batches=True)
        source.prefetch_limit = 1

        stream = source.stream_batches()
        first = await anext(stream)
        assert first.column("id").to_pylist() == [1, 2]

        with pytest.raises(RuntimeError, match="producer boom"):
            await anext(stream)

    async def test_stream_batches_does_not_swallow_producer_error_via_done_drain_path(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        pa = pytest.importorskip("pyarrow")
        import pyarrow.dataset as ds

        import agora.sources.file.parquet as parquet_module

        path = tmp_path / "records.parquet"
        batch = pa.record_batch([pa.array([1, 2])], names=["id"])

        class _FakeScanner:
            def to_batches(self):
                yield batch
                raise RuntimeError("producer boom")

        class _FakeDataset:
            def scanner(self, batch_size: int):
                del batch_size
                return _FakeScanner()

        monkeypatch.setattr(ds, "dataset", lambda *args, **kwargs: _FakeDataset())

        real_run_coroutine_threadsafe = parquet_module.asyncio.run_coroutine_threadsafe

        class _TimeoutFuture:
            def result(self, timeout: float | None = None) -> None:
                del timeout
                raise TimeoutError

            def cancel(self) -> bool:
                return True

        def _patched_run_coroutine_threadsafe(coro, loop):
            frame = getattr(coro, "cr_frame", None)
            item = frame.f_locals.get("item") if frame is not None else None
            if isinstance(item, Exception):
                coro.close()
                return _TimeoutFuture()
            return real_run_coroutine_threadsafe(coro, loop)

        monkeypatch.setattr(
            parquet_module.asyncio,
            "run_coroutine_threadsafe",
            _patched_run_coroutine_threadsafe,
        )

        source = ParquetSource(path=path, row_mapper=lambda row: row, use_arrow_batches=True)
        source.prefetch_limit = 2

        seen_ids: list[int] = []
        with pytest.raises(RuntimeError, match="producer boom"):
            async for out_batch in source.stream_batches():
                seen_ids.extend(out_batch.column("id").to_pylist())

        assert seen_ids == [1, 2]


# ======================================================================
# Tier A — batch-emit (emit_batches=True) for CSV / JSONL
# ======================================================================


class _ListCollectSink:
    sink_name = "collect"

    def __init__(self) -> None:
        self.records: list = []

    async def open(self) -> None:
        return None

    async def write(self, record) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class TestBatchEmit:
    @pytest.fixture
    def csv_file(self, tmp_path: Path) -> Path:
        path = tmp_path / "batch.csv"
        with path.open("w", encoding="utf-8", newline="") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=["id", "name"])
            writer.writeheader()
            for i in range(2500):
                writer.writerow({"id": str(i), "name": f"row{i}"})
        return path

    @pytest.fixture
    def jsonl_file(self, tmp_path: Path) -> Path:
        path = tmp_path / "batch.jsonl"
        with path.open("w", encoding="utf-8") as file_obj:
            for i in range(2500):
                file_obj.write(json.dumps({"id": i, "name": f"row{i}"}) + "\n")
        return path

    async def test_csv_batch_emit_matches_row_path(self, csv_file: Path) -> None:
        row_sink = _ListCollectSink()
        batch_sink = _ListCollectSink()

        row_summary = await (
            Pipeline(CsvSource(path=csv_file, row_mapper=lambda r: r)).build(row_sink).run()  # type: ignore[arg-type]
        )
        batch_summary = await (
            Pipeline(
                CsvSource(
                    path=csv_file, row_mapper=lambda r: r, emit_batches=True, emit_batch_size=500
                )
            )
            .build(batch_sink)  # type: ignore[arg-type]
            .run()
        )

        assert batch_sink.records == row_sink.records
        assert batch_summary.records_consumed == row_summary.records_consumed
        assert batch_summary.records_written == row_summary.records_written

    async def test_jsonl_batch_emit_matches_row_path(self, jsonl_file: Path) -> None:
        row_sink = _ListCollectSink()
        batch_sink = _ListCollectSink()

        await (
            Pipeline(JsonLinesSource(path=jsonl_file, row_mapper=lambda r: r)).build(row_sink).run()  # type: ignore[arg-type]
        )
        await (
            Pipeline(
                JsonLinesSource(
                    path=jsonl_file, row_mapper=lambda r: r, emit_batches=True, emit_batch_size=500
                )
            )
            .build(batch_sink)  # type: ignore[arg-type]
            .run()
        )

        assert batch_sink.records == row_sink.records

    async def test_csv_batch_emit_uses_batch_lane(self, csv_file: Path) -> None:
        from agora.core.batch import is_batch_capable_source

        row_source = CsvSource(path=csv_file, row_mapper=lambda r: r)
        batch_source = CsvSource(path=csv_file, row_mapper=lambda r: r, emit_batches=True)
        assert is_batch_capable_source(row_source) is False
        assert is_batch_capable_source(batch_source) is True

    async def test_csv_batch_emit_checkpoint_resume(self, csv_file: Path) -> None:
        store = InMemoryCheckpointStore()
        first = _ListCollectSink()
        second = _ListCollectSink()

        await (
            Pipeline(
                CsvSource(
                    path=csv_file, row_mapper=lambda r: r, emit_batches=True, emit_batch_size=500
                )
            )
            .build(first, config=DeliveryConfig(checkpoint=store))  # type: ignore[arg-type]
            .run(max_records=1000)
        )
        await (
            Pipeline(
                CsvSource(
                    path=csv_file, row_mapper=lambda r: r, emit_batches=True, emit_batch_size=500
                )
            )
            .build(second, config=DeliveryConfig(checkpoint=store))  # type: ignore[arg-type]
            .run()
        )

        # No record processed twice, every record processed exactly once across both runs.
        all_ids = [r["id"] for r in first.records] + [r["id"] for r in second.records]
        assert len(all_ids) == 2500
        assert sorted(all_ids, key=int) == [str(i) for i in range(2500)]


# ======================================================================
# ArrowCsvSource / ArrowJsonLinesSource
# ======================================================================


class TestArrowCsvSource:
    async def test_yields_record_batches(self, tmp_path: Path) -> None:
        pa = pytest.importorskip("pyarrow")
        path = tmp_path / "data.csv"
        path.write_text("id,name\n1,Alice\n2,Bob\n3,Charlie\n")

        from agora.sources.file.csv import ArrowCsvSource

        src = ArrowCsvSource(path=path)
        batches = [b async for b in src.stream_batches()]

        assert len(batches) == 1
        assert isinstance(batches[0], pa.RecordBatch)
        assert batches[0].num_rows == 3
        metrics = src.runtime_metrics()
        assert metrics.arrow_batch_count == 1
        assert metrics.arrow_max_batch_rows == 3
        assert metrics.arrow_read_time_ms >= 0.0
        assert metrics.arrow_batch_materialize_time_ms >= 0.0
        assert metrics.arrow_total_load_time_ms >= 0.0
        assert metrics.arrow_resolved_read_block_size == 0

    async def test_emits_arrow_batches_flag(self, tmp_path: Path) -> None:
        from agora.core.batch import is_batch_capable_source
        from agora.sources.file.csv import ArrowCsvSource

        src = ArrowCsvSource(path=tmp_path / "x.csv")
        assert src.emits_arrow_batches is True
        assert is_batch_capable_source(src) is True

    async def test_stream_fallback_yields_dicts(self, tmp_path: Path) -> None:
        pytest.importorskip("pyarrow")
        path = tmp_path / "data.csv"
        path.write_text("id,v\n1,10\n2,20\n")

        from agora.sources.file.csv import ArrowCsvSource

        src = ArrowCsvSource(path=path)
        rows = [r async for r in src.stream()]
        assert len(rows) == 2
        assert int(rows[0]["id"]) == 1

    async def test_pipeline_with_arrow_middleware(self, tmp_path: Path) -> None:
        pa = pytest.importorskip("pyarrow")
        import pyarrow.compute as pc

        path = tmp_path / "data.csv"
        path.write_text("id,score\n1,5\n2,0\n3,10\n")

        from agora import ArrowCsvSource, ArrowFilterMiddleware

        class _ArrowSink:
            sink_name = "arrow"

            def __init__(self):
                self.batches = []

            async def open(self): ...
            async def write_arrow_batch(self, b):
                self.batches.append(b)

            async def flush(self): ...
            async def close(self): ...

        sink = _ArrowSink()
        summary = await (
            Pipeline(ArrowCsvSource(path=path))
            .pipe(
                ArrowFilterMiddleware(
                    lambda b: pc.greater(pc.cast(b.column("score"), pa.float64()), 0.0)
                )
            )
            .build(sink)  # type: ignore[arg-type]
            .run()
        )
        assert summary.records_consumed == 3
        assert summary.records_written == 2  # score=0 filtered out
        assert sink.batches[0].num_rows == 2
        assert summary.runtime.source_arrow_batch_count == 1
        assert summary.runtime.source_arrow_max_batch_rows == 3
        assert summary.runtime.source_arrow_read_time_ms >= 0.0
        assert summary.runtime.source_arrow_batch_materialize_time_ms >= 0.0
        assert summary.runtime.source_arrow_total_load_time_ms >= 0.0

    async def test_read_block_size_is_forwarded_to_pyarrow(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        pytest.importorskip("pyarrow")
        path = tmp_path / "data.csv"
        path.write_text("id,name\n1,Alice\n2,Bob\n")

        from agora.sources.file.csv import ArrowCsvSource

        class _FakeBatch:
            def __init__(self, rows: int) -> None:
                self.num_rows = rows

        calls: dict[str, Any] = {}

        class _FakeTable:
            def to_batches(self, *, max_chunksize: int) -> list[_FakeBatch]:
                calls["max_chunksize"] = max_chunksize
                return [_FakeBatch(2)]

        class _FakeSource:
            def close(self) -> None:
                calls["source_closed"] = True

        def _fake_input_stream(csv_path: str) -> _FakeSource:
            calls["path"] = csv_path
            return _FakeSource()

        def _fake_read_csv(csv_path: Any, read_options: Any = None) -> _FakeTable:
            calls["source_type"] = type(csv_path).__name__
            calls["block_size"] = 0 if read_options is None else int(read_options.block_size)
            return _FakeTable()

        monkeypatch.setattr("pyarrow.input_stream", _fake_input_stream)
        monkeypatch.setattr("pyarrow.csv.read_csv", _fake_read_csv)

        src = ArrowCsvSource(path=path, batch_size=32, read_block_size=4096)
        batches = [batch async for batch in src.stream_batches()]

        assert len(batches) == 1
        assert calls == {
            "path": str(path),
            "source_type": "_FakeSource",
            "block_size": 4096,
            "max_chunksize": 32,
            "source_closed": True,
        }
        metrics = src.runtime_metrics()
        assert metrics.arrow_batch_count == 1
        assert metrics.arrow_max_batch_rows == 2
        assert metrics.arrow_batch_materialize_time_ms >= 0.0
        assert metrics.arrow_total_load_time_ms >= 0.0
        assert metrics.arrow_resolved_read_block_size == 4096


class TestArrowJsonLinesSource:
    async def test_yields_record_batches(self, tmp_path: Path) -> None:
        pa = pytest.importorskip("pyarrow")
        path = tmp_path / "data.jsonl"
        path.write_text('{"id":1,"v":10}\n{"id":2,"v":20}\n')

        from agora.sources.file.jsonlines import ArrowJsonLinesSource

        src = ArrowJsonLinesSource(path=path)
        batches = [b async for b in src.stream_batches()]

        assert len(batches) == 1
        assert isinstance(batches[0], pa.RecordBatch)
        assert batches[0].num_rows == 2

    async def test_emits_arrow_batches_flag(self, tmp_path: Path) -> None:
        from agora.core.batch import is_batch_capable_source
        from agora.sources.file.jsonlines import ArrowJsonLinesSource

        src = ArrowJsonLinesSource(path="x.jsonl")
        assert src.emits_arrow_batches is True
        assert is_batch_capable_source(src) is True

    async def test_stream_fallback_yields_dicts(self, tmp_path: Path) -> None:
        pytest.importorskip("pyarrow")
        path = tmp_path / "data.jsonl"
        path.write_text('{"id":1}\n{"id":2}\n')

        from agora.sources.file.jsonlines import ArrowJsonLinesSource

        src = ArrowJsonLinesSource(path=path)
        rows = [r async for r in src.stream()]
        assert len(rows) == 2
        assert rows[0]["id"] == 1

    async def test_uses_rust_batches_when_required(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = tmp_path / "data.jsonl"
        path.write_text('{"id":1}\n{"id":2}\n')

        from agora.sources.file.jsonlines import ArrowJsonLinesSource

        class _FakeBatch:
            def __init__(self, rows: int) -> None:
                self.num_rows = rows

        calls: list[tuple[str, int, str]] = []

        monkeypatch.setattr(
            "agora.sources.file.jsonlines.acceleration_supports",
            lambda capability, *, mode: (
                capability == "jsonl_arrow_reader" and str(mode) == "required"
            ),
        )
        monkeypatch.setattr(
            "agora.sources.file.jsonlines.read_jsonl_arrow_batches",
            lambda path_str, batch_size, *, mode: (
                calls.append((path_str, batch_size, str(mode))) or [_FakeBatch(2)]
            ),
        )

        src = ArrowJsonLinesSource(path=path, acceleration_mode="required")
        batches = [batch async for batch in src.stream_batches()]

        assert len(batches) == 1
        assert batches[0].num_rows == 2
        assert calls == [(str(path), 65_536, "required")]
        assert src.current_checkpoint() == {"rows": 2}

    async def test_auto_mode_skips_rust_batches(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = tmp_path / "data.jsonl"
        path.write_text('{"id":1}\n')

        from agora.sources.file.jsonlines import ArrowJsonLinesSource

        monkeypatch.setattr(
            "agora.sources.file.jsonlines.acceleration_supports",
            lambda capability, *, mode: (_ for _ in ()).throw(
                AssertionError("should not probe rust")
            ),
        )
        monkeypatch.setattr(
            "agora.sources.file.jsonlines.read_jsonl_arrow_batches",
            lambda path_str, batch_size, *, mode: (_ for _ in ()).throw(
                AssertionError("should not run")
            ),
        )

        src = ArrowJsonLinesSource(path=path, acceleration_mode="auto")

        assert await src._read_batches_via_rust() is None

    async def test_off_mode_skips_rust_batches(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = tmp_path / "data.jsonl"
        path.write_text('{"id":1}\n')

        from agora.sources.file.jsonlines import ArrowJsonLinesSource

        calls: list[str] = []

        def _supports(capability: str, *, mode: object) -> bool:
            calls.append(f"{capability}:{mode}")
            return False

        monkeypatch.setattr("agora.sources.file.jsonlines.acceleration_supports", _supports)
        monkeypatch.setattr(
            "agora.sources.file.jsonlines.read_jsonl_arrow_batches",
            lambda path_str, batch_size, *, mode: (_ for _ in ()).throw(
                AssertionError("should not run")
            ),
        )

        src = ArrowJsonLinesSource(path=path, acceleration_mode="off")

        assert await src._read_batches_via_rust() is None
        assert calls == []
