"""
tests/sources/test_file.py
===========================
Tests for FileSource implementations (CsvSource, JsonLinesSource, ParquetSource).
No network access required.
"""

from __future__ import annotations

import csv
import inspect
import json
from pathlib import Path

import pytest

from agora import (
    InMemoryCheckpointStore,
    Pipeline,
    SourceRecordError,
    SourceRecordFailurePolicy,
)
from agora.sources.file import CsvSource, JsonLinesSource, ParquetSource

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
            .build(_CollectSink(first_records), checkpoint=store)  # type: ignore[arg-type]
            .run(max_records=2)
        )

        await (
            Pipeline(CsvSource(path=csv_file, row_mapper=lambda row: row, batch_size=1))
            .build(_CollectSink(second_records), checkpoint=store)  # type: ignore[arg-type]
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
