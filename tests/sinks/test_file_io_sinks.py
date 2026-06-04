from __future__ import annotations

import csv
import json
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from agora.core.context import PipelineContext
from agora.core.metrics import PipelineMetrics
from agora.sinks.file.csv import CsvSink
from agora.sinks.file.jsonlines import JsonLinesSink
from agora.sinks.file.parquet import ParquetSink
from agora.sinks.io.log import LogSink

if TYPE_CHECKING:
    from pathlib import Path

# ============================================================================
# CsvSink Tests
# ============================================================================


async def test_csv_write_creates_file_with_header(tmp_path: Path) -> None:
    """Writing records creates CSV with header."""
    output = tmp_path / "output.csv"
    sink = CsvSink(
        path=output,
        row_mapper=lambda r: {"id": r["id"], "name": r["name"]},
    )

    await sink.write({"id": 1, "name": "Alice"})
    await sink.write({"id": 2, "name": "Bob"})
    await sink.flush()

    content = output.read_text()
    lines = content.strip().split("\n")
    assert len(lines) == 3  # header + 2 rows
    assert lines[0] == "id,name"
    assert "1,Alice" in lines[1]
    assert "2,Bob" in lines[2]


async def test_csv_append_mode_no_duplicate_header(tmp_path: Path) -> None:
    """Append mode doesn't write duplicate header."""
    output = tmp_path / "output.csv"
    output.write_text("id,name\n1,Alice\n")

    sink = CsvSink(
        path=output,
        row_mapper=lambda r: {"id": r["id"], "name": r["name"]},
        append=True,
    )

    await sink.write({"id": 2, "name": "Bob"})
    await sink.flush()

    content = output.read_text()
    lines = content.strip().split("\n")
    assert len(lines) == 3
    assert lines[0] == "id,name"
    assert lines.count("id,name") == 1  # header appears only once


async def test_csv_custom_fieldnames(tmp_path: Path) -> None:
    """Explicit fieldnames controls column order."""
    output = tmp_path / "output.csv"
    sink = CsvSink(
        path=output,
        row_mapper=lambda r: {"id": r["id"], "name": r["name"], "age": r["age"]},
        fieldnames=["name", "id"],  # age excluded
    )

    await sink.write({"id": 1, "name": "Alice", "age": 30})
    await sink.flush()

    content = output.read_text()
    lines = content.strip().split("\n")
    assert lines[0] == "name,id"
    assert "Alice,1" in lines[1]


async def test_csv_custom_delimiter(tmp_path: Path) -> None:
    """Custom delimiter produces TSV."""
    output = tmp_path / "output.tsv"
    sink = CsvSink(
        path=output,
        row_mapper=lambda r: {"id": r["id"], "name": r["name"]},
        delimiter="\t",
    )

    await sink.write({"id": 1, "name": "Alice"})
    await sink.flush()

    content = output.read_text()
    lines = content.strip().split("\n")
    assert lines[0] == "id\tname"
    assert "1\tAlice" in lines[1]


async def test_csv_auto_flush_on_threshold(tmp_path: Path) -> None:
    """Writing flush_every records triggers auto-flush."""
    output = tmp_path / "output.csv"
    sink = CsvSink(
        path=output,
        row_mapper=lambda r: {"id": r["id"]},
        flush_every=2,
    )

    await sink.write({"id": 1})
    assert not output.exists()

    await sink.write({"id": 2})
    # Auto-flush should have happened
    assert output.exists()


async def test_csv_flush_empty_buffer_noop(tmp_path: Path) -> None:
    """Flush with empty buffer does nothing."""
    output = tmp_path / "output.csv"
    sink = CsvSink(
        path=output,
        row_mapper=lambda r: {"id": r["id"]},
    )

    await sink.flush()

    assert not output.exists()


async def test_csv_close_flushes_remaining(tmp_path: Path) -> None:
    """close() flushes pending buffer."""
    output = tmp_path / "output.csv"
    sink = CsvSink(
        path=output,
        row_mapper=lambda r: {"id": r["id"]},
        flush_every=100,
    )

    await sink.write({"id": 1})
    await sink.close()

    assert output.exists()
    content = output.read_text()
    assert "1" in content


async def test_csv_second_flush_appends(tmp_path: Path) -> None:
    """Two flushes produce all rows without duplicate header."""
    output = tmp_path / "output.csv"
    sink = CsvSink(
        path=output,
        row_mapper=lambda r: {"id": r["id"]},
    )

    await sink.write({"id": 1})
    await sink.write({"id": 2})
    await sink.flush()

    await sink.write({"id": 3})
    await sink.write({"id": 4})
    await sink.flush()

    content = output.read_text()
    lines = content.strip().split("\n")
    assert len(lines) == 5  # header + 4 rows
    assert lines.count("id") == 1  # header once


async def test_csv_reuses_open_file_across_flushes(tmp_path: Path) -> None:
    """CsvSink should not reopen the file for each flush."""
    output = tmp_path / "output.csv"
    sink = CsvSink(
        path=output,
        row_mapper=lambda r: {"id": r["id"]},
    )

    with patch("agora.sinks.file.csv.open", wraps=open) as open_mock:
        await sink.write({"id": 1})
        await sink.flush()
        await sink.write({"id": 2})
        await sink.flush()
        await sink.close()

    assert open_mock.call_count == 1


async def test_csv_arrow_batch_falls_back_for_nested_columns(tmp_path: Path) -> None:
    """Arrow CSV fallback stringifies nested values when pyarrow CSV cannot."""
    pa = pytest.importorskip("pyarrow")

    output = tmp_path / "nested.csv"
    sink = CsvSink(path=output, row_mapper=lambda r: r)
    batch = pa.RecordBatch.from_pylist(
        [
            {"id": 1, "names": {"primary": "Alpha", "alternate": ["A"]}},
            {"id": 2, "names": {"primary": "Beta", "alternate": ["B1", "B2"]}},
        ]
    )

    await sink.write_arrow_batch(batch)
    await sink.close()

    content = output.read_text()
    lines = content.strip().split("\n")
    assert lines[0] == "id,names"
    assert "Alpha" in lines[1]
    assert "alternate" in lines[1]
    assert "Beta" in lines[2]


async def test_csv_arrow_batch_records_runtime_downgrade_metrics(tmp_path: Path) -> None:
    """CSV sink exposes when Arrow lane stayed active but sink downgraded internally."""
    pa = pytest.importorskip("pyarrow")

    output = tmp_path / "runtime-downgrade.csv"
    sink = CsvSink(path=output, row_mapper=lambda r: r)
    ctx = PipelineContext(pipeline_id="csv-runtime", metrics=PipelineMetrics())
    ctx.metrics.runtime.arrow_chain_active = True
    ctx.metrics.runtime.arrow_fast_path_active = True
    sink.bind_context(ctx)
    batch = pa.RecordBatch.from_pylist([{"id": 1, "names": {"primary": "Alpha"}}])

    await sink.write_arrow_batch(batch)

    runtime = ctx.metrics.runtime
    assert runtime.arrow_chain_active is True
    assert runtime.arrow_fast_path_active is True
    assert runtime.csv_arrow_native_batch_count == 0
    assert runtime.csv_arrow_native_row_count == 0
    assert runtime.csv_arrow_downgrade_batch_count == 1
    assert runtime.csv_arrow_downgrade_row_count == 1


async def test_csv_arrow_batch_falls_back_when_pyarrow_csv_rejects_payload(tmp_path: Path) -> None:
    """Arrow CSV fallback preserves output when pyarrow CSV rejects the batch."""
    pa = pytest.importorskip("pyarrow")

    output = tmp_path / "fallback.csv"
    sink = CsvSink(path=output, row_mapper=lambda r: {"id": r["id"], "value": r["value"]})
    batch = pa.RecordBatch.from_pylist([{"id": 1, "value": "ok"}])

    with patch("pyarrow.csv.write_csv", side_effect=pa.ArrowInvalid("Invalid UTF8 payload")):
        await sink.write_arrow_batch(batch)
    await sink.close()

    content = output.read_text()
    lines = content.strip().split("\n")
    assert lines == ["id,value", "1,ok"]


async def test_csv_arrow_batch_records_runtime_native_metrics(tmp_path: Path) -> None:
    """CSV sink counts batches that stayed on the native Arrow CSV path."""
    pa = pytest.importorskip("pyarrow")

    output = tmp_path / "runtime-native.csv"
    sink = CsvSink(path=output, row_mapper=lambda r: r)
    ctx = PipelineContext(pipeline_id="csv-runtime", metrics=PipelineMetrics())
    ctx.metrics.runtime.arrow_chain_active = True
    ctx.metrics.runtime.arrow_fast_path_active = True
    sink.bind_context(ctx)
    batch = pa.RecordBatch.from_pylist([{"id": 1, "value": "ok"}, {"id": 2, "value": "yes"}])

    await sink.write_arrow_batch(batch)

    runtime = ctx.metrics.runtime
    assert runtime.csv_arrow_native_batch_count == 1
    assert runtime.csv_arrow_native_row_count == 2
    assert runtime.csv_arrow_downgrade_batch_count == 0
    assert runtime.csv_arrow_downgrade_row_count == 0


async def test_csv_arrow_append_mode_no_duplicate_header(tmp_path: Path) -> None:
    """Arrow-native append mode should not write a second header."""
    pa = pytest.importorskip("pyarrow")

    output = tmp_path / "append-arrow.csv"
    output.write_text("id,value\n1,old\n")
    sink = CsvSink(path=output, row_mapper=lambda r: r, append=True)
    batch = pa.RecordBatch.from_pylist([{"id": 2, "value": "new"}])

    await sink.write_arrow_batch(batch)
    await sink.close()

    lines = output.read_text().strip().splitlines()
    rows = list(csv.reader(lines))
    assert rows == [["id", "value"], ["1", "old"], ["2", "new"]]
    assert lines.count("id,value") == 1


# ============================================================================
# JsonLinesSink Tests
# ============================================================================


async def test_jsonl_write_creates_valid_jsonl(tmp_path: Path) -> None:
    """Each line is valid JSON."""
    output = tmp_path / "output.jsonl"
    sink = JsonLinesSink(path=output)

    await sink.write({"id": 1, "name": "Alice"})
    await sink.write({"id": 2, "name": "Bob"})
    await sink.flush()

    lines = output.read_text().strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"id": 1, "name": "Alice"}
    assert json.loads(lines[1]) == {"id": 2, "name": "Bob"}


async def test_jsonl_append_mode(tmp_path: Path) -> None:
    """Append mode adds to existing file."""
    output = tmp_path / "output.jsonl"
    output.write_text('{"id": 1}\n')

    sink = JsonLinesSink(path=output, append=True)

    await sink.write({"id": 2})
    await sink.flush()

    lines = output.read_text().strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"id": 1}
    assert json.loads(lines[1]) == {"id": 2}


async def test_jsonl_custom_serializer(tmp_path: Path) -> None:
    """Custom serializer is applied per record."""
    output = tmp_path / "output.jsonl"
    sink = JsonLinesSink(
        path=output,
        serializer=lambda r: {"doubled": r["value"] * 2},
    )

    await sink.write({"value": 5})
    await sink.flush()

    lines = output.read_text().strip().split("\n")
    assert json.loads(lines[0]) == {"doubled": 10}


async def test_jsonl_preserves_default_str_for_unknown_values(tmp_path: Path) -> None:
    """Unknown nested values still serialize via str(...) under orjson."""
    output = tmp_path / "output.jsonl"
    sink = JsonLinesSink(path=output)

    class CustomValue:
        def __str__(self) -> str:
            return "custom-value"

    await sink.write({"id": 1, "payload": CustomValue()})
    await sink.flush()

    lines = output.read_text().strip().split("\n")
    assert json.loads(lines[0]) == {"id": 1, "payload": "custom-value"}


async def test_jsonl_default_serializer_model_dump(tmp_path: Path) -> None:
    """Default serializer uses model_dump() if available."""
    output = tmp_path / "output.jsonl"
    sink = JsonLinesSink(path=output)

    class MockModel:
        def model_dump(self):
            return {"id": 1, "name": "test"}

    await sink.write(MockModel())
    await sink.flush()

    lines = output.read_text().strip().split("\n")
    assert json.loads(lines[0]) == {"id": 1, "name": "test"}


async def test_jsonl_auto_flush_on_threshold(tmp_path: Path) -> None:
    """Writing flush_every records triggers auto-flush."""
    output = tmp_path / "output.jsonl"
    sink = JsonLinesSink(path=output, flush_every=2)

    await sink.write({"id": 1})
    assert not output.exists()

    await sink.write({"id": 2})
    assert output.exists()


async def test_jsonl_close_flushes_remaining(tmp_path: Path) -> None:
    """close() flushes pending buffer."""
    output = tmp_path / "output.jsonl"
    sink = JsonLinesSink(path=output, flush_every=100)

    await sink.write({"id": 1})
    await sink.close()

    assert output.exists()


async def test_jsonl_second_flush_appends_to_file(tmp_path: Path) -> None:
    """Two flushes produce all lines."""
    output = tmp_path / "output.jsonl"
    sink = JsonLinesSink(path=output)

    await sink.write({"id": 1})
    await sink.flush()

    await sink.write({"id": 2})
    await sink.flush()

    lines = output.read_text().strip().split("\n")
    assert len(lines) == 2


# ============================================================================
# ParquetSink Tests
# ============================================================================


async def test_parquet_write_creates_file(tmp_path: Path) -> None:
    """Writing records creates readable Parquet file."""
    output = tmp_path / "output.parquet"
    sink = ParquetSink(
        path=output,
        row_mapper=lambda r: {"id": r["id"], "name": r["name"]},
    )

    await sink.write({"id": 1, "name": "Alice"})
    await sink.write({"id": 2, "name": "Bob"})
    await sink.close()

    assert output.exists()

    # Verify readable
    import pyarrow.parquet as pq

    table = pq.read_table(str(output))
    assert len(table) == 2
    assert table.column_names == ["id", "name"]


async def test_parquet_row_mapper_applied(tmp_path: Path) -> None:
    """row_mapper transforms records before writing."""
    output = tmp_path / "output.parquet"
    sink = ParquetSink(
        path=output,
        row_mapper=lambda r: {"doubled": r["value"] * 2},
    )

    await sink.write({"value": 5})
    await sink.close()

    import pyarrow.parquet as pq

    table = pq.read_table(str(output))
    assert table.column_names == ["doubled"]
    assert table.column("doubled")[0].as_py() == 10


async def test_parquet_auto_flush_on_batch_size(tmp_path: Path) -> None:
    """Writing batch_size records triggers auto-flush."""
    output = tmp_path / "output.parquet"
    sink = ParquetSink(
        path=output,
        row_mapper=lambda r: {"id": r},
        batch_size=2,
    )

    await sink.write(1)
    assert not output.exists()

    await sink.write(2)
    # Auto-flush should have happened
    assert output.exists()


async def test_parquet_multiple_flushes_single_file(tmp_path: Path) -> None:
    """Two flushes write to single file with all rows."""
    output = tmp_path / "output.parquet"
    sink = ParquetSink(
        path=output,
        row_mapper=lambda r: {"id": r},
        batch_size=2,
    )

    await sink.write(1)
    await sink.write(2)
    await sink.flush()

    await sink.write(3)
    await sink.write(4)
    await sink.close()

    import pyarrow.parquet as pq

    table = pq.read_table(str(output))
    assert len(table) == 4


async def test_parquet_missing_fields_in_later_batches_are_written_as_nulls(tmp_path: Path) -> None:
    """Later batches can omit earlier fields without breaking the writer schema."""
    output = tmp_path / "output.parquet"
    sink = ParquetSink(
        path=output,
        row_mapper=lambda r: r,
        batch_size=2,
    )

    await sink.write({"id": 1, "name": "Alice"})
    await sink.write({"id": 2, "name": "Bob"})
    await sink.flush()

    await sink.write({"id": 3})
    await sink.write({"id": 4})
    await sink.close()

    import pyarrow.parquet as pq

    table = pq.read_table(str(output))
    assert len(table) == 4
    assert table.column_names == ["id", "name"]
    assert table.column("name")[2].as_py() is None
    assert table.column("name")[3].as_py() is None


async def test_parquet_close_without_writes(tmp_path: Path) -> None:
    """close() with no writes doesn't create file."""
    output = tmp_path / "output.parquet"
    sink = ParquetSink(
        path=output,
        row_mapper=lambda r: {"id": r},
    )

    await sink.close()

    assert not output.exists()


async def test_parquet_missing_dep() -> None:
    """Missing pyarrow raises ImportError."""
    sink = ParquetSink(
        path="/tmp/test.parquet",
        row_mapper=lambda r: {"id": r},
    )
    sink._buffer = [1]  # type: ignore[attr-defined]

    with (
        patch.dict("sys.modules", {"pyarrow": None, "pyarrow.parquet": None}),
        pytest.raises(ImportError, match="pyarrow"),
    ):
        await sink.flush()


# ============================================================================
# LogSink Tests
# ============================================================================


async def test_log_sink_invalid_level_raises() -> None:
    """Invalid log level raises ValueError."""
    with pytest.raises(ValueError, match="invalid level"):
        LogSink(level="trace")


async def test_log_sink_calls_logger_with_event_name() -> None:
    """write() calls logger with event_name."""
    mock_logger = MagicMock()
    sink = LogSink(
        level="info",
        event_name="test_event",
        extra_fn=lambda r: r,  # Pass through dict as-is
    )
    sink._logger = mock_logger  # type: ignore[attr-defined]
    sink._log_fn = mock_logger.info  # type: ignore[attr-defined]

    await sink.write({"id": 1})

    mock_logger.info.assert_called_once()
    call_args = mock_logger.info.call_args
    assert call_args[0][0] == "test_event"
    assert call_args[1]["id"] == 1


async def test_log_sink_custom_extra_fn() -> None:
    """Custom extra_fn extracts fields."""
    mock_logger = MagicMock()
    sink = LogSink(
        level="info",
        event_name="custom_event",
        extra_fn=lambda r: {"extracted": r["value"] * 2},
    )
    sink._logger = mock_logger  # type: ignore[attr-defined]
    sink._log_fn = mock_logger.info  # type: ignore[attr-defined]

    await sink.write({"value": 5})

    call_args = mock_logger.info.call_args
    assert call_args[1]["extracted"] == 10


async def test_log_sink_default_extra_model_dump() -> None:
    """Default extra_fn uses model_dump()."""
    mock_logger = MagicMock()
    sink = LogSink(level="info")
    sink._logger = mock_logger  # type: ignore[attr-defined]
    sink._log_fn = mock_logger.info  # type: ignore[attr-defined]

    class MockModel:
        def model_dump(self):
            return {"id": 1, "name": "test"}

    await sink.write(MockModel())

    call_args = mock_logger.info.call_args
    assert call_args[1]["id"] == 1
    assert call_args[1]["name"] == "test"


async def test_log_sink_default_extra_dict() -> None:
    """Default extra_fn uses __dict__."""
    mock_logger = MagicMock()
    sink = LogSink(level="info")
    sink._logger = mock_logger  # type: ignore[attr-defined]
    sink._log_fn = mock_logger.info  # type: ignore[attr-defined]

    obj = SimpleNamespace(id=2, name="test")
    await sink.write(obj)

    call_args = mock_logger.info.call_args
    assert call_args[1]["id"] == 2
    assert call_args[1]["name"] == "test"


async def test_log_sink_default_extra_fallback_str() -> None:
    """Default extra_fn falls back to str()."""
    mock_logger = MagicMock()
    sink = LogSink(level="info")
    sink._logger = mock_logger  # type: ignore[attr-defined]
    sink._log_fn = mock_logger.info  # type: ignore[attr-defined]

    await sink.write("plain string")

    call_args = mock_logger.info.call_args
    assert call_args[1]["record"] == "plain string"


async def test_log_sink_all_levels() -> None:
    """All log levels work."""
    for level in ["debug", "info", "warning", "error"]:
        mock_logger = MagicMock()
        sink = LogSink(level=level)
        sink._logger = mock_logger  # type: ignore[attr-defined]
        sink._log_fn = getattr(mock_logger, level)  # type: ignore[attr-defined]

        await sink.write({"id": 1})

        getattr(mock_logger, level).assert_called_once()
