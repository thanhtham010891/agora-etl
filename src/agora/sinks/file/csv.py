"""
agora/sinks/file/csv.py
=======================
``CsvSink`` — write records as CSV.

Uses stdlib only (no new dependencies).
Buffered async disk I/O via ``asyncio.to_thread()``.
"""

from __future__ import annotations

import asyncio
import csv as _csv
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct

from agora.core.data_plane import DataPlane
from agora.core.sink import BaseSink

if TYPE_CHECKING:
    from collections.abc import Callable

    from agora.core.context import PipelineContext

T = TypeVar("T")
logger = logstruct.getLogger(__name__)


class CsvSink(BaseSink[T], Generic[T]):
    """Write records as CSV.

    Parameters
    ----------
    path:
        Output file path.
    row_mapper:
        ``(record: T) -> dict[str, Any]`` — converts record to a row dict.
        Keys become column headers.
    fieldnames:
        Explicit column order.  Inferred from first row's keys if omitted.
    append:
        If ``True``, open in append mode and skip writing the header when
        the file already exists.
    flush_every:
        Buffer size before an automatic flush (default: 100).
    delimiter:
        CSV delimiter character (default: ``","``).
    encoding:
        File encoding (default: ``"utf-8"``).
    """

    sink_name = "csv"
    accepted_data_planes = (
        DataPlane.PYTHON_ROWS,
        DataPlane.PYTHON_BATCHES,
        DataPlane.ARROW_BATCHES,
    )
    native_data_planes = accepted_data_planes

    def __init__(
        self,
        path: Path | str,
        row_mapper: Callable[[T], dict[str, Any]],
        fieldnames: list[str] | None = None,
        append: bool = False,
        flush_every: int = 100,
        delimiter: str = ",",
        encoding: str = "utf-8",
    ) -> None:
        self._path = Path(path)
        self._row_mapper = row_mapper
        self._fieldnames = fieldnames
        self._append = append
        self._flush_every = flush_every
        self._delimiter = delimiter
        self._encoding = encoding
        self._buffer: list[T] = []
        self._header_written = False
        self._file: Any | None = None
        self._writer: Any | None = None
        self._ctx: PipelineContext | None = None

    def bind_context(self, ctx: Any) -> None:
        self._ctx = ctx

    async def write(self, record: T) -> None:
        self._buffer.append(record)
        if len(self._buffer) >= self._flush_every:
            await self.flush()

    async def write_batch(self, records: list[T]) -> None:
        self._buffer.extend(records)
        if len(self._buffer) >= self._flush_every:
            await self.flush()

    async def flush(self) -> None:
        if not self._buffer:
            return
        batch = list(self._buffer)
        rows = self._map_records_to_rows(batch)
        await asyncio.to_thread(self._write_rows, rows)
        del self._buffer[: len(batch)]
        self._header_written = True
        logger.debug("csv_sink_flush", path=str(self._path), count=len(rows))

    def _map_records_to_rows(self, records: list[T]) -> list[dict[str, Any]]:
        return [self._row_mapper(record) for record in records]

    def _should_write_header(self) -> bool:
        file_exists = self._path.exists()
        return not self._header_written and not (self._append and file_exists)

    def _ensure_writer(self, rows: list[dict[str, Any]]) -> None:
        if self._writer is not None:
            return

        if not rows:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = self._fieldnames or list(rows[0].keys())
        mode = "a" if self._append else "w"
        self._file = open(self._path, mode, encoding=self._encoding, newline="")  # noqa: SIM115
        self._writer = _csv.DictWriter(
            self._file,
            fieldnames=fieldnames,
            delimiter=self._delimiter,
            extrasaction="ignore",
        )
        if self._should_write_header():
            self._writer.writeheader()

    def _write_rows(self, rows: list[dict[str, Any]]) -> None:
        self._ensure_writer(rows)
        if not rows or self._writer is None or self._file is None:
            return
        self._writer.writerows(rows)
        self._file.flush()

    async def write_arrow_batch(self, batch: Any) -> None:
        """Write a ``pa.RecordBatch`` directly — Arrow-native fast path.

        Uses ``pyarrow.csv.write_csv()`` (C extension) to serialize the batch
        columnar → CSV bytes, bypassing Python ``csv.DictWriter`` per-row overhead.
        Header is written only on the first batch.
        """
        native_write, row_count, downgrade_reason = await asyncio.to_thread(
            self._write_arrow_batch, batch
        )
        runtime = self._ctx.metrics.runtime if self._ctx is not None else None
        if native_write:
            if runtime is not None:
                runtime.csv_arrow_native_batch_count += 1
                runtime.csv_arrow_native_row_count += row_count
            logger.debug("csv_sink_arrow_flush", path=str(self._path), count=row_count)
            return

        if runtime is not None:
            runtime.csv_arrow_downgrade_batch_count += 1
            runtime.csv_arrow_downgrade_row_count += row_count
        bound_logger = self._ctx.log if self._ctx is not None else logger
        bound_logger.info(
            "csv_sink_arrow_downgraded_to_rows",
            path=str(self._path),
            count=row_count,
            reason=downgrade_reason or "unknown",
            arrow_chain_active=bool(runtime and runtime.arrow_chain_active),
            arrow_fast_path_active=bool(runtime and runtime.arrow_fast_path_active),
        )

    def _write_arrow_batch(self, batch: Any) -> tuple[bool, int, str | None]:
        try:
            import io

            import pyarrow as pa
            import pyarrow.csv as pa_csv
        except ImportError:
            raise ImportError(
                "CsvSink.write_arrow_batch() requires pyarrow. "
                "Install via: pip install 'agora-etl[file]'"
            ) from None

        self._path.parent.mkdir(parents=True, exist_ok=True)

        write_options = pa_csv.WriteOptions(
            include_header=self._should_write_header(),
            delimiter=self._delimiter,
        )
        buf = io.BytesIO()
        try:
            pa_csv.write_csv(batch, buf, write_options=write_options)
            rendered = buf.getvalue().decode(self._encoding)
        except pa.ArrowInvalid as exc:
            rows = self._map_records_to_rows(batch.to_pylist())
            self._write_rows(rows)
            self._header_written = True
            return False, len(rows), str(exc)
        except UnicodeDecodeError as exc:
            rows = self._map_records_to_rows(batch.to_pylist())
            self._write_rows(rows)
            self._header_written = True
            return False, len(rows), str(exc)

        mode = "a" if self._header_written or self._append else "w"
        with open(self._path, mode, encoding=self._encoding, newline="") as f:
            f.write(rendered)

        self._header_written = True
        return True, len(batch), None

    async def close(self) -> None:
        await self.flush()
        if self._file is not None:
            await asyncio.to_thread(self._file.close)
            self._file = None
            self._writer = None
        logger.info("csv_sink_closed", path=str(self._path))
