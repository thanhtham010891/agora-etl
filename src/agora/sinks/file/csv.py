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

from agora.core.sink import BaseSink

if TYPE_CHECKING:
    from collections.abc import Callable

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
    batch_writable_native = True

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
        rows = [self._row_mapper(r) for r in batch]
        write_header = not self._header_written and not (self._append and self._path.exists())
        mode = "a" if (self._header_written or self._append) else "w"
        await asyncio.to_thread(self._write_rows, rows, write_header, mode)
        del self._buffer[: len(batch)]
        self._header_written = True
        logger.debug("csv_sink_flush", path=str(self._path), count=len(rows))

    def _write_rows(
        self,
        rows: list[dict[str, Any]],
        write_header: bool,
        mode: str,
    ) -> None:
        if not rows:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = self._fieldnames or list(rows[0].keys())
        with open(self._path, mode, encoding=self._encoding, newline="") as f:
            writer = _csv.DictWriter(
                f,
                fieldnames=fieldnames,
                delimiter=self._delimiter,
                extrasaction="ignore",
            )
            if write_header:
                writer.writeheader()
            writer.writerows(rows)

    async def close(self) -> None:
        await self.flush()
        logger.info("csv_sink_closed", path=str(self._path))
