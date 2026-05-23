"""agora/sources/file/csv.py — CsvSource."""

from __future__ import annotations

import asyncio
import queue
from pathlib import Path
from typing import TYPE_CHECKING, Generic, TypeVar

import logstruct

from agora.core.source import SourceRecordError, SourceRuntimeMetrics
from agora.core.types import SourceRecordFailurePolicy
from agora.sources.file.base import FileSource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from agora.core.checkpoint import Checkpoint

T = TypeVar("T")
logger = logstruct.getLogger(__name__)
_QUEUE_DONE = object()


class CsvSource(FileSource[T], Generic[T]):
    """Stream records from a CSV or TSV file.

    Uses Python's stdlib ``csv`` module — no extra dependencies.

    Parameters
    ----------
    path:
        Path to the ``.csv`` / ``.tsv`` file.
    row_mapper:
        ``(dict[str, str]) -> T | None`` — convert a parsed row dict to
        a record.  Return ``None`` to skip the row.
    delimiter:
        Field delimiter character (default: ``","``; use ``"\\t"`` for TSV).
    has_header:
        If ``True`` (default), treat the first row as column headers and
        pass ``DictReader`` rows to ``row_mapper``.
    fieldnames:
        Explicit column names when ``has_header=False``.
    encoding:
        File encoding (default: ``"utf-8"``).  Use ``"utf-8-sig"`` to
        strip the BOM produced by Excel / Windows tools.
    skip_blank_lines:
        Skip rows where all values are empty (default: ``True``).
    batch_size:
        Number of parsed rows transferred from the file thread to the
        async runtime at a time (default: ``1000``).
    queue_maxsize:
        Maximum number of pending batches held in memory (default: ``2``).
    """

    source_name = "csv"

    def __init__(
        self,
        path: Path,
        row_mapper: Callable[[dict], T | None],
        delimiter: str = ",",
        has_header: bool = True,
        fieldnames: list[str] | None = None,
        encoding: str = "utf-8",
        skip_blank_lines: bool = True,
        batch_size: int = 1000,
        queue_maxsize: int = 2,
        on_record_error: SourceRecordFailurePolicy = SourceRecordFailurePolicy.FAIL_CLOSED,
    ) -> None:
        self._path = Path(path)
        self._row_mapper = row_mapper
        self._delimiter = delimiter
        self._has_header = has_header
        self._fieldnames = fieldnames
        self._encoding = encoding
        self._skip_blank_lines = skip_blank_lines
        self._batch_size = max(batch_size, 1)
        self._queue_maxsize = max(queue_maxsize, 1)
        self._on_record_error = on_record_error
        self._resume_row_number = 0
        self._last_row_number = 0
        self._record_error_count = 0
        self._record_drop_count = 0

    async def prepare_resume(self, checkpoint: Checkpoint | None) -> None:
        if checkpoint is None:
            self._resume_row_number = 0
            return

        value = checkpoint.value if isinstance(checkpoint.value, dict) else {}
        self._resume_row_number = int(value.get("row_number", 0))

    def current_checkpoint(self) -> dict[str, int] | None:
        if self._last_row_number <= 0:
            return None
        return {"row_number": self._last_row_number}

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return SourceRuntimeMetrics(
            record_error_count=self._record_error_count,
            record_drop_count=self._record_drop_count,
        )

    async def read_records(self) -> AsyncIterator[T]:
        self._record_error_count = 0
        self._record_drop_count = 0
        batch_queue: queue.Queue[object] = queue.Queue(maxsize=self._queue_maxsize)
        producer = asyncio.create_task(asyncio.to_thread(self._pump_rows, batch_queue))

        try:
            while True:
                batch_rows = await asyncio.to_thread(batch_queue.get)
                if batch_rows is _QUEUE_DONE:
                    break
                if isinstance(batch_rows, Exception):
                    raise batch_rows

                assert isinstance(batch_rows, list)
                for row_number, row in batch_rows:
                    self._last_row_number = row_number
                    if self._skip_blank_lines and all(value == "" for value in row.values()):
                        continue
                    try:
                        record = self._row_mapper(row)
                        if record is not None:
                            yield record
                        else:
                            self._record_drop_count += 1
                    except Exception as exc:
                        self._record_error_count += 1
                        logger.warning("csv_source_row_error", error=str(exc))
                        if self._on_record_error == SourceRecordFailurePolicy.LOG_AND_CONTINUE:
                            self._record_drop_count += 1
                            continue
                        raise SourceRecordError(
                            exc,
                            record=row,
                            checkpoint=self.current_checkpoint(),
                            source=self.source_name,
                        ) from exc
        finally:
            await producer

    def _pump_rows(self, batch_queue: queue.Queue[object]) -> None:
        import csv

        pending_rows: list[tuple[int, dict]] = []

        try:
            with open(self._path, encoding=self._encoding, newline="") as file_obj:
                if self._has_header:
                    reader = csv.DictReader(file_obj, delimiter=self._delimiter)
                else:
                    reader = csv.DictReader(
                        file_obj,
                        fieldnames=self._fieldnames,
                        delimiter=self._delimiter,
                    )

                for row_number, row in enumerate(reader, start=1):
                    if row_number <= self._resume_row_number:
                        continue
                    pending_rows.append((row_number, dict(row)))
                    if len(pending_rows) >= self._batch_size:
                        batch_queue.put(pending_rows)
                        pending_rows = []

                if pending_rows:
                    batch_queue.put(pending_rows)
        except Exception as exc:
            batch_queue.put(exc)
        finally:
            batch_queue.put(_QUEUE_DONE)
