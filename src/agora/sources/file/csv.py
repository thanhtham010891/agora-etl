"""agora/sources/file/csv.py — CsvSource."""

from __future__ import annotations

import asyncio
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
        Number of parsed rows transferred per async iteration (default: ``1000``).
    queue_maxsize:
        Runtime prefetch depth for the async source queue (default: ``2``).
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
        self.prefetch_limit = max(queue_maxsize, 1)
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
        import csv

        self._record_error_count = 0
        self._record_drop_count = 0

        def _open_reader():
            file_obj = open(self._path, encoding=self._encoding, newline="")  # noqa: SIM115
            if self._has_header:
                reader = csv.DictReader(file_obj, delimiter=self._delimiter)
            else:
                reader = csv.DictReader(
                    file_obj,
                    fieldnames=self._fieldnames,
                    delimiter=self._delimiter,
                )
            return file_obj, reader

        def _read_batch(reader, row_number: int) -> tuple[list[tuple[int, dict]], int]:
            batch: list[tuple[int, dict]] = []
            for row in reader:
                row_number += 1
                if row_number <= self._resume_row_number:
                    continue
                batch.append((row_number, dict(row)))
                if len(batch) >= self._batch_size:
                    break
            return batch, row_number

        file_obj, reader = await asyncio.to_thread(_open_reader)
        row_number = 0
        try:
            while True:
                batch, row_number = await asyncio.to_thread(_read_batch, reader, row_number)
                if not batch:
                    break
                for row_num, row in batch:
                    self._last_row_number = row_num
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
            await asyncio.to_thread(file_obj.close)
