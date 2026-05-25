"""agora/sources/file/parquet.py — ParquetSource."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct

from agora.core.source import SourceRecordError, SourceRuntimeMetrics
from agora.core.types import SourceRecordFailurePolicy
from agora.sources.file.base import FileSource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from agora.core.checkpoint import Checkpoint

T = TypeVar("T")
logger = logstruct.getLogger(__name__)


class ParquetSource(FileSource[T], Generic[T]):
    """Stream records from a Parquet or GeoParquet file.

        Reads in batches using PyArrow's ``ParquetFile.iter_batches()`` to
        avoid loading the entire file into memory.

    Requires: `pip install "agora-etl[file]"`

        Parameters
        ----------
        path:
            Path to the ``.parquet`` file.
        row_mapper:
            ``(dict) -> T | None`` — convert a PyArrow row dict to a record.
            Return None to skip the row.
        batch_size:
            Rows per batch read from PyArrow (default: 1000).
    """

    source_name = "parquet"

    def __init__(
        self,
        path: Path,
        row_mapper: Callable[[dict], T | None],
        batch_size: int = 1000,
        on_record_error: SourceRecordFailurePolicy = SourceRecordFailurePolicy.FAIL_CLOSED,
    ) -> None:
        self._path = Path(path)
        self._row_mapper = row_mapper
        self._batch_size = batch_size
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
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ImportError(
                "PyArrow is required for ParquetSource. Install via: pip install 'agora-etl[file]'"
            ) from exc

        self._record_error_count = 0
        self._record_drop_count = 0

        def _open_reader():
            parquet_file = pq.ParquetFile(str(self._path))
            batch_iter = parquet_file.iter_batches(batch_size=self._batch_size, use_threads=True)
            return parquet_file, batch_iter

        def _read_batch(batch_iter) -> list[dict[str, Any]] | None:
            try:
                batch = next(batch_iter)
            except StopIteration:
                return None
            # Let PyArrow materialize the batch directly instead of copying row-by-row in Python.
            return batch.to_pylist()

        pf, batch_iter = await asyncio.to_thread(_open_reader)
        row_number = self._resume_row_number

        try:
            while True:
                raw_rows = await asyncio.to_thread(_read_batch, batch_iter)
                if raw_rows is None:
                    break
                for row in raw_rows:
                    row_number += 1
                    if row_number <= self._resume_row_number:
                        continue
                    self._last_row_number = row_number
                    try:
                        record = self._row_mapper(row)
                        if record is not None:
                            yield record
                        else:
                            self._record_drop_count += 1
                    except Exception as exc:
                        self._record_error_count += 1
                        logger.warning("parquet_source_row_error", error=str(exc))
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
            await asyncio.to_thread(pf.close)
