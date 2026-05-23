"""agora/sources/file/parquet.py — ParquetSource."""

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


class ParquetSource(FileSource[T], Generic[T]):
    """Stream records from a Parquet or GeoParquet file.

        Reads in batches using PyArrow's ``ParquetFile.iter_batches()`` to
        avoid loading the entire file into memory.

    Requires: ``pip install agora-core``

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
        self._record_error_count = 0
        self._record_drop_count = 0
        batch_queue: queue.Queue[object] = queue.Queue(maxsize=2)
        producer = asyncio.create_task(asyncio.to_thread(self._pump_batches, batch_queue))

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
            await producer

    def _pump_batches(self, batch_queue: queue.Queue[object]) -> None:
        try:
            import pyarrow.parquet as pq
        except ImportError:
            batch_queue.put(
                ImportError(
                    "PyArrow is required for ParquetSource. Install via: pip install 'agora-core'"
                )
            )
            batch_queue.put(_QUEUE_DONE)
            return

        try:
            pf = pq.ParquetFile(str(self._path))
            row_number = 0
            for batch in pf.iter_batches(batch_size=self._batch_size):
                pending_rows: list[tuple[int, dict]] = []
                for row in batch.to_pylist():
                    row_number += 1
                    if row_number <= self._resume_row_number:
                        continue
                    pending_rows.append((row_number, row))
                if pending_rows:
                    batch_queue.put(pending_rows)
        except Exception as exc:
            batch_queue.put(exc)
        finally:
            batch_queue.put(_QUEUE_DONE)
