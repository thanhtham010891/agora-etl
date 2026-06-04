"""agora/sources/file/csv.py — CsvSource."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct

from agora.core.data_plane import DataPlane, SourceDataPlaneSpec
from agora.core.source import SourceRecordError, SourceRuntimeMetrics
from agora.core.types import SourceRecordFailurePolicy
from agora.sources.file.base import FileSource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator
    from io import TextIOWrapper

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
        row_mapper: Callable[[dict[str, Any]], T | None],
        delimiter: str = ",",
        has_header: bool = True,
        fieldnames: list[str] | None = None,
        encoding: str = "utf-8",
        skip_blank_lines: bool = True,
        batch_size: int = 1000,
        queue_maxsize: int = 2,
        on_record_error: SourceRecordFailurePolicy = SourceRecordFailurePolicy.FAIL_CLOSED,
        *,
        emit_batches: bool = False,
        emit_batch_size: int = 5000,
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
        self.supports_rust_prefetch: bool = True
        self.supports_prefetch: bool = False  # linear path uses sync stream() directly
        self._emit_batch_size = max(emit_batch_size, 1)
        self._emit_batches = emit_batches

    def data_plane_spec(self) -> SourceDataPlaneSpec:
        emitted_plane = DataPlane.PYTHON_BATCHES if self._emit_batches else DataPlane.PYTHON_ROWS
        return SourceDataPlaneSpec(
            source_name=self.source_name,
            emitted_plane=emitted_plane,
            supports_batch_emit=self._emit_batches,
            emits_arrow_batches=False,
        )

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

    def stream_sync_batches(self) -> Iterator[Any]:
        """Synchronous generator yielding processed records batch by batch.

        Called by the Rust thread in iter_source_records_rust() — runs in a
        background thread so the asyncio event loop is not blocked.
        Yields individual records (after row_mapper) so the Rust layer only
        needs to push Python objects into RecordBuffer.
        """
        import csv as _csv

        self._record_error_count = 0
        self._record_drop_count = 0

        def _make_reader_with_header(f: TextIOWrapper) -> _csv.DictReader[str]:
            return _csv.DictReader(f, delimiter=self._delimiter)

        def _make_reader_no_header(f: TextIOWrapper) -> _csv.DictReader[str]:
            return _csv.DictReader(f, fieldnames=self._fieldnames, delimiter=self._delimiter)

        reader_factory = _make_reader_with_header if self._has_header else _make_reader_no_header

        with open(self._path, encoding=self._encoding, newline="") as file_obj:
            reader = reader_factory(file_obj)
            row_number = 0
            for row in reader:
                row_number += 1
                if row_number <= self._resume_row_number:
                    continue
                self._last_row_number = row_number
                if self._skip_blank_lines and all(v == "" for v in row.values()):
                    continue
                try:
                    record = self._row_mapper(row)
                    if record is not None:
                        yield record
                    else:
                        self._record_drop_count += 1
                except Exception as exc:
                    self._record_error_count += 1
                    if self._on_record_error == SourceRecordFailurePolicy.LOG_AND_CONTINUE:
                        self._record_drop_count += 1
                        continue
                    from agora.core.source import SourceRecordError

                    raise SourceRecordError(
                        exc,
                        record=row,
                        checkpoint=self.current_checkpoint(),
                        source=self.source_name,
                    ) from exc

    async def stream_batches(self) -> AsyncIterator[list[T]]:
        """Yield ``list[T]`` batches for the batch execution lane.

        Parses directly into ``emit_batch_size`` lists so the batch lane does
        not pay per-record generator yield/resume overhead before regrouping.
        Enabled via ``emit_batches=True``. Yields control to the event loop
        periodically so other coroutines stay responsive.
        """
        import csv as _csv

        self._record_error_count = 0
        self._record_drop_count = 0

        def _make_reader_with_header(f: TextIOWrapper) -> _csv.DictReader[str]:
            return _csv.DictReader(f, delimiter=self._delimiter)

        def _make_reader_no_header(f: TextIOWrapper) -> _csv.DictReader[str]:
            return _csv.DictReader(f, fieldnames=self._fieldnames, delimiter=self._delimiter)

        reader_factory = _make_reader_with_header if self._has_header else _make_reader_no_header

        with open(self._path, encoding=self._encoding, newline="") as file_obj:
            reader = reader_factory(file_obj)
            row_number = 0
            batches_emitted = 0
            batch: list[T] = []

            for row in reader:
                row_number += 1
                if row_number <= self._resume_row_number:
                    continue
                self._last_row_number = row_number
                if self._skip_blank_lines and all(v == "" for v in row.values()):
                    continue
                try:
                    record = self._row_mapper(row)
                except Exception as exc:
                    self._record_error_count += 1
                    if self._on_record_error == SourceRecordFailurePolicy.LOG_AND_CONTINUE:
                        self._record_drop_count += 1
                        continue
                    raise SourceRecordError(
                        exc,
                        record=row,
                        checkpoint=self.current_checkpoint(),
                        source=self.source_name,
                    ) from exc

                if record is None:
                    self._record_drop_count += 1
                    continue

                batch.append(record)
                if len(batch) >= self._emit_batch_size:
                    yield batch
                    batch = []
                    batches_emitted += 1
                    if batches_emitted % 4 == 0:
                        await asyncio.sleep(0)

            if batch:
                yield batch

    async def stream(self) -> AsyncIterator[T]:  # type: ignore[override]
        """Stream records synchronously in the event loop thread.

        Avoids thread boundary and asyncio.Queue overhead entirely.
        Yields control to the event loop every 5000 records so other
        coroutines (health checks, DLQ writes) remain responsive.
        """
        for count, record in enumerate(self.stream_sync_batches(), 1):
            yield record
            if count % 5000 == 0:
                await asyncio.sleep(0)

    async def read_records(self) -> AsyncIterator[T]:
        import csv

        self._record_error_count = 0
        self._record_drop_count = 0

        def _open_reader() -> Any:
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

        def _read_batch(
            reader: Any, row_number: int
        ) -> tuple[list[tuple[int, dict[str, Any]]], int]:
            batch: list[tuple[int, dict[str, Any]]] = []
            for row in reader:
                row_number += 1
                if row_number <= self._resume_row_number:
                    continue
                batch.append((row_number, row))
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


# ======================================================================
# ArrowCsvSource — Arrow-native CSV reader (zero per-row Python objects)
# ======================================================================


class ArrowCsvSource(FileSource[Any]):
    """Read a CSV file as ``pa.RecordBatch`` objects via ``pyarrow.csv``.

    Yields batches directly to the Arrow execution lane — no ``row_mapper``,
    no per-row Python dict allocation. Pair with an Arrow-native sink (e.g.
    ``ParquetSink``) and ``ArrowMapMiddleware``/``ArrowFilterMiddleware`` to
    keep data columnar end-to-end.

    Requires: ``pip install "agora-etl[file]"``

    Parameters
    ----------
    path:
        Path to the ``.csv`` file.
    batch_size:
        Rows per ``pa.RecordBatch`` (default: 65 536).
    """

    source_name = "arrow_csv"
    supports_batch_emit: bool = True
    emits_arrow_batches: bool = True

    def __init__(
        self,
        path: Path,
        batch_size: int = 65_536,
    ) -> None:
        self._path = Path(path)
        self._batch_size = max(batch_size, 1)
        self._rows_read: int = 0

    def current_checkpoint(self) -> dict[str, int] | None:
        return {"rows": self._rows_read} if self._rows_read else None

    async def prepare_resume(self, checkpoint: Any) -> None:
        return None

    def data_plane_spec(self) -> SourceDataPlaneSpec:
        return SourceDataPlaneSpec(
            source_name=self.source_name,
            emitted_plane=DataPlane.ARROW_BATCHES,
            supports_batch_emit=True,
            emits_arrow_batches=True,
        )

    async def stream_batches(self) -> AsyncIterator[Any]:
        try:
            import pyarrow.csv as pacsv
        except ImportError as exc:
            raise ImportError(
                "ArrowCsvSource requires pyarrow. Install via: pip install 'agora-etl[file]'"
            ) from exc

        def _read() -> list[Any]:
            table = pacsv.read_csv(str(self._path))
            return table.to_batches(max_chunksize=self._batch_size)  # type: ignore[no-any-return]

        for batch in await asyncio.to_thread(_read):
            self._rows_read += batch.num_rows
            yield batch

    async def stream(self) -> AsyncIterator[Any]:  # type: ignore[override]
        async for batch in self.stream_batches():
            for row in batch.to_pylist():
                yield row

    async def read_records(self) -> AsyncIterator[Any]:
        async for row in self.stream():
            yield row
