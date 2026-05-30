"""agora/sources/file/jsonlines.py — JsonLinesSource."""

from __future__ import annotations

import asyncio
from pathlib import Path

try:
    import orjson as _json_lib
except ImportError:
    import json as _json_lib  # type: ignore[no-redef]
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

import logstruct

from agora.core.source import SourceRecordError, SourceRuntimeMetrics
from agora.core.types import SourceRecordFailurePolicy
from agora.sources.file.base import FileSource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator

    from agora.core.checkpoint import Checkpoint

T = TypeVar("T")
logger = logstruct.getLogger(__name__)


class JsonLinesSource(FileSource[T], Generic[T]):
    """Stream records from a JSONL (newline-delimited JSON) file.

    Parameters
    ----------
    path:
        Path to the ``.jsonl`` file.
    row_mapper:
        ``(dict) -> T | None`` — convert a parsed JSON object to a record.
        Return None to skip the row.
    encoding:
        File encoding (default: ``"utf-8"``).
    batch_size:
        Number of lines transferred per async iteration (default: ``1000``).
    queue_maxsize:
        Runtime prefetch depth for the async source queue (default: ``2``).
    """

    source_name = "jsonl"

    def __init__(
        self,
        path: Path,
        row_mapper: Callable[[dict[str, Any]], T | None],
        encoding: str = "utf-8",
        batch_size: int = 1000,
        queue_maxsize: int = 2,
        on_record_error: SourceRecordFailurePolicy = SourceRecordFailurePolicy.FAIL_CLOSED,
        *,
        emit_batches: bool = False,
        emit_batch_size: int = 5000,
    ) -> None:
        self._path = Path(path)
        self._row_mapper = row_mapper
        self._encoding = encoding
        self._batch_size = max(batch_size, 1)
        self.prefetch_limit = max(queue_maxsize, 1)
        self._on_record_error = on_record_error
        self._resume_line_number = 0
        self._last_line_number = 0
        self._record_error_count = 0
        self._record_drop_count = 0
        self.supports_rust_prefetch: bool = True
        self.supports_prefetch: bool = False  # linear path uses sync stream() directly
        self._emit_batch_size = max(emit_batch_size, 1)
        self.supports_batch_emit: bool = emit_batches

    async def prepare_resume(self, checkpoint: Checkpoint | None) -> None:
        if checkpoint is None:
            self._resume_line_number = 0
            return

        value = checkpoint.value if isinstance(checkpoint.value, dict) else {}
        self._resume_line_number = int(value.get("line_number", 0))

    def current_checkpoint(self) -> dict[str, int] | None:
        if self._last_line_number <= 0:
            return None
        return {"line_number": self._last_line_number}

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return SourceRuntimeMetrics(
            record_error_count=self._record_error_count,
            record_drop_count=self._record_drop_count,
        )

    def stream_sync_batches(self) -> Iterator[Any]:
        """Synchronous generator yielding processed records one by one.

        Called by the Rust thread in iter_source_records_rust() — runs in a
        background thread so the asyncio event loop is not blocked.
        """
        self._record_error_count = 0
        self._record_drop_count = 0

        with open(self._path, encoding=self._encoding) as file_obj:
            line_number = 0
            for line in file_obj:
                line_number += 1
                if line_number <= self._resume_line_number:
                    continue
                self._last_line_number = line_number
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    parsed = _json_lib.loads(stripped)
                    record = self._row_mapper(parsed)
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
                        record=stripped,
                        checkpoint=self.current_checkpoint(),
                        source=self.source_name,
                    ) from exc

    async def stream_batches(self) -> AsyncIterator[list[T]]:
        """Yield ``list[T]`` batches for the batch execution lane.

        Reuses the per-record parse path (``stream_sync_batches``) and groups
        records into lists of ``emit_batch_size``. Enabled via ``emit_batches=True``.
        """
        batch: list[T] = []
        batches_emitted = 0
        for record in self.stream_sync_batches():
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
        coroutines remain responsive.
        """
        for count, record in enumerate(self.stream_sync_batches(), 1):
            yield record
            if count % 5000 == 0:
                await asyncio.sleep(0)

    async def read_records(self) -> AsyncIterator[T]:
        self._record_error_count = 0
        self._record_drop_count = 0

        def _open_file() -> Any:
            return open(self._path, encoding=self._encoding)

        def _read_batch(
            file_obj: Any, line_number: int
        ) -> tuple[list[tuple[int, str, object]], int]:
            batch: list[tuple[int, str, object]] = []
            for line in file_obj:
                line_number += 1
                if line_number <= self._resume_line_number:
                    continue
                stripped = line.strip()
                if not stripped:
                    batch.append((line_number, stripped, None))
                    continue
                try:
                    obj = _json_lib.loads(stripped)
                except Exception as exc:
                    batch.append((line_number, stripped, exc))
                else:
                    batch.append((line_number, stripped, obj))
                if len(batch) >= self._batch_size:
                    break
            return batch, line_number

        file_obj = await asyncio.to_thread(_open_file)
        line_number = 0
        try:
            while True:
                batch, line_number = await asyncio.to_thread(_read_batch, file_obj, line_number)
                if not batch:
                    break
                for line_num, line, parsed in batch:
                    self._last_line_number = line_num
                    if not line:
                        continue
                    try:
                        if isinstance(parsed, Exception):
                            raise parsed
                        record = self._row_mapper(cast("dict[str, Any]", parsed))
                        if record is not None:
                            yield record
                        else:
                            self._record_drop_count += 1
                    except Exception as exc:
                        self._record_error_count += 1
                        logger.warning("jsonl_source_parse_error", error=str(exc))
                        if self._on_record_error == SourceRecordFailurePolicy.LOG_AND_CONTINUE:
                            self._record_drop_count += 1
                            continue
                        raise SourceRecordError(
                            exc,
                            record=line,
                            checkpoint=self.current_checkpoint(),
                            source=self.source_name,
                        ) from exc
        finally:
            await asyncio.to_thread(file_obj.close)


# ======================================================================
# ArrowJsonLinesSource — Arrow-native JSONL reader
# ======================================================================


class ArrowJsonLinesSource(FileSource[Any]):
    """Read a JSONL file as ``pa.RecordBatch`` objects via ``pyarrow.json``.

    Yields batches directly to the Arrow execution lane — no ``row_mapper``,
    no per-row Python dict allocation. Pair with an Arrow-native sink and
    ``ArrowMapMiddleware``/``ArrowFilterMiddleware`` to keep data columnar.

    Requires: ``pip install "agora-etl[file]"``

    Parameters
    ----------
    path:
        Path to the ``.jsonl`` file.
    batch_size:
        Rows per ``pa.RecordBatch`` (default: 65 536).
    """

    source_name = "arrow_jsonl"
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

    async def stream_batches(self) -> AsyncIterator[Any]:
        try:
            import pyarrow.json as pajson
        except ImportError as exc:
            raise ImportError(
                "ArrowJsonLinesSource requires pyarrow. Install via: pip install 'agora-etl[file]'"
            ) from exc

        def _read() -> list[Any]:
            table = pajson.read_json(str(self._path))
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
