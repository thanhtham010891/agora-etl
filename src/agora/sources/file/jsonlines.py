"""agora/sources/file/jsonlines.py — JsonLinesSource."""

from __future__ import annotations

import asyncio
import json
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
        Number of lines transferred from the file thread to the async
        runtime at a time (default: ``1000``).
    queue_maxsize:
        Maximum number of pending batches held in memory (default: ``2``).
    """

    source_name = "jsonl"

    def __init__(
        self,
        path: Path,
        row_mapper: Callable[[dict], T | None],
        encoding: str = "utf-8",
        batch_size: int = 1000,
        queue_maxsize: int = 2,
        on_record_error: SourceRecordFailurePolicy = SourceRecordFailurePolicy.FAIL_CLOSED,
    ) -> None:
        self._path = Path(path)
        self._row_mapper = row_mapper
        self._encoding = encoding
        self._batch_size = max(batch_size, 1)
        self._queue_maxsize = max(queue_maxsize, 1)
        self._on_record_error = on_record_error
        self._resume_line_number = 0
        self._last_line_number = 0
        self._record_error_count = 0
        self._record_drop_count = 0

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

    async def read_records(self) -> AsyncIterator[T]:
        self._record_error_count = 0
        self._record_drop_count = 0
        batch_queue: queue.Queue[object] = queue.Queue(maxsize=self._queue_maxsize)
        producer = asyncio.create_task(asyncio.to_thread(self._pump_lines, batch_queue))

        try:
            while True:
                batch_lines = await asyncio.to_thread(batch_queue.get)
                if batch_lines is _QUEUE_DONE:
                    break
                if isinstance(batch_lines, Exception):
                    raise batch_lines

                assert isinstance(batch_lines, list)
                for line_number, line in batch_lines:
                    self._last_line_number = line_number
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        record = self._row_mapper(obj)
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
            await producer

    def _pump_lines(self, batch_queue: queue.Queue[object]) -> None:
        pending_lines: list[tuple[int, str]] = []

        try:
            with self._path.open(encoding=self._encoding) as file_obj:
                for line_number, line in enumerate(file_obj, start=1):
                    if line_number <= self._resume_line_number:
                        continue
                    pending_lines.append((line_number, line))
                    if len(pending_lines) >= self._batch_size:
                        batch_queue.put(pending_lines)
                        pending_lines = []

                if pending_lines:
                    batch_queue.put(pending_lines)
        except Exception as exc:
            batch_queue.put(exc)
        finally:
            batch_queue.put(_QUEUE_DONE)
