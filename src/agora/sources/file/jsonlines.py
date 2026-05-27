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
    from collections.abc import AsyncIterator, Callable

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
