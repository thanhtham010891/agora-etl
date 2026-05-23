"""
agora/sinks/file/jsonlines.py
=============================
``JsonLinesSink`` — write records as newline-delimited JSON.

Uses stdlib only (no new dependencies).
Buffered async disk I/O via ``asyncio.to_thread()``.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct

from agora.core.sink import BaseSink

if TYPE_CHECKING:
    from collections.abc import Callable

T = TypeVar("T")
logger = logstruct.getLogger(__name__)


def _default_serializer(record: Any) -> Any:
    """model_dump() → __dict__ → identity."""
    if hasattr(record, "model_dump"):
        return record.model_dump()
    if hasattr(record, "__dict__"):
        return record.__dict__
    return record


class JsonLinesSink(BaseSink[T], Generic[T]):
    """Write records as newline-delimited JSON (JSONL).

    Parameters
    ----------
    path:
        Output file path.
    serializer:
        ``(record: T) -> Any`` — converts record to a JSON-serializable
        object.  Defaults to ``model_dump()`` / ``__dict__`` / identity.
    append:
        If ``True``, append to an existing file.  Default: ``False``
        (overwrite on first flush).
    flush_every:
        Buffer size before an automatic flush (default: 100).
    encoding:
        File encoding (default: ``"utf-8"``).
    """

    sink_name = "jsonl"
    batch_writable_native = True

    def __init__(
        self,
        path: Path | str,
        serializer: Callable[[T], Any] | None = None,
        append: bool = False,
        flush_every: int = 100,
        encoding: str = "utf-8",
    ) -> None:
        self._path = Path(path)
        self._serializer = serializer or _default_serializer
        self._initial_mode = "a" if append else "w"
        self._current_mode = self._initial_mode
        self._flush_every = flush_every
        self._encoding = encoding
        self._buffer: list[T] = []

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
        rows = list(self._buffer)
        mode = self._current_mode
        await asyncio.to_thread(self._write_rows, rows, mode)
        del self._buffer[: len(rows)]
        self._current_mode = "a"
        logger.debug("jsonl_sink_flush", path=str(self._path), count=len(rows))

    def _write_rows(self, rows: list[T], mode: str) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, mode, encoding=self._encoding) as f:
            for record in rows:
                obj = self._serializer(record)
                f.write(json.dumps(obj, ensure_ascii=False, default=str) + "\n")

    async def close(self) -> None:
        await self.flush()
        logger.info("jsonl_sink_closed", path=str(self._path))
