"""
agora/sinks/file/jsonlines.py
=============================
``JsonLinesSink`` — write records as newline-delimited JSON.

Prefers ``orjson`` when available and falls back to stdlib ``json``.
Buffered async disk I/O via ``asyncio.to_thread()``.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

try:
    import orjson as _json_lib

    _ORJSON = True
except ImportError:
    import json as _json_lib  # type: ignore[no-redef]

    _ORJSON = False
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct

from agora.core.data_plane import DataPlane
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


def _stringify_unknown(value: Any) -> str:
    """Preserve the historical ``default=str`` contract for unknown values."""
    return str(value)


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
    accepted_data_planes = (
        DataPlane.PYTHON_ROWS,
        DataPlane.PYTHON_BATCHES,
        DataPlane.ARROW_BATCHES,
    )
    native_data_planes = accepted_data_planes

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
        self._flush_every = flush_every
        self._encoding = encoding
        self._buffer: list[T] = []
        self._file: Any = None

    async def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = await asyncio.to_thread(
            open, self._path, self._initial_mode, -1, self._encoding
        )

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
        if self._file is None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._file = await asyncio.to_thread(
                open, self._path, self._initial_mode, -1, self._encoding
            )
        rows = list(self._buffer)
        try:
            await asyncio.to_thread(self._write_rows, rows)
        except Exception:
            # Restore buffer so caller can retry or inspect failed records
            self._buffer = rows + self._buffer[len(rows) :]
            raise
        del self._buffer[: len(rows)]
        logger.debug("jsonl_sink_flush", path=str(self._path), count=len(rows))

    def _write_rows(self, rows: list[T]) -> None:
        # Batch all rows into a single string — one write syscall per flush
        if _ORJSON:
            chunk = (
                b"\n".join(
                    _json_lib.dumps(
                        self._serializer(r),
                        option=_json_lib.OPT_NON_STR_KEYS,
                        default=_stringify_unknown,
                    )
                    for r in rows
                )
                + b"\n"
            )
            self._file.write(chunk.decode("utf-8"))
        else:
            chunk_str = (
                "\n".join(
                    _json_lib.dumps(self._serializer(r), ensure_ascii=False, default=str)  # type: ignore[call-arg, misc]
                    for r in rows
                )
                + "\n"
            )
            self._file.write(chunk_str)
        self._file.flush()

    async def write_arrow_batch(self, batch: Any) -> None:
        """Write a ``pa.RecordBatch`` directly — Arrow-native fast path.

        Converts the batch to JSONL via ``to_pylist()`` (required for JSON
        serialization) but skips the per-record ``serializer`` call and
        batches all rows into a single write syscall.
        """
        rows = await asyncio.to_thread(batch.to_pylist)
        if not rows:
            return
        if self._file is None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._file = await asyncio.to_thread(
                open, self._path, self._initial_mode, -1, self._encoding
            )
        await asyncio.to_thread(self._write_arrow_rows, rows)
        logger.debug("jsonl_sink_arrow_flush", path=str(self._path), count=len(rows))

    def _write_arrow_rows(self, rows: list[dict[str, Any]]) -> None:
        if _ORJSON:
            chunk = (
                b"\n".join(
                    _json_lib.dumps(
                        r, option=_json_lib.OPT_NON_STR_KEYS, default=_stringify_unknown
                    )
                    for r in rows
                )
                + b"\n"
            )
            self._file.write(chunk.decode("utf-8"))
        else:
            chunk_str = (
                "\n".join(
                    _json_lib.dumps(r, ensure_ascii=False, default=str)  # type: ignore[call-arg, misc]
                    for r in rows
                )
                + "\n"
            )
            self._file.write(chunk_str)
        self._file.flush()

    async def close(self) -> None:
        await self.flush()
        if self._file is not None:
            await asyncio.to_thread(self._file.close)
            self._file = None
        logger.info("jsonl_sink_closed", path=str(self._path))
