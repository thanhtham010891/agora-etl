"""
agora/sinks/file/jsonlines.py
=============================
``JsonLinesSink`` — write records as newline-delimited JSON.

Prefers ``orjson`` when available and falls back to stdlib ``json``.
Buffered async disk I/O via ``asyncio.to_thread()``.
"""

from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path

try:
    import orjson as _json_lib

    _ORJSON = True
except ImportError:
    import json as _json_lib  # type: ignore[no-redef]

    _ORJSON = False
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct

from agora.core.acceleration import (
    AccelerationCapability,
    acceleration_supports,
    make_jsonl_arrow_writer,
)
from agora.core.data_plane import DataPlane
from agora.core.sink import BaseSink

if TYPE_CHECKING:
    from collections.abc import Callable

    from agora.core.context import PipelineContext

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
        self._binary_file = False
        self._has_written = False
        self._ctx: PipelineContext | None = None
        self._rust_arrow_writer: Any = None
        self._rust_arrow_writer_failed = False

    def bind_context(self, ctx: Any) -> None:
        self._ctx = ctx

    async def open(self) -> None:
        self._file = await asyncio.to_thread(self._open_file)

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
            self._file = await asyncio.to_thread(self._open_file)
        rows = self._buffer
        self._buffer = []
        try:
            await asyncio.to_thread(self._write_rows, rows)
        except Exception:
            # Restore buffer so caller can retry or inspect failed records
            self._buffer = rows + self._buffer
            raise
        logger.debug("jsonl_sink_flush", path=str(self._path), count=len(rows))

    def _utf8_binary_fast_path_enabled(self) -> bool:
        normalized = self._encoding.replace("_", "-").lower()
        return normalized in {"utf-8", "utf8"}

    def _acceleration_mode(self) -> str:
        runtime = self._ctx.metrics.runtime if self._ctx is not None else None
        return runtime.acceleration_mode if runtime is not None else "auto"

    def _ensure_rust_arrow_writer(self) -> Any | None:
        if self._rust_arrow_writer is not None:
            return self._rust_arrow_writer
        if self._rust_arrow_writer_failed or not self._utf8_binary_fast_path_enabled():
            return None
        mode = self._acceleration_mode()
        if not acceleration_supports(AccelerationCapability.JSONL_ARROW_WRITER, mode=mode):
            return None
        append = self._initial_mode == "a" or self._has_written or self._path.exists()
        try:
            self._rust_arrow_writer = make_jsonl_arrow_writer(
                str(self._path), append=append, mode=mode
            )
        except Exception:
            self._rust_arrow_writer_failed = True
            return None
        return self._rust_arrow_writer

    def _open_file(self) -> Any:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if self._initial_mode == "a" or self._has_written else "w"
        if self._utf8_binary_fast_path_enabled():
            self._binary_file = True
            return open(self._path, f"{mode}b")
        self._binary_file = False
        return open(self._path, mode, -1, self._encoding)

    def _write_chunk(self, chunk: bytes) -> None:
        if self._binary_file:
            self._file.write(chunk)
        else:
            self._file.write(chunk.decode("utf-8"))

    def _write_arrow_batch_via_rust(self, batch: Any) -> bool:
        rust_writer = self._ensure_rust_arrow_writer()
        if rust_writer is None:
            return False
        write_record_batch = getattr(rust_writer, "write_record_batch", None)
        if not callable(write_record_batch):
            return False
        try:
            write_record_batch(batch)
        except Exception:
            self._rust_arrow_writer_failed = True
            with contextlib.suppress(Exception):
                rust_writer.close()
            self._rust_arrow_writer = None
            return False
        return True

    def _write_rows(self, rows: list[T]) -> None:
        # Batch all rows into a single string — one write syscall per flush
        if _ORJSON:
            serializer = self._serializer
            json_dumps = _json_lib.dumps
            chunk = (
                b"\n".join(
                    json_dumps(
                        serializer(row),
                        option=_json_lib.OPT_NON_STR_KEYS,
                        default=_stringify_unknown,
                    )
                    for row in rows
                )
                + b"\n"
            )
            self._write_chunk(chunk)
        else:
            serializer = self._serializer
            json_dumps = _json_lib.dumps
            chunk_str = (
                "\n".join(
                    json_dumps(serializer(row), ensure_ascii=False, default=str)  # type: ignore[call-arg, misc]
                    for row in rows
                )
                + "\n"
            )
            if self._binary_file:
                self._file.write(chunk_str.encode(self._encoding))
            else:
                self._file.write(chunk_str)
        self._file.flush()
        self._has_written = True

    async def write_arrow_batch(self, batch: Any) -> None:
        """Write a ``pa.RecordBatch`` directly — Arrow-native fast path.

        Prefers a native Arrow -> JSONL writer to avoid ``to_pylist()`` object
        materialization on the hot path. Falls back to the historical Python
        row serialization path when the optional Rust primitive is unavailable.
        """
        if await asyncio.to_thread(self._write_arrow_batch_via_rust, batch):
            self._has_written = True
            logger.debug(
                "jsonl_sink_arrow_flush", path=str(self._path), count=len(batch), native=True
            )
            return
        rows = await asyncio.to_thread(batch.to_pylist)
        if not rows:
            return
        if self._file is None:
            self._file = await asyncio.to_thread(self._open_file)
        await asyncio.to_thread(self._write_arrow_rows, rows)
        self._has_written = True
        logger.debug("jsonl_sink_arrow_flush", path=str(self._path), count=len(rows), native=False)

    def _write_arrow_rows(self, rows: list[dict[str, Any]]) -> None:
        if _ORJSON:
            json_dumps = _json_lib.dumps
            chunk = (
                b"\n".join(
                    json_dumps(row, option=_json_lib.OPT_NON_STR_KEYS, default=_stringify_unknown)
                    for row in rows
                )
                + b"\n"
            )
            self._write_chunk(chunk)
        else:
            json_dumps = _json_lib.dumps
            chunk_str = (
                "\n".join(
                    json_dumps(row, ensure_ascii=False, default=str)  # type: ignore[call-arg, misc]
                    for row in rows
                )
                + "\n"
            )
            if self._binary_file:
                self._file.write(chunk_str.encode(self._encoding))
            else:
                self._file.write(chunk_str)
        self._file.flush()
        self._has_written = True

    async def close(self) -> None:
        await self.flush()
        if self._rust_arrow_writer is not None:
            await asyncio.to_thread(self._rust_arrow_writer.close)
            self._rust_arrow_writer = None
        if self._file is not None:
            await asyncio.to_thread(self._file.close)
            self._file = None
            self._binary_file = False
        logger.info("jsonl_sink_closed", path=str(self._path))
