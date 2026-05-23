"""
agora/sinks/file/parquet.py
===========================
``ParquetSink`` — write records incrementally to a Parquet file.

Requires: `pip install "agora-etl[file]"` (pyarrow)
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct

from agora.core.sink import BaseSink

if TYPE_CHECKING:
    from collections.abc import Callable

T = TypeVar("T")
logger = logstruct.getLogger(__name__)


class ParquetSink(BaseSink[T], Generic[T]):
    """Write records incrementally to a Parquet file via PyArrow.

    Requires: `pip install "agora-etl[file]"`

    Parameters
    ----------
    path:
        Output file path.
    row_mapper:
        ``(record: T) -> dict`` — converts record to a row dict.
    batch_size:
        Records per write batch (default: 1000).
    compression:
        Parquet compression codec (default: ``"snappy"``).
    """

    sink_name = "parquet"
    batch_writable_native = True

    def __init__(
        self,
        path: Path | str,
        row_mapper: Callable[[T], dict[str, Any]],
        batch_size: int = 1000,
        compression: str = "snappy",
    ) -> None:
        self._path = Path(path)
        self._row_mapper = row_mapper
        self._batch_size = batch_size
        self._compression = compression
        self._buffer: list[T] = []
        self._writer: Any = None  # pyarrow.parquet.ParquetWriter

    async def write(self, record: T) -> None:
        self._buffer.append(record)
        if len(self._buffer) >= self._batch_size:
            await self.flush()

    async def write_batch(self, records: list[T]) -> None:
        self._buffer.extend(records)
        if len(self._buffer) >= self._batch_size:
            await self.flush()

    async def flush(self) -> None:
        if not self._buffer:
            return
        rows = [self._row_mapper(r) for r in self._buffer]
        self._buffer.clear()
        await asyncio.to_thread(self._write_batch, rows)
        logger.debug("parquet_sink_flush", path=str(self._path), count=len(rows))

    def _write_batch(self, rows: list[dict[str, Any]]) -> None:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            raise ImportError(
                "ParquetSink requires pyarrow. Install via: pip install 'agora-etl[file]'"
            ) from None

        self._path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(rows)
        if self._writer is None:
            self._writer = pq.ParquetWriter(
                str(self._path), table.schema, compression=self._compression
            )
        self._writer.write_table(table)

    async def close(self) -> None:
        await self.flush()
        if self._writer is not None:
            await asyncio.to_thread(self._writer.close)
            self._writer = None
        logger.info("parquet_sink_closed", path=str(self._path))
