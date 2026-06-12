"""
agora/sinks/file/csv.py
=======================
``CsvSink`` — write records as CSV.

Uses stdlib only (no new dependencies).
Buffered async disk I/O via ``asyncio.to_thread()``.
"""

from __future__ import annotations

import asyncio
import csv as _csv
import io
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

import logstruct

from agora.core.acceleration import (
    AccelerationCapability,
    acceleration_supports,
    make_csv_arrow_writer,
)
from agora.core.data_plane import DataPlane
from agora.core.sink import BaseSink

if TYPE_CHECKING:
    from collections.abc import Callable

    from agora.core.context import PipelineContext

T = TypeVar("T")
logger = logstruct.getLogger(__name__)


@dataclass(slots=True)
class _ArrowCsvWriteResult:
    native_write: bool
    row_count: int
    downgrade_reason: str | None = None
    rendered_bytes: int = 0
    serialize_time_ms: float = 0.0
    buffer_copy_time_ms: float = 0.0
    boundary_write_time_ms: float = 0.0
    downgrade_fallback_time_ms: float = 0.0
    rust_boundary_active: bool = False
    rust_import_time_ms: float = 0.0
    rust_file_open_time_ms: float = 0.0
    rust_metadata_time_ms: float = 0.0
    rust_writer_build_time_ms: float = 0.0
    rust_header_render_time_ms: float = 0.0
    rust_column_build_time_ms: float = 0.0
    rust_row_render_time_ms: float = 0.0
    rust_writer_write_time_ms: float = 0.0
    rust_file_flush_time_ms: float = 0.0


class CsvSink(BaseSink[T], Generic[T]):
    """Write records as CSV.

    Parameters
    ----------
    path:
        Output file path.
    row_mapper:
        ``(record: T) -> dict[str, Any]`` — converts record to a row dict.
        Keys become column headers.
    fieldnames:
        Explicit column order.  Inferred from first row's keys if omitted.
    append:
        If ``True``, open in append mode and skip writing the header when
        the file already exists.
    flush_every:
        Buffer size before an automatic flush (default: 100).
    delimiter:
        CSV delimiter character (default: ``","``).
    encoding:
        File encoding (default: ``"utf-8"``).
    """

    sink_name = "csv"
    accepted_data_planes = (
        DataPlane.PYTHON_ROWS,
        DataPlane.PYTHON_BATCHES,
        DataPlane.ARROW_BATCHES,
    )
    native_data_planes = accepted_data_planes

    def __init__(
        self,
        path: Path | str,
        row_mapper: Callable[[T], dict[str, Any]],
        fieldnames: list[str] | None = None,
        append: bool = False,
        flush_every: int = 100,
        delimiter: str = ",",
        encoding: str = "utf-8",
    ) -> None:
        self._path = Path(path)
        self._row_mapper = row_mapper
        self._fieldnames = fieldnames
        self._append = append
        self._flush_every = flush_every
        self._delimiter = delimiter
        self._encoding = encoding
        self._buffer: list[T] = []
        self._header_written = False
        self._file: Any | None = None
        self._writer: Any | None = None
        self._ctx: PipelineContext | None = None
        self._rust_arrow_writer: Any | None = None
        self._rust_arrow_writer_failed = False

    def bind_context(self, ctx: Any) -> None:
        self._ctx = ctx

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
        batch = list(self._buffer)
        rows = self._map_records_to_rows(batch)
        await asyncio.to_thread(self._write_rows, rows)
        del self._buffer[: len(batch)]
        self._header_written = True
        logger.debug("csv_sink_flush", path=str(self._path), count=len(rows))

    def _map_records_to_rows(self, records: list[T]) -> list[dict[str, Any]]:
        return [self._row_mapper(record) for record in records]

    def _should_write_header(self) -> bool:
        file_exists = self._path.exists()
        return not self._header_written and not (self._append and file_exists)

    def _acceleration_mode(self) -> str:
        runtime = self._ctx.metrics.runtime if self._ctx is not None else None
        return runtime.acceleration_mode if runtime is not None else "auto"

    def _utf8_binary_fast_path_enabled(self) -> bool:
        normalized = self._encoding.replace("_", "-").lower()
        return normalized in {"utf-8", "utf8"}

    def _ensure_rust_arrow_writer(self) -> Any | None:
        if self._rust_arrow_writer is not None:
            return self._rust_arrow_writer
        if self._rust_arrow_writer_failed or not self._utf8_binary_fast_path_enabled():
            return None
        mode = self._acceleration_mode()
        if not acceleration_supports(AccelerationCapability.CSV_ARROW_WRITER, mode=mode):
            return None
        try:
            self._rust_arrow_writer = make_csv_arrow_writer(
                str(self._path), self._append, mode=mode
            )
        except Exception:
            self._rust_arrow_writer_failed = True
            return None
        return self._rust_arrow_writer

    def _ensure_writer(self, rows: list[dict[str, Any]]) -> None:
        if self._writer is not None:
            return

        if not rows:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = self._fieldnames or list(rows[0].keys())
        if self._file is None:
            mode = "a" if self._append or self._header_written else "w"
            self._file = open(self._path, mode, encoding=self._encoding, newline="")  # noqa: SIM115
        self._writer = _csv.DictWriter(
            self._file,
            fieldnames=fieldnames,
            delimiter=self._delimiter,
            extrasaction="ignore",
        )
        if self._should_write_header():
            self._writer.writeheader()

    def _write_rows(self, rows: list[dict[str, Any]]) -> None:
        self._ensure_writer(rows)
        if not rows or self._writer is None or self._file is None:
            return
        self._writer.writerows(rows)
        self._file.flush()

    def _write_arrow_batch_via_rust(self, batch: Any) -> _ArrowCsvWriteResult | None:
        rust_writer = self._ensure_rust_arrow_writer()
        if rust_writer is None:
            return None

        profiled_write = getattr(rust_writer, "write_record_batch_profile", None)
        write_record_batch = getattr(rust_writer, "write_record_batch", None)
        if not callable(profiled_write) and not callable(write_record_batch):
            return None

        try:
            if callable(profiled_write):
                (
                    rendered_bytes,
                    rust_import_time_ms,
                    rust_file_open_time_ms,
                    rust_metadata_time_ms,
                    rust_writer_build_time_ms,
                    rust_header_render_time_ms,
                    rust_column_build_time_ms,
                    rust_row_render_time_ms,
                    rust_writer_write_time_ms,
                    rust_file_flush_time_ms,
                ) = profiled_write(
                    batch,
                    include_header=self._should_write_header(),
                    delimiter=self._delimiter,
                )
            else:
                assert callable(write_record_batch)
                boundary_write_t0 = time.perf_counter()
                rendered_bytes = int(
                    write_record_batch(
                        batch,
                        include_header=self._should_write_header(),
                        delimiter=self._delimiter,
                    )
                )
                rust_import_time_ms = 0.0
                rust_file_open_time_ms = 0.0
                rust_metadata_time_ms = 0.0
                rust_writer_build_time_ms = 0.0
                rust_header_render_time_ms = 0.0
                rust_column_build_time_ms = 0.0
                rust_row_render_time_ms = 0.0
                rust_writer_write_time_ms = 0.0
                rust_file_flush_time_ms = 0.0
                boundary_write_time_ms = (time.perf_counter() - boundary_write_t0) * 1000.0
        except Exception:
            return None

        self._header_written = True
        return _ArrowCsvWriteResult(
            native_write=True,
            row_count=len(batch),
            rendered_bytes=max(rendered_bytes, 0),
            boundary_write_time_ms=(
                boundary_write_time_ms
                if not callable(profiled_write)
                else (
                    float(rust_import_time_ms)
                    + float(rust_file_open_time_ms)
                    + float(rust_metadata_time_ms)
                    + float(rust_writer_build_time_ms)
                    + float(rust_writer_write_time_ms)
                    + float(rust_file_flush_time_ms)
                )
            ),
            rust_boundary_active=True,
            rust_import_time_ms=float(rust_import_time_ms),
            rust_file_open_time_ms=float(rust_file_open_time_ms),
            rust_metadata_time_ms=float(rust_metadata_time_ms),
            rust_writer_build_time_ms=float(rust_writer_build_time_ms),
            rust_header_render_time_ms=float(rust_header_render_time_ms),
            rust_column_build_time_ms=float(rust_column_build_time_ms),
            rust_row_render_time_ms=float(rust_row_render_time_ms),
            rust_writer_write_time_ms=float(rust_writer_write_time_ms),
            rust_file_flush_time_ms=float(rust_file_flush_time_ms),
        )

    async def write_arrow_batch(self, batch: Any) -> None:
        """Write a ``pa.RecordBatch`` directly — Arrow-native fast path.

        Uses ``pyarrow.csv.write_csv()`` (C extension) to serialize the batch
        columnar → CSV bytes, bypassing Python ``csv.DictWriter`` per-row overhead.
        Header is written only on the first batch.
        """
        result = await asyncio.to_thread(self._write_arrow_batch, batch)
        runtime = self._ctx.metrics.runtime if self._ctx is not None else None
        if result.native_write:
            if runtime is not None:
                runtime.csv_arrow_native_batch_count += 1
                runtime.csv_arrow_native_row_count += result.row_count
                runtime.csv_arrow_native_rendered_bytes += result.rendered_bytes
                runtime.csv_arrow_native_serialize_time_ms += result.serialize_time_ms
                runtime.csv_arrow_native_buffer_copy_time_ms += result.buffer_copy_time_ms
                runtime.csv_arrow_native_boundary_write_time_ms += result.boundary_write_time_ms
                runtime.csv_arrow_native_rust_boundary_batch_count += int(
                    result.rust_boundary_active
                )
                runtime.csv_arrow_rust_import_time_ms += result.rust_import_time_ms
                runtime.csv_arrow_rust_file_open_time_ms += result.rust_file_open_time_ms
                runtime.csv_arrow_rust_metadata_time_ms += result.rust_metadata_time_ms
                runtime.csv_arrow_rust_writer_build_time_ms += result.rust_writer_build_time_ms
                runtime.csv_arrow_rust_header_render_time_ms += result.rust_header_render_time_ms
                runtime.csv_arrow_rust_column_build_time_ms += result.rust_column_build_time_ms
                runtime.csv_arrow_rust_row_render_time_ms += result.rust_row_render_time_ms
                runtime.csv_arrow_rust_writer_write_time_ms += result.rust_writer_write_time_ms
                runtime.csv_arrow_rust_file_flush_time_ms += result.rust_file_flush_time_ms
            logger.debug(
                "csv_sink_arrow_flush",
                path=str(self._path),
                count=result.row_count,
                rendered_bytes=result.rendered_bytes,
                serialize_time_ms=round(result.serialize_time_ms, 3),
                buffer_copy_time_ms=round(result.buffer_copy_time_ms, 3),
                boundary_write_time_ms=round(result.boundary_write_time_ms, 3),
                rust_boundary_active=result.rust_boundary_active,
                rust_import_time_ms=round(result.rust_import_time_ms, 3),
                rust_file_open_time_ms=round(result.rust_file_open_time_ms, 3),
                rust_metadata_time_ms=round(result.rust_metadata_time_ms, 3),
                rust_writer_build_time_ms=round(result.rust_writer_build_time_ms, 3),
                rust_header_render_time_ms=round(result.rust_header_render_time_ms, 3),
                rust_column_build_time_ms=round(result.rust_column_build_time_ms, 3),
                rust_row_render_time_ms=round(result.rust_row_render_time_ms, 3),
                rust_writer_write_time_ms=round(result.rust_writer_write_time_ms, 3),
                rust_file_flush_time_ms=round(result.rust_file_flush_time_ms, 3),
            )
            return

        if runtime is not None:
            runtime.csv_arrow_downgrade_batch_count += 1
            runtime.csv_arrow_downgrade_row_count += result.row_count
            runtime.csv_arrow_downgrade_fallback_time_ms += result.downgrade_fallback_time_ms
        bound_logger = self._ctx.log if self._ctx is not None else logger
        bound_logger.info(
            "csv_sink_arrow_downgraded_to_rows",
            path=str(self._path),
            count=result.row_count,
            reason=result.downgrade_reason or "unknown",
            fallback_time_ms=round(result.downgrade_fallback_time_ms, 3),
            arrow_chain_active=bool(runtime and runtime.arrow_chain_active),
            arrow_fast_path_active=bool(runtime and runtime.arrow_fast_path_active),
        )

    def _write_arrow_batch(self, batch: Any) -> _ArrowCsvWriteResult:
        try:
            import pyarrow as pa
        except ImportError:
            raise ImportError(
                "CsvSink.write_arrow_batch() requires pyarrow. "
                "Install via: pip install 'agora-etl[file]'"
            ) from None

        self._path.parent.mkdir(parents=True, exist_ok=True)

        rust_result = self._write_arrow_batch_via_rust(batch)
        if rust_result is not None:
            return rust_result

        import pyarrow.csv as _pa_csv

        pa_csv = cast("Any", _pa_csv)

        write_options = pa_csv.WriteOptions(
            include_header=self._should_write_header(),
            delimiter=self._delimiter,
        )
        buf = io.BytesIO()
        try:
            serialize_t0 = time.perf_counter()
            pa_csv.write_csv(batch, buf, write_options=write_options)
            serialize_time_ms = (time.perf_counter() - serialize_t0) * 1000.0
            buffer_copy_t0 = time.perf_counter()
            rendered = buf.getvalue()
            buffer_copy_time_ms = (time.perf_counter() - buffer_copy_t0) * 1000.0
        except pa.ArrowInvalid as exc:
            fallback_t0 = time.perf_counter()
            rows = self._map_records_to_rows(batch.to_pylist())
            self._write_rows(rows)
            self._header_written = True
            return _ArrowCsvWriteResult(
                native_write=False,
                row_count=len(rows),
                downgrade_reason=str(exc),
                downgrade_fallback_time_ms=(time.perf_counter() - fallback_t0) * 1000.0,
            )
        except UnicodeDecodeError as exc:
            fallback_t0 = time.perf_counter()
            rows = self._map_records_to_rows(batch.to_pylist())
            self._write_rows(rows)
            self._header_written = True
            return _ArrowCsvWriteResult(
                native_write=False,
                row_count=len(rows),
                downgrade_reason=str(exc),
                downgrade_fallback_time_ms=(time.perf_counter() - fallback_t0) * 1000.0,
            )

        boundary_write_t0 = time.perf_counter()
        if self._utf8_binary_fast_path_enabled():
            mode = "ab" if self._header_written or self._append else "wb"
            with open(self._path, mode) as file_obj:
                file_obj.write(rendered)
        else:
            mode = "a" if self._header_written or self._append else "w"
            with open(self._path, mode, encoding=self._encoding, newline="") as file_obj:
                file_obj.write(rendered.decode(self._encoding))
        boundary_write_time_ms = (time.perf_counter() - boundary_write_t0) * 1000.0
        self._header_written = True
        return _ArrowCsvWriteResult(
            native_write=True,
            row_count=len(batch),
            rendered_bytes=len(rendered),
            serialize_time_ms=serialize_time_ms,
            buffer_copy_time_ms=buffer_copy_time_ms,
            boundary_write_time_ms=boundary_write_time_ms,
            rust_boundary_active=False,
        )

    async def close(self) -> None:
        await self.flush()
        if self._rust_arrow_writer is not None:
            await asyncio.to_thread(self._rust_arrow_writer.close)
            self._rust_arrow_writer = None
        if self._file is not None:
            await asyncio.to_thread(self._file.close)
            self._file = None
            self._writer = None
        logger.info("csv_sink_closed", path=str(self._path))
