"""agora/sources/file/parquet.py — ParquetSource."""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
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

_BATCH_QUEUE_DONE = object()


class ParquetSource(FileSource[T], Generic[T]):
    """Stream records from a Parquet or GeoParquet file.

        Reads in batches using PyArrow's ``ParquetFile.iter_batches()`` to
        avoid loading the entire file into memory.

    Requires: `pip install "agora-etl[file]"`

        Parameters
        ----------
        path:
            Path to the ``.parquet`` file.
        row_mapper:
            ``(dict) -> T | None`` — convert a PyArrow row dict to a record.
            Return None to skip the row.
        batch_size:
            Rows per batch read from PyArrow (default: 1000).
        use_arrow_batches:
            When True, enables the 0.2.0 batch execution lane. Arrow
            ``RecordBatch`` objects are yielded directly; ``row_mapper`` is
            bypassed. Use when the downstream sink is Arrow-native
            (e.g. ``ParquetSink``) or when throughput matters more than
            domain object mapping.
    """

    source_name = "parquet"
    supports_batch_emit: bool = False

    def __init__(
        self,
        path: Path,
        row_mapper: Callable[[dict[str, Any]], T | None],
        batch_size: int = 1000,
        on_record_error: SourceRecordFailurePolicy = SourceRecordFailurePolicy.FAIL_CLOSED,
        *,
        use_arrow_batches: bool = False,
    ) -> None:
        self._path = Path(path)
        self._row_mapper = row_mapper
        self._batch_size = batch_size
        self._on_record_error = on_record_error
        self._resume_row_number = 0
        self._last_row_number = 0
        self._record_error_count = 0
        self._record_drop_count = 0
        self.supports_batch_emit = use_arrow_batches
        self.emits_arrow_batches = use_arrow_batches
        self._arrow_batch_size = 65536 if use_arrow_batches else self._batch_size
        self.supports_prefetch: bool = False
        self.supports_rust_prefetch: bool = True
        self.prefetch_limit: int = 10  # larger buffer for Parquet — to_pylist() is CPU-heavy

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

    def stream_sync_batches(self):
        """Synchronous generator yielding processed records via row_mapper.

        Used by the per-record path (use_arrow_batches=False) to run file I/O
        synchronously in the event loop thread — same pattern as CsvSource and
        JsonLinesSource. Avoids asyncio.to_thread() overhead per batch.
        """
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ImportError(
                "PyArrow is required for ParquetSource. Install via: pip install 'agora-etl[file]'"
            ) from exc

        self._record_error_count = 0
        self._record_drop_count = 0

        pf = pq.ParquetFile(str(self._path))
        row_number = self._resume_row_number
        try:
            for batch in pf.iter_batches(batch_size=self._batch_size, use_threads=True):
                batch_len = len(batch)
                if row_number + batch_len <= self._resume_row_number:
                    row_number += batch_len
                    continue
                rows = batch.to_pylist()
                for row in rows:
                    row_number += 1
                    if row_number <= self._resume_row_number:
                        continue
                    self._last_row_number = row_number
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
        finally:
            pf.close()

    async def stream(self) -> AsyncIterator[Any]:  # type: ignore[override]
        """Stream records synchronously in the event loop thread for linear pipelines.

        For buffered pipelines, iter_source_records uses _iter_prefetched_rust
        which calls stream_sync_batches() in a background thread — so to_pylist()
        does not block the event loop.

        For linear pipelines (no buffered stages), sync stream is fine since
        the pipeline processes records one at a time anyway.
        """
        count = 0
        for record in self.stream_sync_batches():
            yield record
            count += 1
            if count % 5000 == 0:
                import asyncio
                await asyncio.sleep(0)

    async def stream_batches(self) -> AsyncIterator[Any]:
        """Yield ``pa.RecordBatch`` objects directly — no row materialization.

        Uses a producer thread + asyncio.Queue pattern: the PyArrow read loop
        runs entirely in one background thread and pushes batches into a
        bounded async queue. The async consumer yields them with no per-batch
        thread boundary crossings — only 1 thread is started for the entire
        file regardless of file size.
        """
        try:
            import pyarrow as _pa  # noqa: F401 — pre-import in main thread to avoid lazy import in producer thread
            import pyarrow.dataset as _ds  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "PyArrow is required for ParquetSource. Install via: pip install 'agora-etl[file]'"
            ) from exc

        self._record_error_count = 0
        self._record_drop_count = 0

        loop = asyncio.get_running_loop()
        queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=self.prefetch_limit or 4)
        stop_event = threading.Event()
        done_event = threading.Event()
        resume_row = self._resume_row_number

        def _producer() -> None:
            try:
                import pyarrow.dataset as ds

                dataset = ds.dataset(str(self._path), format="parquet")
                scanner = dataset.scanner(batch_size=self._arrow_batch_size)

                row_number = resume_row
                for batch in scanner.to_batches():
                    if stop_event.is_set():
                        break

                    batch_len = len(batch)

                    if row_number + batch_len <= resume_row:
                        row_number += batch_len
                        continue

                    if row_number < resume_row:
                        skip = resume_row - row_number
                        batch = batch.slice(skip)
                        row_number = resume_row

                    row_number += len(batch)

                    while not stop_event.is_set():
                        try:
                            asyncio.run_coroutine_threadsafe(
                                queue.put(batch), loop
                            ).result(timeout=0.05)
                            break
                        except TimeoutError:
                            continue
            except Exception as exc:
                if not stop_event.is_set():
                    try:
                        asyncio.run_coroutine_threadsafe(
                            queue.put(exc), loop
                        ).result(timeout=1.0)
                    except Exception:
                        pass
            finally:
                # Signal consumer via threading.Event — never blocks on event loop
                done_event.set()
                # Also try to push sentinel in case consumer is still waiting
                if not stop_event.is_set():
                    try:
                        asyncio.run_coroutine_threadsafe(
                            queue.put(_BATCH_QUEUE_DONE), loop
                        ).result(timeout=1.0)
                    except Exception:
                        pass

        producer = threading.Thread(target=_producer, daemon=True)
        producer.start()

        try:
            while True:
                # Check done_event to avoid blocking forever if producer finished
                if done_event.is_set() and queue.empty():
                    break
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=0.1)
                except TimeoutError:
                    if done_event.is_set():
                        break
                    continue
                if item is _BATCH_QUEUE_DONE:
                    break
                if isinstance(item, Exception):
                    raise item
                self._last_row_number += len(item)
                yield item
                while not queue.empty():
                    try:
                        extra = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    if extra is _BATCH_QUEUE_DONE:
                        return
                    if isinstance(extra, Exception):
                        raise extra
                    self._last_row_number += len(extra)
                    yield extra
        finally:
            stop_event.set()
            producer.join(timeout=5.0)

    async def read_records(self) -> AsyncIterator[T]:
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ImportError(
                "PyArrow is required for ParquetSource. Install via: pip install 'agora-etl[file]'"
            ) from exc

        self._record_error_count = 0
        self._record_drop_count = 0

        def _open_reader() -> Any:
            parquet_file = pq.ParquetFile(str(self._path))
            batch_iter = parquet_file.iter_batches(batch_size=self._batch_size, use_threads=True)
            return parquet_file, batch_iter

        def _read_batch(batch_iter: Any) -> list[dict[str, Any]] | None:
            try:
                batch = next(batch_iter)
            except StopIteration:
                return None
            return cast("list[dict[str, Any]]", batch.to_pylist())

        pf, batch_iter = await asyncio.to_thread(_open_reader)
        row_number = self._resume_row_number

        try:
            while True:
                raw_rows = await asyncio.to_thread(_read_batch, batch_iter)
                if raw_rows is None:
                    break
                for row in raw_rows:
                    row_number += 1
                    if row_number <= self._resume_row_number:
                        continue
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
            await asyncio.to_thread(pf.close)
