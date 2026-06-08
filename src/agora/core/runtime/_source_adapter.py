"""Source runtime adapter — normalises source capabilities into one stream contract."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from agora.core.checkpoint import is_checkpoint_capable
from agora.core.constants import (
    DEFAULT_PREFETCH_LIMIT,
    PRODUCER_JOIN_TIMEOUT_S,
    RUST_PREFETCH_WAIT_TIMEOUT_MS,
)
from agora.core.runtime._delivery import SourceQueueError, SourceRecord
from agora.core.source import (
    DeliveryHookSource,
    prefetch_limit_for,
    source_delivery_success_callback,
    source_runtime_metrics,
)

try:
    from agora_rs import MetricsAccumulator, RecordBuffer

    try:
        _test_ma = MetricsAccumulator()
        _test_rb = RecordBuffer(1)
        del _test_ma, _test_rb
        _RUST_AVAILABLE = True
    except Exception:
        _RUST_AVAILABLE = False
except ImportError:
    _RUST_AVAILABLE = False

    class RecordBuffer:  # type: ignore[no-redef]
        """Placeholder — agora-rs not installed. Allows monkeypatching in tests."""

        def __init__(self, capacity: int) -> None:
            raise ImportError("agora-etl-rs is not installed.")

    class MetricsAccumulator:  # type: ignore[no-redef]
        """Placeholder — agora-rs not installed. Allows monkeypatching in tests."""

        def __init__(self, flush_interval: int = 100) -> None:
            raise ImportError("agora-etl-rs is not installed.")


if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from agora.core.context import PipelineContext
    from agora.core.source import BaseSource

SOURCE_QUEUE_DONE = object()


@dataclass(slots=True)
class SourceRuntimeAdapter:
    """Normalises source capabilities into a single SourceRecord stream.

    Owns: prefetch, sync batch streaming, delivery hook capture, and
    runtime metric extraction. Lane code calls iter_source_records() and
    sync_runtime_metrics() — it never touches the source directly.
    """

    source: BaseSource[Any]
    has_buffered_stages: bool

    def sync_runtime_metrics(self, ctx: PipelineContext) -> None:
        metrics = source_runtime_metrics(self.source)
        ctx.metrics.runtime.source_record_error_count = metrics.record_error_count
        ctx.metrics.runtime.source_record_drop_count = metrics.record_drop_count

    def _make_source_record(
        self,
        record: Any,
        *,
        checkpoint_capable: bool,
        has_delivery_hook: bool,
    ) -> SourceRecord:
        return SourceRecord(
            raw=record,
            checkpoint=self.source.current_checkpoint() if checkpoint_capable else None,
            on_success=source_delivery_success_callback(self.source) if has_delivery_hook else None,
        )

    async def iter_source_records(self, ctx: PipelineContext) -> AsyncGenerator[SourceRecord, None]:
        prefetch_limit = prefetch_limit_for(self.source)
        checkpoint_capable = is_checkpoint_capable(self.source)
        has_delivery_hook = isinstance(self.source, DeliveryHookSource)

        # Rust prefetch is an explicit source capability. Buffered pipelines may
        # use it when the extension is installed, but async-only sources must
        # stay on the Python queue path even if Rust is available globally.
        use_rust_prefetch = (
            _RUST_AVAILABLE
            and getattr(self.source, "supports_rust_prefetch", False)
            and self.has_buffered_stages
        )
        if use_rust_prefetch:
            ctx.metrics.runtime.source_prefetch_enabled = True
            ctx.metrics.runtime.source_prefetch_limit = prefetch_limit or DEFAULT_PREFETCH_LIMIT
            ctx.metrics.runtime.rust_prefetch_active = True
            ctx.log.info(
                "pipeline_source_prefetch_enabled",
                source=self.source.source_name,
                prefetch_limit=prefetch_limit or DEFAULT_PREFETCH_LIMIT,
            )
            async for record in self._iter_prefetched_rust(
                ctx, prefetch_limit or DEFAULT_PREFETCH_LIMIT
            ):
                yield record
            return

        if prefetch_limit <= 0:
            if checkpoint_capable or has_delivery_hook:
                async for record in self.source.stream():
                    yield self._make_source_record(
                        record,
                        checkpoint_capable=checkpoint_capable,
                        has_delivery_hook=has_delivery_hook,
                    )
            else:
                async for record in self.source.stream():
                    yield SourceRecord(raw=record)
            return

        ctx.metrics.runtime.source_prefetch_enabled = True
        ctx.metrics.runtime.source_prefetch_limit = prefetch_limit
        ctx.log.info(
            "pipeline_source_prefetch_enabled",
            source=self.source.source_name,
            prefetch_limit=prefetch_limit,
        )
        # Non-buffered pipelines with prefetch_limit > 0 use the Python path only.
        if use_rust_prefetch:
            async for record in self._iter_prefetched_rust(ctx, prefetch_limit):
                yield record
        else:
            async for record in self._iter_prefetched_python(ctx, prefetch_limit):
                yield record

    async def _iter_prefetched_rust(
        self,
        ctx: PipelineContext,
        prefetch_limit: int,
    ) -> AsyncGenerator[SourceRecord, None]:
        import threading

        buf = RecordBuffer(prefetch_limit)
        _stream_sync = getattr(self.source, "stream_sync_batches", None)
        checkpoint_capable = is_checkpoint_capable(self.source)
        has_delivery_hook = isinstance(self.source, DeliveryHookSource)
        error_holder: list[Exception] = []

        def _producer() -> None:
            pending_batch: list[SourceRecord] = []

            def _flush_pending_batch() -> bool:
                if not pending_batch:
                    return True
                pushed = buf.push_batch(pending_batch)
                ctx.metrics.runtime.rust_prefetch_push_batch_count += 1
                if pushed < len(pending_batch):
                    return False
                pending_batch.clear()
                return True

            try:
                if _stream_sync is not None:
                    for record in _stream_sync():
                        sr = self._make_source_record(
                            record,
                            checkpoint_capable=checkpoint_capable,
                            has_delivery_hook=has_delivery_hook,
                        )
                        pending_batch.append(sr)
                        if len(pending_batch) >= prefetch_limit and not _flush_pending_batch():
                            break
                    else:
                        _flush_pending_batch()
                else:
                    raise RuntimeError(
                        f"Source '{self.source.source_name}' uses Rust prefetch but does not "
                        "implement stream_sync_batches(). Async-only sources cannot be driven "
                        "from the prefetch producer thread (their stream() would run on a "
                        "different event loop than the rest of the pipeline, corrupting any "
                        "shared async resources). Implement stream_sync_batches() for a "
                        "synchronous read path, or set prefetch_limit=0 to use the in-loop path."
                    )
            except Exception as exc:
                error_holder.append(exc)
            finally:
                buf.close()

        producer = threading.Thread(target=_producer, daemon=True)
        producer.start()

        try:
            while True:
                batch = buf.pop_batch(prefetch_limit)
                if batch:
                    ctx.metrics.runtime.rust_prefetch_batch_drain_count += 1
                    ctx.metrics.runtime.source_prefetch_max_depth = max(
                        ctx.metrics.runtime.source_prefetch_max_depth,
                        len(batch),
                    )
                    for item in batch:
                        yield cast("SourceRecord", item)
                    continue

                if buf.is_done():
                    break

                ctx.metrics.runtime.source_prefetch_block_count += 1
                ctx.metrics.runtime.rust_prefetch_wait_count += 1
                ready = await asyncio.to_thread(buf.wait_for_item, RUST_PREFETCH_WAIT_TIMEOUT_MS)
                if not ready and buf.is_done():
                    break
        finally:
            buf.close()
            producer.join(timeout=PRODUCER_JOIN_TIMEOUT_S)

        if error_holder:
            raise error_holder[0]

    async def _iter_prefetched_python(
        self,
        ctx: PipelineContext,
        prefetch_limit: int,
    ) -> AsyncGenerator[SourceRecord, None]:
        source_queue: asyncio.Queue[object] = asyncio.Queue(maxsize=prefetch_limit)
        checkpoint_capable = is_checkpoint_capable(self.source)
        has_delivery_hook = isinstance(self.source, DeliveryHookSource)

        async def _pump_source() -> None:
            try:
                async for record in self.source.stream():
                    if source_queue.full():
                        ctx.metrics.runtime.source_prefetch_block_count += 1
                    await source_queue.put(
                        self._make_source_record(
                            record,
                            checkpoint_capable=checkpoint_capable,
                            has_delivery_hook=has_delivery_hook,
                        )
                    )
                    ctx.metrics.runtime.source_prefetch_max_depth = max(
                        ctx.metrics.runtime.source_prefetch_max_depth,
                        source_queue.qsize(),
                    )
            except Exception as exc:
                await source_queue.put(SourceQueueError(exc))
            finally:
                await asyncio.shield(source_queue.put(SOURCE_QUEUE_DONE))

        producer_task = asyncio.create_task(_pump_source())

        try:
            while True:
                item = await source_queue.get()
                if item is SOURCE_QUEUE_DONE:
                    break
                if isinstance(item, SourceQueueError):
                    raise item.exc
                yield cast("SourceRecord", item)
        finally:
            while not source_queue.empty():
                source_queue.get_nowait()
            if not producer_task.done():
                producer_task.cancel()
            with suppress(asyncio.CancelledError):
                await producer_task
            while not source_queue.empty():
                source_queue.get_nowait()

    @staticmethod
    def make_metrics_accumulator(flush_interval: int) -> Any:
        return MetricsAccumulator(flush_interval=flush_interval)

    @staticmethod
    def rust_available() -> bool:
        return _RUST_AVAILABLE
