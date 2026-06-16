"""Writer transport — owns write invocation mechanics for the delivery layer."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from agora.core.fencing import assert_run_fence_active

if TYPE_CHECKING:
    from agora.core.context import PipelineContext
    from agora.core.writer import Writer, WriteResult


@dataclass(slots=True)
class WriterTransport:
    """Owns write invocation mechanics: calling the writer, timing, and flush metrics.

    DeliveryEngine calls this for every write operation and receives raw
    WriteResult objects back. It never touches the writer directly.
    """

    writer: Writer[Any]

    async def write_one(
        self,
        ctx: PipelineContext,
        record: Any,
    ) -> WriteResult:
        t0 = time.monotonic()
        await assert_run_fence_active(ctx)
        with ctx.trace_span("writer.write", writer=type(self.writer).__name__):
            result = await self.writer.write(record)
        self._record_flush_metrics(ctx, 1, (time.monotonic() - t0) * 1000)
        return result

    async def write_batch(
        self,
        ctx: PipelineContext,
        records: list[Any],
    ) -> tuple[list[WriteResult], float]:
        """Write a batch and return (results, elapsed_ms)."""
        t0 = time.monotonic()
        await assert_run_fence_active(ctx)
        with ctx.trace_span(
            "writer.write_batch",
            writer=type(self.writer).__name__,
            batch_size=len(records),
        ):
            results = await self.writer.write_batch(records)
        elapsed_ms = (time.monotonic() - t0) * 1000
        self._record_flush_metrics(ctx, len(records), elapsed_ms)
        return results, elapsed_ms

    async def write_arrow_batch(
        self,
        ctx: PipelineContext,
        arrow_sink: Any,
        batch: Any,
    ) -> None:
        batch_size = len(batch)
        t0 = time.monotonic()
        await assert_run_fence_active(ctx)
        with ctx.trace_span(
            "writer.write_arrow_batch",
            writer=type(self.writer).__name__,
            batch_size=batch_size,
        ):
            await arrow_sink.write_arrow_batch(batch)
        self._record_flush_metrics(ctx, batch_size, (time.monotonic() - t0) * 1000)

    @staticmethod
    def _record_flush_metrics(
        ctx: PipelineContext,
        batch_size: int,
        elapsed_ms: float,
    ) -> None:
        ctx.metrics.runtime.writer_flush_count += 1
        ctx.metrics.runtime.writer_flush_time_ms += elapsed_ms
        ctx.metrics.runtime.writer_flush_max_batch_size = max(
            ctx.metrics.runtime.writer_flush_max_batch_size,
            batch_size,
        )

    async def flush(self, ctx: PipelineContext | None = None) -> None:
        if ctx is not None:
            await assert_run_fence_active(ctx)
        await self.writer.flush()

    async def close(self) -> None:
        await self.writer.close()

    async def open(self) -> None:
        await self.writer.open()
