"""Sink, DLQ, and checkpoint delivery for processed pipeline records."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from agora.core.checkpoint import (
    Checkpoint,
    CheckpointStore,
    CheckpointValue,
)
from agora.core.dlq import DLQRecord
from agora.core.types import CheckpointFailurePolicy, DLQFailurePolicy, SinkFailurePolicy

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.core.context import PipelineContext
    from agora.core.middleware import MiddlewareFailure
    from agora.core.sink import BaseSink
    from agora.core.writer import Writer


@dataclass(slots=True)
class SourceQueueError:
    exc: Exception


@dataclass(slots=True)
class SourceRecord:
    raw: Any
    checkpoint: CheckpointValue = None
    on_success: Callable[[], Awaitable[None]] | None = None


@dataclass(slots=True)
class PendingWrite:
    processed: Any
    raw: Any
    checkpoint: CheckpointValue = None
    on_success: Callable[[], Awaitable[None]] | None = None


@dataclass(slots=True)
class ProcessedSourceRecord:
    source_record: SourceRecord
    result: Any | None
    failure: MiddlewareFailure | None = None


class RecordDeliveryError(RuntimeError):
    """Raised when sink delivery must fail the pipeline."""

    def __init__(self, exc: Exception) -> None:
        super().__init__(str(exc))
        self.original = exc


@dataclass
class CheckpointState:
    """Encapsulates mutable checkpoint state during pipeline execution."""

    processed_count: int = 0
    last_saved_value: CheckpointValue = None

    def increment(self) -> None:
        self.processed_count += 1

    def should_save(self, current_value: CheckpointValue, every: int) -> bool:
        if current_value is None or current_value == self.last_saved_value:
            return False
        return self.processed_count % every == 0

    def mark_saved(self, value: CheckpointValue) -> None:
        self.last_saved_value = value


@dataclass(slots=True)
class RunState:
    """Mutable execution state shared by runtime helpers."""

    ctx: PipelineContext
    checkpoint_state: CheckpointState
    pending_writes: list[PendingWrite]
    processed_count: int = 0


@dataclass(slots=True)
class RecordDeliveryCoordinator:
    """Owns sink, DLQ, and checkpoint side effects for processed records."""

    writer: Writer[Any]
    source_name: str
    current_checkpoint: Callable[[], CheckpointValue]
    dlq_sink: BaseSink[DLQRecord] | None
    dlq_failure_policy: DLQFailurePolicy
    sink_failure_policy: SinkFailurePolicy
    checkpoint_store: CheckpointStore | None
    checkpoint_failure_policy: CheckpointFailurePolicy
    checkpoint_key: str
    checkpoint_every: int

    async def write_to_dlq(
        self,
        ctx: PipelineContext,
        stage: str,
        exc: Exception,
        *,
        record: Any = None,
        original_record: Any | None = None,
        processed_record: Any | None = None,
        source: str | None = None,
        checkpoint: Any | None = None,
        middleware: str | None = None,
        sink: str | None = None,
    ) -> bool:
        if self.dlq_sink is None:
            return False

        try:
            with ctx.trace_span(
                "dlq.write",
                stage=stage,
                source=source or self.source_name,
                sink=sink or getattr(self.dlq_sink, "sink_name", "dlq"),
            ):
                await self.dlq_sink.write(
                    DLQRecord(
                        pipeline_id=ctx.pipeline_id,
                        run_id=ctx.run_id,
                        stage=stage,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                        record=(
                            original_record
                            if original_record is not None
                            else record
                            if record is not None
                            else processed_record
                        ),
                        source=source or self.source_name,
                        checkpoint=checkpoint
                        if checkpoint is not None
                        else self.current_checkpoint(),
                        middleware=middleware,
                        sink=sink,
                        original_record=original_record,
                        processed_record=processed_record,
                    )
                )
            return True
        except Exception:
            ctx.metrics.runtime.dlq_failure_count += 1
            ctx.log.exception("pipeline_dlq_write_error", stage=stage, error=str(exc))
            if self.dlq_failure_policy == DLQFailurePolicy.RAISE:
                raise
            return False

    def prepare_checkpoint(
        self,
        ctx: PipelineContext,
        checkpoint_state: CheckpointState,
        checkpoint_value: CheckpointValue,
    ) -> Checkpoint | None:
        if self.checkpoint_store is None or checkpoint_value is None:
            return None

        checkpoint_state.increment()
        if not checkpoint_state.should_save(checkpoint_value, self.checkpoint_every):
            return None

        return Checkpoint(
            pipeline_id=ctx.pipeline_id,
            run_id=ctx.run_id,
            source=self.source_name,
            value=checkpoint_value,
        )

    async def persist_checkpoint(
        self,
        ctx: PipelineContext,
        checkpoint_state: CheckpointState,
        checkpoint: Checkpoint,
        *,
        batch_size: int = 1,
    ) -> None:
        t0 = time.monotonic()
        try:
            if self.checkpoint_store is None:
                raise RuntimeError("checkpoint_store is None — cannot persist checkpoint")
            with ctx.trace_span(
                "checkpoint.save",
                checkpoint_key=self.checkpoint_key,
                source=self.source_name,
                batch_size=batch_size,
            ):
                await self.checkpoint_store.save(self.checkpoint_key, checkpoint)
        except Exception:
            ctx.metrics.runtime.checkpoint_failure_count += 1
            if self.checkpoint_failure_policy == CheckpointFailurePolicy.LOG_AND_CONTINUE:
                ctx.log.exception(
                    "pipeline_checkpoint_save_error",
                    checkpoint_key=self.checkpoint_key,
                    source=self.source_name,
                )
                checkpoint_state.mark_saved(checkpoint.value)
                return
            raise
        checkpoint_state.mark_saved(checkpoint.value)
        ctx.metrics.runtime.checkpoint_save_time_ms += (time.monotonic() - t0) * 1000
        ctx.metrics.last_checkpoint = checkpoint
        ctx.metrics.runtime.checkpoint_save_count += 1
        ctx.metrics.runtime.checkpoint_save_max_batch_size = max(
            ctx.metrics.runtime.checkpoint_save_max_batch_size,
            batch_size,
        )

    async def save_checkpoint(
        self,
        ctx: PipelineContext,
        checkpoint_state: CheckpointState,
        checkpoint_value: CheckpointValue,
    ) -> None:
        checkpoint = self.prepare_checkpoint(ctx, checkpoint_state, checkpoint_value)
        if checkpoint is None:
            return
        await self.persist_checkpoint(ctx, checkpoint_state, checkpoint, batch_size=1)

    async def drop_record(
        self,
        state: RunState,
        checkpoint_value: CheckpointValue,
        *,
        failure: MiddlewareFailure | None = None,
        on_success: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        if failure is not None:
            await self.write_to_dlq(
                ctx=state.ctx,
                stage=failure.stage,
                exc=failure.exception,
                record=failure.record,
                original_record=failure.record,
                checkpoint=checkpoint_value,
                middleware=failure.middleware,
            )
            state.ctx.metrics.records_errored += 1
        else:
            state.ctx.metrics.records_dropped += 1
        await self.save_checkpoint(state.ctx, state.checkpoint_state, checkpoint_value)
        if on_success is not None:
            await on_success()

    async def write_processed_record(
        self,
        state: RunState,
        result: Any | None,
        raw_record: Any,
        checkpoint_value: CheckpointValue,
        *,
        failure: MiddlewareFailure | None = None,
        on_success: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        if result is None:
            await self.drop_record(state, checkpoint_value, failure=failure, on_success=on_success)
            return

        try:
            with state.ctx.trace_span("writer.write", writer=type(self.writer).__name__):
                write_result = await self.writer.write(result)
        except Exception as exc:
            state.ctx.log.exception("pipeline_write_error")
            routed = await self.write_to_dlq(
                ctx=state.ctx,
                stage="sink_write",
                exc=exc,
                record=raw_record,
                original_record=raw_record,
                processed_record=result,
                checkpoint=checkpoint_value,
            )
            state.ctx.metrics.records_errored += 1
            if routed or self.sink_failure_policy == SinkFailurePolicy.LOG_AND_CONTINUE:
                await self.save_checkpoint(state.ctx, state.checkpoint_state, checkpoint_value)
                if routed and on_success is not None:
                    await on_success()
            elif self.sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
                raise RecordDeliveryError(exc) from exc
            return

        if write_result.errors:
            if write_result.written:
                state.ctx.metrics.records_written += 1
            routed_all = True
            for err in write_result.errors:
                state.ctx.log.error("pipeline_sink_error", error=str(err))
                routed = await self.write_to_dlq(
                    ctx=state.ctx,
                    stage="sink_write",
                    exc=err,
                    record=raw_record,
                    original_record=raw_record,
                    processed_record=result,
                    checkpoint=checkpoint_value,
                )
                routed_all = routed_all and routed
            state.ctx.metrics.records_errored += len(write_result.errors)
            if routed_all or self.sink_failure_policy == SinkFailurePolicy.LOG_AND_CONTINUE:
                await self.save_checkpoint(state.ctx, state.checkpoint_state, checkpoint_value)
                if on_success is not None:
                    await on_success()
            elif self.sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
                raise RecordDeliveryError(write_result.errors[0]) from write_result.errors[0]
            return

        if not write_result.written:
            state.ctx.log.warning("pipeline_unrouted_record")
            state.ctx.metrics.records_dropped += 1
            await self.save_checkpoint(state.ctx, state.checkpoint_state, checkpoint_value)
            if on_success is not None:
                await on_success()
            return

        state.ctx.metrics.records_written += 1
        await self.save_checkpoint(state.ctx, state.checkpoint_state, checkpoint_value)
        if on_success is not None:
            await on_success()

    async def flush_pending_writes(self, state: RunState) -> None:
        if not state.pending_writes:
            return

        batch = list(state.pending_writes)
        state.pending_writes.clear()
        state.ctx.metrics.runtime.writer_flush_count += 1
        state.ctx.metrics.runtime.writer_flush_max_batch_size = max(
            state.ctx.metrics.runtime.writer_flush_max_batch_size,
            len(batch),
        )

        t0 = time.monotonic()
        delivered_hooks: list[Callable[[], Awaitable[None]]] = []
        try:
            with state.ctx.trace_span(
                "writer.write_batch",
                writer=type(self.writer).__name__,
                batch_size=len(batch),
            ):
                write_results = await self.writer.write_batch([item.processed for item in batch])
        except Exception as exc:
            state.ctx.metrics.runtime.writer_flush_time_ms += (time.monotonic() - t0) * 1000
            state.ctx.log.exception("pipeline_write_batch_error", batch_size=len(batch))
            unrouted_error: Exception | None = None
            pending_checkpoint: Checkpoint | None = None
            pending_checkpoint_batch_size = 0
            for item in batch:
                routed = await self.write_to_dlq(
                    ctx=state.ctx,
                    stage="sink_write",
                    exc=exc,
                    record=item.raw,
                    original_record=item.raw,
                    processed_record=item.processed,
                    checkpoint=item.checkpoint,
                )
                state.ctx.metrics.records_errored += 1
                if routed or self.sink_failure_policy == SinkFailurePolicy.LOG_AND_CONTINUE:
                    checkpoint = self.prepare_checkpoint(
                        state.ctx, state.checkpoint_state, item.checkpoint
                    )
                    if checkpoint is not None:
                        pending_checkpoint = checkpoint
                        pending_checkpoint_batch_size += 1
                    if item.on_success is not None:
                        delivered_hooks.append(item.on_success)
                elif unrouted_error is None:
                    if pending_checkpoint is not None:
                        await self.persist_checkpoint(
                            state.ctx,
                            state.checkpoint_state,
                            pending_checkpoint,
                            batch_size=pending_checkpoint_batch_size,
                        )
                        pending_checkpoint = None
                        pending_checkpoint_batch_size = 0
                    unrouted_error = exc
            if pending_checkpoint is not None:
                await self.persist_checkpoint(
                    state.ctx,
                    state.checkpoint_state,
                    pending_checkpoint,
                    batch_size=pending_checkpoint_batch_size,
                )
            for hook in delivered_hooks:
                await hook()
            if (
                unrouted_error is not None
                and self.sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED
            ):
                raise RecordDeliveryError(unrouted_error) from unrouted_error
            return
        state.ctx.metrics.runtime.writer_flush_time_ms += (time.monotonic() - t0) * 1000

        if len(write_results) != len(batch):
            raise RuntimeError(
                "Writer.write_batch() must return one WriteResult per input record. "
                f"Expected {len(batch)}, got {len(write_results)}."
            )

        pending_checkpoint = None
        pending_checkpoint_batch_size = 0

        for item, write_result in zip(batch, write_results, strict=True):
            if write_result.errors:
                if write_result.written:
                    state.ctx.metrics.records_written += 1
                routed_all = True
                for err in write_result.errors:
                    state.ctx.log.error("pipeline_sink_error", error=str(err))
                    routed = await self.write_to_dlq(
                        ctx=state.ctx,
                        stage="sink_write",
                        exc=err,
                        record=item.raw,
                        original_record=item.raw,
                        processed_record=item.processed,
                        checkpoint=item.checkpoint,
                    )
                    routed_all = routed_all and routed
                state.ctx.metrics.records_errored += len(write_result.errors)
                if routed_all or self.sink_failure_policy == SinkFailurePolicy.LOG_AND_CONTINUE:
                    checkpoint = self.prepare_checkpoint(
                        state.ctx, state.checkpoint_state, item.checkpoint
                    )
                    if checkpoint is not None:
                        pending_checkpoint = checkpoint
                        pending_checkpoint_batch_size += 1
                    if item.on_success is not None:
                        delivered_hooks.append(item.on_success)
                elif self.sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
                    if pending_checkpoint is not None:
                        await self.persist_checkpoint(
                            state.ctx,
                            state.checkpoint_state,
                            pending_checkpoint,
                            batch_size=pending_checkpoint_batch_size,
                        )
                    for hook in delivered_hooks:
                        await hook()
                    raise RecordDeliveryError(write_result.errors[0]) from write_result.errors[0]
                continue

            if not write_result.written:
                state.ctx.log.warning("pipeline_unrouted_record")
                state.ctx.metrics.records_dropped += 1
                checkpoint = self.prepare_checkpoint(
                    state.ctx, state.checkpoint_state, item.checkpoint
                )
                if checkpoint is not None:
                    pending_checkpoint = checkpoint
                    pending_checkpoint_batch_size += 1
                if item.on_success is not None:
                    delivered_hooks.append(item.on_success)
                continue

            state.ctx.metrics.records_written += 1
            checkpoint = self.prepare_checkpoint(state.ctx, state.checkpoint_state, item.checkpoint)
            if checkpoint is not None:
                pending_checkpoint = checkpoint
                pending_checkpoint_batch_size += 1
            if item.on_success is not None:
                delivered_hooks.append(item.on_success)

        if pending_checkpoint is not None:
            await self.persist_checkpoint(
                state.ctx,
                state.checkpoint_state,
                pending_checkpoint,
                batch_size=pending_checkpoint_batch_size,
            )
        for hook in delivered_hooks:
            await hook()

    async def queue_processed_record(
        self,
        state: RunState,
        result: Any | None,
        raw_record: Any,
        checkpoint_value: CheckpointValue,
        writer_batch_size: int,
        *,
        failure: MiddlewareFailure | None = None,
        on_success: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        if result is None:
            await self.drop_record(state, checkpoint_value, failure=failure, on_success=on_success)
            return

        state.pending_writes.append(
            PendingWrite(
                processed=result,
                raw=raw_record,
                checkpoint=checkpoint_value,
                on_success=on_success,
            )
        )
        if len(state.pending_writes) >= writer_batch_size:
            await self.flush_pending_writes(state)

    async def dispatch_processed_result(
        self,
        state: RunState,
        result: Any | None,
        raw_record: Any,
        checkpoint_value: CheckpointValue,
        writer_batch_size: int,
        *,
        failure: MiddlewareFailure | None = None,
        on_success: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        if writer_batch_size <= 1:
            await self.write_processed_record(
                state,
                result,
                raw_record,
                checkpoint_value,
                failure=failure,
                on_success=on_success,
            )
            return
        await self.queue_processed_record(
            state,
            result,
            raw_record,
            checkpoint_value,
            writer_batch_size,
            failure=failure,
            on_success=on_success,
        )
