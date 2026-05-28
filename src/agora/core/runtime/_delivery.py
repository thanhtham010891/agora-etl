"""Sink, DLQ, and checkpoint delivery for processed pipeline records."""

from __future__ import annotations

import time
from abc import ABC
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

    from agora.core.batch import BatchFailure
    from agora.core.context import PipelineContext
    from agora.core.middleware import MiddlewareFailure
    from agora.core.runtime._writer_transport import WriterTransport
    from agora.core.sink import BaseSink


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


class CommitOutcome(ABC):  # noqa: B024
    """Abstract base for typed delivery outcomes."""

    __slots__ = ()


@dataclass(slots=True)
class CheckpointedOutcome(CommitOutcome):
    """Base for outcomes that carry a checkpoint and optional hook."""

    checkpoint: CheckpointValue
    on_success: Callable[[], Awaitable[None]] | None = None


@dataclass(slots=True)
class Written(CheckpointedOutcome):
    """Record was durably written to the sink."""


@dataclass(slots=True)
class Dropped(CheckpointedOutcome):
    """Record was filtered or had no sink route — not an error."""


@dataclass(slots=True)
class ErroredRouted(CheckpointedOutcome):
    """Record failed but was successfully routed to the DLQ."""


@dataclass(slots=True)
class ErroredUnrouted(CommitOutcome):
    """Record failed and could not be routed to the DLQ."""

    exc: Exception


@dataclass
class CheckpointState:
    """Encapsulates mutable checkpoint state during pipeline execution."""

    processed_count: int = 0
    last_saved_value: CheckpointValue = None

    def increment(self) -> None:
        self.processed_count += 1

    def increment_by(self, count: int) -> None:
        self.processed_count += max(0, count)

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
class DeliveryEngine:
    """Own sink, DLQ, and checkpoint side effects for processed records."""

    transport: WriterTransport
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

    async def _resolve_write_result(
        self,
        ctx: PipelineContext,
        write_result: Any,
        raw_record: Any,
        processed_record: Any,
        checkpoint_value: CheckpointValue,
        on_success: Callable[[], Awaitable[None]] | None,
    ) -> CommitOutcome:
        """Convert a WriteResult into a typed CommitOutcome.

        Metrics are applied only after all DLQ routing is complete to keep
        records_written and records_errored consistent even if DLQ raises.
        """
        if write_result.errors:
            routed_all = True
            for err in write_result.errors:
                ctx.log.error("pipeline_sink_error", error=str(err))
                routed = await self.write_to_dlq(
                    ctx=ctx,
                    stage="sink_write",
                    exc=err,
                    record=raw_record,
                    original_record=raw_record,
                    processed_record=processed_record,
                    checkpoint=checkpoint_value,
                )
                routed_all = routed_all and routed
            # Count one error per record, not per error object.
            ctx.metrics.records_errored += 1
            if write_result.written:
                ctx.metrics.records_written += 1
            if routed_all or self.sink_failure_policy == SinkFailurePolicy.LOG_AND_CONTINUE:
                return ErroredRouted(checkpoint=checkpoint_value, on_success=on_success)
            return ErroredUnrouted(exc=write_result.errors[0])

        if not write_result.written:
            ctx.log.warning("pipeline_unrouted_record")
            ctx.metrics.records_dropped += 1
            return Dropped(checkpoint=checkpoint_value, on_success=on_success)

        ctx.metrics.records_written += 1
        return Written(checkpoint=checkpoint_value, on_success=on_success)

    async def _commit_outcome(
        self,
        state: RunState,
        outcome: CommitOutcome,
    ) -> None:
        """Apply checkpoint and hook side effects for a CommitOutcome."""
        if isinstance(outcome, ErroredUnrouted):
            if self.sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
                raise RecordDeliveryError(outcome.exc) from outcome.exc
            # LOG_AND_CONTINUE: still advance checkpoint so we don't fall behind.
            return
        assert isinstance(outcome, CheckpointedOutcome)
        await self.save_checkpoint(state.ctx, state.checkpoint_state, outcome.checkpoint)
        if outcome.on_success is not None:
            await outcome.on_success()

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
            write_result = await self.transport.write_one(state.ctx, result)
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

        outcome = await self._resolve_write_result(
            state.ctx, write_result, raw_record, result, checkpoint_value, on_success
        )
        await self._commit_outcome(state, outcome)

    async def flush_pending_writes(self, state: RunState) -> None:
        if not state.pending_writes:
            return

        batch = list(state.pending_writes)
        state.pending_writes.clear()

        try:
            write_results, _ = await self.transport.write_batch(
                state.ctx, [item.processed for item in batch]
            )
        except Exception as exc:
            state.ctx.log.exception("pipeline_write_batch_error", batch_size=len(batch))
            pending_checkpoint: Checkpoint | None = None
            pending_checkpoint_batch_size = 0
            delivered_hooks: list[Callable[[], Awaitable[None]]] = []
            unrouted_error: Exception | None = None
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

        if len(write_results) != len(batch):
            raise RuntimeError(
                "Writer.write_batch() must return one WriteResult per input record. "
                f"Expected {len(batch)}, got {len(write_results)}."
            )

        outcomes: list[CommitOutcome] = []
        for item, wr in zip(batch, write_results, strict=True):
            outcome = await self._resolve_write_result(
                state.ctx, wr, item.raw, item.processed, item.checkpoint, item.on_success
            )
            outcomes.append(outcome)

        pending_checkpoint = None
        pending_checkpoint_batch_size = 0
        delivered_hooks = []

        for outcome in outcomes:
            if isinstance(outcome, ErroredUnrouted):
                # Flush accumulated checkpoint/hooks before raising or continuing.
                if pending_checkpoint is not None:
                    await self.persist_checkpoint(
                        state.ctx,
                        state.checkpoint_state,
                        pending_checkpoint,
                        batch_size=pending_checkpoint_batch_size,
                    )
                    pending_checkpoint = None
                    pending_checkpoint_batch_size = 0
                for hook in delivered_hooks:
                    await hook()
                delivered_hooks = []
                if self.sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
                    raise RecordDeliveryError(outcome.exc) from outcome.exc
                # LOG_AND_CONTINUE: skip checkpoint advancement for this record.
                continue
            assert isinstance(outcome, CheckpointedOutcome)
            checkpoint = self.prepare_checkpoint(
                state.ctx, state.checkpoint_state, outcome.checkpoint
            )
            if checkpoint is not None:
                pending_checkpoint = checkpoint
                pending_checkpoint_batch_size += 1
            if outcome.on_success is not None:
                delivered_hooks.append(outcome.on_success)

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

    async def flush_batch_direct(
        self,
        state: RunState,
        processed_list: list[Any],
        raw_list: list[Any],
        checkpoint_list: list[Any],
    ) -> None:
        """Flush a pre-built batch directly to the writer — no PendingWrite allocation.

        Fast path for the Rust LinearBatchBuffer integration. Assumes:
        - No dropped records (None values already filtered by caller)
        - No per-record on_success hooks
        """
        if not processed_list:
            return

        try:
            write_results, _ = await self.transport.write_batch(state.ctx, processed_list)
        except Exception as exc:
            state.ctx.log.exception("pipeline_write_batch_error", batch_size=len(processed_list))
            state.ctx.metrics.records_errored += len(processed_list)
            if self.sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
                raise RecordDeliveryError(exc) from exc
            return

        if all(wr.ok for wr in write_results):
            state.ctx.metrics.records_written += len(processed_list)
            if checkpoint_list and self.checkpoint_store is not None:
                last_checkpoint = checkpoint_list[-1]
                if last_checkpoint is not None:
                    await self.save_batch_checkpoint(state, last_checkpoint, len(processed_list))
            return

        first_unrouted_error: Exception | None = None
        for processed, raw, checkpoint, wr in zip(
            processed_list, raw_list, checkpoint_list, write_results, strict=True
        ):
            if wr.errors:
                state.ctx.metrics.records_errored += 1
                routed = True
                for err in wr.errors:
                    ok = await self.write_to_dlq(
                        ctx=state.ctx,
                        stage="sink_write",
                        exc=err,
                        record=raw,
                        original_record=raw,
                        processed_record=processed,
                        checkpoint=checkpoint,
                    )
                    routed = routed and ok
                if (
                    not routed
                    and self.sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED
                    and first_unrouted_error is None
                ):
                    first_unrouted_error = wr.errors[0]
            elif wr.written:
                state.ctx.metrics.records_written += 1

        if first_unrouted_error is not None:
            raise RecordDeliveryError(first_unrouted_error) from first_unrouted_error

    async def write_batch_result(
        self,
        state: RunState,
        results: list[Any | None],
        raw_batch: list[Any],
        checkpoint_value: CheckpointValue,
        *,
        batch_failure: BatchFailure | None = None,
    ) -> None:
        if batch_failure is not None:
            state.ctx.metrics.records_errored += len(raw_batch)
            if self.dlq_sink is not None:
                for raw_record in raw_batch:
                    await self.write_to_dlq(
                        ctx=state.ctx,
                        stage="batch_middleware",
                        exc=batch_failure.exception,
                        record=raw_record,
                        original_record=raw_record,
                        middleware=batch_failure.middleware,
                        checkpoint=checkpoint_value,
                    )
                await self.save_batch_checkpoint(state, checkpoint_value, len(raw_batch))
            elif self.sink_failure_policy == SinkFailurePolicy.LOG_AND_CONTINUE:
                state.ctx.log.exception(
                    "batch_middleware_error_log_and_continue",
                    middleware=batch_failure.middleware,
                    batch_size=len(raw_batch),
                    error=str(batch_failure.exception),
                )
                await self.save_batch_checkpoint(state, checkpoint_value, len(raw_batch))
            else:
                raise RecordDeliveryError(batch_failure.exception) from batch_failure.exception
            return

        active_items = [
            (raw_record, processed_record)
            for raw_record, processed_record in zip(raw_batch, results, strict=True)
            if processed_record is not None
        ]
        to_write = [processed_record for _raw_record, processed_record in active_items]
        dropped = len(results) - len(to_write)
        state.ctx.metrics.records_dropped += dropped

        if not to_write:
            await self.save_batch_checkpoint(state, checkpoint_value, len(results))
            return

        try:
            write_results, _ = await self.transport.write_batch(state.ctx, to_write)
        except Exception as exc:
            state.ctx.log.exception("batch_write_error", batch_size=len(to_write))
            routed = True
            for raw_record, processed_record in active_items:
                ok = await self.write_to_dlq(
                    ctx=state.ctx,
                    stage="sink_write",
                    exc=exc,
                    record=raw_record,
                    original_record=raw_record,
                    processed_record=processed_record,
                    checkpoint=checkpoint_value,
                )
                routed = routed and ok
            state.ctx.metrics.records_errored += len(to_write)
            if routed or self.sink_failure_policy == SinkFailurePolicy.LOG_AND_CONTINUE:
                await self.save_batch_checkpoint(state, checkpoint_value, len(results))
            elif self.sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
                raise RecordDeliveryError(exc) from exc
            return

        if len(write_results) != len(to_write):
            raise RuntimeError(
                "Writer.write_batch() must return one WriteResult per batch input record. "
                f"Expected {len(to_write)}, got {len(write_results)}."
            )

        # Resolve each write result into a CommitOutcome.
        outcomes: list[CommitOutcome] = []
        for (raw_record, processed_record), wr in zip(active_items, write_results, strict=True):
            outcome = await self._resolve_write_result(
                state.ctx, wr, raw_record, processed_record, checkpoint_value, None
            )
            outcomes.append(outcome)

        errored_unrouted: Exception | None = None
        for outcome in outcomes:
            if isinstance(outcome, ErroredUnrouted) and errored_unrouted is None:
                errored_unrouted = outcome.exc
            # Written/Dropped/ErroredRouted all advance the batch checkpoint below.

        routed_all = errored_unrouted is None
        if routed_all or self.sink_failure_policy == SinkFailurePolicy.LOG_AND_CONTINUE:
            await self.save_batch_checkpoint(state, checkpoint_value, len(results))
        if not routed_all and self.sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
            raise RecordDeliveryError(errored_unrouted) from errored_unrouted  # type: ignore[arg-type]

    async def save_batch_checkpoint(
        self,
        state: RunState,
        checkpoint_value: CheckpointValue,
        batch_size: int,
    ) -> None:
        """Save checkpoint for an entire batch while honoring checkpoint_every."""
        if self.checkpoint_store is None or checkpoint_value is None or batch_size <= 0:
            return
        state.checkpoint_state.increment_by(batch_size)
        if not state.checkpoint_state.should_save(checkpoint_value, self.checkpoint_every):
            return
        checkpoint = Checkpoint(
            pipeline_id=state.ctx.pipeline_id,
            run_id=state.ctx.run_id,
            source=self.source_name,
            value=checkpoint_value,
        )
        await self.persist_checkpoint(
            state.ctx,
            state.checkpoint_state,
            checkpoint,
            batch_size=batch_size,
        )
