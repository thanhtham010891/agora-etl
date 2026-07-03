"""Sink, DLQ, and checkpoint delivery for processed pipeline records."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from agora.core.fencing import FenceLostError
from agora.core.runtime._delivery_batch_write import (
    write_batch_filtered,
    write_batch_middleware_failure,
    write_batch_passthrough,
)
from agora.core.runtime._delivery_batching import commit_outcomes, flush_batch_outcomes
from agora.core.runtime._delivery_pending import (
    close_pending_write_owner,
    ensure_pending_write_owner,
    run_pending_write_owner,
    uses_pending_write_owner,
    wait_for_pending_write_capacity,
)
from agora.core.runtime._delivery_results import (
    commit_outcome,
    resolve_write_result,
    validate_write_result_count,
)
from agora.core.runtime._delivery_state import CheckpointState, make_checkpoint_state
from agora.core.runtime._delivery_support import CheckpointManager, DLQWriter
from agora.core.runtime._delivery_types import (
    CheckpointedOutcome,
    CommitOutcome,
    Dropped,
    ErroredRouted,
    ErroredUnrouted,
    PendingWrite,
    ProcessedSourceRecord,
    RecordDeliveryError,
    RunState,
    SourceQueueError,
    SourceRecord,
    Written,
)
from agora.core.types import CheckpointFailurePolicy, DLQFailurePolicy, SinkFailurePolicy

__all__ = [
    "CheckpointState",
    "CheckpointedOutcome",
    "CommitOutcome",
    "DeliveryEngine",
    "Dropped",
    "ErroredRouted",
    "ErroredUnrouted",
    "PendingWrite",
    "ProcessedSourceRecord",
    "RecordDeliveryError",
    "RunState",
    "SourceQueueError",
    "SourceRecord",
    "Written",
    "make_checkpoint_state",
]

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.core.batch import BatchFailure
    from agora.core.checkpoint import Checkpoint, CheckpointStore, CheckpointValue
    from agora.core.context import PipelineContext
    from agora.core.dlq import DLQRecord
    from agora.core.middleware import MiddlewareFailure
    from agora.core.runtime._writer_transport import WriterTransport
    from agora.core.sink import BaseSink


@dataclass(slots=True)
class DeliveryEngine:
    """Own sink, DLQ, and checkpoint side effects for processed records."""

    transport: WriterTransport
    source_name: str
    current_checkpoint: Callable[[], CheckpointValue]
    dlq_sink: BaseSink[DLQRecord] | None
    dlq_failure_policy: DLQFailurePolicy
    dlq_redactor: Callable[[Any], Any] | None
    sink_failure_policy: SinkFailurePolicy
    checkpoint_store: CheckpointStore | None
    checkpoint_failure_policy: CheckpointFailurePolicy
    checkpoint_key: str
    checkpoint_every: int
    batch_flush_interval_ms: int | None = None
    _checkpoints: CheckpointManager = field(init=False, repr=False)
    _dlq: DLQWriter = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._checkpoints = CheckpointManager(
            source_name=self.source_name,
            checkpoint_store=self.checkpoint_store,
            checkpoint_failure_policy=self.checkpoint_failure_policy,
            checkpoint_key=self.checkpoint_key,
            checkpoint_every=self.checkpoint_every,
        )
        self._dlq = DLQWriter(
            source_name=self.source_name,
            current_checkpoint=self.current_checkpoint,
            dlq_sink=self.dlq_sink,
            dlq_failure_policy=self.dlq_failure_policy,
            dlq_redactor=self.dlq_redactor,
        )

    def _success_hook_for(
        self,
        ctx: PipelineContext,
        raw_record: Any,
        processed_record: Any | None,
        on_success: Callable[[], Awaitable[None]] | None,
    ) -> Callable[[], Awaitable[None]] | None:
        hooks: list[Callable[[], Awaitable[None]]] = []
        if on_success is not None:
            hooks.append(on_success)
        if processed_record is None:
            hooks.extend(ctx.pop_success_hooks(raw_record))
        else:
            hooks.extend(ctx.pop_success_hooks(raw_record, processed_record))
        if not hooks:
            return None

        async def _run_success_hooks() -> None:
            for hook in hooks:
                await hook()

        return _run_success_hooks

    def _uses_pending_write_owner(
        self,
        writer_batch_size: int,
    ) -> bool:
        return uses_pending_write_owner(
            writer_batch_size=writer_batch_size,
            batch_flush_interval_ms=self.batch_flush_interval_ms,
        )

    async def _ensure_pending_write_owner(
        self,
        state: RunState,
        writer_batch_size: int,
    ) -> None:
        await ensure_pending_write_owner(
            state=state,
            writer_batch_size=writer_batch_size,
            batch_flush_interval_ms=self.batch_flush_interval_ms,
            owner_name=f"{state.ctx.pipeline_id}-pending-write-owner",
            owner_runner=self._run_pending_write_owner,
        )

    async def _run_pending_write_owner(self, state: RunState) -> None:
        await run_pending_write_owner(
            state=state,
            flush_once=self._flush_pending_writes_once,
        )

    async def _wait_for_pending_write_capacity(
        self,
        state: RunState,
        writer_batch_size: int,
    ) -> None:
        await wait_for_pending_write_capacity(
            state=state,
            writer_batch_size=writer_batch_size,
        )

    async def close_pending_write_owner(self, state: RunState) -> None:
        await close_pending_write_owner(state)

    def _pending_write_uses_owner(
        self,
        state: RunState,
        writer_batch_size: int,
    ) -> bool:
        uses_owner = state.pending_write_uses_owner
        if uses_owner is None:
            uses_owner = self._uses_pending_write_owner(writer_batch_size)
            state.pending_write_uses_owner = uses_owner
        return uses_owner

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
        return await self._dlq.write(
            ctx,
            stage,
            exc,
            record=record,
            original_record=original_record,
            processed_record=processed_record,
            source=source,
            checkpoint=checkpoint,
            middleware=middleware,
            sink=sink,
        )

    def prepare_checkpoint(
        self,
        ctx: PipelineContext,
        checkpoint_state: CheckpointState,
        checkpoint_value: CheckpointValue,
    ) -> Checkpoint | None:
        return self._checkpoints.prepare(ctx, checkpoint_state, checkpoint_value)

    async def persist_checkpoint(
        self,
        ctx: PipelineContext,
        checkpoint_state: CheckpointState,
        checkpoint: Checkpoint,
        *,
        batch_size: int = 1,
    ) -> None:
        await self._checkpoints.persist(
            ctx,
            checkpoint_state,
            checkpoint,
            batch_size=batch_size,
        )

    async def save_checkpoint(
        self,
        ctx: PipelineContext,
        checkpoint_state: CheckpointState,
        checkpoint_value: CheckpointValue,
    ) -> None:
        await self._checkpoints.save(ctx, checkpoint_state, checkpoint_value)

    async def drop_record(
        self,
        state: RunState,
        checkpoint_value: CheckpointValue,
        *,
        failure: MiddlewareFailure | None = None,
        on_success: Callable[[], Awaitable[None]] | None = None,
        hook_record: Any | None = None,
    ) -> None:
        if hook_record is not None:
            on_success = self._success_hook_for(state.ctx, hook_record, None, on_success)
        elif failure is not None:
            on_success = self._success_hook_for(state.ctx, failure.record, None, on_success)
        if failure is not None:
            routed = await self.write_to_dlq(
                ctx=state.ctx,
                stage=failure.stage,
                exc=failure.exception,
                record=failure.record,
                original_record=failure.record,
                checkpoint=checkpoint_value,
                middleware=failure.middleware,
            )
            state.ctx.metrics.records_errored += 1
            if self.dlq_sink is not None and not routed:
                await state.ctx.discard_success_hooks(failure.record)
                if self.checkpoint_store is not None and checkpoint_value is not None:
                    raise RecordDeliveryError(failure.exception) from failure.exception
                return
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
        on_success = self._success_hook_for(ctx, raw_record, processed_record, on_success)
        return await resolve_write_result(
            ctx=ctx,
            write_result=write_result,
            raw_record=raw_record,
            processed_record=processed_record,
            checkpoint_value=checkpoint_value,
            on_success=on_success,
            sink_failure_policy=self.sink_failure_policy,
            write_to_dlq=self.write_to_dlq,
        )

    async def _commit_outcome(
        self,
        state: RunState,
        outcome: CommitOutcome,
    ) -> None:
        await commit_outcome(
            state=state,
            outcome=outcome,
            sink_failure_policy=self.sink_failure_policy,
            save_checkpoint=self.save_checkpoint,
        )

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
            await self.drop_record(
                state,
                checkpoint_value,
                failure=failure,
                on_success=on_success,
                hook_record=raw_record,
            )
            return

        try:
            write_result = await self.transport.write_one(state.ctx, result)
        except FenceLostError:
            raise
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
                if routed:
                    committed_hook = self._success_hook_for(
                        state.ctx,
                        raw_record,
                        result,
                        on_success,
                    )
                    if committed_hook is not None:
                        await committed_hook()
                else:
                    await state.ctx.discard_success_hooks(raw_record, result)
            elif self.sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
                await state.ctx.discard_success_hooks(raw_record, result)
                raise RecordDeliveryError(exc) from exc
            return

        outcome = await self._resolve_write_result(
            state.ctx, write_result, raw_record, result, checkpoint_value, on_success
        )
        await self._commit_outcome(state, outcome)

    async def _flush_batch_outcomes(
        self,
        state: RunState,
        exc: Exception,
        processed_list: list[Any],
        raw_list: list[Any],
        checkpoint_list: list[CheckpointValue],
        on_success_list: list[Callable[[], Awaitable[None]] | None],
    ) -> None:
        """Handle a whole-batch write failure: DLQ-route each record, accumulate
        checkpoint/hooks, then flush them together and raise if needed.

        Shared by flush_pending_writes and flush_batch_direct to avoid duplication.
        """
        await flush_batch_outcomes(
            state=state,
            exc=exc,
            processed_list=processed_list,
            raw_list=raw_list,
            checkpoint_list=checkpoint_list,
            on_success_list=on_success_list,
            sink_failure_policy=self.sink_failure_policy,
            write_to_dlq=self.write_to_dlq,
            prepare_checkpoint=self.prepare_checkpoint,
            persist_checkpoint=self.persist_checkpoint,
        )

    async def _commit_outcomes(
        self,
        state: RunState,
        outcomes: list[CommitOutcome],
    ) -> None:
        """Drain a list of CommitOutcomes: accumulate checkpoint/hooks, raise on
        unrouted errors.

        Shared by flush_pending_writes and flush_batch_direct to avoid duplication.
        """
        await commit_outcomes(
            state=state,
            outcomes=outcomes,
            sink_failure_policy=self.sink_failure_policy,
            prepare_checkpoint=self.prepare_checkpoint,
            persist_checkpoint=self.persist_checkpoint,
        )

    async def _commit_pending_write_success_batch(
        self,
        state: RunState,
        batch: list[PendingWrite],
    ) -> None:
        state.ctx.metrics.records_written += len(batch)

        pending_checkpoint: Checkpoint | None = None
        pending_checkpoint_batch_size = 0
        delivered_hooks: list[Callable[[], Awaitable[None]]] = []

        for item in batch:
            checkpoint = self.prepare_checkpoint(
                state.ctx,
                state.checkpoint_state,
                item.checkpoint,
            )
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

    async def _flush_pending_writes_once(self, state: RunState) -> None:
        if not state.pending_writes:
            return

        batch = state.pending_writes
        state.pending_writes = []

        try:
            write_results, _ = await self.transport.write_batch(
                state.ctx, [item.processed for item in batch]
            )
        except FenceLostError:
            raise
        except Exception as exc:
            state.ctx.log.exception("pipeline_write_batch_error", batch_size=len(batch))
            await self._flush_batch_outcomes(
                state,
                exc,
                processed_list=[item.processed for item in batch],
                raw_list=[item.raw for item in batch],
                checkpoint_list=[item.checkpoint for item in batch],
                on_success_list=[item.on_success for item in batch],
            )
            return

        validate_write_result_count(expected=len(batch), actual=len(write_results))

        if all(wr.ok for wr in write_results):
            await self._commit_pending_write_success_batch(state, batch)
            return

        outcomes: list[CommitOutcome] = []
        for item, wr in zip(batch, write_results, strict=True):
            outcome = await self._resolve_write_result(
                state.ctx, wr, item.raw, item.processed, item.checkpoint, item.on_success
            )
            outcomes.append(outcome)

        await self._commit_outcomes(state, outcomes)

    async def flush_pending_writes(self, state: RunState) -> None:
        if state.pending_write_owner_task is not None:
            notify = state.pending_write_notify
            assert notify is not None
            notify.set()
            await self.close_pending_write_owner(state)
        await self._flush_pending_writes_once(state)

    async def queue_success_record(
        self,
        state: RunState,
        result: Any,
        raw_record: Any,
        checkpoint_value: CheckpointValue,
        writer_batch_size: int,
        *,
        on_success: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        on_success = self._success_hook_for(state.ctx, raw_record, result, on_success)
        uses_pending_write_owner = self._pending_write_uses_owner(state, writer_batch_size)
        if uses_pending_write_owner:
            if state.pending_write_owner_task is None:
                await self._ensure_pending_write_owner(state, writer_batch_size)
            flushed = state.pending_write_flushed
            assert flushed is not None
            flushed.clear()
        pending_writes = state.pending_writes
        pending_writes.append(
            PendingWrite(
                processed=result,
                raw=raw_record,
                checkpoint=checkpoint_value,
                on_success=on_success,
            )
        )
        pending_count = len(pending_writes)
        if uses_pending_write_owner:
            notify = state.pending_write_notify
            assert notify is not None
            notify.set()
            if pending_count >= writer_batch_size:
                await self._wait_for_pending_write_capacity(state, writer_batch_size)
            return
        if pending_count >= writer_batch_size:
            await self.flush_pending_writes(state)

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
            await self.drop_record(
                state,
                checkpoint_value,
                failure=failure,
                on_success=on_success,
                hook_record=raw_record,
            )
            return
        await self.queue_success_record(
            state,
            result,
            raw_record,
            checkpoint_value,
            writer_batch_size,
            on_success=on_success,
        )

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
        if result is None:
            await self.drop_record(
                state,
                checkpoint_value,
                failure=failure,
                on_success=on_success,
                hook_record=raw_record,
            )
            return
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
        await self.queue_success_record(
            state,
            result,
            raw_record,
            checkpoint_value,
            writer_batch_size,
            on_success=on_success,
        )

    async def flush_batch_direct(
        self,
        state: RunState,
        processed_list: list[Any],
        raw_list: list[Any],
        checkpoint_list: list[Any],
        *,
        on_success_list: list[Callable[[], Awaitable[None]] | None] | None = None,
    ) -> None:
        """Flush a pre-built batch directly to the writer — no PendingWrite allocation.

        Fast path for the Rust LinearBatchBuffer integration. Avoids PendingWrite
        allocation while preserving the same checkpoint and hook semantics as the
        regular batched write path.
        """
        if not processed_list:
            return

        if on_success_list is None:
            on_success_list = [None] * len(processed_list)
        if not (
            len(processed_list) == len(raw_list) == len(checkpoint_list) == len(on_success_list)
        ):
            raise RuntimeError(
                "flush_batch_direct() requires processed/raw/checkpoint/on_success lists "
                "with matching lengths."
            )
        on_success_list = [
            self._success_hook_for(state.ctx, raw, processed, on_success)
            for raw, processed, on_success in zip(
                raw_list,
                processed_list,
                on_success_list,
                strict=True,
            )
        ]

        try:
            write_results, _ = await self.transport.write_batch(state.ctx, processed_list)
        except FenceLostError:
            raise
        except Exception as exc:
            state.ctx.log.exception("pipeline_write_batch_error", batch_size=len(processed_list))
            await self._flush_batch_outcomes(
                state,
                exc,
                processed_list=processed_list,
                raw_list=raw_list,
                checkpoint_list=checkpoint_list,
                on_success_list=on_success_list,
            )
            return

        validate_write_result_count(expected=len(processed_list), actual=len(write_results))

        if all(wr.ok for wr in write_results):
            state.ctx.metrics.records_written += len(processed_list)
            if checkpoint_list and self.checkpoint_store is not None:
                last_checkpoint = checkpoint_list[-1]
                if last_checkpoint is not None:
                    await self.save_batch_checkpoint(state, last_checkpoint, len(processed_list))
            for hook in on_success_list:
                if hook is not None:
                    await hook()
            return

        outcomes: list[CommitOutcome] = []
        for processed, raw, checkpoint, on_success, wr in zip(
            processed_list,
            raw_list,
            checkpoint_list,
            on_success_list,
            write_results,
            strict=True,
        ):
            outcome = await self._resolve_write_result(
                state.ctx, wr, raw, processed, checkpoint, on_success
            )
            outcomes.append(outcome)

        await self._commit_outcomes(state, outcomes)

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
            await write_batch_middleware_failure(
                state=state,
                raw_batch=raw_batch,
                checkpoint_value=checkpoint_value,
                batch_failure=batch_failure,
                dlq_sink_present=self.dlq_sink is not None,
                sink_failure_policy=self.sink_failure_policy,
                write_to_dlq=self.write_to_dlq,
                save_batch_checkpoint=self.save_batch_checkpoint,
            )
        elif results is raw_batch:
            await write_batch_passthrough(
                state=state,
                raw_batch=raw_batch,
                checkpoint_value=checkpoint_value,
                sink_failure_policy=self.sink_failure_policy,
                write_batch=self.transport.write_batch,
                write_to_dlq=self.write_to_dlq,
                save_batch_checkpoint=self.save_batch_checkpoint,
                resolve_write_result=self._resolve_write_result,
                commit_outcomes=self._commit_outcomes,
                success_hook_for=self._success_hook_for,
            )
        else:
            await write_batch_filtered(
                state=state,
                results=results,
                raw_batch=raw_batch,
                checkpoint_value=checkpoint_value,
                sink_failure_policy=self.sink_failure_policy,
                write_batch=self.transport.write_batch,
                write_to_dlq=self.write_to_dlq,
                save_batch_checkpoint=self.save_batch_checkpoint,
                resolve_write_result=self._resolve_write_result,
                commit_outcomes=self._commit_outcomes,
                success_hook_for=self._success_hook_for,
            )

    async def save_batch_checkpoint(
        self,
        state: RunState,
        checkpoint_value: CheckpointValue,
        batch_size: int,
    ) -> None:
        """Save checkpoint for an entire batch while honoring checkpoint_every."""
        await self._checkpoints.save_batch(
            state.ctx,
            state.checkpoint_state,
            checkpoint_value,
            batch_size,
        )
