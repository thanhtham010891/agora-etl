"""Batch outcome helpers for runtime delivery."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora.core.runtime._delivery_types import (
    CheckpointedOutcome,
    CommitOutcome,
    ErroredUnrouted,
    RecordDeliveryError,
)
from agora.core.types import SinkFailurePolicy

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.core.checkpoint import Checkpoint
    from agora.core.context import PipelineContext
    from agora.core.runtime._delivery_types import RunState


async def flush_batch_outcomes(
    *,
    state: RunState,
    exc: Exception,
    processed_list: list[Any],
    raw_list: list[Any],
    checkpoint_list: list[Any],
    on_success_list: list[Callable[[], Awaitable[None]] | None],
    sink_failure_policy: SinkFailurePolicy,
    write_to_dlq: Callable[..., Awaitable[bool]],
    prepare_checkpoint: Callable[[PipelineContext, Any, Any], Checkpoint | None],
    persist_checkpoint: Callable[..., Awaitable[None]],
) -> None:
    """Route a failed batch through DLQ/checkpoint handling."""
    pending_checkpoint: Checkpoint | None = None
    pending_checkpoint_batch_size = 0
    delivered_hooks: list[Callable[[], Awaitable[None]]] = []

    for raw, processed, checkpoint_value, on_success in zip(
        raw_list, processed_list, checkpoint_list, on_success_list, strict=True
    ):
        routed = await write_to_dlq(
            ctx=state.ctx,
            stage="sink_write",
            exc=exc,
            record=raw,
            original_record=raw,
            processed_record=processed,
            checkpoint=checkpoint_value,
        )
        state.ctx.metrics.records_errored += 1
        if routed or sink_failure_policy == SinkFailurePolicy.LOG_AND_CONTINUE:
            checkpoint = prepare_checkpoint(state.ctx, state.checkpoint_state, checkpoint_value)
            if checkpoint is not None:
                pending_checkpoint = checkpoint
                pending_checkpoint_batch_size += 1
            if routed and on_success is not None:
                delivered_hooks.append(on_success)
        else:
            if pending_checkpoint is not None:
                await persist_checkpoint(
                    state.ctx,
                    state.checkpoint_state,
                    pending_checkpoint,
                    batch_size=pending_checkpoint_batch_size,
                )
                pending_checkpoint = None
                pending_checkpoint_batch_size = 0
            for hook in delivered_hooks:
                await hook()
            raise RecordDeliveryError(exc) from exc

    if pending_checkpoint is not None:
        await persist_checkpoint(
            state.ctx,
            state.checkpoint_state,
            pending_checkpoint,
            batch_size=pending_checkpoint_batch_size,
        )
    for hook in delivered_hooks:
        await hook()


async def commit_outcomes(
    *,
    state: RunState,
    outcomes: list[CommitOutcome],
    sink_failure_policy: SinkFailurePolicy,
    prepare_checkpoint: Callable[[PipelineContext, Any, Any], Checkpoint | None],
    persist_checkpoint: Callable[..., Awaitable[None]],
) -> None:
    """Apply checkpoint and hook side effects for a batch of outcomes."""
    pending_checkpoint: Checkpoint | None = None
    pending_checkpoint_batch_size = 0
    delivered_hooks: list[Callable[[], Awaitable[None]]] = []

    for outcome in outcomes:
        if isinstance(outcome, ErroredUnrouted):
            if pending_checkpoint is not None:
                await persist_checkpoint(
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
            if sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
                raise RecordDeliveryError(outcome.exc) from outcome.exc
            continue

        if not isinstance(outcome, CheckpointedOutcome):
            raise TypeError(f"Unexpected outcome type: {type(outcome)!r}")

        checkpoint = prepare_checkpoint(state.ctx, state.checkpoint_state, outcome.checkpoint)
        if checkpoint is not None:
            pending_checkpoint = checkpoint
            pending_checkpoint_batch_size += 1
        if outcome.on_success is not None:
            delivered_hooks.append(outcome.on_success)

    if pending_checkpoint is not None:
        await persist_checkpoint(
            state.ctx,
            state.checkpoint_state,
            pending_checkpoint,
            batch_size=pending_checkpoint_batch_size,
        )
    for hook in delivered_hooks:
        await hook()
