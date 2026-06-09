"""Write-result resolution helpers for runtime delivery."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora.core.runtime._delivery_types import (
    CheckpointedOutcome,
    CommitOutcome,
    Dropped,
    ErroredRouted,
    ErroredUnrouted,
    RecordDeliveryError,
    Written,
)
from agora.core.types import SinkFailurePolicy

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.core.context import PipelineContext
    from agora.core.runtime._delivery_types import RunState


async def resolve_write_result(
    *,
    ctx: PipelineContext,
    write_result: Any,
    raw_record: Any,
    processed_record: Any,
    checkpoint_value: Any,
    on_success: Callable[[], Awaitable[None]] | None,
    sink_failure_policy: SinkFailurePolicy,
    write_to_dlq: Callable[..., Awaitable[bool]],
) -> CommitOutcome:
    """Convert a writer result into a typed commit outcome."""
    if write_result.errors:
        routed_all = True
        for err in write_result.errors:
            ctx.log.error("pipeline_sink_error", error=str(err))
            routed = await write_to_dlq(
                ctx=ctx,
                stage="sink_write",
                exc=err,
                record=raw_record,
                original_record=raw_record,
                processed_record=processed_record,
                checkpoint=checkpoint_value,
            )
            routed_all = routed_all and routed
        ctx.metrics.records_errored += 1
        if write_result.written:
            ctx.metrics.records_written += 1
        if routed_all or sink_failure_policy == SinkFailurePolicy.LOG_AND_CONTINUE:
            ack_hook = on_success if (write_result.written or routed_all) else None
            return ErroredRouted(checkpoint=checkpoint_value, on_success=ack_hook)
        return ErroredUnrouted(exc=write_result.errors[0])

    if not write_result.written:
        ctx.log.warning("pipeline_unrouted_record")
        ctx.metrics.records_dropped += 1
        return Dropped(checkpoint=checkpoint_value, on_success=on_success)

    ctx.metrics.records_written += 1
    return Written(checkpoint=checkpoint_value, on_success=on_success)


async def commit_outcome(
    *,
    state: RunState,
    outcome: CommitOutcome,
    sink_failure_policy: SinkFailurePolicy,
    save_checkpoint: Callable[..., Awaitable[None]],
) -> None:
    """Apply checkpoint and hook side effects for a single outcome."""
    if isinstance(outcome, ErroredUnrouted):
        if sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
            raise RecordDeliveryError(outcome.exc) from outcome.exc
        return

    assert isinstance(outcome, CheckpointedOutcome)
    await save_checkpoint(state.ctx, state.checkpoint_state, outcome.checkpoint)
    if outcome.on_success is not None:
        await outcome.on_success()


def validate_write_result_count(*, expected: int, actual: int) -> None:
    """Ensure batch writer returns one result per input record."""
    if actual != expected:
        raise RuntimeError(
            "Writer.write_batch() must return one WriteResult per input record. "
            f"Expected {expected}, got {actual}."
        )
