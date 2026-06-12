"""Batch write strategies for runtime delivery."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora.core.runtime._delivery_types import CommitOutcome, ErroredUnrouted, RecordDeliveryError
from agora.core.types import SinkFailurePolicy

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.core.batch import BatchFailure
    from agora.core.runtime._delivery_types import RunState


async def write_batch_middleware_failure(
    *,
    state: RunState,
    raw_batch: list[Any],
    checkpoint_value: Any,
    batch_failure: BatchFailure,
    dlq_sink_present: bool,
    sink_failure_policy: SinkFailurePolicy,
    write_to_dlq: Callable[..., Awaitable[bool]],
    save_batch_checkpoint: Callable[..., Awaitable[None]],
) -> None:
    """Handle a batch that failed entirely at the middleware stage."""
    state.ctx.metrics.records_errored += len(raw_batch)
    if dlq_sink_present:
        for raw_record in raw_batch:
            await write_to_dlq(
                ctx=state.ctx,
                stage="batch_middleware",
                exc=batch_failure.exception,
                record=raw_record,
                original_record=raw_record,
                middleware=batch_failure.middleware,
                checkpoint=checkpoint_value,
            )
        await save_batch_checkpoint(state, checkpoint_value, len(raw_batch))
        return

    if sink_failure_policy == SinkFailurePolicy.LOG_AND_CONTINUE:
        state.ctx.log.exception(
            "batch_middleware_error_log_and_continue",
            middleware=batch_failure.middleware,
            batch_size=len(raw_batch),
            error=str(batch_failure.exception),
        )
        await save_batch_checkpoint(state, checkpoint_value, len(raw_batch))
        return

    raise RecordDeliveryError(batch_failure.exception) from batch_failure.exception


async def write_batch_passthrough(
    *,
    state: RunState,
    raw_batch: list[Any],
    checkpoint_value: Any,
    sink_failure_policy: SinkFailurePolicy,
    write_batch: Callable[..., Awaitable[tuple[list[Any], float]]],
    write_to_dlq: Callable[..., Awaitable[bool]],
    save_batch_checkpoint: Callable[..., Awaitable[None]],
    resolve_write_result: Callable[..., Awaitable[CommitOutcome]],
) -> None:
    """Write a batch where results is raw_batch (no middleware transformation)."""
    try:
        write_results, _ = await write_batch(state.ctx, raw_batch)
    except Exception as exc:
        state.ctx.log.exception("batch_write_error", batch_size=len(raw_batch))
        routed = True
        for record in raw_batch:
            ok = await write_to_dlq(
                ctx=state.ctx,
                stage="sink_write",
                exc=exc,
                record=record,
                original_record=record,
                processed_record=record,
                checkpoint=checkpoint_value,
            )
            routed = routed and ok
        state.ctx.metrics.records_errored += len(raw_batch)
        if routed or sink_failure_policy == SinkFailurePolicy.LOG_AND_CONTINUE:
            await save_batch_checkpoint(state, checkpoint_value, len(raw_batch))
        elif sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
            raise RecordDeliveryError(exc) from exc
        return

    _validate_write_results_length(len(raw_batch), len(write_results), batch_input_kind="batch")

    if all(write_result.ok for write_result in write_results):
        state.ctx.metrics.records_written += len(raw_batch)
        await save_batch_checkpoint(state, checkpoint_value, len(raw_batch))
        return

    errored_unrouted: Exception | None = None
    for record, write_result in zip(raw_batch, write_results, strict=True):
        outcome = await resolve_write_result(
            state.ctx,
            write_result,
            record,
            record,
            checkpoint_value,
            None,
        )
        if isinstance(outcome, ErroredUnrouted) and errored_unrouted is None:
            errored_unrouted = outcome.exc

    await _finalize_batch_outcomes(
        state=state,
        checkpoint_value=checkpoint_value,
        batch_size=len(raw_batch),
        errored_unrouted=errored_unrouted,
        sink_failure_policy=sink_failure_policy,
        save_batch_checkpoint=save_batch_checkpoint,
    )


async def write_batch_filtered(
    *,
    state: RunState,
    results: list[Any | None],
    raw_batch: list[Any],
    checkpoint_value: Any,
    sink_failure_policy: SinkFailurePolicy,
    write_batch: Callable[..., Awaitable[tuple[list[Any], float]]],
    write_to_dlq: Callable[..., Awaitable[bool]],
    save_batch_checkpoint: Callable[..., Awaitable[None]],
    resolve_write_result: Callable[..., Awaitable[CommitOutcome]],
) -> None:
    """Write a batch where middleware may have dropped some records."""
    has_drops = False
    for processed_record in results:
        if processed_record is None:
            has_drops = True
            break

    active_items: list[tuple[Any, Any]] | None
    if not has_drops:
        active_items = None
        to_write = results
        dropped = 0
    else:
        active_items = [
            (raw_record, processed_record)
            for raw_record, processed_record in zip(raw_batch, results, strict=True)
            if processed_record is not None
        ]
        to_write = [processed for _raw, processed in active_items]
        dropped = len(results) - len(to_write)
    state.ctx.metrics.records_dropped += dropped

    if not to_write:
        await save_batch_checkpoint(state, checkpoint_value, len(results))
        return

    try:
        write_results, _ = await write_batch(state.ctx, to_write)
    except Exception as exc:
        state.ctx.log.exception("batch_write_error", batch_size=len(to_write))
        routed = True
        pairs = zip(raw_batch, to_write, strict=True) if active_items is None else active_items
        for raw_record, processed_record in pairs:
            ok = await write_to_dlq(
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
        if routed or sink_failure_policy == SinkFailurePolicy.LOG_AND_CONTINUE:
            await save_batch_checkpoint(state, checkpoint_value, len(results))
        elif sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
            raise RecordDeliveryError(exc) from exc
        return

    _validate_write_results_length(len(to_write), len(write_results), batch_input_kind="batch")

    if all(write_result.ok for write_result in write_results):
        state.ctx.metrics.records_written += len(to_write)
        await save_batch_checkpoint(state, checkpoint_value, len(results))
        return

    outcomes: list[CommitOutcome] = []
    pairs = zip(raw_batch, to_write, strict=True) if active_items is None else active_items
    for (raw_record, processed_record), write_result in zip(pairs, write_results, strict=True):
        outcome = await resolve_write_result(
            state.ctx,
            write_result,
            raw_record,
            processed_record,
            checkpoint_value,
            None,
        )
        outcomes.append(outcome)

    errored_unrouted: Exception | None = None
    for outcome in outcomes:
        if isinstance(outcome, ErroredUnrouted) and errored_unrouted is None:
            errored_unrouted = outcome.exc

    await _finalize_batch_outcomes(
        state=state,
        checkpoint_value=checkpoint_value,
        batch_size=len(results),
        errored_unrouted=errored_unrouted,
        sink_failure_policy=sink_failure_policy,
        save_batch_checkpoint=save_batch_checkpoint,
    )


def _validate_write_results_length(
    expected: int,
    actual: int,
    *,
    batch_input_kind: str,
) -> None:
    if actual != expected:
        raise RuntimeError(
            f"Writer.write_batch() must return one WriteResult per {batch_input_kind} input record. "
            f"Expected {expected}, got {actual}."
        )


async def _finalize_batch_outcomes(
    *,
    state: RunState,
    checkpoint_value: Any,
    batch_size: int,
    errored_unrouted: Exception | None,
    sink_failure_policy: SinkFailurePolicy,
    save_batch_checkpoint: Callable[..., Awaitable[None]],
) -> None:
    routed_all = errored_unrouted is None
    if routed_all or sink_failure_policy == SinkFailurePolicy.LOG_AND_CONTINUE:
        await save_batch_checkpoint(state, checkpoint_value, batch_size)
    if not routed_all and sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
        assert errored_unrouted is not None
        raise RecordDeliveryError(errored_unrouted) from errored_unrouted
