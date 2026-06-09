"""Buffered-stage result resolution helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora.core.middleware import MiddlewareFailure
from agora.core.runtime._delivery import ProcessedSourceRecord, RunState, SourceRecord

if TYPE_CHECKING:
    from agora.core.middleware import MiddlewareChain
    from agora.core.runtime._delivery import DeliveryEngine


def buffered_stage_failure(
    *,
    source_record: SourceRecord,
    buffered_name: str,
    exc: Exception,
) -> ProcessedSourceRecord:
    """Wrap a buffered stage exception in the standard processed-record envelope."""
    return ProcessedSourceRecord(
        source_record=source_record,
        result=None,
        failure=MiddlewareFailure(
            stage="buffered_middleware",
            record=source_record.raw,
            middleware=buffered_name,
            exception=exc,
        ),
    )


async def dispatch_resolved_buffered_record(
    *,
    chain: MiddlewareChain[Any, Any],
    delivery: DeliveryEngine,
    writer_batch_size: int,
    state: RunState,
    future: Any,
    split_index: int,
    buffered_name: str,
    source_record: SourceRecord,
) -> None:
    """Resolve a buffered stage future and dispatch its normalized result."""
    try:
        processed_record = await future
    except Exception as exc:
        state.ctx.log.exception("pipeline_buffered_stage_error", middleware=buffered_name)
        processed_record = buffered_stage_failure(
            source_record=source_record,
            buffered_name=buffered_name,
            exc=exc,
        )

    if not isinstance(processed_record, ProcessedSourceRecord):
        processed_record = await _coerce_processed_record(
            chain=chain,
            split_index=split_index,
            source_record=source_record,
            buffered_result=processed_record,
            ctx=state.ctx,
        )

    await delivery.dispatch_processed_result(
        state,
        processed_record.result,
        processed_record.source_record.raw,
        processed_record.source_record.checkpoint,
        writer_batch_size,
        failure=processed_record.failure,
        on_success=processed_record.source_record.on_success,
    )


async def _coerce_processed_record(
    *,
    chain: MiddlewareChain[Any, Any],
    split_index: int,
    source_record: SourceRecord,
    buffered_result: Any,
    ctx: Any,
) -> ProcessedSourceRecord:
    if buffered_result is None:
        return ProcessedSourceRecord(
            source_record=source_record,
            result=None,
        )

    final_result = await chain.process_range(
        split_index + 1,
        chain.middleware_count(),
        buffered_result,
        ctx,
    )
    return ProcessedSourceRecord(
        source_record=source_record,
        result=final_result.value,
        failure=final_result.failure,
    )
