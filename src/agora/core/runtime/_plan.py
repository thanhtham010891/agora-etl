"""Runtime planning primitives for Agora pipeline execution."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora.core.batch import is_batch_capable_source
from agora.core.data_plane import DataPlane
from agora.core.runtime._plan_middleware import (
    arrow_chain_selected,
    buffered_stage_specs,
    lane_reason,
    maybe_advise_arrow_fast_path,
    middleware_execution_plan,
    validate_middleware_chain_compatibility,
)
from agora.core.runtime._plan_types import (
    BufferedStageSpec,
    MiddlewareExecutionPlan,
    RuntimeLane,
    RuntimePlan,
    WriterExecutionPlan,
    WriterSinkPlan,
)
from agora.core.runtime._plan_writer import (
    direct_flush_eligible,
    writer_accepts_arrow_batches,
    writer_has_arrow_batch_path,
    writer_input_data_plane_reason,
    writer_sink_plans,
)
from agora.core.source import prefetch_limit_for, source_data_plane_spec
from agora.core.source._contracts import source_has_delivery_success_callback

if TYPE_CHECKING:
    from agora.core.middleware import MiddlewareChain
    from agora.core.source import BaseSource
    from agora.core.writer import Writer


def build_runtime_plan(
    source: BaseSource[Any],
    chain: MiddlewareChain[Any, Any],
    writer: Writer[Any],
    *,
    writer_batch_size: int,
) -> RuntimePlan:
    """Build the immutable runtime plan for a prepared pipeline."""

    buffered_stages = buffered_stage_specs(chain)
    batch_source = is_batch_capable_source(source)
    source_spec = source_data_plane_spec(source)
    has_delivery_hooks = source_has_delivery_success_callback(source)
    stream_sync_batches = getattr(source, "stream_sync_batches", None)
    validate_middleware_chain_compatibility(source, chain, source_spec=source_spec)

    if batch_source:
        lane = RuntimeLane.BATCH
    elif buffered_stages:
        lane = RuntimeLane.BUFFERED
    else:
        lane = RuntimeLane.LINEAR
    lane_reason_text = lane_reason(batch_source=batch_source, buffered_stages=buffered_stages)

    middleware_plan = middleware_execution_plan(source_spec, chain, batch_source=batch_source)
    arrow_chain = batch_source and arrow_chain_selected(source_spec, chain)
    sink_accepts_arrow = writer_has_arrow_batch_path(writer)
    writer_accepts_arrow = writer_accepts_arrow_batches(writer)
    arrow_fast_path = arrow_chain and sink_accepts_arrow
    maybe_advise_arrow_fast_path(
        source,
        chain,
        arrow_chain=arrow_chain,
        sink_accepts_arrow=sink_accepts_arrow,
    )
    writer_input_data_plane = middleware_plan.output_data_plane
    if writer_input_data_plane == DataPlane.ARROW_BATCHES and not writer_accepts_arrow:
        writer_input_data_plane = DataPlane.PYTHON_BATCHES
    sink_plans = writer_sink_plans(writer, input_data_plane=writer_input_data_plane)
    writer_plan = WriterExecutionPlan(
        batch_size=max(writer_batch_size, 1),
        input_data_plane=writer_input_data_plane,
        input_data_plane_reason=writer_input_data_plane_reason(
            middleware_output_data_plane=middleware_plan.output_data_plane,
            writer_input_data_plane=writer_input_data_plane,
            arrow_fast_path=arrow_fast_path,
            sink_plans=sink_plans,
        ),
        direct_flush_eligible=direct_flush_eligible(source, writer, writer_batch_size),
        arrow_fast_path=arrow_fast_path,
        arrow_chain=arrow_chain,
        sink_plans=sink_plans,
    )
    return RuntimePlan(
        lane=lane,
        lane_reason=lane_reason_text,
        source_name=source.source_name,
        batch_source=batch_source,
        has_delivery_hooks=has_delivery_hooks,
        source_prefetch_limit=prefetch_limit_for(source),
        source_supports_rust_prefetch=bool(getattr(source, "supports_rust_prefetch", False)),
        source_has_sync_prefetch_path=callable(stream_sync_batches),
        source=source_spec,
        middleware=middleware_plan,
        buffered_stages=buffered_stages,
        writer=writer_plan,
    )


__all__ = [
    "BufferedStageSpec",
    "MiddlewareExecutionPlan",
    "RuntimeLane",
    "RuntimePlan",
    "WriterExecutionPlan",
    "WriterSinkPlan",
    "build_runtime_plan",
]
