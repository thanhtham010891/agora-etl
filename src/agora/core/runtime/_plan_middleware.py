"""Middleware-side runtime planning helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora.core.data_plane import DataPlane, SourceDataPlaneSpec
from agora.core.errors import PipelineError
from agora.core.runtime._plan_types import BufferedStageSpec, MiddlewareExecutionPlan

if TYPE_CHECKING:
    from agora.core.middleware import MiddlewareChain
    from agora.core.source import BaseSource


def buffered_stage_specs(chain: MiddlewareChain[Any, Any]) -> tuple[BufferedStageSpec, ...]:
    """Return buffered stages that justify switching to the buffered lane."""
    return tuple(
        BufferedStageSpec(
            index=index,
            middleware=middleware,
            name=getattr(middleware, "name", "buffered"),
            concurrency=concurrency,
        )
        for index, middleware in chain.buffered_stages()
        if (concurrency := max(1, getattr(middleware, "min_concurrency", 1))) > 1
    )


def arrow_chain_selected(
    source_spec: SourceDataPlaneSpec,
    chain: MiddlewareChain[Any, Any],
) -> bool:
    """Return whether the chain remains Arrow-native end to end."""
    if source_spec.emitted_plane != DataPlane.ARROW_BATCHES:
        return False
    return chain.middleware_count() == 0 or chain.has_only_arrow_batch_stages()


def format_middleware_mode_matrix(chain: MiddlewareChain[Any, Any]) -> str:
    """Render the middleware data-plane matrix for diagnostics."""
    matrix = chain.stage_mode_matrix()
    if not matrix:
        return "<empty>"
    return " -> ".join(f"{spec.name}[{spec.data_plane.value}]" for spec in matrix)


def validate_middleware_chain_compatibility(
    source: BaseSource[Any],
    chain: MiddlewareChain[Any, Any],
    *,
    source_spec: SourceDataPlaneSpec,
) -> None:
    """Validate the source/chain data-plane matrix."""
    if not chain.has_arrow_batch_stages():
        return

    matrix = format_middleware_mode_matrix(chain)
    if source_spec.emitted_plane != DataPlane.ARROW_BATCHES:
        raise PipelineError(
            "Arrow middleware requires an Arrow-emitting batch source. "
            f"Found source={source.source_name!r}, source_plane={source_spec.emitted_plane.value}, "
            f"middleware_matrix={matrix}.",
            stage="pipeline_build",
            source_name=source.source_name,
        )

    if chain.has_mixed_data_planes():
        raise PipelineError(
            "Middleware chain mixes incompatible data planes. "
            "Arrow stages and Python row/list-dict stages cannot coexist in one chain. "
            f"middleware_matrix={matrix}.",
            stage="pipeline_build",
            source_name=source.source_name,
        )


def middleware_execution_plan(
    source_spec: SourceDataPlaneSpec,
    chain: MiddlewareChain[Any, Any],
    *,
    batch_source: bool,
) -> MiddlewareExecutionPlan:
    """Derive middleware data-plane transitions for the runtime plan."""
    stage_modes = chain.stage_mode_matrix()
    output_data_plane = source_spec.emitted_plane
    if stage_modes:
        if chain.has_only_arrow_batch_stages():
            output_data_plane = DataPlane.ARROW_BATCHES
        elif batch_source:
            output_data_plane = DataPlane.PYTHON_BATCHES
        else:
            output_data_plane = DataPlane.PYTHON_ROWS
    materializes_arrow_to_rows = (
        source_spec.emitted_plane == DataPlane.ARROW_BATCHES
        and output_data_plane != DataPlane.ARROW_BATCHES
    )
    materialization_reason = None
    if materializes_arrow_to_rows:
        materialization_reason = (
            "source emits arrow_batches, but the middleware chain contains Python-row stages, "
            "so Arrow batches materialize once before middleware execution"
        )
    return MiddlewareExecutionPlan(
        stages=stage_modes,
        input_data_plane=source_spec.emitted_plane,
        output_data_plane=output_data_plane,
        materializes_arrow_to_rows=materializes_arrow_to_rows,
        materialization_reason=materialization_reason,
    )


def lane_reason(
    *,
    batch_source: bool,
    buffered_stages: tuple[BufferedStageSpec, ...],
) -> str:
    """Explain why the runtime selected a given execution lane."""
    if batch_source:
        return "source advertises batch emission, so the runtime selects the batch lane"
    if buffered_stages:
        stage_list = ", ".join(
            f"{stage.name}(min_concurrency={stage.concurrency})" for stage in buffered_stages
        )
        return (
            "middleware stages request concurrent submit() execution, so the runtime selects "
            f"the buffered lane: {stage_list}"
        )
    return "source streams Python rows and no buffered stage requires submit() concurrency"
