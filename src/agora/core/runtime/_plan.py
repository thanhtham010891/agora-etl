"""Runtime planning primitives for Agora pipeline execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from agora.core.batch import is_batch_capable_source
from agora.core.data_plane import DataPlane, SourceDataPlaneSpec
from agora.core.errors import PipelineError
from agora.core.sink import sink_capabilities, writer_target_data_plane_specs
from agora.core.source import DeliveryHookSource, source_data_plane_spec

if TYPE_CHECKING:
    from agora.core.middleware import MiddlewareChain, MiddlewareModeSpec
    from agora.core.source import BaseSource
    from agora.core.writer import Writer


class RuntimeLane(StrEnum):
    """Execution lane selected for a prepared pipeline run."""

    LINEAR = "linear"
    BUFFERED = "buffered"
    BATCH = "batch"


@dataclass(frozen=True, slots=True)
class BufferedStageSpec:
    """Runtime-selected buffered middleware stage."""

    index: int
    middleware: Any
    name: str
    concurrency: int


@dataclass(frozen=True, slots=True)
class MiddlewareExecutionPlan:
    """Derived middleware data-plane decisions for a single runtime plan."""

    stages: tuple[MiddlewareModeSpec, ...] = field(default_factory=tuple)
    input_data_plane: DataPlane = DataPlane.PYTHON_ROWS
    output_data_plane: DataPlane = DataPlane.PYTHON_ROWS
    materializes_arrow_to_rows: bool = False
    materialization_reason: str | None = None


@dataclass(frozen=True, slots=True)
class WriterSinkPlan:
    """Resolved sink-level write mode behind a writer."""

    sink_name: str
    accepted_data_planes: tuple[DataPlane, ...]
    native_data_planes: tuple[DataPlane, ...]
    selected_data_plane: DataPlane
    downgraded_from_input: bool = False
    selection_reason: str = ""


@dataclass(frozen=True, slots=True)
class WriterExecutionPlan:
    """Derived writer-side data-plane and write-path decisions."""

    batch_size: int
    input_data_plane: DataPlane = DataPlane.PYTHON_ROWS
    input_data_plane_reason: str = ""
    direct_flush_eligible: bool = False
    arrow_fast_path: bool = False
    arrow_chain: bool = False
    sink_plans: tuple[WriterSinkPlan, ...] = field(default_factory=tuple)

    @property
    def downgraded_sink_count(self) -> int:
        return sum(1 for sink in self.sink_plans if sink.downgraded_from_input)


@dataclass(frozen=True, slots=True)
class RuntimePlan:
    """Immutable execution plan built once before pipeline run starts."""

    lane: RuntimeLane
    lane_reason: str
    source_name: str
    batch_source: bool
    has_delivery_hooks: bool
    source: SourceDataPlaneSpec = field(
        default_factory=lambda: SourceDataPlaneSpec(
            source_name="source",
            emitted_plane=DataPlane.PYTHON_ROWS,
            supports_batch_emit=False,
            emits_arrow_batches=False,
        )
    )
    middleware: MiddlewareExecutionPlan = field(default_factory=MiddlewareExecutionPlan)
    buffered_stages: tuple[BufferedStageSpec, ...] = field(default_factory=tuple)
    writer: WriterExecutionPlan = field(default_factory=lambda: WriterExecutionPlan(batch_size=1))

    @property
    def uses_buffered_lane(self) -> bool:
        return self.lane == RuntimeLane.BUFFERED

    @property
    def uses_batch_lane(self) -> bool:
        return self.lane == RuntimeLane.BATCH


def _buffered_stage_specs(chain: MiddlewareChain[Any, Any]) -> tuple[BufferedStageSpec, ...]:
    # Only middleware that asks for concurrency > 1 justifies the buffered lane.
    # A submit()-capable stage with min_concurrency == 1 gains nothing from
    # per-record task orchestration, so it runs on the linear lane via process().
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


def _direct_flush_eligible(
    source: BaseSource[Any],
    writer: Writer[Any],
    writer_batch_size: int,
) -> bool:
    if writer_batch_size <= 1:
        return False
    writer_caps = getattr(writer, "_sink_batch_writable", None)
    return bool(writer_caps is not None and len(writer_caps) == 1 and writer_caps[0])


def _arrow_chain_selected(
    source_spec: SourceDataPlaneSpec,
    chain: MiddlewareChain[Any, Any],
) -> bool:
    if source_spec.emitted_plane != DataPlane.ARROW_BATCHES:
        return False
    return chain.middleware_count() == 0 or chain.has_only_arrow_batch_stages()


def _writer_has_arrow_batch_path(writer: Writer[Any]) -> bool:
    sink_specs = writer_target_data_plane_specs(writer)
    if getattr(writer, "_sinks", None) is not None and sink_specs:
        return any(DataPlane.ARROW_BATCHES in spec.native_planes for spec in sink_specs)
    capabilities = sink_capabilities(writer)
    return DataPlane.ARROW_BATCHES in capabilities.native_data_planes


def _format_middleware_mode_matrix(chain: MiddlewareChain[Any, Any]) -> str:
    matrix = chain.stage_mode_matrix()
    if not matrix:
        return "<empty>"
    return " -> ".join(f"{spec.name}[{spec.data_plane.value}]" for spec in matrix)


def _validate_middleware_chain_compatibility(
    source: BaseSource[Any],
    chain: MiddlewareChain[Any, Any],
    *,
    source_spec: SourceDataPlaneSpec,
) -> None:
    """Validate the source/chain data-plane matrix.

    Supported combinations:
    - python-rows source/list batch -> python-rows chain
    - arrow source -> all-arrow chain
    - arrow source -> python-rows chain (materialize once before the chain)

    Rejected combinations:
    - non-arrow source -> any Arrow middleware stage
    - any chain that mixes Arrow stages with python row/list-dict stages
    """
    if not chain.has_arrow_batch_stages():
        return

    matrix = _format_middleware_mode_matrix(chain)
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


def _middleware_execution_plan(
    source_spec: SourceDataPlaneSpec,
    chain: MiddlewareChain[Any, Any],
    *,
    batch_source: bool,
) -> MiddlewareExecutionPlan:
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


def _sink_selection_reason(
    *,
    input_data_plane: DataPlane,
    selected_data_plane: DataPlane,
    native_data_planes: tuple[DataPlane, ...],
) -> str:
    if selected_data_plane == input_data_plane:
        return f"sink accepts {input_data_plane.value} natively"
    native = ", ".join(plane.value for plane in native_data_planes) or "python_rows"
    return (
        f"sink has no native {input_data_plane.value} path; writer downgrades to "
        f"{selected_data_plane.value} at the sink boundary (native={native})"
    )


def _writer_sink_plans(
    writer: Writer[Any],
    *,
    input_data_plane: DataPlane,
) -> tuple[WriterSinkPlan, ...]:
    sink_specs = writer_target_data_plane_specs(writer)
    if not sink_specs:
        return ()
    return tuple(
        WriterSinkPlan(
            sink_name=spec.sink_name,
            accepted_data_planes=spec.accepted_planes,
            native_data_planes=spec.native_planes,
            selected_data_plane=(selected := spec.selected_plane_for(input_data_plane)),
            downgraded_from_input=spec.downgraded_from(input_data_plane),
            selection_reason=_sink_selection_reason(
                input_data_plane=input_data_plane,
                selected_data_plane=selected,
                native_data_planes=spec.native_planes,
            ),
        )
        for spec in sink_specs
    )


def _lane_reason(
    *,
    batch_source: bool,
    buffered_stages: tuple[BufferedStageSpec, ...],
) -> str:
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


def _writer_input_data_plane_reason(
    *,
    middleware_output_data_plane: DataPlane,
    arrow_fast_path: bool,
) -> str:
    if middleware_output_data_plane != DataPlane.ARROW_BATCHES:
        return f"writer receives middleware output as {middleware_output_data_plane.value}"
    if arrow_fast_path:
        return (
            "writer keeps arrow_batches because the source/middleware chain stays Arrow-native "
            "and at least one sink exposes an Arrow batch write path"
        )
    return (
        "writer materializes arrow_batches to python_batches because the writer has no "
        "Arrow batch write path"
    )


def build_runtime_plan(
    source: BaseSource[Any],
    chain: MiddlewareChain[Any, Any],
    writer: Writer[Any],
    *,
    writer_batch_size: int,
) -> RuntimePlan:
    """Build the immutable runtime plan for a prepared pipeline."""

    buffered_stages = _buffered_stage_specs(chain)
    batch_source = is_batch_capable_source(source)
    source_spec = source_data_plane_spec(source)
    has_delivery_hooks = isinstance(source, DeliveryHookSource)
    _validate_middleware_chain_compatibility(source, chain, source_spec=source_spec)

    if batch_source:
        lane = RuntimeLane.BATCH
    elif buffered_stages:
        lane = RuntimeLane.BUFFERED
    else:
        lane = RuntimeLane.LINEAR
    lane_reason = _lane_reason(batch_source=batch_source, buffered_stages=buffered_stages)

    middleware_plan = _middleware_execution_plan(source_spec, chain, batch_source=batch_source)
    arrow_chain = batch_source and _arrow_chain_selected(source_spec, chain)
    arrow_fast_path = arrow_chain and _writer_has_arrow_batch_path(writer)
    writer_input_data_plane = middleware_plan.output_data_plane
    if writer_input_data_plane == DataPlane.ARROW_BATCHES and not arrow_fast_path:
        writer_input_data_plane = DataPlane.PYTHON_BATCHES
    writer_plan = WriterExecutionPlan(
        batch_size=max(writer_batch_size, 1),
        input_data_plane=writer_input_data_plane,
        input_data_plane_reason=_writer_input_data_plane_reason(
            middleware_output_data_plane=middleware_plan.output_data_plane,
            arrow_fast_path=arrow_fast_path,
        ),
        direct_flush_eligible=_direct_flush_eligible(source, writer, writer_batch_size),
        arrow_fast_path=arrow_fast_path,
        arrow_chain=arrow_chain,
        sink_plans=_writer_sink_plans(writer, input_data_plane=writer_input_data_plane),
    )
    return RuntimePlan(
        lane=lane,
        lane_reason=lane_reason,
        source_name=source.source_name,
        batch_source=batch_source,
        has_delivery_hooks=has_delivery_hooks,
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
