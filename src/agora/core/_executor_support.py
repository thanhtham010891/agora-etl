"""Helpers for pipeline execution orchestration."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from agora.core._pipeline_support import resolved_performance_profile_settings
from agora.core.acceleration import require_acceleration
from agora.core.runtime import (
    DeliveryEngine,
    ExecutionCoordinator,
    RuntimePlan,
    build_runtime_plan,
)
from agora.core.session import PipelineLifecycleController, PipelineRunState

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.core._executor_types import PipelineRuntimeSpec
    from agora.core.context import PipelineContext


@dataclass(slots=True)
class PreparedExecution:
    """Resolved execution objects needed for one pipeline run."""

    spec: PipelineRuntimeSpec
    lifecycle: PipelineLifecycleController
    state: PipelineRunState
    coordinator: DeliveryEngine
    plan: RuntimePlan
    execution: ExecutionCoordinator


def prepare_execution(
    spec: PipelineRuntimeSpec,
    *,
    max_records: int | None,
    run_id: str | None,
) -> PreparedExecution:
    """Build the runtime collaborators for one pipeline execution."""
    acceleration_status = require_acceleration(spec.config.acceleration_mode)
    if max_records is not None:
        spec = replace(spec, source=spec.source.limit(max_records))

    lifecycle = PipelineLifecycleController(spec)
    state = lifecycle.create_run_state(run_id=run_id, source_limit=max_records)
    coordinator = lifecycle.make_delivery_coordinator()
    plan = build_runtime_plan(
        spec.source,
        spec.chain,
        spec.writer,
        writer_batch_size=spec.config.batch_size,
    )
    execution = ExecutionCoordinator(
        source=spec.source,
        chain=spec.chain,
        writer_batch_size=spec.config.batch_size,
        delivery=coordinator,
        plan=plan,
        max_buffer_size=spec.config.max_buffer_size,
        backpressure=spec.config.backpressure,
        acceleration_mode=spec.config.acceleration_mode,
        performance_profile=spec.config.performance_profile,
    )
    runtime = state.ctx.metrics.runtime
    runtime.acceleration_mode = acceleration_status.mode.value
    runtime.acceleration_profile = spec.config.performance_profile
    runtime.acceleration_profile_settings = resolved_performance_profile_settings(
        spec.config,
        source_prefetch_limit=plan.source_prefetch_limit,
    ).to_dict()
    runtime.acceleration_available = acceleration_status.enabled
    runtime.acceleration_package_version = acceleration_status.version or ""
    runtime.acceleration_compatible = acceleration_status.compatible
    runtime.acceleration_capabilities = tuple(
        sorted(capability.value for capability in acceleration_status.capabilities)
    )
    runtime.direct_flush_eligible = plan.writer.direct_flush_eligible
    runtime.direct_flush_inactive_reason = _direct_flush_inactive_reason(
        eligible=plan.writer.direct_flush_eligible,
        batch_size=spec.config.batch_size,
        batch_flush_interval_ms=spec.config.batch_flush_interval_ms,
        lane=plan.lane.value,
        rust_available=execution.rust_available(),
    )
    return PreparedExecution(
        spec=spec,
        lifecycle=lifecycle,
        state=state,
        coordinator=coordinator,
        plan=plan,
        execution=execution,
    )


async def invoke_live_metrics_callback(
    callback: Callable[[PipelineContext], Awaitable[None]],
    ctx: PipelineContext,
) -> Exception | None:
    """Run the live-metrics callback and return any exception for caller logging."""
    try:
        await callback(ctx)
    except Exception as exc:
        return exc
    return None


def source_stream_trace_attrs(execution: ExecutionCoordinator) -> dict[str, object]:
    """Build source-stream trace attributes from a prepared execution."""
    return {
        "source": execution.source.source_name,
        "buffered": execution.plan.uses_buffered_lane,
        "lane": execution.plan.lane,
        "batch_source": execution.plan.batch_source,
        "source_data_plane": execution.plan.source.emitted_plane,
        "writer_input_data_plane": execution.plan.writer.input_data_plane,
        "buffered_stage_count": len(execution.plan.buffered_stages),
        "direct_flush_eligible": execution.plan.writer.direct_flush_eligible,
        "arrow_fast_path_eligible": execution.plan.writer.arrow_fast_path,
        "arrow_chain_eligible": execution.plan.writer.arrow_chain,
    }


def pipeline_run_trace_attrs(
    spec: PipelineRuntimeSpec, plan: RuntimePlan, run_id: str
) -> dict[str, object]:
    """Build pipeline-run trace attributes from a prepared runtime plan."""
    return {
        "pipeline_id": spec.pipeline_id,
        "run_id": run_id,
        "source": spec.source.source_name,
        "planned_lane": plan.lane,
        "batch_source": plan.batch_source,
        "source_data_plane": plan.source.emitted_plane,
        "writer_input_data_plane": plan.writer.input_data_plane,
        "downgraded_sink_count": plan.writer.downgraded_sink_count,
        "buffered_stage_count": len(plan.buffered_stages),
        "direct_flush_eligible": plan.writer.direct_flush_eligible,
        "arrow_fast_path_eligible": plan.writer.arrow_fast_path,
        "arrow_chain_eligible": plan.writer.arrow_chain,
        "acceleration_mode": str(spec.config.acceleration_mode),
        "acceleration_profile": spec.config.performance_profile,
    }


def apply_runtime_trace_attributes(span: Any, runtime: Any) -> None:
    """Attach final runtime metrics to a tracing span."""
    span.set_attribute("execution_lane", runtime.execution_lane)
    span.set_attribute("source_data_plane", runtime.source_data_plane)
    span.set_attribute("writer_input_data_plane", runtime.writer_input_data_plane)
    span.set_attribute("direct_flush_active", runtime.direct_flush_active)
    span.set_attribute("direct_flush_eligible", runtime.direct_flush_eligible)
    span.set_attribute("direct_flush_inactive_reason", runtime.direct_flush_inactive_reason)
    span.set_attribute("acceleration_mode", runtime.acceleration_mode)
    span.set_attribute("acceleration_available", runtime.acceleration_available)
    span.set_attribute("acceleration_profile", runtime.acceleration_profile)
    span.set_attribute("arrow_fast_path_active", runtime.arrow_fast_path_active)
    span.set_attribute("arrow_chain_active", runtime.arrow_chain_active)
    span.set_attribute(
        "writer_downgraded_sink_count",
        runtime.writer_downgraded_sink_count,
    )
    span.set_attribute("rust_prefetch_active", runtime.rust_prefetch_active)


def _direct_flush_inactive_reason(
    *,
    eligible: bool,
    batch_size: int,
    batch_flush_interval_ms: int | None,
    lane: str,
    rust_available: bool,
) -> str:
    if eligible:
        return ""
    if lane != "linear":
        return f"direct flush only applies to the linear lane (planned lane: {lane})"
    if batch_size <= 1:
        return "writer batch size is 1"
    if batch_flush_interval_ms is not None and batch_flush_interval_ms > 0:
        return "batch flush interval requires the pending-write owner path"
    if not rust_available:
        return "Rust linear batch buffer is unavailable or disabled"
    return "writer shape is not safe for direct flush"
