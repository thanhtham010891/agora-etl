"""Support helpers for pipeline lifecycle control."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from agora.core.checkpoint import is_checkpoint_capable
from agora.core.context import PipelineContext
from agora.core.metrics import PipelineMetrics
from agora.core.runtime import DeliveryEngine
from agora.core.session._state import PipelineRunState
from agora.core.sink import bind_context_if_supported
from agora.core.tracing import NoopTracer
from agora.core.types import CheckpointFailurePolicy

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.core._executor_types import PipelineRuntimeSpec
    from agora.core.runtime._writer_transport import WriterTransport


def create_run_state(
    spec: PipelineRuntimeSpec,
    *,
    run_id: str | None,
    source_limit: int | None,
) -> PipelineRunState:
    """Create the mutable run state and pre-wired pipeline context."""
    metrics = PipelineMetrics()
    ctx = PipelineContext(
        pipeline_id=spec.pipeline_id,
        metrics=metrics,
        run_id=run_id or str(uuid.uuid4()),
        tracer=spec.config.tracer or NoopTracer(),
    )
    ctx.log.info("pipeline_start", source_limit=source_limit)
    return PipelineRunState(ctx=ctx)


def make_delivery_engine(
    spec: PipelineRuntimeSpec,
    *,
    transport: WriterTransport,
) -> DeliveryEngine:
    """Construct the delivery engine for one prepared runtime spec."""
    config = spec.config
    return DeliveryEngine(
        transport=transport,
        source_name=spec.source.source_name,
        current_checkpoint=spec.source.current_checkpoint,
        dlq_sink=config.dlq,
        dlq_failure_policy=config.dlq_failure_policy,
        sink_failure_policy=config.sink_failure_policy,
        checkpoint_store=config.checkpoint,
        checkpoint_failure_policy=config.checkpoint_failure_policy,
        checkpoint_key=config.checkpoint_key or spec.pipeline_id,
        checkpoint_every=config.checkpoint_every,
        batch_flush_interval_ms=config.batch_flush_interval_ms,
    )


async def restore_checkpoint(spec: PipelineRuntimeSpec, ctx: PipelineContext) -> None:
    """Restore checkpoint state into the source when configured."""
    if spec.config.checkpoint is None:
        return

    if not is_checkpoint_capable(spec.source):
        ctx.log.warning(
            "pipeline_checkpoint_unsupported_source",
            source=spec.source.source_name,
            checkpoint_key=spec.config.checkpoint_key,
        )
        return

    ctx.metrics.runtime.checkpoint_enabled = True
    checkpoint = None
    try:
        with ctx.trace_span(
            "checkpoint.load",
            checkpoint_key=spec.config.checkpoint_key,
            source=spec.source.source_name,
        ) as span:
            checkpoint = await spec.config.checkpoint.load(
                spec.config.checkpoint_key or spec.pipeline_id
            )
            if span is not None:
                span.set_attribute("checkpoint.loaded", checkpoint is not None)
            await spec.source.prepare_resume(checkpoint)
    except Exception:
        ctx.metrics.runtime.checkpoint_failure_count += 1
        if spec.config.checkpoint_failure_policy == CheckpointFailurePolicy.LOG_AND_CONTINUE:
            ctx.log.exception(
                "pipeline_checkpoint_load_error",
                checkpoint_key=spec.config.checkpoint_key,
            )
            return
        raise

    if checkpoint is not None:
        ctx.metrics.last_checkpoint = checkpoint
        ctx.log.info(
            "pipeline_checkpoint_loaded",
            checkpoint_key=spec.config.checkpoint_key,
            source=checkpoint.source,
        )


async def open_runtime_sinks(
    spec: PipelineRuntimeSpec,
    ctx: PipelineContext,
) -> tuple[bool, bool]:
    """Bind and open writer/DLQ sinks, rolling back partial opens on failure."""
    writer_opened = False
    dlq_opened = False
    writer_open_attempted = False
    dlq_open_attempted = False
    try:
        bind_context_if_supported(spec.writer, ctx)
        writer_open_attempted = True
        with ctx.trace_span("writer.open", writer=type(spec.writer).__name__):
            await spec.writer.open()
        writer_opened = True
        if spec.config.dlq is not None:
            bind_context_if_supported(spec.config.dlq, ctx)
            dlq_open_attempted = True
            with ctx.trace_span("dlq.open", sink=spec.config.dlq.sink_name):
                await spec.config.dlq.open()
            dlq_opened = True
    except Exception:
        await rollback_open_failure(
            spec,
            ctx,
            writer_open_attempted=writer_open_attempted,
            dlq_open_attempted=dlq_open_attempted,
        )
        raise
    return writer_opened, dlq_opened


async def rollback_open_failure(
    spec: PipelineRuntimeSpec,
    ctx: PipelineContext,
    *,
    writer_open_attempted: bool,
    dlq_open_attempted: bool,
) -> None:
    """Best-effort close partially opened sinks after startup failure."""
    if dlq_open_attempted and spec.config.dlq is not None:
        try:
            if not bool(getattr(spec.config.dlq, "_open_rolled_back", False)):
                await spec.config.dlq.close()
        except Exception as exc:
            ctx.log.exception(
                "pipeline_dlq_close_error_after_open_failure",
                error=str(exc),
            )
    if writer_open_attempted:
        try:
            if not bool(getattr(spec.writer, "_open_rolled_back", False)):
                await spec.writer.close()
        except Exception as exc:
            ctx.log.exception(
                "pipeline_writer_close_error_after_open_failure",
                error=str(exc),
            )


async def shutdown_runtime_components(
    spec: PipelineRuntimeSpec,
    state: PipelineRunState,
    *,
    transport: WriterTransport | None,
) -> Exception | None:
    """Flush and close runtime resources, suppressing only when run already failed."""
    first_error: Exception | None = None

    async def _capture(name: str, func: Callable[[], Awaitable[Any]]) -> None:
        nonlocal first_error
        try:
            await func()
        except Exception as exc:
            state.ctx.log.exception(name, error=str(exc))
            if first_error is None:
                first_error = exc

    if state.middlewares_started:
        await _capture(
            "pipeline_middleware_stop_error",
            lambda: spec.chain.stop_all(state.ctx),
        )

    if state.dlq_opened and spec.config.dlq is not None:
        await _capture("pipeline_dlq_flush_error", spec.config.dlq.flush)
        await _capture("pipeline_dlq_close_error", spec.config.dlq.close)

    if state.writer_opened and transport is not None:
        await _capture("pipeline_writer_flush_error", transport.flush)
        await _capture("pipeline_writer_close_error", transport.close)

    if spec.config.checkpoint is not None:
        await _capture(
            "pipeline_checkpoint_store_close_error",
            spec.config.checkpoint.close,
        )

    if state.suppress_shutdown_exceptions:
        if first_error is not None:
            state.ctx.log.warning("pipeline_shutdown_error_suppressed", error=str(first_error))
        return None
    return first_error
