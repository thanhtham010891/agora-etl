"""Pipeline execution orchestration extracted from ``BoundPipeline``."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from agora.core.errors import PipelineError
from agora.core.runtime import (
    DeliveryEngine,
    ExecutionCoordinator,
    RecordDeliveryError,
    build_runtime_plan,
    make_checkpoint_state,
)
from agora.core.session import PipelineLifecycleController, PipelineRunState
from agora.core.source import SourceRecordError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.core.context import PipelineContext
    from agora.core.metrics import PipelineRunSummary
    from agora.core.middleware import MiddlewareChain
    from agora.core.source import BaseSource
    from agora.core.types import DeliveryConfig
    from agora.core.writer import Writer


@dataclass(slots=True)
class PipelineRuntimeSpec:
    """Immutable runtime inputs needed to execute a prepared pipeline."""

    source: BaseSource[Any]
    chain: MiddlewareChain[Any, Any]
    writer: Writer[Any]
    pipeline_id: str
    config: DeliveryConfig
    live_metrics_callback: Callable[[PipelineContext], Awaitable[None]] | None = None


class PipelineExecutor:
    """Execute a prepared pipeline runtime spec.

    ``BoundPipeline`` remains the user-facing prepared pipeline object.
    ``PipelineExecutor`` owns orchestration concerns such as lifecycle order,
    checkpoint restore, DLQ delivery, and cleanup sequencing.
    """

    def __init__(self, spec: PipelineRuntimeSpec) -> None:
        self._spec = spec
        self._lifecycle = PipelineLifecycleController(spec)

    async def _report_live_metrics(
        self,
        state: PipelineRunState,
        execution: ExecutionCoordinator,
        stop_event: asyncio.Event,
    ) -> None:
        callback = self._spec.live_metrics_callback
        if callback is None:
            return

        while not stop_event.is_set():
            execution.sync_source_runtime_metrics(state.ctx)
            try:
                await callback(state.ctx)
            except Exception as exc:
                state.ctx.log.warning("pipeline_live_metrics_callback_error", error=str(exc))
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=1.0)
            except TimeoutError:
                continue

    async def _run_stream(
        self,
        state: PipelineRunState,
        execution: ExecutionCoordinator,
        *,
        source: BaseSource[Any],
    ) -> None:
        checkpoint_state = make_checkpoint_state()
        source_opened = False
        stream_error: BaseException | None = None

        try:
            await source.open()
            source_opened = True
            with state.ctx.trace_span(
                "source.stream",
                source=source.source_name,
                buffered=execution.plan.uses_buffered_lane,
                lane=execution.plan.lane,
                batch_source=execution.plan.batch_source,
                source_data_plane=execution.plan.source.emitted_plane,
                writer_input_data_plane=execution.plan.writer.input_data_plane,
                buffered_stage_count=len(execution.plan.buffered_stages),
                direct_flush_eligible=execution.plan.writer.direct_flush_eligible,
                arrow_fast_path_eligible=execution.plan.writer.arrow_fast_path,
                arrow_chain_eligible=execution.plan.writer.arrow_chain,
            ):
                await execution.run(
                    state.ctx,
                    checkpoint_state,
                )
        except BaseException as exc:
            stream_error = exc
        finally:
            if source_opened:
                try:
                    await source.close()
                except Exception as exc:
                    if stream_error is None:
                        raise
                    state.ctx.log.exception(
                        "pipeline_source_close_error_suppressed",
                        error=str(exc),
                    )

        if stream_error is not None:
            raise stream_error

    async def _handle_run_error(
        self,
        state: PipelineRunState,
        coordinator: DeliveryEngine,
        exc: BaseException,
    ) -> None:
        if isinstance(exc, KeyboardInterrupt):
            state.ctx.log.info("pipeline_interrupted")
            state.interrupted = True
            return

        state.run_error = exc
        if isinstance(exc, RecordDeliveryError):
            return

        state.metrics.records_errored += 1
        if not state.dlq_opened:
            return
        if isinstance(exc, SourceRecordError):
            await coordinator.write_to_dlq(
                ctx=state.ctx,
                stage=exc.stage,
                exc=exc.original,
                record=exc.record,
                original_record=exc.record,
                checkpoint=exc.checkpoint,
                source=exc.source or self._spec.source.source_name,
            )
            return

        await coordinator.write_to_dlq(
            ctx=state.ctx,
            record=None,
            stage="source_stream",
            exc=exc if isinstance(exc, Exception) else Exception(str(exc)),
        )

    def _raise_terminal_error(
        self, state: PipelineRunState, shutdown_error: Exception | None
    ) -> None:
        if state.run_error is not None:
            exc = state.run_error
            if isinstance(exc, (RecordDeliveryError, SourceRecordError)):
                original: BaseException = exc.original
            else:
                original = exc

            # If the original exception is already a PipelineError, enrich it
            # with runtime context and re-raise. Otherwise re-raise the original
            # exception unchanged so existing callers are not surprised.
            if isinstance(original, PipelineError):
                stage = "source_stream"
                if isinstance(exc, SourceRecordError):
                    stage = exc.stage or "source_stream"
                elif isinstance(exc, RecordDeliveryError):
                    stage = "sink"
                raise original.with_context(
                    pipeline_id=state.ctx.pipeline_id,
                    run_id=state.ctx.run_id,
                    stage=original.stage or stage,
                    source_name=original.source_name or self._spec.source.source_name,
                ) from original.__cause__

            raise original

        if shutdown_error is not None:
            raise shutdown_error

    async def execute(
        self,
        *,
        max_records: int | None = None,
        run_id: str | None = None,
    ) -> PipelineRunSummary:
        spec = self._spec
        if max_records is not None:
            spec = replace(spec, source=spec.source.limit(max_records))

        lifecycle = PipelineLifecycleController(spec)
        state = lifecycle.create_run_state(run_id=run_id, source_limit=max_records)
        await lifecycle.restore_checkpoint(state.ctx)
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
        )
        shutdown_error: Exception | None = None
        cancellation_error: asyncio.CancelledError | None = None
        live_metrics_stop: asyncio.Event | None = None
        live_metrics_task: asyncio.Task[None] | None = None

        if max_records is not None:
            state.ctx.log.info(
                "pipeline_source_limit_applied",
                source=spec.source.source_name,
                source_limit=max_records,
            )

        with state.ctx.trace_span(
            "pipeline.run",
            pipeline_id=spec.pipeline_id,
            run_id=state.ctx.run_id,
            source=spec.source.source_name,
            planned_lane=plan.lane,
            batch_source=plan.batch_source,
            source_data_plane=plan.source.emitted_plane,
            writer_input_data_plane=plan.writer.input_data_plane,
            downgraded_sink_count=plan.writer.downgraded_sink_count,
            buffered_stage_count=len(plan.buffered_stages),
            direct_flush_eligible=plan.writer.direct_flush_eligible,
            arrow_fast_path_eligible=plan.writer.arrow_fast_path,
            arrow_chain_eligible=plan.writer.arrow_chain,
        ) as span:
            try:
                await lifecycle.start_runtime(state)
                if spec.live_metrics_callback is not None:
                    execution.sync_source_runtime_metrics(state.ctx)
                    try:
                        await spec.live_metrics_callback(state.ctx)
                    except Exception as exc:
                        state.ctx.log.warning(
                            "pipeline_live_metrics_callback_error",
                            error=str(exc),
                        )
                    live_metrics_stop = asyncio.Event()
                    live_metrics_task = asyncio.create_task(
                        self._report_live_metrics(state, execution, live_metrics_stop),
                        name=f"{spec.pipeline_id}-live-metrics",
                    )
                await self._run_stream(state, execution, source=spec.source)
            except asyncio.CancelledError as exc:
                state.ctx.log.info("pipeline_cancelled")
                state.interrupted = True
                cancellation_error = exc
            except KeyboardInterrupt as exc:
                await self._handle_run_error(state, coordinator, exc)
            except Exception as exc:
                await self._handle_run_error(state, coordinator, exc)
            finally:
                if live_metrics_stop is not None:
                    live_metrics_stop.set()
                if live_metrics_task is not None:
                    with contextlib.suppress(Exception):
                        await live_metrics_task
                shutdown_error = await lifecycle.shutdown_runtime(state)
                execution.sync_source_runtime_metrics(state.ctx)
                if span is not None:
                    runtime = state.ctx.metrics.runtime
                    span.set_attribute("execution_lane", runtime.execution_lane)
                    span.set_attribute("source_data_plane", runtime.source_data_plane)
                    span.set_attribute("writer_input_data_plane", runtime.writer_input_data_plane)
                    span.set_attribute("direct_flush_active", runtime.direct_flush_active)
                    span.set_attribute("arrow_fast_path_active", runtime.arrow_fast_path_active)
                    span.set_attribute("arrow_chain_active", runtime.arrow_chain_active)
                    span.set_attribute(
                        "writer_downgraded_sink_count",
                        runtime.writer_downgraded_sink_count,
                    )
                    span.set_attribute("rust_prefetch_active", runtime.rust_prefetch_active)

        if cancellation_error is not None:
            raise cancellation_error
        self._raise_terminal_error(state, shutdown_error)
        return state.complete()


__all__ = ["PipelineExecutor", "PipelineRuntimeSpec"]
