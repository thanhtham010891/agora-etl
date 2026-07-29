"""Pipeline execution orchestration extracted from ``BoundPipeline``."""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any

from agora.core._executor_support import (
    apply_runtime_trace_attributes,
    invoke_live_metrics_callback,
    pipeline_run_trace_attrs,
    prepare_execution,
    source_stream_trace_attrs,
)
from agora.core._executor_types import PipelineRuntimeSpec
from agora.core.acceleration import AccelerationCapability, acceleration_supports
from agora.core.errors import PipelineError
from agora.core.fencing import FenceLostError
from agora.core.runtime import (
    DeliveryEngine,
    ExecutionCoordinator,
    RecordDeliveryError,
    _CheckpointSaveError,
    make_checkpoint_state,
)
from agora.core.source import SourceRecordError

if TYPE_CHECKING:
    from agora.core.metrics import PipelineRunSummary
    from agora.core.session import PipelineRunState
    from agora.core.source import BaseSource


class PipelineExecutor:
    """Execute a prepared pipeline runtime spec.

    ``BoundPipeline`` remains the user-facing prepared pipeline object.
    ``PipelineExecutor`` owns orchestration concerns such as lifecycle order,
    checkpoint restore, DLQ delivery, and cleanup sequencing.
    """

    def __init__(self, spec: PipelineRuntimeSpec) -> None:
        self._spec = spec

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
            callback_error = await invoke_live_metrics_callback(callback, state.ctx)
            if callback_error is not None:
                state.ctx.log.warning(
                    "pipeline_live_metrics_callback_error",
                    error=str(callback_error),
                )
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
        checkpoint_state = make_checkpoint_state(mode=self._spec.config.acceleration_mode)
        state.ctx.metrics.runtime.rust_checkpoint_state_active = acceleration_supports(
            AccelerationCapability.CHECKPOINT_STATE,
            mode=self._spec.config.acceleration_mode,
        )
        source_opened = False
        stream_error: BaseException | None = None

        try:
            await source.open()
            source_opened = True
            with state.ctx.trace_span("source.stream", **source_stream_trace_attrs(execution)):
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
        if isinstance(exc, FenceLostError):
            return
        if isinstance(exc, (RecordDeliveryError, _CheckpointSaveError)):
            return

        state.metrics.records_errored += 1
        if not state.dlq_opened:
            return
        if isinstance(exc, SourceRecordError):
            routed = await coordinator.write_to_dlq(
                ctx=state.ctx,
                stage=exc.stage,
                exc=exc.original,
                record=exc.record,
                original_record=exc.record,
                checkpoint=exc.checkpoint,
                source=exc.source or self._spec.source.source_name,
            )
            if routed and exc.on_handled is not None:
                await exc.on_handled()
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
            if isinstance(exc, (RecordDeliveryError, SourceRecordError, _CheckpointSaveError)):
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
        prepared = prepare_execution(self._spec, max_records=max_records, run_id=run_id)
        spec = prepared.spec
        lifecycle = prepared.lifecycle
        state = prepared.state
        await lifecycle.restore_checkpoint(state.ctx)
        coordinator = prepared.coordinator
        plan = prepared.plan
        execution = prepared.execution
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
            **pipeline_run_trace_attrs(spec, plan, state.ctx.run_id),
        ) as span:
            try:
                await lifecycle.start_runtime(state)
                if spec.live_metrics_callback is not None:
                    execution.sync_source_runtime_metrics(state.ctx)
                    callback_error = await invoke_live_metrics_callback(
                        spec.live_metrics_callback,
                        state.ctx,
                    )
                    if callback_error is not None:
                        state.ctx.log.warning(
                            "pipeline_live_metrics_callback_error",
                            error=str(callback_error),
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
                    apply_runtime_trace_attributes(span, state.ctx.metrics.runtime)

        if cancellation_error is not None:
            raise cancellation_error
        self._raise_terminal_error(state, shutdown_error)
        return state.complete()


__all__ = ["PipelineExecutor", "PipelineRuntimeSpec"]
