"""Pipeline execution orchestration extracted from ``BoundPipeline``."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from agora.core.runtime import (
    CheckpointState,
    DeliveryEngine,
    ExecutionCoordinator,
    RecordDeliveryError,
    build_runtime_plan,
)
from agora.core.session import PipelineLifecycleController, PipelineRunState
from agora.core.source import SourceRecordError

if TYPE_CHECKING:
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


class PipelineExecutor:
    """Execute a prepared pipeline runtime spec.

    ``BoundPipeline`` remains the user-facing prepared pipeline object.
    ``PipelineExecutor`` owns orchestration concerns such as lifecycle order,
    checkpoint restore, DLQ delivery, and cleanup sequencing.
    """

    def __init__(self, spec: PipelineRuntimeSpec) -> None:
        self._spec = spec
        self._lifecycle = PipelineLifecycleController(spec)

    async def _run_stream(
        self,
        state: PipelineRunState,
        execution: ExecutionCoordinator,
    ) -> None:
        checkpoint_state = CheckpointState()

        async with self._spec.source:
            with state.ctx.trace_span(
                "source.stream",
                source=self._spec.source.source_name,
                buffered=execution.plan.uses_buffered_lane,
                lane=execution.plan.lane,
            ):
                await execution.run(
                    state.ctx,
                    checkpoint_state,
                    state.max_records,
                )

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
            if isinstance(state.run_error, RecordDeliveryError):
                raise state.run_error.original
            if isinstance(state.run_error, SourceRecordError):
                raise state.run_error.original
            raise state.run_error
        if shutdown_error is not None:
            raise shutdown_error

    async def execute(
        self,
        *,
        max_records: int | None = None,
        run_id: str | None = None,
    ) -> PipelineRunSummary:
        state = self._lifecycle.create_run_state(run_id=run_id, max_records=max_records)
        await self._lifecycle.restore_checkpoint(state.ctx)
        coordinator = self._lifecycle.make_delivery_coordinator()
        plan = build_runtime_plan(
            self._spec.source,
            self._spec.chain,
            self._spec.writer,
            writer_batch_size=self._spec.config.batch_size,
        )
        execution = ExecutionCoordinator(
            source=self._spec.source,
            chain=self._spec.chain,
            writer_batch_size=self._spec.config.batch_size,
            delivery=coordinator,
            plan=plan,
            max_buffer_size=self._spec.config.max_buffer_size,
            backpressure=self._spec.config.backpressure,
        )
        shutdown_error: Exception | None = None
        cancellation_error: asyncio.CancelledError | None = None

        with state.ctx.trace_span(
            "pipeline.run",
            pipeline_id=self._spec.pipeline_id,
            run_id=state.ctx.run_id,
            source=self._spec.source.source_name,
        ):
            try:
                await self._lifecycle.start_runtime(state)
                await self._run_stream(state, execution)
            except asyncio.CancelledError as exc:
                state.ctx.log.info("pipeline_cancelled")
                state.interrupted = True
                cancellation_error = exc
            except KeyboardInterrupt as exc:
                await self._handle_run_error(state, coordinator, exc)
            except Exception as exc:
                await self._handle_run_error(state, coordinator, exc)
            finally:
                shutdown_error = await self._lifecycle.shutdown_runtime(state)
                execution.sync_source_runtime_metrics(state.ctx)

        if cancellation_error is not None:
            raise cancellation_error
        self._raise_terminal_error(state, shutdown_error)
        return state.complete()


__all__ = ["PipelineExecutor", "PipelineRuntimeSpec"]
