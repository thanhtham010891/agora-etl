"""Pipeline execution orchestration extracted from ``BoundPipeline``."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from agora.core.runtime import (
    CheckpointState,
    ExecutionCoordinator,
    RecordDeliveryError,
)
from agora.core.session import PipelineLifecycleController, PipelineRunState
from agora.core.source import SourceRecordError

if TYPE_CHECKING:
    from agora.core.checkpoint import CheckpointStore
    from agora.core.dlq import DLQRecord
    from agora.core.metrics import PipelineRunSummary
    from agora.core.middleware import MiddlewareChain
    from agora.core.sink import BaseSink
    from agora.core.source import BaseSource
    from agora.core.tracing import PipelineTracer
    from agora.core.types import CheckpointFailurePolicy, DLQFailurePolicy, SinkFailurePolicy
    from agora.core.writer import Writer


@dataclass(slots=True)
class PipelineRuntimeSpec:
    """Immutable runtime inputs needed to execute a prepared pipeline."""

    source: BaseSource[Any]
    chain: MiddlewareChain[Any, Any]
    writer: Writer[Any]
    pipeline_id: str
    dlq_sink: BaseSink[DLQRecord] | None
    dlq_failure_policy: DLQFailurePolicy
    checkpoint_store: CheckpointStore | None
    checkpoint_failure_policy: CheckpointFailurePolicy
    checkpoint_key: str
    checkpoint_every: int
    writer_batch_size: int
    sink_failure_policy: SinkFailurePolicy
    tracer: PipelineTracer
    max_buffer_size: int | None = None
    adaptive_backpressure: bool = False
    adaptive_min_buffer_size: int = 1
    adaptive_max_buffer_size: int | None = None
    adaptive_scale_up_step: int = 1
    adaptive_scale_down_step: int = 1
    adaptive_writer_slow_ms: float = 25.0
    adaptive_checkpoint_slow_ms: float = 10.0


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
                buffered=bool(self._spec.chain.buffered_stages()),
            ):
                buffered_stages = self._spec.chain.buffered_stages()
                source_records = execution.iter_source_records(state.ctx)

                if not buffered_stages:
                    await execution.run_linear_pipeline(
                        state.ctx,
                        source_records,
                        checkpoint_state,
                        state.max_records,
                    )
                    return

                await execution.run_buffered_pipeline(
                    state.ctx,
                    source_records,
                    checkpoint_state,
                    state.max_records,
                    buffered_stages,
                )

    async def _handle_run_error(
        self,
        state: PipelineRunState,
        coordinator,
        exc: Exception,
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
            exc=exc,
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
        execution = ExecutionCoordinator(
            source=self._spec.source,
            chain=self._spec.chain,
            writer_batch_size=self._spec.writer_batch_size,
            delivery=coordinator,
            max_buffer_size=self._spec.max_buffer_size,
            adaptive_backpressure=self._spec.adaptive_backpressure,
            adaptive_min_buffer_size=self._spec.adaptive_min_buffer_size,
            adaptive_max_buffer_size=self._spec.adaptive_max_buffer_size,
            adaptive_scale_up_step=self._spec.adaptive_scale_up_step,
            adaptive_scale_down_step=self._spec.adaptive_scale_down_step,
            adaptive_writer_slow_ms=self._spec.adaptive_writer_slow_ms,
            adaptive_checkpoint_slow_ms=self._spec.adaptive_checkpoint_slow_ms,
        )
        shutdown_error: Exception | None = None

        with state.ctx.trace_span(
            "pipeline.run",
            pipeline_id=self._spec.pipeline_id,
            run_id=state.ctx.run_id,
            source=self._spec.source.source_name,
        ):
            try:
                await self._lifecycle.start_runtime(state)
                await self._run_stream(state, execution)
            except KeyboardInterrupt as exc:
                await self._handle_run_error(state, coordinator, exc)
            except Exception as exc:
                await self._handle_run_error(state, coordinator, exc)
            finally:
                shutdown_error = await self._lifecycle.shutdown_runtime(state)
                execution.sync_source_runtime_metrics(state.ctx)

        self._raise_terminal_error(state, shutdown_error)
        return state.complete()


__all__ = ["PipelineExecutor", "PipelineRuntimeSpec"]
