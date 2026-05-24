"""Pipeline run session and lifecycle helpers."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from agora.core.checkpoint import is_checkpoint_capable
from agora.core.context import PipelineContext
from agora.core.metrics import PipelineMetrics
from agora.core.runtime import RecordDeliveryCoordinator
from agora.core.sink import bind_context_if_supported
from agora.core.types import CheckpointFailurePolicy

if TYPE_CHECKING:
    from agora.core.executor import PipelineRuntimeSpec
    from agora.core.metrics import PipelineRunSummary


@dataclass(slots=True)
class PipelineRunState:
    """Mutable run-scoped state used by the pipeline executor."""

    ctx: PipelineContext
    max_records: int | None
    middlewares_started: bool = False
    writer_opened: bool = False
    dlq_opened: bool = False
    interrupted: bool = False
    run_error: Exception | None = None

    @property
    def metrics(self) -> PipelineMetrics:
        return self.ctx.metrics

    @property
    def suppress_shutdown_exceptions(self) -> bool:
        return self.interrupted or self.run_error is not None

    def complete(self) -> PipelineRunSummary:
        """Finalize and return the immutable summary for this run."""
        summary = self.metrics.snapshot()
        self.ctx.log.info(
            "pipeline_complete",
            consumed=summary.records_consumed,
            written=summary.records_written,
            dropped=summary.records_dropped,
            errors=summary.records_errored,
            elapsed=f"{summary.elapsed_seconds:.1f}s",
        )
        return summary


class PipelineLifecycleController:
    """Own runtime lifecycle sequencing for a prepared pipeline."""

    def __init__(self, spec: PipelineRuntimeSpec) -> None:
        self._spec = spec

    def create_run_state(
        self,
        *,
        run_id: str | None,
        max_records: int | None,
    ) -> PipelineRunState:
        metrics = PipelineMetrics()
        ctx = PipelineContext(
            pipeline_id=self._spec.pipeline_id,
            metrics=metrics,
            run_id=run_id or str(uuid.uuid4()),
            tracer=self._spec.tracer,
        )
        ctx.log.info("pipeline_start", max_records=max_records)
        return PipelineRunState(ctx=ctx, max_records=max_records)

    def make_delivery_coordinator(self) -> RecordDeliveryCoordinator:
        return RecordDeliveryCoordinator(
            writer=self._spec.writer,
            source_name=self._spec.source.source_name,
            current_checkpoint=self._spec.source.current_checkpoint,
            dlq_sink=self._spec.dlq_sink,
            dlq_failure_policy=self._spec.dlq_failure_policy,
            sink_failure_policy=self._spec.sink_failure_policy,
            checkpoint_store=self._spec.checkpoint_store,
            checkpoint_failure_policy=self._spec.checkpoint_failure_policy,
            checkpoint_key=self._spec.checkpoint_key,
            checkpoint_every=self._spec.checkpoint_every,
        )

    async def restore_checkpoint(self, ctx: PipelineContext) -> None:
        if self._spec.checkpoint_store is None:
            return

        if not is_checkpoint_capable(self._spec.source):
            ctx.log.warning(
                "pipeline_checkpoint_unsupported_source",
                source=self._spec.source.source_name,
                checkpoint_key=self._spec.checkpoint_key,
            )
            return

        ctx.metrics.runtime.checkpoint_enabled = True
        try:
            with ctx.trace_span(
                "checkpoint.load",
                checkpoint_key=self._spec.checkpoint_key,
                source=self._spec.source.source_name,
            ) as span:
                checkpoint = await self._spec.checkpoint_store.load(self._spec.checkpoint_key)
                span.set_attribute("checkpoint.loaded", checkpoint is not None)
                await self._spec.source.prepare_resume(checkpoint)
        except Exception:
            ctx.metrics.runtime.checkpoint_failure_count += 1
            if self._spec.checkpoint_failure_policy == CheckpointFailurePolicy.LOG_AND_CONTINUE:
                ctx.log.exception(
                    "pipeline_checkpoint_load_error",
                    checkpoint_key=self._spec.checkpoint_key,
                )
                return
            raise

        if checkpoint is not None:
            ctx.metrics.last_checkpoint = checkpoint
            ctx.log.info(
                "pipeline_checkpoint_loaded",
                checkpoint_key=self._spec.checkpoint_key,
                source=checkpoint.source,
            )

    async def start_runtime(self, state: PipelineRunState) -> None:
        await self._spec.chain.start_all(state.ctx)
        state.middlewares_started = True
        state.writer_opened, state.dlq_opened = await self._open_sinks(state.ctx)

    async def shutdown_runtime(self, state: PipelineRunState) -> Exception | None:
        first_error: Exception | None = None

        async def _capture(name: str, func) -> None:
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
                lambda: self._spec.chain.stop_all(state.ctx),
            )

        if state.dlq_opened and self._spec.dlq_sink is not None:
            await _capture("pipeline_dlq_flush_error", self._spec.dlq_sink.flush)
            await _capture("pipeline_dlq_close_error", self._spec.dlq_sink.close)

        if state.writer_opened:
            await _capture("pipeline_writer_flush_error", self._spec.writer.flush)
            await _capture("pipeline_writer_close_error", self._spec.writer.close)

        if self._spec.checkpoint_store is not None:
            await _capture(
                "pipeline_checkpoint_store_close_error", self._spec.checkpoint_store.close
            )

        if state.suppress_shutdown_exceptions:
            if first_error is not None:
                state.ctx.log.warning("pipeline_shutdown_error_suppressed", error=str(first_error))
            return None
        return first_error

    async def _open_sinks(self, ctx: PipelineContext) -> tuple[bool, bool]:
        writer_opened = False
        dlq_opened = False
        try:
            bind_context_if_supported(self._spec.writer, ctx)
            with ctx.trace_span("writer.open", writer=type(self._spec.writer).__name__):
                await self._spec.writer.open()
            writer_opened = True
            if self._spec.dlq_sink is not None:
                bind_context_if_supported(self._spec.dlq_sink, ctx)
                with ctx.trace_span("dlq.open", sink=self._spec.dlq_sink.sink_name):
                    await self._spec.dlq_sink.open()
                dlq_opened = True
        except Exception:
            if dlq_opened and self._spec.dlq_sink is not None:
                try:
                    await self._spec.dlq_sink.close()
                except Exception as exc:
                    ctx.log.exception(
                        "pipeline_dlq_close_error_after_open_failure",
                        error=str(exc),
                    )
            if writer_opened:
                try:
                    await self._spec.writer.close()
                except Exception as exc:
                    ctx.log.exception(
                        "pipeline_writer_close_error_after_open_failure",
                        error=str(exc),
                    )
            raise
        return writer_opened, dlq_opened


__all__ = ["PipelineLifecycleController", "PipelineRunState"]
