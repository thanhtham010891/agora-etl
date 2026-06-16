"""Internal helper components for DLQ and checkpoint side effects."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from agora.core.checkpoint import Checkpoint, CheckpointStore, CheckpointValue
from agora.core.dlq import DLQRecord
from agora.core.fencing import assert_run_fence_active
from agora.core.types import CheckpointFailurePolicy, DLQFailurePolicy

if TYPE_CHECKING:
    from collections.abc import Callable

    from agora.core.context import PipelineContext
    from agora.core.runtime._delivery import CheckpointState
    from agora.core.sink import BaseSink


@dataclass(slots=True)
class DLQWriter:
    """Encapsulate DLQ write side effects for the delivery engine."""

    source_name: str
    current_checkpoint: Callable[[], CheckpointValue]
    dlq_sink: BaseSink[DLQRecord] | None
    dlq_failure_policy: DLQFailurePolicy
    dlq_redactor: Callable[[Any], Any] | None = None

    def _redact(self, value: Any) -> Any:
        if self.dlq_redactor is None:
            return value
        return self.dlq_redactor(value)

    async def write(
        self,
        ctx: PipelineContext,
        stage: str,
        exc: Exception,
        *,
        record: Any = None,
        original_record: Any | None = None,
        processed_record: Any | None = None,
        source: str | None = None,
        checkpoint: Any | None = None,
        middleware: str | None = None,
        sink: str | None = None,
    ) -> bool:
        if self.dlq_sink is None:
            return False

        try:
            await assert_run_fence_active(ctx)
            with ctx.trace_span(
                "dlq.write",
                stage=stage,
                source=source or self.source_name,
                sink=sink or getattr(self.dlq_sink, "sink_name", "dlq"),
            ):
                await self.dlq_sink.write(
                    DLQRecord(
                        pipeline_id=ctx.pipeline_id,
                        run_id=ctx.run_id,
                        stage=stage,
                        error_type=type(exc).__name__,
                        error_message=str(self._redact(str(exc))),
                        record=(
                            self._redact(original_record)
                            if original_record is not None
                            else self._redact(record)
                            if record is not None
                            else self._redact(processed_record)
                        ),
                        source=source or self.source_name,
                        checkpoint=(
                            self._redact(
                                checkpoint if checkpoint is not None else self.current_checkpoint()
                            )
                        ),
                        details=self._redact(getattr(exc, "dlq_details", None)),
                        middleware=middleware,
                        sink=sink,
                        original_record=self._redact(original_record),
                        processed_record=self._redact(processed_record),
                    )
                )
            return True
        except Exception:
            ctx.metrics.runtime.dlq_failure_count += 1
            ctx.log.exception("pipeline_dlq_write_error", stage=stage, error=str(exc))
            if self.dlq_failure_policy == DLQFailurePolicy.RAISE:
                raise
            return False


@dataclass(slots=True)
class CheckpointManager:
    """Encapsulate checkpoint persistence policy for the delivery engine."""

    source_name: str
    checkpoint_store: CheckpointStore | None
    checkpoint_failure_policy: CheckpointFailurePolicy
    checkpoint_key: str
    checkpoint_every: int

    def prepare(
        self,
        ctx: PipelineContext,
        checkpoint_state: CheckpointState,
        checkpoint_value: CheckpointValue,
    ) -> Checkpoint | None:
        if self.checkpoint_store is None or checkpoint_value is None:
            return None

        checkpoint_state.increment()
        if not checkpoint_state.should_save(checkpoint_value, self.checkpoint_every):
            return None

        return Checkpoint(
            pipeline_id=ctx.pipeline_id,
            run_id=ctx.run_id,
            source=self.source_name,
            value=checkpoint_value,
        )

    async def persist(
        self,
        ctx: PipelineContext,
        checkpoint_state: CheckpointState,
        checkpoint: Checkpoint,
        *,
        batch_size: int = 1,
    ) -> None:
        t0 = time.monotonic()
        try:
            await assert_run_fence_active(ctx)
            if self.checkpoint_store is None:
                raise RuntimeError("checkpoint_store is None — cannot persist checkpoint")
            with ctx.trace_span(
                "checkpoint.save",
                checkpoint_key=self.checkpoint_key,
                source=self.source_name,
                batch_size=batch_size,
            ):
                await self.checkpoint_store.save(self.checkpoint_key, checkpoint)
        except Exception:
            ctx.metrics.runtime.checkpoint_failure_count += 1
            if self.checkpoint_failure_policy == CheckpointFailurePolicy.LOG_AND_CONTINUE:
                ctx.log.exception(
                    "pipeline_checkpoint_save_error",
                    checkpoint_key=self.checkpoint_key,
                    source=self.source_name,
                )
                checkpoint_state.mark_saved(checkpoint.value)
                return
            raise
        checkpoint_state.mark_saved(checkpoint.value)
        ctx.metrics.runtime.checkpoint_save_time_ms += (time.monotonic() - t0) * 1000
        ctx.metrics.last_checkpoint = checkpoint
        ctx.metrics.runtime.checkpoint_save_count += 1
        ctx.metrics.runtime.checkpoint_save_max_batch_size = max(
            ctx.metrics.runtime.checkpoint_save_max_batch_size,
            batch_size,
        )

    async def save(
        self,
        ctx: PipelineContext,
        checkpoint_state: CheckpointState,
        checkpoint_value: CheckpointValue,
    ) -> None:
        checkpoint = self.prepare(ctx, checkpoint_state, checkpoint_value)
        if checkpoint is None:
            return
        await self.persist(ctx, checkpoint_state, checkpoint, batch_size=1)

    async def save_batch(
        self,
        ctx: PipelineContext,
        checkpoint_state: CheckpointState,
        checkpoint_value: CheckpointValue,
        batch_size: int,
    ) -> None:
        if self.checkpoint_store is None or checkpoint_value is None or batch_size <= 0:
            return
        checkpoint_state.increment_by(batch_size)
        if not checkpoint_state.should_save(checkpoint_value, self.checkpoint_every):
            return
        checkpoint = Checkpoint(
            pipeline_id=ctx.pipeline_id,
            run_id=ctx.run_id,
            source=self.source_name,
            value=checkpoint_value,
        )
        await self.persist(
            ctx,
            checkpoint_state,
            checkpoint,
            batch_size=batch_size,
        )
