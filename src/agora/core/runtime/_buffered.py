"""Buffered-stage execution coordinator and adaptive backpressure controller."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from agora.core.middleware import MiddlewareFailure
from agora.core.runtime._delivery import (
    CheckpointState,
    DeliveryEngine,
    ProcessedSourceRecord,
    RunState,
    SourceRecord,
)
from agora.core.runtime._lanes import BatchLaneStrategy, BufferedLaneStrategy, LinearLaneStrategy
from agora.core.runtime._plan import RuntimeLane, RuntimePlan
from agora.core.runtime._source_adapter import SourceRuntimeAdapter

try:
    from agora_rs import LinearBatchBuffer

    try:
        _test = LinearBatchBuffer(1, 1)
        del _test
        _RUST_AVAILABLE = True
    except Exception:
        _RUST_AVAILABLE = False
except ImportError:
    _RUST_AVAILABLE = False

    class LinearBatchBuffer:  # type: ignore[no-redef]
        """Placeholder — agora-rs not installed. Allows monkeypatching in tests."""

        def __init__(self, batch_size: int, metrics_flush_interval: int) -> None:
            raise ImportError("agora-etl-rs is not installed.")


if TYPE_CHECKING:
    from agora.core.context import PipelineContext
    from agora.core.middleware import MiddlewareChain
    from agora.core.source import BaseSource
    from agora.core.types import Backpressure


@dataclass(slots=True)
class AdaptiveBackpressureController:
    """Tune buffered-stage in-flight limits from writer/checkpoint pressure."""

    current_limit: int
    min_limit: int
    max_limit: int
    scale_up_step: int
    scale_down_step: int
    writer_slow_ms: float
    checkpoint_slow_ms: float
    last_writer_flush_count: int = 0
    last_writer_flush_time_ms: float = 0.0
    last_checkpoint_save_count: int = 0
    last_checkpoint_save_time_ms: float = 0.0

    def observe(self, runtime_metrics: Any, pending_count: int) -> int:
        writer_flush_count = runtime_metrics.writer_flush_count
        checkpoint_save_count = runtime_metrics.checkpoint_save_count
        writer_flush_delta = writer_flush_count - self.last_writer_flush_count
        checkpoint_save_delta = checkpoint_save_count - self.last_checkpoint_save_count
        writer_time_delta = runtime_metrics.writer_flush_time_ms - self.last_writer_flush_time_ms
        checkpoint_time_delta = (
            runtime_metrics.checkpoint_save_time_ms - self.last_checkpoint_save_time_ms
        )

        self.last_writer_flush_count = writer_flush_count
        self.last_writer_flush_time_ms = runtime_metrics.writer_flush_time_ms
        self.last_checkpoint_save_count = checkpoint_save_count
        self.last_checkpoint_save_time_ms = runtime_metrics.checkpoint_save_time_ms

        saw_pressure_signal = writer_flush_delta > 0 or checkpoint_save_delta > 0
        if not saw_pressure_signal:
            return self.current_limit

        writer_flush_avg = writer_time_delta / writer_flush_delta if writer_flush_delta > 0 else 0.0
        checkpoint_save_avg = (
            checkpoint_time_delta / checkpoint_save_delta if checkpoint_save_delta > 0 else 0.0
        )

        writer_is_slow = writer_flush_delta > 0 and writer_flush_avg >= self.writer_slow_ms
        checkpoint_is_slow = (
            checkpoint_save_delta > 0 and checkpoint_save_avg >= self.checkpoint_slow_ms
        )
        if writer_is_slow or checkpoint_is_slow:
            next_limit = max(self.min_limit, self.current_limit - self.scale_down_step)
            if next_limit < self.current_limit:
                self.current_limit = next_limit
                runtime_metrics.adaptive_backpressure_scale_down_count += 1
            return self.current_limit

        writer_fast_threshold = self.writer_slow_ms / 4 if self.writer_slow_ms > 0 else 0.0
        checkpoint_fast_threshold = (
            self.checkpoint_slow_ms / 4 if self.checkpoint_slow_ms > 0 else 0.0
        )
        writer_is_fast = writer_flush_delta == 0 or writer_flush_avg <= writer_fast_threshold
        checkpoint_is_fast = (
            checkpoint_save_delta == 0 or checkpoint_save_avg <= checkpoint_fast_threshold
        )
        backlog_saturated = pending_count >= self.current_limit
        if backlog_saturated and writer_is_fast and checkpoint_is_fast:
            next_limit = min(self.max_limit, self.current_limit + self.scale_up_step)
            if next_limit > self.current_limit:
                self.current_limit = next_limit
                runtime_metrics.adaptive_backpressure_scale_up_count += 1
        return self.current_limit


@dataclass(slots=True)
class ExecutionCoordinator:
    """Thin dispatcher — routes execution to the correct lane strategy."""

    source: BaseSource[Any]
    chain: MiddlewareChain[Any, Any]
    writer_batch_size: int
    delivery: DeliveryEngine
    plan: RuntimePlan
    max_buffer_size: int | None = None
    backpressure: Backpressure | None = None
    _linear_lane: LinearLaneStrategy = field(init=False, repr=False)
    _buffered_lane: BufferedLaneStrategy = field(init=False, repr=False)
    _batch_lane: BatchLaneStrategy = field(init=False, repr=False)
    _source_adapter: SourceRuntimeAdapter = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._linear_lane = LinearLaneStrategy(self)
        self._buffered_lane = BufferedLaneStrategy(self)
        self._batch_lane = BatchLaneStrategy(self)
        self._source_adapter = SourceRuntimeAdapter(
            source=self.source,
            has_buffered_stages=bool(self.plan.buffered_stages),
        )

    def _build_adaptive_backpressure_controller(
        self,
        base_limit: int,
    ) -> AdaptiveBackpressureController | None:
        bp = self.backpressure
        if bp is None:
            return None

        adaptive_ceiling = bp.max_buffer_size
        if adaptive_ceiling is None:
            adaptive_ceiling = max(base_limit * 4, base_limit, bp.min_buffer_size)

        if self.max_buffer_size is not None:
            adaptive_ceiling = min(adaptive_ceiling, self.max_buffer_size)

        adaptive_ceiling = max(1, adaptive_ceiling)
        min_limit = min(max(1, bp.min_buffer_size), adaptive_ceiling)
        current_limit = min(max(base_limit, min_limit), adaptive_ceiling)
        return AdaptiveBackpressureController(
            current_limit=current_limit,
            min_limit=min_limit,
            max_limit=adaptive_ceiling,
            scale_up_step=max(1, bp.scale_up_step),
            scale_down_step=max(1, bp.scale_down_step),
            writer_slow_ms=max(0.0, bp.writer_slow_ms),
            checkpoint_slow_ms=max(0.0, bp.checkpoint_slow_ms),
        )

    async def resolve_buffered_record(
        self,
        state: RunState,
        future: Any,
        split_index: int,
        buffered_name: str,
        source_record: SourceRecord,
    ) -> None:
        try:
            processed_record = await future
        except Exception as exc:
            state.ctx.log.exception("pipeline_buffered_stage_error", middleware=buffered_name)
            processed_record = ProcessedSourceRecord(
                source_record=source_record,
                result=None,
                failure=MiddlewareFailure(
                    stage="buffered_middleware",
                    record=source_record.raw,
                    middleware=buffered_name,
                    exception=exc,
                ),
            )

        if not isinstance(processed_record, ProcessedSourceRecord):
            buffered_result = processed_record
            if buffered_result is None:
                await self.delivery.dispatch_processed_result(
                    state,
                    None,
                    source_record.raw,
                    source_record.checkpoint,
                    self.writer_batch_size,
                    on_success=source_record.on_success,
                )
                return

            final_result = await self.chain.process_range(
                split_index + 1,
                self.chain.middleware_count(),
                buffered_result,
                state.ctx,
            )
            processed_record = ProcessedSourceRecord(
                source_record=source_record,
                result=final_result.value,
                failure=final_result.failure,
            )

        await self.delivery.dispatch_processed_result(
            state,
            processed_record.result,
            processed_record.source_record.raw,
            processed_record.source_record.checkpoint,
            self.writer_batch_size,
            failure=processed_record.failure,
            on_success=processed_record.source_record.on_success,
        )

    def reached_max_records(
        self,
        ctx: PipelineContext,
        count: int,
        max_records: int | None,
    ) -> bool:
        if max_records is None or count < max_records:
            return False
        ctx.log.info("pipeline_max_records_reached", max_records=max_records)
        return True

    def sync_source_runtime_metrics(self, ctx: PipelineContext) -> None:
        self._source_adapter.sync_runtime_metrics(ctx)

    @staticmethod
    def rust_available() -> bool:
        return _RUST_AVAILABLE

    @staticmethod
    def make_linear_batch_buffer(batch_size: int, flush_interval: int) -> Any:
        if not _RUST_AVAILABLE:
            raise RuntimeError("Rust extension unavailable — cannot create LinearBatchBuffer")
        return LinearBatchBuffer(batch_size, flush_interval)

    @staticmethod
    def make_metrics_accumulator(flush_interval: int) -> Any:
        return SourceRuntimeAdapter.make_metrics_accumulator(flush_interval)

    async def run(
        self,
        ctx: PipelineContext,
        checkpoint_state: CheckpointState,
        max_records: int | None,
    ) -> None:
        ctx.metrics.runtime.execution_lane = self.plan.lane.value
        if self.plan.lane == RuntimeLane.BATCH:
            await self._batch_lane.run(ctx, checkpoint_state, max_records)
            return

        source_records = self._source_adapter.iter_source_records(ctx)
        if self.plan.lane == RuntimeLane.BUFFERED:
            await self._buffered_lane.run(
                ctx,
                source_records,
                checkpoint_state,
                max_records,
                self.plan.buffered_stages,
            )
            return

        await self._linear_lane.run(ctx, source_records, checkpoint_state, max_records)
