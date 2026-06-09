"""Buffered-stage execution coordinator and adaptive backpressure controller."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from agora.core.runtime._buffered_backpressure import AdaptiveBackpressureController
from agora.core.runtime._buffered_resolver import dispatch_resolved_buffered_record
from agora.core.runtime._buffered_rust import RUST_AVAILABLE as _RUST_AVAILABLE
from agora.core.runtime._buffered_rust import LinearBatchBuffer
from agora.core.runtime._lanes import BatchLaneStrategy, BufferedLaneStrategy, LinearLaneStrategy
from agora.core.runtime._plan import RuntimeLane, RuntimePlan
from agora.core.runtime._source_adapter import SourceRuntimeAdapter

__all__ = [
    "_RUST_AVAILABLE",
    "AdaptiveBackpressureController",
    "ExecutionCoordinator",
    "LinearBatchBuffer",
]


if TYPE_CHECKING:
    from agora.core.context import PipelineContext
    from agora.core.middleware import MiddlewareChain
    from agora.core.runtime._delivery import CheckpointState, DeliveryEngine, RunState, SourceRecord
    from agora.core.source import BaseSource
    from agora.core.types import Backpressure


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
        await dispatch_resolved_buffered_record(
            chain=self.chain,
            delivery=self.delivery,
            writer_batch_size=self.writer_batch_size,
            state=state,
            future=future,
            split_index=split_index,
            buffered_name=buffered_name,
            source_record=source_record,
        )

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
    ) -> None:
        ctx.metrics.runtime.execution_lane = self.plan.lane.value
        ctx.metrics.runtime.source_data_plane = self.plan.source.emitted_plane.value
        ctx.metrics.runtime.writer_input_data_plane = self.plan.writer.input_data_plane.value
        ctx.metrics.runtime.writer_downgraded_sink_count = self.plan.writer.downgraded_sink_count
        if self.plan.lane == RuntimeLane.BATCH:
            await self._batch_lane.run(ctx, checkpoint_state)
            return

        source_records = self._source_adapter.iter_source_records(ctx)
        if self.plan.lane == RuntimeLane.BUFFERED:
            await self._buffered_lane.run(
                ctx,
                source_records,
                checkpoint_state,
                self.plan.buffered_stages,
            )
            return

        await self._linear_lane.run(ctx, source_records, checkpoint_state)
