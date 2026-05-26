"""Buffered-stage execution coordinator and adaptive backpressure controller."""

from __future__ import annotations

import asyncio
import time
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from agora.core.checkpoint import is_checkpoint_capable
from agora.core.middleware import MiddlewareFailure
from agora.core.runtime._delivery import (
    CheckpointState,
    ProcessedSourceRecord,
    RecordDeliveryCoordinator,
    RecordDeliveryError,
    RunState,
    SourceQueueError,
    SourceRecord,
)
from agora.core.source import (
    DeliveryHookSource,
    prefetch_limit_for,
    source_runtime_metrics,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from agora.core.context import PipelineContext
    from agora.core.middleware import MiddlewareChain
    from agora.core.source import BaseSource


SOURCE_QUEUE_DONE = object()


@dataclass(slots=True)
class BufferedStageSpec:
    index: int
    middleware: Any
    name: str
    concurrency: int


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
    """Owns source iteration, prefetch, and buffered-stage draining."""

    source: BaseSource[Any]
    chain: MiddlewareChain[Any, Any]
    writer_batch_size: int
    delivery: RecordDeliveryCoordinator
    max_buffer_size: int | None = None
    adaptive_backpressure: bool = False
    adaptive_min_buffer_size: int = 1
    adaptive_max_buffer_size: int | None = None
    adaptive_scale_up_step: int = 1
    adaptive_scale_down_step: int = 1
    adaptive_writer_slow_ms: float = 25.0
    adaptive_checkpoint_slow_ms: float = 10.0

    def _build_adaptive_backpressure_controller(
        self,
        base_limit: int,
    ) -> AdaptiveBackpressureController | None:
        if not self.adaptive_backpressure:
            return None

        adaptive_ceiling = self.adaptive_max_buffer_size
        if adaptive_ceiling is None:
            adaptive_ceiling = max(base_limit * 4, base_limit, self.adaptive_min_buffer_size)

        if self.max_buffer_size is not None:
            adaptive_ceiling = min(adaptive_ceiling, self.max_buffer_size)

        adaptive_ceiling = max(1, adaptive_ceiling)
        min_limit = min(max(1, self.adaptive_min_buffer_size), adaptive_ceiling)
        current_limit = min(max(base_limit, min_limit), adaptive_ceiling)
        return AdaptiveBackpressureController(
            current_limit=current_limit,
            min_limit=min_limit,
            max_limit=adaptive_ceiling,
            scale_up_step=max(1, self.adaptive_scale_up_step),
            scale_down_step=max(1, self.adaptive_scale_down_step),
            writer_slow_ms=max(0.0, self.adaptive_writer_slow_ms),
            checkpoint_slow_ms=max(0.0, self.adaptive_checkpoint_slow_ms),
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

    async def iter_prefetched_source_records(
        self,
        ctx: PipelineContext,
        prefetch_limit: int,
    ) -> AsyncGenerator[SourceRecord, None]:
        source_queue: asyncio.Queue[object] = asyncio.Queue(maxsize=prefetch_limit)

        async def _pump_source() -> None:
            # Cache the isinstance check — runtime_checkable Protocol checks are
            # expensive and the source type never changes mid-run.
            _has_delivery_hook = isinstance(self.source, DeliveryHookSource)
            try:
                async for record in self.source.stream():
                    if source_queue.full():
                        ctx.metrics.runtime.source_prefetch_block_count += 1
                    await source_queue.put(
                        SourceRecord(
                            raw=record,
                            checkpoint=self.source.current_checkpoint(),
                            on_success=self.source.delivery_success_callback()
                            if _has_delivery_hook
                            else None,
                        )
                    )
                    ctx.metrics.runtime.source_prefetch_max_depth = max(
                        ctx.metrics.runtime.source_prefetch_max_depth,
                        source_queue.qsize(),
                    )
            except Exception as exc:
                await source_queue.put(SourceQueueError(exc))
            finally:
                await source_queue.put(SOURCE_QUEUE_DONE)

        producer_task = asyncio.create_task(_pump_source())

        try:
            while True:
                item = await source_queue.get()
                if item is SOURCE_QUEUE_DONE:
                    break
                if isinstance(item, SourceQueueError):
                    raise item.exc
                yield item
        finally:
            # Drain queue first so producer unblocks from await source_queue.put()
            while not source_queue.empty():
                source_queue.get_nowait()
            if not producer_task.done():
                producer_task.cancel()
            with suppress(asyncio.CancelledError):
                await producer_task
            # Final drain in case producer added items after cancel
            while not source_queue.empty():
                source_queue.get_nowait()

    async def iter_source_records(self, ctx: PipelineContext) -> AsyncGenerator[SourceRecord, None]:
        prefetch_limit = prefetch_limit_for(self.source)
        checkpoint_capable = is_checkpoint_capable(self.source)
        has_delivery_hook = isinstance(self.source, DeliveryHookSource)

        if prefetch_limit <= 0:
            async for record in self.source.stream():
                yield SourceRecord(
                    raw=record,
                    checkpoint=self.source.current_checkpoint() if checkpoint_capable else None,
                    on_success=self.source.delivery_success_callback()
                    if has_delivery_hook
                    else None,
                )
            return

        ctx.metrics.runtime.source_prefetch_enabled = True
        ctx.metrics.runtime.source_prefetch_limit = prefetch_limit
        ctx.log.info(
            "pipeline_source_prefetch_enabled",
            source=self.source.source_name,
            prefetch_limit=prefetch_limit,
        )

        async for record in self.iter_prefetched_source_records(ctx, prefetch_limit):
            yield record

    def sync_source_runtime_metrics(self, ctx: PipelineContext) -> None:
        metrics = source_runtime_metrics(self.source)
        ctx.metrics.runtime.source_record_error_count = metrics.record_error_count
        ctx.metrics.runtime.source_record_drop_count = metrics.record_drop_count

    async def run_linear_pipeline(
        self,
        ctx: PipelineContext,
        source_records: AsyncGenerator[SourceRecord, None],
        checkpoint_state: CheckpointState,
        max_records: int | None,
    ) -> None:
        state = RunState(ctx=ctx, checkpoint_state=checkpoint_state, pending_writes=[])
        source_error: Exception | None = None

        # Cache branch decisions — never change mid-run.
        _use_batching = self.writer_batch_size > 1
        _batch_size = self.writer_batch_size
        _max_records = max_records
        _has_max = max_records is not None
        _source_name = self.source.source_name
        _metrics = ctx.metrics

        try:
            async for source_record in source_records:
                _metrics.records_consumed += 1
                _metrics.by_source[_source_name] = _metrics.by_source.get(_source_name, 0) + 1
                state.processed_count += 1

                result = await self.chain.process(source_record.raw, ctx)
                if _use_batching:
                    await self.delivery.queue_processed_record(
                        state,
                        result.value,
                        source_record.raw,
                        source_record.checkpoint,
                        _batch_size,
                        failure=result.failure,
                        on_success=source_record.on_success,
                    )
                else:
                    await self.delivery.write_processed_record(
                        state,
                        result.value,
                        source_record.raw,
                        source_record.checkpoint,
                        failure=result.failure,
                        on_success=source_record.on_success,
                    )
                if _has_max and state.processed_count >= _max_records:
                    ctx.log.info("pipeline_max_records_reached", max_records=_max_records)
                    break
        except Exception as exc:
            source_error = exc

        await self.delivery.flush_pending_writes(state)
        if source_error is not None:
            raise source_error

    async def run_buffered_pipeline(
        self,
        ctx: PipelineContext,
        source_records: AsyncGenerator[SourceRecord, None],
        checkpoint_state: CheckpointState,
        max_records: int | None,
        buffered_stages: list[tuple[int, Any]],
    ) -> None:
        stage_plan = [
            BufferedStageSpec(
                index=index,
                middleware=middleware,
                name=getattr(middleware, "name", "buffered"),
                concurrency=max(1, getattr(middleware, "min_concurrency", 1)),
            )
            for index, middleware in buffered_stages
        ]
        buffered_name = stage_plan[0].name
        split_index = stage_plan[0].index
        stage_limit = max(1, sum(stage.concurrency for stage in stage_plan))
        adaptive_controller = self._build_adaptive_backpressure_controller(stage_limit)
        if adaptive_controller is None:
            if self.max_buffer_size is not None:
                stage_limit = min(stage_limit, self.max_buffer_size)
            stage_limit = max(1, stage_limit)
        else:
            stage_limit = adaptive_controller.current_limit
            ctx.metrics.runtime.adaptive_backpressure_enabled = True
            ctx.metrics.runtime.adaptive_backpressure_min_limit = adaptive_controller.min_limit
            ctx.metrics.runtime.adaptive_backpressure_max_limit = adaptive_controller.max_limit
        ctx.metrics.runtime.buffered_stage_limit = stage_limit
        pending_tasks: dict[int, tuple[Any, SourceRecord]] = {}
        next_sequence = 0
        next_commit = 0
        state = RunState(ctx=ctx, checkpoint_state=checkpoint_state, pending_writes=[])
        source_error: BaseException | None = None

        try:
            async for source_record in source_records:
                ctx.metrics.records_consumed += 1
                ctx.metrics.inc_source(self.source.source_name)
                state.processed_count += 1

                prefix_result = await self.chain.process_range(
                    0, split_index, source_record.raw, ctx
                )
                if prefix_result.value is None:
                    await self.delivery.dispatch_processed_result(
                        state,
                        None,
                        source_record.raw,
                        source_record.checkpoint,
                        self.writer_batch_size,
                        failure=prefix_result.failure,
                        on_success=source_record.on_success,
                    )
                    if self.reached_max_records(ctx, state.processed_count, max_records):
                        break
                    continue

                pending_tasks[next_sequence] = (
                    asyncio.create_task(
                        self.process_record_through_buffered_stages(
                            source_record,
                            ctx,
                            stage_plan,
                            prefix_result.value,
                        )
                    ),
                    source_record,
                )
                next_sequence += 1
                ctx.metrics.runtime.buffered_stage_max_in_flight = max(
                    ctx.metrics.runtime.buffered_stage_max_in_flight,
                    len(pending_tasks),
                )
                while len(pending_tasks) >= stage_limit:
                    next_commit = await self._drain_ready_buffered_records(
                        state,
                        pending_tasks,
                        next_commit,
                        split_index,
                        buffered_name,
                    )
                    if len(pending_tasks) < stage_limit:
                        break
                    if adaptive_controller is not None:
                        stage_limit = adaptive_controller.observe(
                            ctx.metrics.runtime, pending_count=len(pending_tasks)
                        )
                        ctx.metrics.runtime.buffered_stage_limit = stage_limit
                        if len(pending_tasks) < stage_limit:
                            break
                    ctx.metrics.runtime.buffered_stage_drain_count += 1
                    next_commit = await self._commit_next_buffered_record(
                        state,
                        pending_tasks,
                        next_commit,
                        split_index,
                        buffered_name,
                    )
                if self.reached_max_records(ctx, state.processed_count, max_records):
                    break
        except BaseException as exc:
            source_error = exc

        if self._must_abort_pending_work(source_error):
            await self._abort_pending_work(state, pending_tasks)
            raise source_error

        try:
            # Yield control so just-submitted tasks reach their buffered submit point
            # before the final drain sequence begins.
            await asyncio.sleep(0)
            while pending_tasks:
                await self.chain.drain_buffered(ctx)
                await asyncio.sleep(0)
                next_commit = await self._drain_ready_buffered_records(
                    state,
                    pending_tasks,
                    next_commit,
                    split_index,
                    buffered_name,
                )
                if not pending_tasks:
                    break
                next_commit = await self._commit_next_buffered_record(
                    state,
                    pending_tasks,
                    next_commit,
                    split_index,
                    buffered_name,
                )

            await self.delivery.flush_pending_writes(state)
        except BaseException:
            await self._abort_pending_work(state, pending_tasks)
            raise

        if source_error is not None:
            raise source_error

    @staticmethod
    def _must_abort_pending_work(exc: BaseException | None) -> bool:
        return isinstance(exc, (RecordDeliveryError, asyncio.CancelledError, KeyboardInterrupt))

    async def _abort_pending_work(
        self,
        state: RunState,
        pending_tasks: dict[int, tuple[Any, SourceRecord]],
    ) -> None:
        state.pending_writes.clear()
        await self._cancel_pending_tasks(pending_tasks)

    async def _cancel_pending_tasks(
        self,
        pending_tasks: dict[int, tuple[Any, SourceRecord]],
    ) -> None:
        tasks = [future for future, _source_record in pending_tasks.values()]
        pending_tasks.clear()
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def process_record_through_buffered_stages(
        self,
        source_record: SourceRecord,
        ctx: PipelineContext,
        stage_plan: list[BufferedStageSpec],
        current: Any,
    ) -> ProcessedSourceRecord:
        start = stage_plan[0].index

        for stage in stage_plan:
            if start < stage.index:
                prefix_result = await self.chain.process_range(start, stage.index, current, ctx)
                if prefix_result.value is None:
                    return ProcessedSourceRecord(
                        source_record=source_record,
                        result=None,
                        failure=prefix_result.failure,
                    )
                current = prefix_result.value

            t0 = time.monotonic()
            m_metrics = ctx.metrics.middleware(stage.name)
            m_metrics.records_in += 1

            try:
                with ctx.trace_span(
                    "middleware.process", middleware=stage.name, execution_mode="buffered"
                ):
                    future = await stage.middleware.submit(current, ctx)
                    current = await future
            except Exception as exc:
                m_metrics.records_errored += 1
                m_metrics.total_time_ms += (time.monotonic() - t0) * 1000
                return ProcessedSourceRecord(
                    source_record=source_record,
                    result=None,
                    failure=MiddlewareFailure(
                        stage="buffered_middleware",
                        record=source_record.raw,
                        middleware=stage.name,
                        exception=exc,
                    ),
                )

            m_metrics.total_time_ms += (time.monotonic() - t0) * 1000
            if current is None:
                m_metrics.records_dropped += 1
                return ProcessedSourceRecord(source_record=source_record, result=None)

            m_metrics.records_out += 1
            start = stage.index + 1

        final_result = await self.chain.process_range(
            start, self.chain.middleware_count(), current, ctx
        )
        return ProcessedSourceRecord(
            source_record=source_record,
            result=final_result.value,
            failure=final_result.failure,
        )

    async def _drain_ready_buffered_records(
        self,
        state: RunState,
        pending_tasks: dict[int, tuple[Any, SourceRecord]],
        next_commit: int,
        split_index: int,
        buffered_name: str,
    ) -> int:
        # Drain all consecutive completed tasks in sequence order to preserve output ordering.
        while True:
            entry = pending_tasks.get(next_commit)
            if entry is None:
                return next_commit
            future, source_record = entry
            if not future.done():
                return next_commit
            pending_tasks.pop(next_commit)
            await self.resolve_buffered_record(
                state, future, split_index, buffered_name, source_record
            )
            next_commit += 1

    async def _commit_next_buffered_record(
        self,
        state: RunState,
        pending_tasks: dict[int, tuple[Any, SourceRecord]],
        next_commit: int,
        split_index: int,
        buffered_name: str,
    ) -> int:
        # Await the next in-order task, preserving output ordering even when tasks complete
        # out of order.
        entry = pending_tasks.get(next_commit)
        if entry is None:
            return next_commit
        future, source_record = entry
        await future
        pending_tasks.pop(next_commit)
        await self.resolve_buffered_record(state, future, split_index, buffered_name, source_record)
        return next_commit + 1
