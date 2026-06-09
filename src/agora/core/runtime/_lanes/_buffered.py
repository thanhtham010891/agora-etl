"""Buffered lane execution strategy."""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from agora.core.middleware import MiddlewareFailure
from agora.core.runtime._delivery import (
    CheckpointState,
    ProcessedSourceRecord,
    RecordDeliveryError,
    RunState,
    SourceRecord,
)
from agora.core.runtime._hot_metrics import HotPathMetrics

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from agora.core.context import PipelineContext
    from agora.core.runtime._buffered import ExecutionCoordinator
    from agora.core.runtime._plan import BufferedStageSpec


@dataclass(slots=True)
class BufferedLaneStrategy:
    coordinator: ExecutionCoordinator

    async def run(
        self,
        ctx: PipelineContext,
        source_records: AsyncGenerator[SourceRecord, None],
        checkpoint_state: CheckpointState,
        buffered_stages: tuple[BufferedStageSpec, ...],
    ) -> None:
        if not buffered_stages:
            raise RuntimeError("buffered lane selected without buffered stages")

        c = self.coordinator
        buffered_name = buffered_stages[0].name
        split_index = buffered_stages[0].index
        stage_limit = max(1, sum(stage.concurrency for stage in buffered_stages))
        adaptive_controller = c._build_adaptive_backpressure_controller(stage_limit)
        if adaptive_controller is None:
            if c.max_buffer_size is not None:
                stage_limit = min(stage_limit, c.max_buffer_size)
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
        hot = HotPathMetrics.for_source(c.source.source_name, metrics=ctx.metrics)

        try:
            async for source_record in source_records:
                if hot.inc_consumed():
                    hot.flush(ctx.metrics)
                state.processed_count += 1

                prefix_result = await c.chain.process_range(0, split_index, source_record.raw, ctx)
                if prefix_result.value is None:
                    await c.delivery.dispatch_processed_result(
                        state,
                        None,
                        source_record.raw,
                        source_record.checkpoint,
                        c.writer_batch_size,
                        failure=prefix_result.failure,
                        on_success=source_record.on_success,
                    )
                    continue

                pending_tasks[next_sequence] = (
                    asyncio.create_task(
                        self.process_record_through_buffered_stages(
                            source_record,
                            ctx,
                            buffered_stages,
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
        except BaseException as exc:
            source_error = exc

        hot.flush_final(ctx.metrics)

        if self._must_abort_pending_work(source_error):
            await self._abort_pending_work(state, pending_tasks)
            assert source_error is not None
            raise source_error

        try:
            await asyncio.sleep(0)
            while pending_tasks:
                await c.chain.drain_buffered(ctx)
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

            await c.delivery.flush_pending_writes(state)
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
        with contextlib.suppress(BaseException):
            await self.coordinator.delivery.close_pending_write_owner(state)
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
        stage_plan: tuple[BufferedStageSpec, ...],
        current: Any,
    ) -> ProcessedSourceRecord:
        c = self.coordinator
        start = stage_plan[0].index

        for stage in stage_plan:
            if start < stage.index:
                prefix_result = await c.chain.process_range(start, stage.index, current, ctx)
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
            finally:
                m_metrics.total_time_ms += (time.monotonic() - t0) * 1000
            if current is None:
                m_metrics.records_dropped += 1
                return ProcessedSourceRecord(source_record=source_record, result=None)

            m_metrics.records_out += 1
            start = stage.index + 1

        final_result = await c.chain.process_range(start, c.chain.middleware_count(), current, ctx)
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
        c = self.coordinator
        while True:
            entry = pending_tasks.get(next_commit)
            if entry is None:
                return next_commit
            future, source_record = entry
            if not future.done():
                return next_commit
            pending_tasks.pop(next_commit)
            await c.resolve_buffered_record(
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
        c = self.coordinator
        entry = pending_tasks.get(next_commit)
        if entry is None:
            return next_commit
        future, source_record = entry
        pending_tasks.pop(next_commit)
        await c.resolve_buffered_record(state, future, split_index, buffered_name, source_record)
        return next_commit + 1
