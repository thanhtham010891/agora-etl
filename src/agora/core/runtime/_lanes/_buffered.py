"""Buffered lane execution strategy."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import deque
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
from agora.core.runtime._delivery_support import _CheckpointSaveError
from agora.core.runtime._hot_metrics import HotPathMetrics, RustHotPathMetrics
from agora.core.tracing import NoopTracer

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from agora.core.context import PipelineContext
    from agora.core.runtime._buffered import ExecutionCoordinator
    from agora.core.runtime._plan import BufferedStageSpec


@dataclass(slots=True)
class _SingleBufferedPendingRecord:
    future: Any | None
    source_record: SourceRecord
    buffered_name: str
    suffix_start: int
    suffix_stop: int
    middleware_metrics: Any
    started_at: float
    immediate_result: Any | None = None
    immediate_failure: MiddlewareFailure | None = None
    has_immediate_outcome: bool = False
    metrics_finalized: bool = False


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

        pending_tasks: deque[tuple[Any, SourceRecord]] = deque()
        state = RunState(ctx=ctx, checkpoint_state=checkpoint_state, pending_writes=[])
        source_error: BaseException | None = None
        hot = HotPathMetrics.for_source(
            c.source.source_name,
            metrics=ctx.metrics,
            acceleration_mode=c.acceleration_mode,
        )

        if len(buffered_stages) == 1:
            await self._run_single_buffered_stage(
                ctx,
                source_records,
                state,
                hot,
                buffered_stages[0],
                stage_limit,
                adaptive_controller,
                buffered_name,
                split_index,
            )
            return

        try:
            async for source_record in source_records:
                if hot.inc_consumed():
                    hot.flush(ctx.metrics)
                state.processed_count += 1

                prefix_value, prefix_failure = await c.chain.process_range_outcome(
                    0,
                    split_index,
                    source_record.raw,
                    ctx,
                )
                if prefix_value is None:
                    await c.delivery.dispatch_processed_result(
                        state,
                        None,
                        source_record.raw,
                        source_record.checkpoint,
                        c.writer_batch_size,
                        failure=prefix_failure,
                        on_success=source_record.on_success,
                    )
                    continue

                pending_tasks.append(
                    (
                        asyncio.create_task(
                            self.process_record_through_buffered_stages(
                                source_record,
                                ctx,
                                buffered_stages,
                                prefix_value,
                            )
                        ),
                        source_record,
                    )
                )
                ctx.metrics.runtime.buffered_stage_max_in_flight = max(
                    ctx.metrics.runtime.buffered_stage_max_in_flight,
                    len(pending_tasks),
                )

                while len(pending_tasks) >= stage_limit:
                    ctx.metrics.runtime.buffered_stage_drain_count += 1
                    if adaptive_controller is not None:
                        stage_limit = adaptive_controller.observe(
                            ctx.metrics.runtime, pending_count=len(pending_tasks)
                        )
                        ctx.metrics.runtime.buffered_stage_limit = stage_limit
                    await self._drain_ready_buffered_records(
                        state,
                        pending_tasks,
                        split_index,
                        buffered_name,
                    )
                    if len(pending_tasks) < stage_limit:
                        break
                    await self._commit_next_buffered_record(
                        state,
                        pending_tasks,
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
                await self._drain_ready_buffered_records(
                    state,
                    pending_tasks,
                    split_index,
                    buffered_name,
                )
                if not pending_tasks:
                    break
                await self._commit_next_buffered_record(
                    state,
                    pending_tasks,
                    split_index,
                    buffered_name,
                )

            await c.delivery.flush_pending_writes(state)
        except BaseException:
            await self._abort_pending_work(state, pending_tasks)
            raise

        if source_error is not None:
            raise source_error

    async def _run_single_buffered_stage(
        self,
        ctx: PipelineContext,
        source_records: AsyncGenerator[SourceRecord, None],
        state: RunState,
        hot: HotPathMetrics | RustHotPathMetrics,
        stage: BufferedStageSpec,
        stage_limit: int,
        adaptive_controller: Any,
        buffered_name: str,
        split_index: int,
    ) -> None:
        c = self.coordinator
        pending_records: deque[_SingleBufferedPendingRecord] = deque()
        source_error: BaseException | None = None
        suffix_start = stage.index + 1
        suffix_stop = c.chain.middleware_count()

        try:
            async for source_record in source_records:
                if hot.inc_consumed():
                    hot.flush(ctx.metrics)
                state.processed_count += 1

                prefix_value, prefix_failure = await c.chain.process_range_outcome(
                    0,
                    split_index,
                    source_record.raw,
                    ctx,
                )
                if prefix_value is None:
                    await c.delivery.dispatch_processed_result(
                        state,
                        None,
                        source_record.raw,
                        source_record.checkpoint,
                        c.writer_batch_size,
                        failure=prefix_failure,
                        on_success=source_record.on_success,
                    )
                    continue

                pending_records.append(
                    await self._submit_single_buffered_record(
                        stage,
                        source_record,
                        ctx,
                        prefix_value,
                        suffix_start=suffix_start,
                        suffix_stop=suffix_stop,
                    )
                )
                ctx.metrics.runtime.buffered_stage_max_in_flight = max(
                    ctx.metrics.runtime.buffered_stage_max_in_flight,
                    len(pending_records),
                )

                while len(pending_records) >= stage_limit:
                    ctx.metrics.runtime.buffered_stage_drain_count += 1
                    if adaptive_controller is not None:
                        stage_limit = adaptive_controller.observe(
                            ctx.metrics.runtime, pending_count=len(pending_records)
                        )
                        ctx.metrics.runtime.buffered_stage_limit = stage_limit
                    await self._drain_ready_single_buffered_records(
                        state,
                        pending_records,
                    )
                    if len(pending_records) < stage_limit:
                        break
                    await self._commit_next_single_buffered_record(
                        state,
                        pending_records,
                    )
        except BaseException as exc:
            source_error = exc

        hot.flush_final(ctx.metrics)

        if self._must_abort_pending_work(source_error):
            await self._abort_single_stage_pending_work(state, pending_records)
            assert source_error is not None
            raise source_error

        try:
            await asyncio.sleep(0)
            while pending_records:
                await c.chain.drain_buffered(ctx)
                await asyncio.sleep(0)
                await self._drain_ready_single_buffered_records(
                    state,
                    pending_records,
                )
                if not pending_records:
                    break
                await self._commit_next_single_buffered_record(
                    state,
                    pending_records,
                )

            await c.delivery.flush_pending_writes(state)
        except BaseException:
            await self._abort_single_stage_pending_work(state, pending_records)
            raise

        if source_error is not None:
            raise source_error

    @staticmethod
    def _must_abort_pending_work(exc: BaseException | None) -> bool:
        return isinstance(
            exc,
            (RecordDeliveryError, _CheckpointSaveError, asyncio.CancelledError, KeyboardInterrupt),
        )

    async def _abort_pending_work(
        self,
        state: RunState,
        pending_tasks: deque[tuple[Any, SourceRecord]],
    ) -> None:
        state.pending_writes.clear()
        with contextlib.suppress(BaseException):
            await self.coordinator.delivery.close_pending_write_owner(state)
        await self._cancel_pending_tasks(pending_tasks)

    async def _abort_single_stage_pending_work(
        self,
        state: RunState,
        pending_records: deque[_SingleBufferedPendingRecord],
    ) -> None:
        state.pending_writes.clear()
        with contextlib.suppress(BaseException):
            await self.coordinator.delivery.close_pending_write_owner(state)
        entries = list(pending_records)
        pending_records.clear()
        for entry in entries:
            self._finalize_single_buffered_metrics(entry)
        futures = [entry.future for entry in entries if entry.future is not None]
        for future in futures:
            if not future.done():
                future.cancel()
        if futures:
            await asyncio.gather(*futures, return_exceptions=True)

    async def _cancel_pending_tasks(
        self,
        pending_tasks: deque[tuple[Any, SourceRecord]],
    ) -> None:
        tasks = [future for future, _source_record in pending_tasks]
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
                prefix_value, prefix_failure = await c.chain.process_range_outcome(
                    start,
                    stage.index,
                    current,
                    ctx,
                )
                if prefix_value is None:
                    return ProcessedSourceRecord(
                        source_record=source_record,
                        result=None,
                        failure=prefix_failure,
                    )
                current = prefix_value

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

        final_value, final_failure = await c.chain.process_range_outcome(
            start,
            c.chain.middleware_count(),
            current,
            ctx,
        )
        return ProcessedSourceRecord(
            source_record=source_record,
            result=final_value,
            failure=final_failure,
        )

    async def _submit_single_buffered_record(
        self,
        stage: BufferedStageSpec,
        source_record: SourceRecord,
        ctx: PipelineContext,
        current: Any,
        *,
        suffix_start: int,
        suffix_stop: int | None = None,
    ) -> _SingleBufferedPendingRecord:
        if suffix_stop is None:
            suffix_stop = suffix_start
        m_metrics = ctx.metrics.middleware(stage.name)
        m_metrics.records_in += 1
        started_at = time.monotonic()
        submit = stage.middleware.submit

        try:
            if type(ctx.tracer) is NoopTracer:
                future = await submit(current, ctx)
            else:
                with ctx.trace_span(
                    "middleware.process",
                    middleware=stage.name,
                    execution_mode="buffered",
                ):
                    future = await submit(current, ctx)
        except Exception as exc:
            return _SingleBufferedPendingRecord(
                future=None,
                source_record=source_record,
                buffered_name=stage.name,
                suffix_start=suffix_start,
                suffix_stop=suffix_stop,
                middleware_metrics=m_metrics,
                started_at=started_at,
                immediate_failure=MiddlewareFailure(
                    stage="buffered_middleware",
                    record=source_record.raw,
                    middleware=stage.name,
                    exception=exc,
                ),
                has_immediate_outcome=True,
            )

        entry = _SingleBufferedPendingRecord(
            future=future,
            source_record=source_record,
            buffered_name=stage.name,
            suffix_start=suffix_start,
            suffix_stop=suffix_stop,
            middleware_metrics=m_metrics,
            started_at=started_at,
        )
        if future.done():
            self._finalize_single_buffered_metrics(entry)
        return entry

    def _finalize_single_buffered_metrics(
        self,
        entry: _SingleBufferedPendingRecord,
    ) -> None:
        if entry.metrics_finalized:
            return

        future = entry.future
        if entry.has_immediate_outcome:
            result = entry.immediate_result
            failure = entry.immediate_failure
        else:
            if future is None:
                return
            if not future.done():
                return
            try:
                result = future.result()
            except asyncio.CancelledError:
                return
            except Exception:
                entry.middleware_metrics.records_errored += 1
                entry.middleware_metrics.total_time_ms += (
                    time.monotonic() - entry.started_at
                ) * 1000
                entry.metrics_finalized = True
                return
            failure = None

        if failure is not None:
            entry.middleware_metrics.records_errored += 1
        elif isinstance(result, ProcessedSourceRecord):
            if result.failure is not None:
                entry.middleware_metrics.records_errored += 1
            elif result.result is None:
                entry.middleware_metrics.records_dropped += 1
            else:
                entry.middleware_metrics.records_out += 1
        elif result is None:
            entry.middleware_metrics.records_dropped += 1
        else:
            entry.middleware_metrics.records_out += 1

        entry.middleware_metrics.total_time_ms += (time.monotonic() - entry.started_at) * 1000
        entry.metrics_finalized = True

    async def _resolve_single_buffered_record(
        self,
        state: RunState,
        entry: _SingleBufferedPendingRecord,
    ) -> None:
        c = self.coordinator
        source_record = entry.source_record
        result_value: Any | None
        result_failure: MiddlewareFailure | None
        try:
            if entry.has_immediate_outcome:
                result_value = entry.immediate_result
                result_failure = entry.immediate_failure
            else:
                assert entry.future is not None
                try:
                    buffered_result = await entry.future
                except Exception as exc:
                    result_value = None
                    result_failure = MiddlewareFailure(
                        stage="buffered_middleware",
                        record=source_record.raw,
                        middleware=entry.buffered_name,
                        exception=exc,
                    )
                else:
                    if isinstance(buffered_result, ProcessedSourceRecord):
                        source_record = buffered_result.source_record
                        result_value = buffered_result.result
                        result_failure = buffered_result.failure
                    elif buffered_result is None:
                        result_value = None
                        result_failure = None
                    elif entry.suffix_start < entry.suffix_stop:
                        result_value, result_failure = await c.chain.process_range_outcome(
                            entry.suffix_start,
                            entry.suffix_stop,
                            buffered_result,
                            state.ctx,
                        )
                    else:
                        result_value = buffered_result
                        result_failure = None
        finally:
            self._finalize_single_buffered_metrics(entry)

        await c.delivery.dispatch_processed_result(
            state,
            result_value,
            source_record.raw,
            source_record.checkpoint,
            c.writer_batch_size,
            failure=result_failure,
            on_success=source_record.on_success,
        )

    async def _drain_ready_single_buffered_records(
        self,
        state: RunState,
        pending_records: deque[_SingleBufferedPendingRecord],
    ) -> None:
        while True:
            if not pending_records:
                return
            entry = pending_records[0]
            future = entry.future
            if future is not None and not future.done():
                return
            pending_records.popleft()
            await self._resolve_single_buffered_record(state, entry)

    async def _commit_next_single_buffered_record(
        self,
        state: RunState,
        pending_records: deque[_SingleBufferedPendingRecord],
    ) -> None:
        if not pending_records:
            return
        entry = pending_records.popleft()
        await self._resolve_single_buffered_record(state, entry)

    async def _drain_ready_buffered_records(
        self,
        state: RunState,
        pending_tasks: deque[tuple[Any, SourceRecord]],
        split_index: int,
        buffered_name: str,
    ) -> None:
        c = self.coordinator
        while True:
            if not pending_tasks:
                return
            entry = pending_tasks[0]
            future, source_record = entry
            if not future.done():
                return
            pending_tasks.popleft()
            await c.resolve_buffered_record(
                state, future, split_index, buffered_name, source_record
            )

    async def _commit_next_buffered_record(
        self,
        state: RunState,
        pending_tasks: deque[tuple[Any, SourceRecord]],
        split_index: int,
        buffered_name: str,
    ) -> None:
        c = self.coordinator
        if not pending_tasks:
            return
        entry = pending_tasks.popleft()
        future, source_record = entry
        await c.resolve_buffered_record(state, future, split_index, buffered_name, source_record)
