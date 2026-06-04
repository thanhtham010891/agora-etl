"""Lane strategy implementations for Agora runtime execution."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from agora.core.batch import is_arrow_native_sink
from agora.core.constants import LINEAR_FLUSH_INTERVAL
from agora.core.middleware import MiddlewareFailure
from agora.core.runtime._delivery import (
    CheckpointState,
    ProcessedSourceRecord,
    RecordDeliveryError,
    RunState,
    SourceRecord,
)
from agora.core.runtime._hot_metrics import HotPathMetrics
from agora.core.runtime._plan import BufferedStageSpec  # noqa: TC001
from agora.core.types import SinkFailurePolicy

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from agora.core.context import PipelineContext
    from agora.core.runtime._buffered import ExecutionCoordinator


@dataclass(slots=True)
class LinearLaneStrategy:
    coordinator: ExecutionCoordinator

    async def run(
        self,
        ctx: PipelineContext,
        source_records: AsyncGenerator[SourceRecord, None],
        checkpoint_state: CheckpointState,
    ) -> None:
        c = self.coordinator
        state = RunState(ctx=ctx, checkpoint_state=checkpoint_state, pending_writes=[])
        source_error: Exception | None = None

        batch_size = c.writer_batch_size
        source_name = c.source.source_name
        metrics = ctx.metrics

        if c.rust_available() and batch_size > 1:
            buf = c.make_linear_batch_buffer(batch_size, LINEAR_FLUSH_INTERVAL)
            use_direct_flush = c.plan.writer.direct_flush_eligible
            metrics.runtime.direct_flush_active = use_direct_flush

            try:
                async for source_record in source_records:
                    state.processed_count += 1
                    if buf.inc_consumed(source_name):
                        buf.flush_metrics(metrics)

                    result = await c.chain.process(source_record.raw, ctx)
                    if result.value is None:
                        await c.delivery.drop_record(
                            state,
                            source_record.checkpoint,
                            failure=result.failure,
                            on_success=source_record.on_success,
                        )
                        continue

                    if buf.push(
                        result.value,
                        source_record.raw,
                        source_record.checkpoint,
                        source_record.on_success,
                    ):
                        if use_direct_flush:
                            processed_list, raw_list, checkpoint_list, on_success_list = (
                                buf.take_flush_batch()
                            )
                            await c.delivery.flush_batch_direct(
                                state,
                                processed_list,
                                raw_list,
                                checkpoint_list,
                                on_success_list=on_success_list,
                            )
                        else:
                            batch = buf.take_batch()
                            for processed, raw, checkpoint, on_success in batch:
                                await c.delivery.queue_processed_record(
                                    state,
                                    processed,
                                    raw,
                                    checkpoint,
                                    batch_size,
                                    failure=None,
                                    on_success=on_success,
                                )

            except Exception as exc:
                source_error = exc

            if buf.len() > 0:
                if use_direct_flush:
                    processed_list, raw_list, checkpoint_list, on_success_list = (
                        buf.take_flush_batch()
                    )
                    await c.delivery.flush_batch_direct(
                        state,
                        processed_list,
                        raw_list,
                        checkpoint_list,
                        on_success_list=on_success_list,
                    )
                else:
                    remaining = buf.take_batch()
                    for processed, raw, checkpoint, on_success in remaining:
                        await c.delivery.queue_processed_record(
                            state,
                            processed,
                            raw,
                            checkpoint,
                            batch_size,
                            failure=None,
                            on_success=on_success,
                        )
                    await c.delivery.flush_pending_writes(state)
            buf.flush_metrics_final(metrics)
            if source_error is not None:
                raise source_error
            return

        # The standalone Rust MetricsAccumulator does not currently outperform
        # the Python hot path on the linear lane. Keep this path on
        # HotPathMetrics even when Rust prefetch support is available.
        hot = HotPathMetrics.for_source(source_name, metrics=metrics)

        try:
            async for source_record in source_records:
                if hot.inc_consumed():
                    hot.flush(metrics)
                state.processed_count += 1

                result = await c.chain.process(source_record.raw, ctx)
                if batch_size > 1:
                    await c.delivery.queue_processed_record(
                        state,
                        result.value,
                        source_record.raw,
                        source_record.checkpoint,
                        batch_size,
                        failure=result.failure,
                        on_success=source_record.on_success,
                    )
                else:
                    await c.delivery.write_processed_record(
                        state,
                        result.value,
                        source_record.raw,
                        source_record.checkpoint,
                        failure=result.failure,
                        on_success=source_record.on_success,
                    )
        except Exception as exc:
            source_error = exc

        hot.flush_final(metrics)

        await c.delivery.flush_pending_writes(state)
        if source_error is not None:
            raise source_error


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


@dataclass(slots=True)
class BatchLaneStrategy:
    coordinator: ExecutionCoordinator

    async def run(
        self,
        ctx: PipelineContext,
        checkpoint_state: CheckpointState,
    ) -> None:
        c = self.coordinator
        state = RunState(ctx=ctx, checkpoint_state=checkpoint_state, pending_writes=[])
        source_name = c.source.source_name
        metrics = ctx.metrics
        hot = HotPathMetrics.for_source(source_name, metrics=metrics)

        arrow_writer: Any = c.delivery.transport.writer
        metrics.runtime.arrow_fast_path_active = c.plan.writer.arrow_fast_path
        metrics.runtime.arrow_chain_active = c.plan.writer.arrow_chain

        pipelined_stage = c.chain.first_pipelined_batch_stage()
        if pipelined_stage is not None:
            if pipelined_stage.arrow_stage:
                if not c.plan.writer.arrow_chain:
                    raise RuntimeError(f"{pipelined_stage.name} requires an all-Arrow batch chain.")
                await self._run_pipelined_arrow_batches(
                    ctx,
                    state,
                    hot,
                    arrow_writer,
                    pipelined_stage,
                )
                hot.flush_final(metrics)
                return

            await self._run_pipelined_list_batches(
                ctx,
                state,
                hot,
                pipelined_stage,
            )
            hot.flush_final(metrics)
            return

        async for batch in cast("Any", c.source).stream_batches():
            checkpoint_value = c.source.current_checkpoint()

            if c.plan.writer.arrow_chain:
                batch_size = len(batch)
                hot.inc_consumed(batch_size)

                arrow_result = await c.chain.process_arrow_batch(batch, ctx)

                await self._finalize_arrow_batch_result(
                    state,
                    arrow_result,
                    checkpoint_value=checkpoint_value,
                    batch_size=batch_size,
                    arrow_writer=arrow_writer,
                    hot=hot,
                )
            else:
                raw_batch = batch.to_pylist() if hasattr(batch, "to_pylist") else list(batch)
                batch_size = len(raw_batch)
                hot.inc_consumed(batch_size)

                batch_result = await c.chain.process_batch(raw_batch, ctx)
                await self._finalize_list_batch_result(
                    state,
                    batch_result,
                    raw_batch=raw_batch,
                    checkpoint_value=checkpoint_value,
                    hot=hot,
                )

        hot.flush_final(metrics)

    async def _finalize_list_batch_result(
        self,
        state: RunState,
        batch_result: Any,
        *,
        raw_batch: list[Any],
        checkpoint_value: Any,
        hot: HotPathMetrics,
    ) -> None:
        c = self.coordinator
        await c.delivery.write_batch_result(
            state,
            batch_result.results,
            raw_batch,
            checkpoint_value,
            batch_failure=batch_result.failure,
        )
        hot.flush(state.ctx.metrics)

    async def _finalize_arrow_batch_result(
        self,
        state: RunState,
        batch_result: Any,
        *,
        checkpoint_value: Any,
        batch_size: int,
        arrow_writer: Any,
        hot: HotPathMetrics,
    ) -> None:
        c = self.coordinator
        metrics = state.ctx.metrics

        if batch_result.failure is not None:
            metrics.records_errored += batch_size
            if c.delivery.sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
                raise RecordDeliveryError(
                    batch_result.failure.exception
                ) from batch_result.failure.exception
            state.ctx.log.exception(
                "arrow_batch_middleware_error",
                batch_size=batch_size,
                error=str(batch_result.failure.exception),
            )
            await c.delivery.save_batch_checkpoint(state, checkpoint_value, batch_size)
            hot.flush(metrics)
            return

        out_batch = cast("Any", batch_result.results[0])
        written_size = len(out_batch)
        if written_size == 0:
            await c.delivery.save_batch_checkpoint(state, checkpoint_value, batch_size)
            hot.flush(metrics)
            return

        try:
            if is_arrow_native_sink(arrow_writer):
                await c.delivery.transport.write_arrow_batch(state.ctx, arrow_writer, out_batch)
            else:
                rows = await asyncio.to_thread(out_batch.to_pylist)
                results, _elapsed_ms = await c.delivery.transport.write_batch(state.ctx, rows)
                first_error = next(
                    (error for result in results for error in result.errors),
                    None,
                )
                if first_error is not None:
                    raise first_error
                if not all(result.written for result in results):
                    raise RuntimeError("arrow batch fallback write did not write all records")
            hot.inc_written(written_size)
            if hot.inc_consumed(0):
                hot.flush(metrics)
        except Exception as exc:
            metrics.records_errored += written_size
            if c.delivery.sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
                raise RecordDeliveryError(exc) from exc
            state.ctx.log.exception(
                "arrow_batch_write_error",
                batch_size=written_size,
                error=str(exc),
            )

        await c.delivery.save_batch_checkpoint(state, checkpoint_value, batch_size)
        hot.flush(metrics)

    async def _run_pipelined_list_batches(
        self,
        ctx: PipelineContext,
        state: RunState,
        hot: HotPathMetrics,
        stage: Any,
    ) -> None:
        c = self.coordinator
        stage_limit = stage.max_in_flight
        if c.max_buffer_size is not None:
            stage_limit = min(stage_limit, c.max_buffer_size)
        stage_limit = max(1, stage_limit)
        ctx.metrics.runtime.process_batch_stage_limit = stage_limit

        pending_batches: dict[int, tuple[Any, list[Any], Any]] = {}
        next_sequence = 0
        next_commit = 0
        source_error: BaseException | None = None

        try:
            async for batch in cast("Any", c.source).stream_batches():
                checkpoint_value = c.source.current_checkpoint()
                raw_batch = batch.to_pylist() if hasattr(batch, "to_pylist") else list(batch)
                batch_size = len(raw_batch)
                hot.inc_consumed(batch_size)

                prefix_result = await c.chain.process_batch_range(0, stage.index, raw_batch, ctx)
                if prefix_result.failure is not None:
                    await self._finalize_list_batch_result(
                        state,
                        prefix_result,
                        raw_batch=raw_batch,
                        checkpoint_value=checkpoint_value,
                        hot=hot,
                    )
                else:
                    pending_batches[next_sequence] = (
                        await stage.middleware.submit_batch(prefix_result.results, ctx),
                        raw_batch,
                        checkpoint_value,
                    )
                    next_sequence += 1
                    ctx.metrics.runtime.process_batch_stage_max_in_flight = max(
                        ctx.metrics.runtime.process_batch_stage_max_in_flight,
                        len(pending_batches),
                    )

                while len(pending_batches) >= stage_limit:
                    next_commit = await self._drain_ready_pipelined_list_batches(
                        state,
                        pending_batches,
                        next_commit,
                        stage,
                        hot,
                    )
                    if len(pending_batches) < stage_limit:
                        break
                    ctx.metrics.runtime.process_batch_stage_drain_count += 1
                    next_commit = await self._commit_next_pipelined_list_batch(
                        state,
                        pending_batches,
                        next_commit,
                        stage,
                        hot,
                    )
        except BaseException as exc:
            source_error = exc

        if isinstance(
            source_error, (RecordDeliveryError, asyncio.CancelledError, KeyboardInterrupt)
        ):
            await self._cancel_pending_batch_tasks(state, pending_batches, stage=stage)
            raise source_error

        try:
            while pending_batches:
                await c.chain.drain_pipelined_batches(ctx)
                await asyncio.sleep(0)
                next_commit = await self._drain_ready_pipelined_list_batches(
                    state,
                    pending_batches,
                    next_commit,
                    stage,
                    hot,
                )
                if not pending_batches:
                    break
                next_commit = await self._commit_next_pipelined_list_batch(
                    state,
                    pending_batches,
                    next_commit,
                    stage,
                    hot,
                )
        except BaseException:
            await self._cancel_pending_batch_tasks(state, pending_batches, stage=stage)
            raise

        if source_error is not None:
            raise source_error

    async def _run_pipelined_arrow_batches(
        self,
        ctx: PipelineContext,
        state: RunState,
        hot: HotPathMetrics,
        arrow_writer: Any,
        stage: Any,
    ) -> None:
        c = self.coordinator
        stage_limit = stage.max_in_flight
        if c.max_buffer_size is not None:
            stage_limit = min(stage_limit, c.max_buffer_size)
        stage_limit = max(1, stage_limit)
        ctx.metrics.runtime.process_batch_stage_limit = stage_limit

        pending_batches: dict[int, tuple[Any, int, Any]] = {}
        next_sequence = 0
        next_commit = 0
        source_error: BaseException | None = None

        try:
            async for batch in cast("Any", c.source).stream_batches():
                checkpoint_value = c.source.current_checkpoint()
                batch_size = len(batch)
                hot.inc_consumed(batch_size)

                prefix_result = await c.chain.process_arrow_batch_range(0, stage.index, batch, ctx)
                if prefix_result.failure is not None:
                    await self._finalize_arrow_batch_result(
                        state,
                        prefix_result,
                        checkpoint_value=checkpoint_value,
                        batch_size=batch_size,
                        arrow_writer=arrow_writer,
                        hot=hot,
                    )
                else:
                    prefix_batch = cast("Any", prefix_result.results[0])
                    if len(prefix_batch) == 0:
                        await self._finalize_arrow_batch_result(
                            state,
                            prefix_result,
                            checkpoint_value=checkpoint_value,
                            batch_size=batch_size,
                            arrow_writer=arrow_writer,
                            hot=hot,
                        )
                    else:
                        pending_batches[next_sequence] = (
                            await stage.middleware.submit_batch(prefix_batch, ctx),
                            batch_size,
                            checkpoint_value,
                        )
                        next_sequence += 1
                        ctx.metrics.runtime.process_batch_stage_max_in_flight = max(
                            ctx.metrics.runtime.process_batch_stage_max_in_flight,
                            len(pending_batches),
                        )

                while len(pending_batches) >= stage_limit:
                    next_commit = await self._drain_ready_pipelined_arrow_batches(
                        state,
                        pending_batches,
                        next_commit,
                        stage,
                        arrow_writer,
                        hot,
                    )
                    if len(pending_batches) < stage_limit:
                        break
                    ctx.metrics.runtime.process_batch_stage_drain_count += 1
                    next_commit = await self._commit_next_pipelined_arrow_batch(
                        state,
                        pending_batches,
                        next_commit,
                        stage,
                        arrow_writer,
                        hot,
                    )
        except BaseException as exc:
            source_error = exc

        if isinstance(
            source_error, (RecordDeliveryError, asyncio.CancelledError, KeyboardInterrupt)
        ):
            await self._cancel_pending_batch_tasks(state, pending_batches, stage=stage)
            raise source_error

        try:
            while pending_batches:
                await c.chain.drain_pipelined_batches(ctx)
                await asyncio.sleep(0)
                next_commit = await self._drain_ready_pipelined_arrow_batches(
                    state,
                    pending_batches,
                    next_commit,
                    stage,
                    arrow_writer,
                    hot,
                )
                if not pending_batches:
                    break
                next_commit = await self._commit_next_pipelined_arrow_batch(
                    state,
                    pending_batches,
                    next_commit,
                    stage,
                    arrow_writer,
                    hot,
                )
        except BaseException:
            await self._cancel_pending_batch_tasks(state, pending_batches, stage=stage)
            raise

        if source_error is not None:
            raise source_error

    async def _drain_ready_pipelined_list_batches(
        self,
        state: RunState,
        pending_batches: dict[int, tuple[Any, list[Any], Any]],
        next_commit: int,
        stage: Any,
        hot: HotPathMetrics,
    ) -> int:
        while True:
            entry = pending_batches.get(next_commit)
            if entry is None:
                return next_commit
            future, raw_batch, checkpoint_value = entry
            if not future.done():
                return next_commit
            pending_batches.pop(next_commit)
            await self._resolve_pipelined_list_batch(
                state,
                future,
                raw_batch,
                checkpoint_value,
                stage,
                hot,
            )
            next_commit += 1

    async def _commit_next_pipelined_list_batch(
        self,
        state: RunState,
        pending_batches: dict[int, tuple[Any, list[Any], Any]],
        next_commit: int,
        stage: Any,
        hot: HotPathMetrics,
    ) -> int:
        entry = pending_batches.get(next_commit)
        if entry is None:
            return next_commit
        future, raw_batch, checkpoint_value = entry
        pending_batches.pop(next_commit)
        await self._resolve_pipelined_list_batch(
            state,
            future,
            raw_batch,
            checkpoint_value,
            stage,
            hot,
        )
        return next_commit + 1

    async def _resolve_pipelined_list_batch(
        self,
        state: RunState,
        future: Any,
        raw_batch: list[Any],
        checkpoint_value: Any,
        stage: Any,
        hot: HotPathMetrics,
    ) -> None:
        c = self.coordinator
        from agora.core.batch import BatchFailure, BatchProcessResult

        try:
            stage_result = await future
            batch_result = await c.chain.process_batch_range(
                stage.index + 1,
                c.chain.middleware_count(),
                list(stage_result),
                state.ctx,
            )
        except Exception as exc:
            batch_result = BatchProcessResult(
                results=[],
                failure=BatchFailure(batch=[], exception=exc, middleware=stage.name),
            )

        await self._finalize_list_batch_result(
            state,
            batch_result,
            raw_batch=raw_batch,
            checkpoint_value=checkpoint_value,
            hot=hot,
        )

    async def _drain_ready_pipelined_arrow_batches(
        self,
        state: RunState,
        pending_batches: dict[int, tuple[Any, int, Any]],
        next_commit: int,
        stage: Any,
        arrow_writer: Any,
        hot: HotPathMetrics,
    ) -> int:
        while True:
            entry = pending_batches.get(next_commit)
            if entry is None:
                return next_commit
            future, batch_size, checkpoint_value = entry
            if not future.done():
                return next_commit
            pending_batches.pop(next_commit)
            await self._resolve_pipelined_arrow_batch(
                state,
                future,
                batch_size,
                checkpoint_value,
                stage,
                arrow_writer,
                hot,
            )
            next_commit += 1

    async def _commit_next_pipelined_arrow_batch(
        self,
        state: RunState,
        pending_batches: dict[int, tuple[Any, int, Any]],
        next_commit: int,
        stage: Any,
        arrow_writer: Any,
        hot: HotPathMetrics,
    ) -> int:
        entry = pending_batches.get(next_commit)
        if entry is None:
            return next_commit
        future, batch_size, checkpoint_value = entry
        pending_batches.pop(next_commit)
        await self._resolve_pipelined_arrow_batch(
            state,
            future,
            batch_size,
            checkpoint_value,
            stage,
            arrow_writer,
            hot,
        )
        return next_commit + 1

    async def _resolve_pipelined_arrow_batch(
        self,
        state: RunState,
        future: Any,
        batch_size: int,
        checkpoint_value: Any,
        stage: Any,
        arrow_writer: Any,
        hot: HotPathMetrics,
    ) -> None:
        c = self.coordinator
        from agora.core.batch import BatchFailure, BatchProcessResult

        try:
            stage_result = await future
            batch_result = await c.chain.process_arrow_batch_range(
                stage.index + 1,
                c.chain.middleware_count(),
                stage_result,
                state.ctx,
            )
        except Exception as exc:
            batch_result = BatchProcessResult(
                results=[],
                failure=BatchFailure(batch=[], exception=exc, middleware=stage.name),
            )

        await self._finalize_arrow_batch_result(
            state,
            batch_result,
            checkpoint_value=checkpoint_value,
            batch_size=batch_size,
            arrow_writer=arrow_writer,
            hot=hot,
        )

    async def _cancel_pending_batch_tasks(
        self,
        state: RunState,
        pending_batches: dict[int, tuple[Any, Any, Any]],
        *,
        stage: Any,
    ) -> None:
        tasks = [future for future, _payload, _checkpoint in pending_batches.values()]
        pending_batches.clear()
        abort = getattr(stage.middleware, "abort_in_flight_batches", None)
        if abort is not None:
            await abort(state.ctx, reason="pipeline_abort")
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
