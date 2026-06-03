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
        max_records: int | None,
    ) -> None:
        c = self.coordinator
        state = RunState(ctx=ctx, checkpoint_state=checkpoint_state, pending_writes=[])
        source_error: Exception | None = None

        batch_size = c.writer_batch_size
        has_max = max_records is not None
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

                    if has_max and state.processed_count >= max_records:  # type: ignore[operator]
                        ctx.log.info("pipeline_max_records_reached", max_records=max_records)
                        break
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
                if has_max and state.processed_count >= max_records:  # type: ignore[operator]
                    ctx.log.info("pipeline_max_records_reached", max_records=max_records)
                    break
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
        max_records: int | None,
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
                    if c.reached_max_records(ctx, state.processed_count, max_records):
                        break
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
                if c.reached_max_records(ctx, state.processed_count, max_records):
                    break
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
        max_records: int | None,
    ) -> None:
        c = self.coordinator
        state = RunState(ctx=ctx, checkpoint_state=checkpoint_state, pending_writes=[])
        source_name = c.source.source_name
        metrics = ctx.metrics
        has_max = max_records is not None
        records_consumed = 0
        hot = HotPathMetrics.for_source(source_name, metrics=metrics)

        arrow_sink: Any = None
        if c.plan.writer.arrow_fast_path:
            writer = c.delivery.transport.writer
            if is_arrow_native_sink(writer):
                arrow_sink = writer
            else:
                inner_sinks = getattr(writer, "_sinks", None)
                if inner_sinks and len(inner_sinks) == 1 and is_arrow_native_sink(inner_sinks[0]):
                    arrow_sink = inner_sinks[0]
        metrics.runtime.arrow_fast_path_active = arrow_sink is not None
        metrics.runtime.arrow_chain_active = arrow_sink is not None and c.plan.writer.arrow_chain

        async for batch in cast("Any", c.source).stream_batches():
            checkpoint_value = c.source.current_checkpoint()

            if arrow_sink is not None:
                batch_size = len(batch)
                hot.inc_consumed(batch_size)
                records_consumed += batch_size

                if c.plan.writer.arrow_chain:
                    # Arrow-native middleware chain: transform the RecordBatch
                    # without materialising any Python row objects.
                    arrow_result = await c.chain.process_arrow_batch(batch, ctx)
                    if arrow_result.failure is not None:
                        metrics.records_errored += batch_size
                        if c.delivery.sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
                            raise RecordDeliveryError(
                                arrow_result.failure.exception
                            ) from arrow_result.failure.exception
                        ctx.log.exception(
                            "arrow_batch_middleware_error",
                            batch_size=batch_size,
                            error=str(arrow_result.failure.exception),
                        )
                        await c.delivery.save_batch_checkpoint(state, checkpoint_value, batch_size)
                        hot.flush(metrics)
                        continue
                    out_batch = cast("Any", arrow_result.results[0])
                    written_size = len(out_batch)
                    if written_size == 0:
                        # All rows filtered out — advance checkpoint, skip write.
                        await c.delivery.save_batch_checkpoint(state, checkpoint_value, batch_size)
                        hot.flush(metrics)
                        continue
                else:
                    out_batch = batch
                    written_size = batch_size

                try:
                    await c.delivery.transport.write_arrow_batch(ctx, arrow_sink, out_batch)
                    hot.inc_written(written_size)
                    if hot.inc_consumed(0):
                        hot.flush(metrics)
                except Exception as exc:
                    metrics.records_errored += written_size
                    if c.delivery.sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
                        raise RecordDeliveryError(exc) from exc
                    ctx.log.exception(
                        "arrow_batch_write_error",
                        batch_size=written_size,
                        error=str(exc),
                    )

                await c.delivery.save_batch_checkpoint(state, checkpoint_value, batch_size)
                hot.flush(metrics)
            else:
                raw_batch = batch.to_pylist() if hasattr(batch, "to_pylist") else list(batch)
                batch_size = len(raw_batch)
                hot.inc_consumed(batch_size)
                records_consumed += batch_size

                batch_result = await c.chain.process_batch(raw_batch, ctx)
                await c.delivery.write_batch_result(
                    state,
                    batch_result.results,
                    raw_batch,
                    checkpoint_value,
                    batch_failure=batch_result.failure,
                )
                hot.flush(metrics)

            if has_max and records_consumed >= max_records:  # type: ignore[operator]
                ctx.log.info("pipeline_max_records_reached", max_records=max_records)
                break

        hot.flush_final(metrics)
