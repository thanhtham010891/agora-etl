"""Batch lane execution strategy."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from agora.core.batch import is_arrow_native_sink
from agora.core.runtime._delivery import CheckpointState, RecordDeliveryError, RunState
from agora.core.runtime._hot_metrics import HotPathMetrics
from agora.core.types import SinkFailurePolicy

if TYPE_CHECKING:
    from agora.core.context import PipelineContext
    from agora.core.runtime._buffered import ExecutionCoordinator


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
                    raw_batch=batch,
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
        raw_batch: Any,
        checkpoint_value: Any,
        batch_size: int,
        arrow_writer: Any,
        hot: HotPathMetrics,
    ) -> None:
        c = self.coordinator
        metrics = state.ctx.metrics

        async def _raw_rows() -> list[Any]:
            return await asyncio.to_thread(raw_batch.to_pylist)

        async def _processed_rows(batch: Any) -> list[Any]:
            return await asyncio.to_thread(batch.to_pylist)

        if batch_result.failure is not None:
            metrics.records_errored += batch_size
            routed = True
            if c.delivery.dlq_sink is not None:
                for raw_record in await _raw_rows():
                    ok = await c.delivery.write_to_dlq(
                        ctx=state.ctx,
                        stage="batch_middleware",
                        exc=batch_result.failure.exception,
                        record=raw_record,
                        original_record=raw_record,
                        middleware=batch_result.failure.middleware,
                        checkpoint=checkpoint_value,
                    )
                    routed = routed and ok
            if routed or c.delivery.sink_failure_policy == SinkFailurePolicy.LOG_AND_CONTINUE:
                state.ctx.log.exception(
                    "arrow_batch_middleware_error",
                    batch_size=batch_size,
                    error=str(batch_result.failure.exception),
                )
                await c.delivery.save_batch_checkpoint(state, checkpoint_value, batch_size)
            elif c.delivery.sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
                raise RecordDeliveryError(
                    batch_result.failure.exception
                ) from batch_result.failure.exception
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
            routed = True
            if c.delivery.dlq_sink is not None:
                raw_rows = await _raw_rows()
                processed_rows = await _processed_rows(out_batch)
                for raw_record, processed_record in zip(raw_rows, processed_rows, strict=True):
                    ok = await c.delivery.write_to_dlq(
                        ctx=state.ctx,
                        stage="sink_write",
                        exc=exc,
                        record=raw_record,
                        original_record=raw_record,
                        processed_record=processed_record,
                        checkpoint=checkpoint_value,
                    )
                    routed = routed and ok
            if routed or c.delivery.sink_failure_policy == SinkFailurePolicy.LOG_AND_CONTINUE:
                state.ctx.log.exception(
                    "arrow_batch_write_error",
                    batch_size=written_size,
                    error=str(exc),
                )
            elif c.delivery.sink_failure_policy == SinkFailurePolicy.FAIL_CLOSED:
                raise RecordDeliveryError(exc) from exc

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

        pending_batches: dict[int, tuple[Any, Any, int, Any]] = {}
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
                        raw_batch=batch,
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
                            raw_batch=batch,
                            checkpoint_value=checkpoint_value,
                            batch_size=batch_size,
                            arrow_writer=arrow_writer,
                            hot=hot,
                        )
                    else:
                        pending_batches[next_sequence] = (
                            await stage.middleware.submit_batch(prefix_batch, ctx),
                            batch,
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
        pending_batches: dict[int, tuple[Any, Any, int, Any]],
        next_commit: int,
        stage: Any,
        arrow_writer: Any,
        hot: HotPathMetrics,
    ) -> int:
        while True:
            entry = pending_batches.get(next_commit)
            if entry is None:
                return next_commit
            future, raw_batch, batch_size, checkpoint_value = entry
            if not future.done():
                return next_commit
            pending_batches.pop(next_commit)
            await self._resolve_pipelined_arrow_batch(
                state,
                future,
                raw_batch,
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
        pending_batches: dict[int, tuple[Any, Any, int, Any]],
        next_commit: int,
        stage: Any,
        arrow_writer: Any,
        hot: HotPathMetrics,
    ) -> int:
        entry = pending_batches.get(next_commit)
        if entry is None:
            return next_commit
        future, raw_batch, batch_size, checkpoint_value = entry
        pending_batches.pop(next_commit)
        await self._resolve_pipelined_arrow_batch(
            state,
            future,
            raw_batch,
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
        raw_batch: Any,
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
            raw_batch=raw_batch,
            checkpoint_value=checkpoint_value,
            batch_size=batch_size,
            arrow_writer=arrow_writer,
            hot=hot,
        )

    async def _cancel_pending_batch_tasks(
        self,
        state: RunState,
        pending_batches: dict[int, tuple[Any, ...]],
        *,
        stage: Any,
    ) -> None:
        tasks = [entry[0] for entry in pending_batches.values()]
        pending_batches.clear()
        abort = getattr(stage.middleware, "abort_in_flight_batches", None)
        if abort is not None:
            await abort(state.ctx, reason="pipeline_abort")
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
