"""Middleware chain orchestration."""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from agora.core.acceleration import (
    AccelerationCapability,
    AccelerationMode,
    acceleration_supports,
    make_sync_builtin_chain_executor,
    normalize_acceleration_mode,
)
from agora.core.middleware._types import (
    MiddlewareDataPlane,
    MiddlewareFailure,
    MiddlewareModeSpec,
    MiddlewareProcessResult,
    PipelinedBatchStageSpec,
)
from agora.core.tracing import NoopTracer

if TYPE_CHECKING:
    from agora.core.batch import BatchProcessResult
    from agora.core.context import PipelineContext

T = TypeVar("T")
U = TypeVar("U")


def _rust_sync_builtin_executor_enabled() -> bool:
    return os.getenv("AGORA_EXPERIMENTAL_RUST_SYNC_CHAIN", "") == "1"


class MiddlewareChain(Generic[T, U]):
    """Internal wrapper around the ordered list of middlewares."""

    def __init__(
        self,
        middlewares: list[Any],
        *,
        acceleration_mode: AccelerationMode | str = AccelerationMode.AUTO,
    ) -> None:
        self._middlewares = middlewares
        self._middleware_count = len(middlewares)
        self._middleware_names = [
            getattr(middleware, "name", type(middleware).__name__) for middleware in middlewares
        ]
        self._row_trace_attributes = [
            {
                "middleware": middleware_name,
                "execution_mode": "linear",
            }
            for middleware_name in self._middleware_names
        ]
        self._row_fast_processes = [
            getattr(middleware, "_row_fast_process", None) for middleware in middlewares
        ]
        self._row_rust_builtin_fast = [
            bool(getattr(middleware, "_rust_sync_builtin_fast", False))
            for middleware in middlewares
        ]
        self._row_rust_builtin_prefix_counts = [0]
        builtin_fast_count = 0
        for is_builtin_fast in self._row_rust_builtin_fast:
            if is_builtin_fast:
                builtin_fast_count += 1
            self._row_rust_builtin_prefix_counts.append(builtin_fast_count)
        self._acceleration_mode = normalize_acceleration_mode(acceleration_mode)
        self._sync_builtin_chain_executor: Any | None = None
        self._refresh_sync_builtin_chain_executor()

    def set_acceleration_mode(self, mode: AccelerationMode | str) -> None:
        self._acceleration_mode = normalize_acceleration_mode(mode)
        self._refresh_sync_builtin_chain_executor()

    def _refresh_sync_builtin_chain_executor(self) -> None:
        self._sync_builtin_chain_executor = None
        if not _rust_sync_builtin_executor_enabled():
            return
        if not any(self._row_rust_builtin_fast):
            return
        if not acceleration_supports(
            AccelerationCapability.SYNC_BUILTIN_CHAIN_EXECUTOR,
            mode=self._acceleration_mode,
        ):
            return
        callables = [
            row_fast_process if is_builtin_fast else None
            for row_fast_process, is_builtin_fast in zip(
                self._row_fast_processes,
                self._row_rust_builtin_fast,
                strict=True,
            )
        ]
        self._sync_builtin_chain_executor = make_sync_builtin_chain_executor(
            callables,
            self._middleware_names,
            mode=self._acceleration_mode,
        )

    def _range_is_rust_sync_builtin_fast(self, start: int, stop: int) -> bool:
        if start < 0 or stop > self._middleware_count or start >= stop:
            return False
        prefix_counts = self._row_rust_builtin_prefix_counts
        return (prefix_counts[stop] - prefix_counts[start]) == (stop - start)

    def has_batch_stages(self) -> bool:
        """Return ``True`` if any middleware in the chain is a batch stage."""
        from agora.core.batch import BatchMiddleware

        return any(isinstance(middleware, BatchMiddleware) for middleware in self._middlewares)

    def is_empty(self) -> bool:
        """Return ``True`` when the chain has no middleware stages."""
        return self._middleware_count == 0

    def stage_mode_matrix(self) -> tuple[MiddlewareModeSpec, ...]:
        """Return the middleware data-plane matrix used for compatibility checks."""
        from agora.core.batch import ArrowBatchMiddleware

        matrix: list[MiddlewareModeSpec] = []
        for index, middleware in enumerate(self._middlewares):
            data_plane = MiddlewareDataPlane.ARROW_BATCHES
            if not isinstance(middleware, ArrowBatchMiddleware):
                data_plane = MiddlewareDataPlane.PYTHON_ROWS
            matrix.append(
                MiddlewareModeSpec(
                    index=index,
                    name=getattr(middleware, "name", type(middleware).__name__),
                    data_plane=data_plane,
                )
            )
        return tuple(matrix)

    def has_arrow_batch_stages(self) -> bool:
        """Return ``True`` if any middleware in the chain expects Arrow batches."""
        return any(
            spec.data_plane == MiddlewareDataPlane.ARROW_BATCHES
            for spec in self.stage_mode_matrix()
        )

    def has_mixed_data_planes(self) -> bool:
        """Return ``True`` when Arrow and Python-row stages are mixed in one chain."""
        planes = {spec.data_plane for spec in self.stage_mode_matrix()}
        return len(planes) > 1

    def has_only_arrow_batch_stages(self) -> bool:
        """Return ``True`` when every stage in a non-empty chain is Arrow-native."""
        matrix = self.stage_mode_matrix()
        return bool(matrix) and all(
            spec.data_plane == MiddlewareDataPlane.ARROW_BATCHES for spec in matrix
        )

    async def process_arrow_batch(
        self,
        batch: Any,
        ctx: PipelineContext,
    ) -> BatchProcessResult:
        """Run *batch* through every Arrow-native stage in order."""
        return await self.process_arrow_batch_range(0, len(self._middlewares), batch, ctx)

    async def process_arrow_batch_range(
        self,
        start: int,
        stop: int,
        batch: Any,
        ctx: PipelineContext,
    ) -> BatchProcessResult:
        """Run an Arrow-native batch through a slice of the middleware chain."""
        from agora.core.batch import ArrowBatchMiddleware, BatchFailure, BatchProcessResult

        current = batch
        for middleware in self._middlewares[start:stop]:
            if not isinstance(middleware, ArrowBatchMiddleware):
                continue
            t0 = time.monotonic()
            m_metrics = ctx.metrics.middleware(middleware.name)
            m_metrics.records_in += len(current)
            try:
                with ctx.trace_span(
                    "middleware.process_arrow_batch",
                    middleware=middleware.name,
                    batch_size=len(current),
                ):
                    current = await middleware.process_arrow_batch(current, ctx)
            except Exception as exc:
                m_metrics.records_errored += len(current)
                m_metrics.total_time_ms += (time.monotonic() - t0) * 1000
                ctx.log.exception(
                    "arrow_batch_middleware_error",
                    middleware=middleware.name,
                    batch_size=len(current),
                )
                return BatchProcessResult(
                    results=[],
                    failure=BatchFailure(
                        batch=[],
                        exception=exc,
                        middleware=middleware.name,
                    ),
                )
            finally:
                m_metrics.total_time_ms += (time.monotonic() - t0) * 1000

            m_metrics.records_out += len(current)
            if len(current) == 0:
                break

        return BatchProcessResult(results=[current])

    async def process_batch(
        self,
        records: list[Any],
        ctx: PipelineContext,
    ) -> BatchProcessResult:
        """Run *records* through the chain in batch mode."""
        from agora.core.batch import BatchProcessResult

        if not self._middlewares:
            return BatchProcessResult(results=records)

        return await self.process_batch_range(0, len(self._middlewares), records, ctx)

    async def process_batch_range(
        self,
        start: int,
        stop: int,
        records: list[Any],
        ctx: PipelineContext,
    ) -> BatchProcessResult:
        """Run *records* through a slice of the chain in batch mode."""
        from agora.core.batch import BatchProcessResult

        current: list[Any] = list(records)

        for idx in range(start, stop):
            middleware = self._middlewares[idx]
            result = await middleware.apply_in_batch(current, ctx, self, idx)
            if isinstance(result, BatchProcessResult):
                return result
            current = result

        return BatchProcessResult(results=current)

    def buffered_stages(self) -> list[tuple[int, Any]]:
        """Return every middleware that supports buffered submission."""
        stages: list[tuple[int, Any]] = []
        for index, middleware in enumerate(self._middlewares):
            if callable(getattr(middleware, "submit", None)):
                stages.append((index, middleware))
        return stages

    def first_buffered_stage(self) -> tuple[int, Any] | None:
        """Return the first middleware that supports buffered submission."""
        stages = self.buffered_stages()
        if not stages:
            return None
        return stages[0]

    def pipelined_batch_stages(self) -> tuple[PipelinedBatchStageSpec, ...]:
        """Return every middleware that can submit whole batches concurrently."""
        from agora.core.batch import ArrowBatchMiddleware

        stages: list[PipelinedBatchStageSpec] = []
        for index, middleware in enumerate(self._middlewares):
            submit_batch = getattr(middleware, "submit_batch", None)
            max_in_flight = max(1, int(getattr(middleware, "batch_in_flight_limit", 1)))
            if not callable(submit_batch) or max_in_flight <= 1:
                continue
            stages.append(
                PipelinedBatchStageSpec(
                    index=index,
                    middleware=middleware,
                    name=getattr(middleware, "name", "pipelined_batch"),
                    max_in_flight=max_in_flight,
                    ordered=bool(getattr(middleware, "ordered_batch_commits", True)),
                    arrow_stage=isinstance(middleware, ArrowBatchMiddleware),
                )
            )
        return tuple(stages)

    def first_pipelined_batch_stage(self) -> PipelinedBatchStageSpec | None:
        stages = self.pipelined_batch_stages()
        if not stages:
            return None
        return stages[0]

    async def drain_buffered(self, ctx: PipelineContext) -> None:
        """Ask buffered middlewares to flush pending records before shutdown."""
        for middleware in self._middlewares:
            drain_pending = getattr(middleware, "drain_pending", None)
            if callable(drain_pending):
                await drain_pending(ctx)

    async def drain_pipelined_batches(self, ctx: PipelineContext) -> None:
        """Ask pipelined batch middlewares to flush any batch-local buffers."""
        for middleware in self._middlewares:
            drain_pending = getattr(middleware, "drain_pending_batches", None)
            if callable(drain_pending):
                await drain_pending(ctx)

    async def start_all(self, ctx: PipelineContext) -> None:
        started: list[Any] = []
        for middleware in self._middlewares:
            try:
                await middleware.on_start(ctx)
            except Exception:
                await self._rollback_started_middlewares(ctx, middleware, started)
                raise
            started.append(middleware)

    async def stop_all(self, ctx: PipelineContext) -> None:
        for middleware in reversed(self._middlewares):
            try:
                await middleware.on_stop(ctx)
            except Exception as exc:
                ctx.log.exception(
                    "middleware_stop_error",
                    middleware=getattr(middleware, "name", type(middleware).__name__),
                    error=str(exc),
                )

    async def _rollback_started_middlewares(
        self,
        ctx: PipelineContext,
        failing: Any,
        started: list[Any],
    ) -> None:
        for middleware in [failing, *reversed(started)]:
            try:
                await middleware.on_stop(ctx)
            except Exception as exc:
                ctx.log.exception(
                    "middleware_start_rollback_error",
                    middleware=getattr(middleware, "name", type(middleware).__name__),
                    error=str(exc),
                )

    def middleware_count(self) -> int:
        return self._middleware_count

    async def process(self, record: Any, ctx: PipelineContext) -> MiddlewareProcessResult:
        """Run *record* through the chain and return a structured outcome."""
        value, failure = await self.process_outcome(record, ctx)
        return MiddlewareProcessResult(value=value, failure=failure)

    async def process_outcome(
        self,
        record: Any,
        ctx: PipelineContext,
    ) -> tuple[Any | None, MiddlewareFailure | None]:
        """Run *record* through the chain and return the raw value/failure pair."""
        middleware_count = self._middleware_count
        if middleware_count == 0:
            return record, None
        return await self.process_range_outcome(0, middleware_count, record, ctx)

    async def process_range(
        self,
        start: int,
        stop: int,
        record: Any,
        ctx: PipelineContext,
    ) -> MiddlewareProcessResult:
        """Run a slice of the middleware chain and return a structured outcome."""
        value, failure = await self.process_range_outcome(start, stop, record, ctx)
        return MiddlewareProcessResult(value=value, failure=failure)

    async def process_range_outcome(
        self,
        start: int,
        stop: int,
        record: Any,
        ctx: PipelineContext,
    ) -> tuple[Any | None, MiddlewareFailure | None]:
        """Run a slice of the middleware chain and return the raw value/failure pair."""
        if start >= stop:
            return record, None

        current = record
        middlewares = self._middlewares
        middleware_names = self._middleware_names
        row_fast_processes = self._row_fast_processes

        if (
            self._sync_builtin_chain_executor is not None
            and type(ctx.tracer) is NoopTracer
            and (stop - start) > 1
            and self._range_is_rust_sync_builtin_fast(start, stop)
        ):
            log_exception = ctx.log.exception
            try:
                current = self._sync_builtin_chain_executor.process_range(start, stop, record, ctx)
            except Exception as exc:
                stage_index = getattr(exc, "_agora_stage_index", start)
                if not isinstance(stage_index, int) or stage_index < start or stage_index >= stop:
                    stage_index = start
                middleware = middlewares[stage_index]
                middleware_name = middleware_names[stage_index]
                await middleware.on_error(record, exc, ctx)
                log_exception(
                    "middleware_chain_error",
                    middleware=middleware_name,
                )
                return None, MiddlewareFailure(
                    stage="middleware",
                    record=record,
                    middleware=middleware_name,
                    exception=exc,
                )
            if current is None:
                return None, None
            return current, None

        if type(ctx.tracer) is NoopTracer:
            return await self._process_range_outcome_no_trace(
                start,
                stop,
                record,
                ctx,
                middlewares=middlewares,
                middleware_names=middleware_names,
                row_fast_processes=row_fast_processes,
            )

        current = record
        middleware_metrics = ctx.metrics.middleware
        log_exception = ctx.log.exception
        start_trace_span = ctx._start_trace_span
        finish_trace_span = ctx._finish_trace_span
        row_trace_attributes = self._row_trace_attributes

        for idx in range(start, stop):
            middleware = middlewares[idx]
            middleware_name = middleware_names[idx]
            t0 = time.monotonic()
            m_metrics = middleware_metrics(middleware_name)
            m_metrics.records_in += 1
            previous = current

            try:
                span = start_trace_span(
                    "middleware.process",
                    attributes=row_trace_attributes[idx],
                    normalize=False,
                    share_attributes=True,
                )
                try:
                    row_fast_process = row_fast_processes[idx]
                    if row_fast_process is None:
                        current = await middleware.process(current, ctx)
                    else:
                        current = row_fast_process(current)
                except Exception as exc:
                    finish_trace_span(span, exc)
                    raise
                else:
                    finish_trace_span(span)
            except Exception as exc:
                m_metrics.records_errored += 1
                await middleware.on_error(record, exc, ctx)
                log_exception(
                    "middleware_chain_error",
                    middleware=middleware_name,
                )
                return None, MiddlewareFailure(
                    stage="middleware",
                    record=record,
                    middleware=middleware_name,
                    exception=exc,
                )
            finally:
                m_metrics.total_time_ms += (time.monotonic() - t0) * 1000

            if current is None:
                m_metrics.records_dropped += 1
                return None, None

            ctx.transfer_success_hooks(previous, current)
            m_metrics.records_out += 1

        return current, None

    async def _process_range_outcome_no_trace(
        self,
        start: int,
        stop: int,
        record: Any,
        ctx: PipelineContext,
        *,
        middlewares: list[Any],
        middleware_names: list[str],
        row_fast_processes: list[Any],
    ) -> tuple[Any | None, MiddlewareFailure | None]:
        current = record
        middleware_metrics = ctx.metrics.middleware
        log_exception = ctx.log.exception

        for idx in range(start, stop):
            middleware = middlewares[idx]
            middleware_name = middleware_names[idx]
            t0 = time.monotonic()
            m_metrics = middleware_metrics(middleware_name)
            m_metrics.records_in += 1
            previous = current

            try:
                row_fast_process = row_fast_processes[idx]
                if row_fast_process is None:
                    current = await middleware.process(current, ctx)
                else:
                    current = row_fast_process(current)
            except Exception as exc:
                m_metrics.records_errored += 1
                await middleware.on_error(record, exc, ctx)
                log_exception(
                    "middleware_chain_error",
                    middleware=middleware_name,
                )
                return None, MiddlewareFailure(
                    stage="middleware",
                    record=record,
                    middleware=middleware_name,
                    exception=exc,
                )
            finally:
                m_metrics.total_time_ms += (time.monotonic() - t0) * 1000

            if current is None:
                m_metrics.records_dropped += 1
                return None, None

            ctx.transfer_success_hooks(previous, current)
            m_metrics.records_out += 1

        return current, None
