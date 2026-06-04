"""
agora/core/middleware.py
========================
Middleware abstraction — the composable building blocks of an agora pipeline.

Every data transformation in agora is a ``Middleware[T, U]``:
  - receives records of type T
  - emits records of type U (may be the same type)
  - returns None to drop a record

Built-in middlewares
--------------------
- ``TransformMiddleware``   — 1:1 async transformation (T → U)
- ``FilterMiddleware``      — predicate-based drop (T → T | None)
- ``MapMiddleware``         — sync function shorthand (T → U)
- ``BatchMiddleware``       — buffer N records → emit as list
- ``RouteMiddleware``       — dispatch to sub-middleware by key
- ``RetryMiddleware``       — retry on exception with back-off

Custom middleware
-----------------
Subclass ``Middleware[T, U]`` and implement ``process()``::

    class MyEnricher(Middleware[Place, Place]):
        name = "my_enricher"

        async def process(self, record: Place, ctx: PipelineContext) -> Place:
            record.extra = await fetch_extra(record.id)
            return record
"""

from __future__ import annotations

import asyncio
import inspect
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

import logstruct

from agora.core.batch import BatchMiddleware, BatchProcessResult
from agora.core.data_plane import DataPlane

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.core.context import PipelineContext

T = TypeVar("T")
U = TypeVar("U")

logger = logstruct.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MiddlewareFailure:
    """Structured middleware failure surfaced to the runtime."""

    stage: str
    record: Any
    middleware: str
    exception: Exception


@dataclass(frozen=True, slots=True)
class MiddlewareProcessResult:
    """Outcome of processing a record through some or all middlewares."""

    value: Any | None
    failure: MiddlewareFailure | None = None


@dataclass(frozen=True, slots=True)
class PipelinedBatchStageSpec:
    """Runtime-selected batch stage that can submit whole batches concurrently."""

    index: int
    middleware: Any
    name: str
    max_in_flight: int
    ordered: bool
    arrow_stage: bool


class MiddlewareDataPlane(StrEnum):
    """Logical data plane flowing between middleware stages."""

    PYTHON_ROWS = DataPlane.PYTHON_ROWS.value
    ARROW_BATCHES = DataPlane.ARROW_BATCHES.value


@dataclass(frozen=True, slots=True)
class MiddlewareModeSpec:
    """One row in the middleware compatibility matrix."""

    index: int
    name: str
    data_plane: MiddlewareDataPlane


# ======================================================================
# Base Middleware
# ======================================================================


class Middleware(ABC, Generic[T, U]):
    """Abstract async middleware.

    Each middleware receives one record of type T and returns either:
    - A record of type U (pass downstream)
    - ``None`` (drop the record — it will not reach the next stage)

    Hooks
    -----
    ``on_start(ctx)``  — called once before the pipeline loop begins
    ``on_stop(ctx)``   — called once after the pipeline loop ends
    ``on_error(...)``  — called when ``process()`` raises an exception
    """

    # Override in subclasses for clearer logs / metrics.
    name: str = "middleware"

    @abstractmethod
    async def process(self, record: T, ctx: PipelineContext) -> U | None:
        """Transform *record*.  Return None to drop it."""

    # ------------------------------------------------------------------ #
    # Lifecycle hooks (no-op defaults)                                     #
    # ------------------------------------------------------------------ #

    async def on_start(self, ctx: PipelineContext) -> None:
        """Called once before the pipeline loop starts."""

    async def on_stop(self, ctx: PipelineContext) -> None:
        """Called once after the pipeline loop ends (even on error)."""

    async def on_error(
        self,
        record: T,
        exc: Exception,
        ctx: PipelineContext,
    ) -> None:
        """Called when process() raises.  Default: log and continue."""
        ctx.log.exception(
            "middleware_error",
            middleware=self.name,
            error=str(exc),
        )

    async def apply_in_batch(
        self,
        current: list[Any],
        ctx: PipelineContext,
        chain: Any,
        idx: int,
    ) -> Any:
        """Double-dispatch hook: apply self per-record within a batch context.

        Returns the updated list, or a BatchProcessResult on batch-level failure.
        Default: process each non-None record individually via process_range().
        """

        next_batch: list[Any] = []
        for record in current:
            if record is None:
                next_batch.append(None)
                continue
            result = await chain.process_range(idx, idx + 1, record, ctx)
            if result.failure is not None:
                next_batch.append(None)
            else:
                next_batch.append(result.value)
        return next_batch


# ======================================================================
# Built-in middlewares
# ======================================================================


class MapMiddleware(Middleware[T, U]):
    """Apply a sync or async callable to each record (T → U).

    Usage::

        .pipe(MapMiddleware(lambda r: r.to_uppercase(), name="uppercaser"))
    """

    def __init__(
        self,
        fn: Callable[[T], U | None] | Callable[[T], Awaitable[U | None]],
        name: str = "map",
    ) -> None:
        self.name = name
        self._fn = fn
        self._fn_is_async = inspect.iscoroutinefunction(fn)

    async def process(self, record: T, ctx: PipelineContext) -> U | None:
        del ctx
        if self._fn_is_async:
            async_fn = cast("Callable[[T], Awaitable[U | None]]", self._fn)
            return await async_fn(record)
        sync_fn = cast("Callable[[T], U | None]", self._fn)
        return sync_fn(record)

    async def apply_in_batch(
        self,
        current: list[Any],
        ctx: PipelineContext,
        chain: Any,
        idx: int,
    ) -> Any:
        if not self._fn_is_async:
            return await self._apply_sync_map_batch(current, ctx)
        return await super().apply_in_batch(current, ctx, chain, idx)

    async def _apply_sync_map_batch(
        self,
        current: list[Any],
        ctx: PipelineContext,
    ) -> list[Any]:
        active_count = sum(1 for r in current if r is not None)
        if active_count == 0:
            return current

        t0 = time.monotonic()
        m_metrics = ctx.metrics.middleware(self.name)
        m_metrics.records_in += active_count
        sync_fn = cast("Callable[[Any], Any | None]", self._fn)
        next_batch: list[Any] = []
        dropped = errors = written = 0

        with ctx.trace_span(
            "middleware.process_batch_fast",
            middleware=self.name,
            execution_mode="batch_fast_map",
            batch_size=active_count,
        ):
            for record in current:
                if record is None:
                    next_batch.append(None)
                    continue
                try:
                    result = sync_fn(record)
                except Exception as exc:
                    errors += 1
                    await self.on_error(record, exc, ctx)
                    ctx.log.exception("middleware_chain_error", middleware=self.name)
                    next_batch.append(None)
                    continue
                if result is None:
                    dropped += 1
                else:
                    written += 1
                next_batch.append(result)

        m_metrics.records_errored += errors
        m_metrics.records_dropped += dropped
        m_metrics.records_out += written
        m_metrics.total_time_ms += (time.monotonic() - t0) * 1000
        return next_batch


class FilterMiddleware(Middleware[T, T]):
    """Drop records that don't satisfy *predicate* (T → T | None).

    Usage::

        .pipe(FilterMiddleware(lambda r: r.confidence > 0.8, name="confidence_filter"))
    """

    def __init__(self, predicate: Callable[[T], bool], name: str = "filter") -> None:
        self.name = name
        self._predicate = predicate

    async def process(self, record: T, ctx: PipelineContext) -> T | None:
        if inspect.iscoroutinefunction(self._predicate):
            keep = await self._predicate(record)  # type: ignore[misc, unused-ignore]
        else:
            keep = self._predicate(record)
        return record if keep else None

    async def apply_in_batch(
        self,
        current: list[Any],
        ctx: PipelineContext,
        chain: Any,
        idx: int,
    ) -> Any:
        if not inspect.iscoroutinefunction(self._predicate):
            return await self._apply_sync_filter_batch(current, ctx)
        return await super().apply_in_batch(current, ctx, chain, idx)

    async def _apply_sync_filter_batch(
        self,
        current: list[Any],
        ctx: PipelineContext,
    ) -> list[Any]:
        active_count = sum(1 for r in current if r is not None)
        if active_count == 0:
            return current

        t0 = time.monotonic()
        m_metrics = ctx.metrics.middleware(self.name)
        m_metrics.records_in += active_count
        predicate = self._predicate
        next_batch: list[Any] = []
        dropped = errors = written = 0

        with ctx.trace_span(
            "middleware.process_batch_fast",
            middleware=self.name,
            execution_mode="batch_fast_filter",
            batch_size=active_count,
        ):
            for record in current:
                if record is None:
                    next_batch.append(None)
                    continue
                try:
                    keep = predicate(record)
                except Exception as exc:
                    errors += 1
                    await self.on_error(record, exc, ctx)
                    ctx.log.exception("middleware_chain_error", middleware=self.name)
                    next_batch.append(None)
                    continue
                if keep:
                    written += 1
                    next_batch.append(record)
                else:
                    dropped += 1
                    next_batch.append(None)

        m_metrics.records_errored += errors
        m_metrics.records_dropped += dropped
        m_metrics.records_out += written
        m_metrics.total_time_ms += (time.monotonic() - t0) * 1000
        return next_batch


class BatchMapMiddleware(BatchMiddleware[T, U]):
    """Batch-native ``MapMiddleware``: apply *fn* across a whole batch in one call.

    Use on the batch execution lane (with a ``supports_batch_emit`` source) to
    avoid per-record dispatch. Same semantics as ``MapMiddleware`` — ``fn``
    returning ``None`` drops that record.
    """

    name = "batch_map"

    def __init__(
        self,
        fn: Callable[[T], U | None] | Callable[[T], Awaitable[U | None]],
        name: str = "batch_map",
    ) -> None:
        self.name = name
        self._fn = fn
        self._fn_is_async = inspect.iscoroutinefunction(fn)

    async def process_batch(self, records: list[T], ctx: PipelineContext) -> list[U | None]:
        del ctx
        if self._fn_is_async:
            async_fn = cast("Callable[[T], Awaitable[U | None]]", self._fn)
            return list(await asyncio.gather(*(async_fn(r) for r in records)))
        sync_fn = cast("Callable[[T], U | None]", self._fn)
        return [sync_fn(r) for r in records]


class BatchFilterMiddleware(BatchMiddleware[T, T]):
    """Batch-native ``FilterMiddleware``: evaluate *predicate* across a batch.

    Records failing the predicate become ``None`` slots (dropped), matching
    ``FilterMiddleware`` semantics on the per-record path.
    """

    name = "batch_filter"

    def __init__(self, predicate: Callable[[T], bool], name: str = "batch_filter") -> None:
        self.name = name
        self._predicate = predicate
        self._predicate_is_async = inspect.iscoroutinefunction(predicate)

    async def process_batch(self, records: list[T], ctx: PipelineContext) -> list[T | None]:
        del ctx
        if self._predicate_is_async:
            async_pred = cast("Callable[[T], Awaitable[bool]]", self._predicate)
            keeps = await asyncio.gather(*(async_pred(r) for r in records))
        else:
            keeps = [self._predicate(r) for r in records]
        return [r if keep else None for r, keep in zip(records, keeps, strict=True)]


class RouteMiddleware(Middleware[T, U]):
    """Dispatch each record to a sub-middleware based on a key function.

    Useful for source-multiplexed pipelines where different sources
    require different transformations.

    Usage::

        .pipe(
            RouteMiddleware(key=lambda e: e.source)
            .route("source_a", NormalizerA())
            .route("source_b", NormalizerB())
        )
    """

    def __init__(self, key: Callable[[T], str], name: str = "router") -> None:
        self.name = name
        self._key = key
        self._routes: dict[str, Middleware[T, U]] = {}
        self._default: Middleware[T, U] | None = None

    def route(self, source_key: str, middleware: Middleware[T, U]) -> RouteMiddleware[T, U]:
        """Register *middleware* for records matching *source_key*."""
        self._routes[source_key] = middleware
        return self

    def default(self, middleware: Middleware[T, U]) -> RouteMiddleware[T, U]:
        """Fallback middleware for unmatched records."""
        self._default = middleware
        return self

    async def on_start(self, ctx: PipelineContext) -> None:
        for m in self._routes.values():
            await m.on_start(ctx)
        if self._default:
            await self._default.on_start(ctx)

    async def on_stop(self, ctx: PipelineContext) -> None:
        for m in self._routes.values():
            await m.on_stop(ctx)
        if self._default:
            await self._default.on_stop(ctx)

    async def process(self, record: T, ctx: PipelineContext) -> U | None:
        if inspect.iscoroutinefunction(self._key):
            key = await self._key(record)  # type: ignore[misc, unused-ignore]
        else:
            key = self._key(record)

        middleware = self._routes.get(key) or self._default
        if middleware is None:
            ctx.log.warning("router_no_route", key=key, middleware=self.name)
            return None

        t0 = time.monotonic()
        m_metrics = ctx.metrics.middleware(middleware.name)
        m_metrics.records_in += 1
        try:
            result = await middleware.process(record, ctx)
        except Exception as exc:
            m_metrics.records_errored += 1
            await middleware.on_error(record, exc, ctx)
            raise
        finally:
            m_metrics.total_time_ms += (time.monotonic() - t0) * 1000
        if result is None:
            m_metrics.records_dropped += 1
        else:
            m_metrics.records_out += 1
        return result


class RetryMiddleware(Middleware[T, T]):
    """Retry *inner* middleware on exception with exponential back-off.

    Usage::

        .pipe(RetryMiddleware(inner=EnrichMiddleware(), max_retries=3, backoff_base=1.5))
    """

    def __init__(
        self,
        inner: Middleware[T, T],
        max_retries: int = 3,
        backoff_base: float = 2.0,
        exceptions: tuple[type[Exception], ...] = (Exception,),
    ) -> None:
        self.name = f"retry({inner.name})"
        self._inner = inner
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._exceptions = exceptions

    async def on_start(self, ctx: PipelineContext) -> None:
        await self._inner.on_start(ctx)

    async def on_stop(self, ctx: PipelineContext) -> None:
        await self._inner.on_stop(ctx)

    async def process(self, record: T, ctx: PipelineContext) -> T | None:
        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 2):
            try:
                return await self._inner.process(record, ctx)
            except self._exceptions as exc:
                last_exc = exc
                if attempt > self._max_retries:
                    raise
                wait = self._backoff_base**attempt
                ctx.log.warning(
                    "retry_backoff",
                    middleware=self.name,
                    attempt=attempt,
                    wait_seconds=wait,
                )
                await asyncio.sleep(wait)
        raise RuntimeError("Unreachable") from last_exc


# ======================================================================
# MiddlewareChain — internal: wraps the list of middlewares for the runner
# ======================================================================


class MiddlewareChain(Generic[T, U]):
    """Internal: wraps the ordered list of middlewares used by BoundPipeline.

    Not part of the public API — users interact with ``Pipeline.pipe()``.
    """

    def __init__(self, middlewares: list[Any]) -> None:
        self._middlewares = middlewares

    def has_batch_stages(self) -> bool:
        """Return True if any middleware in the chain is a BatchMiddleware."""
        from agora.core.batch import BatchMiddleware

        return any(isinstance(m, BatchMiddleware) for m in self._middlewares)

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
        """Return True if any middleware in the chain expects Arrow batches."""
        return any(
            spec.data_plane == MiddlewareDataPlane.ARROW_BATCHES
            for spec in self.stage_mode_matrix()
        )

    def has_mixed_data_planes(self) -> bool:
        """Return True when Arrow and Python-row stages are mixed in one chain."""
        planes = {spec.data_plane for spec in self.stage_mode_matrix()}
        return len(planes) > 1

    def has_only_arrow_batch_stages(self) -> bool:
        """Return True when the chain is non-empty and every stage is an ArrowBatchMiddleware."""
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
        """Run *records* through the chain in batch mode.

        Each middleware handles itself via ``apply_in_batch()`` — no isinstance
        dispatch here. BatchMiddleware stages process the whole batch at once;
        MapMiddleware/FilterMiddleware use sync fast-paths; ArrowBatchMiddleware
        is skipped (data is already list[dict]); all others fall back to per-record.

        If a BatchMiddleware raises, returns a BatchProcessResult with ``failure``
        set — the entire batch is considered failed (Option A).
        """
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
        for m in reversed(self._middlewares):
            try:
                await m.on_stop(ctx)
            except Exception as exc:
                ctx.log.exception(
                    "middleware_stop_error",
                    middleware=getattr(m, "name", type(m).__name__),
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
        return len(self._middlewares)

    async def process(self, record: Any, ctx: PipelineContext) -> MiddlewareProcessResult:
        """Run record through the chain and return a structured outcome."""
        if not self._middlewares:
            return MiddlewareProcessResult(value=record)
        return await self.process_range(0, len(self._middlewares), record, ctx)

    async def process_range(
        self,
        start: int,
        stop: int,
        record: Any,
        ctx: PipelineContext,
    ) -> MiddlewareProcessResult:
        """Run a slice of the middleware chain and return a structured outcome."""
        current = record
        for middleware in self._middlewares[start:stop]:
            t0 = time.monotonic()
            m_metrics = ctx.metrics.middleware(middleware.name)
            m_metrics.records_in += 1

            try:
                with ctx.trace_span(
                    "middleware.process",
                    middleware=middleware.name,
                    execution_mode="linear",
                ):
                    current = await middleware.process(current, ctx)
            except Exception as exc:
                m_metrics.records_errored += 1
                await middleware.on_error(record, exc, ctx)
                ctx.log.exception(
                    "middleware_chain_error",
                    middleware=middleware.name,
                )
                return MiddlewareProcessResult(
                    value=None,
                    failure=MiddlewareFailure(
                        stage="middleware",
                        record=record,
                        middleware=middleware.name,
                        exception=exc,
                    ),
                )
            finally:
                m_metrics.total_time_ms += (time.monotonic() - t0) * 1000

            if current is None:
                m_metrics.records_dropped += 1
                return MiddlewareProcessResult(value=None)

            m_metrics.records_out += 1

        return MiddlewareProcessResult(value=current)
