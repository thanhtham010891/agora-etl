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
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

import logstruct

from agora.core.batch import BatchMiddleware

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.core.batch import BatchProcessResult
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

    def has_only_arrow_batch_stages(self) -> bool:
        """Return True when the chain is non-empty and every stage is an ArrowBatchMiddleware."""
        from agora.core.batch import ArrowBatchMiddleware

        return bool(self._middlewares) and all(
            isinstance(m, ArrowBatchMiddleware) for m in self._middlewares
        )

    async def process_arrow_batch(
        self,
        batch: Any,
        ctx: PipelineContext,
    ) -> BatchProcessResult:
        """Run *batch* (a ``pa.RecordBatch``) through every Arrow-native stage in order.

        Each stage receives the output of the previous one. A stage returning a
        zero-row batch is treated as "drop the whole batch" — downstream stages
        are skipped and the empty batch is returned. If a stage raises, the
        exception is wrapped in a ``BatchProcessResult`` with ``failure`` set
        (same Option-A semantics as ``process_batch``).
        """
        from agora.core.batch import ArrowBatchMiddleware, BatchFailure, BatchProcessResult

        current = batch
        for middleware in self._middlewares:
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

        For ``BatchMiddleware`` stages: calls ``process_batch(records, ctx)``.
        For regular ``Middleware`` stages: applies per-record within the batch.

        If a ``BatchMiddleware`` raises, returns a ``BatchProcessResult`` with
        ``failure`` set — the entire batch is considered failed (Option A).
        """
        from agora.core.batch import BatchFailure, BatchMiddleware, BatchProcessResult

        if not self._middlewares:
            return BatchProcessResult(results=list(records))

        current: list[Any | None] = list(records)

        for idx, middleware in enumerate(self._middlewares):
            if isinstance(middleware, BatchMiddleware):
                t0 = time.monotonic()
                m_metrics = ctx.metrics.middleware(middleware.name)
                m_metrics.records_in += len(current)
                non_none = [r for r in current if r is not None]
                try:
                    with ctx.trace_span(
                        "middleware.process_batch",
                        middleware=middleware.name,
                        batch_size=len(non_none),
                    ):
                        batch_results = await middleware.process_batch(non_none, ctx)
                except Exception as exc:
                    m_metrics.records_errored += len(non_none)
                    m_metrics.total_time_ms += (time.monotonic() - t0) * 1000
                    ctx.log.exception(
                        "batch_middleware_error",
                        middleware=middleware.name,
                        batch_size=len(non_none),
                    )
                    return BatchProcessResult(
                        results=[],
                        failure=BatchFailure(
                            batch=non_none,
                            exception=exc,
                            middleware=middleware.name,
                        ),
                    )
                finally:
                    m_metrics.total_time_ms += (time.monotonic() - t0) * 1000

                if len(batch_results) != len(non_none):
                    raise RuntimeError(
                        f"BatchMiddleware '{middleware.name}' returned {len(batch_results)} "
                        f"results for {len(non_none)} inputs — lengths must match."
                    )

                # Re-map results back to original positions (preserving None slots)
                result_iter = iter(batch_results)
                current = [next(result_iter) if r is not None else None for r in current]
                dropped = sum(1 for r in current if r is None) - sum(
                    1 for r in records if r is None
                )
                m_metrics.records_dropped += max(0, dropped)
                m_metrics.records_out += sum(1 for r in current if r is not None)
            else:
                # ArrowBatchMiddleware in a mixed chain: data is already list[dict]
                # (to_pylist was called), so the Arrow stage cannot operate on it.
                # Pass through unchanged — the arrow fast path is disabled for mixed chains.
                from agora.core.batch import ArrowBatchMiddleware as _ArrowBatchMW

                if isinstance(middleware, _ArrowBatchMW):
                    continue
                # Regular Middleware — apply per-record within the batch
                next_batch: list[Any | None] = []
                for record in current:
                    if record is None:
                        next_batch.append(None)
                        continue
                    result = await self.process_range(
                        idx,
                        idx + 1,
                        record,
                        ctx,
                    )
                    if result.failure is not None:
                        # Per-record failure in batch context: drop the record,
                        # do not abort the batch (batch failure = BatchMiddleware only)
                        next_batch.append(None)
                    else:
                        next_batch.append(result.value)
                current = next_batch

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

    async def drain_buffered(self, ctx: PipelineContext) -> None:
        """Ask buffered middlewares to flush pending records before shutdown."""
        for middleware in self._middlewares:
            drain_pending = getattr(middleware, "drain_pending", None)
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
