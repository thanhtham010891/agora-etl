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
