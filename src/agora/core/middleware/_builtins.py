"""Built-in middleware implementations."""

from __future__ import annotations

import asyncio
import inspect
import time
from typing import TYPE_CHECKING, Any, TypeVar, cast

from agora.core.batch import BatchMiddleware
from agora.core.middleware.base import Middleware

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.core.context import PipelineContext

T = TypeVar("T")
U = TypeVar("U")


class MapMiddleware(Middleware[T, U]):
    """Apply a sync or async callable to each record (T -> U)."""

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
        active_count = sum(1 for record in current if record is not None)
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
    """Drop records that do not satisfy *predicate*."""

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
        active_count = sum(1 for record in current if record is not None)
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
    """Batch-native ``MapMiddleware`` that applies *fn* across a whole batch."""

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
            return list(await asyncio.gather(*(async_fn(record) for record in records)))
        sync_fn = cast("Callable[[T], U | None]", self._fn)
        return [sync_fn(record) for record in records]


class BatchFilterMiddleware(BatchMiddleware[T, T]):
    """Batch-native ``FilterMiddleware`` that evaluates *predicate* across a batch."""

    name = "batch_filter"

    def __init__(self, predicate: Callable[[T], bool], name: str = "batch_filter") -> None:
        self.name = name
        self._predicate = predicate
        self._predicate_is_async = inspect.iscoroutinefunction(predicate)

    async def process_batch(self, records: list[T], ctx: PipelineContext) -> list[T | None]:
        del ctx
        if self._predicate_is_async:
            async_pred = cast("Callable[[T], Awaitable[bool]]", self._predicate)
            keeps = await asyncio.gather(*(async_pred(record) for record in records))
        else:
            keeps = [self._predicate(record) for record in records]
        return [record if keep else None for record, keep in zip(records, keeps, strict=True)]


class RouteMiddleware(Middleware[T, U]):
    """Dispatch each record to a sub-middleware based on a key function."""

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

    def _unique_middlewares(self) -> list[Middleware[T, U]]:
        seen: set[int] = set()
        ordered: list[Middleware[T, U]] = []
        for middleware in self._routes.values():
            middleware_id = id(middleware)
            if middleware_id in seen:
                continue
            seen.add(middleware_id)
            ordered.append(middleware)
        if self._default is not None and id(self._default) not in seen:
            ordered.append(self._default)
        return ordered

    async def on_start(self, ctx: PipelineContext) -> None:
        for middleware in self._unique_middlewares():
            await middleware.on_start(ctx)

    async def on_stop(self, ctx: PipelineContext) -> None:
        for middleware in self._unique_middlewares():
            await middleware.on_stop(ctx)

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
    """Retry *inner* middleware on exception with exponential back-off."""

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
