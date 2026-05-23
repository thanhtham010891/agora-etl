"""
agora/middlewares/enrich.py
============================
``EnrichMiddleware[T, T]`` — async enrichment via external lookup.

Pattern: for each record, call an async ``enrich()`` function that can
fetch from an external API, database, or cache to add more data.

If ``enrich()`` returns None, the record passes through unchanged.
If it returns a new record, that replaces the original.
If it raises, behaviour is controlled by ``on_error``.

Usage::

    async def add_region(poi: POI, ctx: PipelineContext) -> POI | None:
        region = await geo_db.lookup(poi.coordinates)
        return poi.model_copy(update={"region": region})

    pipeline = (
        Pipeline(source)
        .pipe(EnrichMiddleware(add_region))
        .build(sink)
    )

The enricher can also be a class instance with an ``__call__`` method,
allowing stateful enrichers (connection pool, cache, etc.)::

    class RegionEnricher:
        def __init__(self, db_url: str):
            self._pool = None
            self._db_url = db_url

        async def on_start(self, ctx):
            self._pool = await create_pool(self._db_url)

        async def on_stop(self, ctx):
            await self._pool.close()

        async def __call__(self, record, ctx):
            region = await self._pool.fetchval(...)
            return record.model_copy(update={"region": region})

    enricher = RegionEnricher(db_url=cfg.database_url)
    pipeline = Pipeline(source).pipe(EnrichMiddleware(enricher)).build(sink)
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Generic, TypeVar

import logstruct

from agora.core.middleware import Middleware
from agora.core.types import OnError

if TYPE_CHECKING:
    from agora.core.context import PipelineContext

T = TypeVar("T")
logger = logstruct.getLogger(__name__)


# Enricher callable type: (record, ctx) -> T | None  (may be async)
EnricherFn = Callable[["T", "PipelineContext"], "Awaitable[T | None] | T | None"]


class EnrichMiddleware(Middleware[T, T], Generic[T]):
    """Async enrichment middleware.

    Calls ``enricher(record, ctx)`` for every record.  The enricher can be:
    - An async function: ``async def enrich(record, ctx) -> T | None``
    - A sync function:   ``def enrich(record, ctx) -> T | None``
    - A callable object with ``__call__`` (supports stateful enrichers
      with ``on_start`` / ``on_stop`` hooks)

    Parameters
    ----------
    enricher:
        The enricher callable.
    on_error:
        ``"passthrough"`` (default) — return original record on error.
        ``"drop"`` — drop the record on error.
        ``"raise"`` — re-raise the error (stops pipeline).
    """

    name = "enrich"

    def __init__(
        self,
        enricher: EnricherFn,
        on_error: OnError = OnError.PASSTHROUGH,
    ) -> None:
        self._enricher = enricher
        self._on_error = on_error

    async def on_start(self, ctx: PipelineContext) -> None:
        if hasattr(self._enricher, "on_start"):
            await self._enricher.on_start(ctx)  # type: ignore[union-attr]

    async def on_stop(self, ctx: PipelineContext) -> None:
        if hasattr(self._enricher, "on_stop"):
            await self._enricher.on_stop(ctx)  # type: ignore[union-attr]

    async def process(self, record: T, ctx: PipelineContext) -> T | None:
        try:
            # Check if enricher is async before calling it, so we don't miss
            # async callable objects whose __call__ is a coroutinefunction.
            if asyncio.iscoroutinefunction(self._enricher):
                result = await self._enricher(record, ctx)
            else:
                result = self._enricher(record, ctx)
                # Fallback: handle sync callables that return an awaitable
                # (e.g. a non-coroutinefunction that returns a coroutine)
                if asyncio.iscoroutine(result):
                    result = await result
            return result if result is not None else record
        except Exception as exc:
            ctx.log.warning("enrich_middleware_error", error=str(exc))
            if self._on_error == "raise":
                raise
            if self._on_error == "drop":
                return None
            return record  # passthrough
