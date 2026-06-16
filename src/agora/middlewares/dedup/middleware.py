"""
agora/dedup/middleware.py
=========================
``DedupMiddleware`` — pluggable dedup as a first-class pipeline stage.

Combines a ``DedupStore`` (where keys are persisted) with an optional
strategy (how to compare keys).

For simple exact-key dedup::

    DedupMiddleware(key=lambda r: r.slug)

For fuzzy name-based dedup (like data-collector's SlugDeduplicator)::

    DedupMiddleware(
        key=lambda r: r.name.lower(),
        store=InMemoryStore(),
        strategy=FuzzyMatchStrategy(threshold=0.82),
    )

For distributed dedup across pods::

    from agora_plugins.redis import RedisStore

    DedupMiddleware(
        key=lambda r: r.slug,
        store=RedisStore(url="redis://redis:6379", ttl_seconds=86400),
    )
"""

from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING, Generic, TypeVar

import logstruct

from agora.core.middleware import Middleware
from agora.core.types import DedupStoreFailurePolicy
from agora.middlewares.dedup.stores.memory import InMemoryStore

if TYPE_CHECKING:
    from collections.abc import Callable

    from agora.core.context import PipelineContext
    from agora.middlewares.dedup.stores.base import DedupStore
    from agora.middlewares.dedup.strategies.base import DedupStrategy

T = TypeVar("T")

logger = logstruct.getLogger(__name__)


class DedupMiddleware(Middleware[T, T], Generic[T]):
    """Dedup records by a computed key.

    Strategy (optional)
    -------------------
    When a ``strategy`` is provided, dedup works differently:
    - Exact (default): key must match an already-seen key exactly
    - Fuzzy: key is similar to an already-seen key within threshold

    Note: fuzzy dedup requires loading ALL seen keys into memory to compare
    against. Performance is O(n) per record where n = number of seen keys,
    capped at ``max_fuzzy_keys`` (default 100,000). Beyond that cap, dedup
    falls back to exact store-only matching. For large-scale fuzzy dedup,
    use a vector store plugin instead.

    Parameters
    ----------
    key:
        Callable that extracts a string dedup key from a record.
    store:
        ``DedupStore`` implementation. Defaults to ``InMemoryStore()``.
    strategy:
        Optional comparison strategy. When ``None``, uses exact match
        (``key in store``). Provide ``FuzzyMatchStrategy`` for fuzzy.
    max_fuzzy_keys:
        Maximum number of keys to hold in memory for fuzzy comparison.
        When exceeded, falls back to store-only dedup and logs a warning.
        Default: 100,000.
    name:
        Middleware name (shown in metrics / logs).
    store_failure_policy:
        Behavior when the backing dedup store raises.
        - ``fail_closed``: re-raise and let the pipeline treat the record as a
          middleware failure
        - ``fail_open``: log and pass the record through unchanged
    """

    def __init__(
        self,
        key: Callable[[T], str],
        store: DedupStore[str] | None = None,
        strategy: DedupStrategy | None = None,
        max_fuzzy_keys: int = 100_000,
        name: str = "dedup",
        store_failure_policy: DedupStoreFailurePolicy = DedupStoreFailurePolicy.FAIL_CLOSED,
    ) -> None:
        self.name = name
        self._key = key
        self._store: DedupStore[str] = store if store is not None else InMemoryStore()
        self._strategy = strategy
        self._max_fuzzy_keys = max_fuzzy_keys
        self._store_failure_policy = store_failure_policy
        # For fuzzy dedup: we also need an in-memory list of seen keys
        self._seen_keys: list[str] | None = [] if strategy is not None else None
        self._pending_keys: set[str] = set()
        self._pending_fuzzy_keys: list[str] = []
        self._fuzzy_overflow_warned: bool = False

    async def on_stop(self, ctx: PipelineContext) -> None:
        self._pending_keys.clear()
        self._pending_fuzzy_keys.clear()
        await self._store.close()

    def _handle_store_failure(
        self,
        record: T,
        ctx: PipelineContext,
        key: str,
        exc: Exception,
    ) -> T:
        if self._store_failure_policy == DedupStoreFailurePolicy.FAIL_OPEN:
            ctx.log.warning(
                "dedup_store_fail_open",
                key=key,
                middleware=self.name,
                error=str(exc),
            )
            return record
        raise exc

    def _register_commit_marker(self, record: T, ctx: PipelineContext, key: str) -> None:
        self._pending_keys.add(key)

        async def _commit() -> None:
            try:
                await self._store.add(key)
                if self._seen_keys is not None and key not in self._seen_keys:
                    self._seen_keys.append(key)
            finally:
                self._pending_keys.discard(key)
                with suppress(ValueError):
                    self._pending_fuzzy_keys.remove(key)

        async def _discard() -> None:
            self._pending_keys.discard(key)
            with suppress(ValueError):
                self._pending_fuzzy_keys.remove(key)

        ctx.register_success_hook(record, _commit, on_discard=_discard)

    async def process(self, record: T, ctx: PipelineContext) -> T | None:
        key = self._key(record)

        # Fuzzy path: compare against all seen keys
        if self._strategy is not None and self._seen_keys is not None:
            # Guard: cap in-memory keys to prevent unbounded growth (W8 fix)
            if len(self._seen_keys) >= self._max_fuzzy_keys:
                if not self._fuzzy_overflow_warned:
                    ctx.log.warning(
                        "dedup_fuzzy_overflow",
                        limit=self._max_fuzzy_keys,
                        middleware=self.name,
                    )
                    self._fuzzy_overflow_warned = True
                # Fallback: store-only check (no fuzzy comparison)
                try:
                    if key in self._pending_keys or await self._store.exists(key):
                        return None
                    self._register_commit_marker(record, ctx, key)
                except Exception as exc:
                    return self._handle_store_failure(record, ctx, key, exc)
                return record

            for seen in [*self._seen_keys, *self._pending_fuzzy_keys]:
                if self._strategy.is_duplicate(key, seen):
                    ctx.log.debug(
                        "dedup_fuzzy_skip",
                        key=key,
                        matched=seen,
                        middleware=self.name,
                    )
                    return None
            # Not a duplicate — remember this key
            self._pending_fuzzy_keys.append(key)
            try:
                self._register_commit_marker(record, ctx, key)
            except Exception as exc:
                return self._handle_store_failure(record, ctx, key, exc)
            return record

        # Exact path: reserve locally, but persist only after the record is
        # committed by the delivery layer. This avoids losing records when a
        # downstream sink fails after dedup has seen the key.
        try:
            is_new = key not in self._pending_keys and not await self._store.exists(key)
        except Exception as exc:
            return self._handle_store_failure(record, ctx, key, exc)

        if not is_new:
            ctx.log.debug("dedup_exact_skip", key=key, middleware=self.name)
            return None

        self._register_commit_marker(record, ctx, key)
        return record
