"""Exact dedup stores built on shared state backends."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, cast

from agora.middlewares.dedup.stores.base import DedupStore

if TYPE_CHECKING:
    from agora.state import MembershipKeyStore


class BackendDedupStore(DedupStore[str]):
    """Dedup adapter over a shared state-backed membership store."""

    def __init__(
        self,
        store: MembershipKeyStore,
        *,
        default_ttl_seconds: int | None = None,
        offload_blocking_calls: bool = False,
    ) -> None:
        self._store = store
        self._default_ttl_seconds = default_ttl_seconds
        self._offload_blocking_calls = offload_blocking_calls

    async def exists(self, key: str) -> bool:
        return cast("bool", await self._call(self._store.contains, key))

    async def add(self, key: str) -> None:
        await self._call(self._store.add, key, ttl_s=self._default_ttl_seconds)

    async def mark_if_new(self, key: str, *, ttl_seconds: int | None = None) -> bool:
        ttl = self._default_ttl_seconds if ttl_seconds is None else ttl_seconds
        return cast("bool", await self._call(self._store.mark_if_new, key, ttl_s=ttl))

    async def close(self) -> None:
        await self._call(self._store.close)

    async def _call(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        if self._offload_blocking_calls:
            return await asyncio.to_thread(func, *args, **kwargs)
        return func(*args, **kwargs)
