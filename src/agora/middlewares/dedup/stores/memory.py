"""
agora/dedup/stores/memory.py
============================
In-memory dedup store backed by a shared ``MemoryBackend``.

Suitable for single-process pipelines. Data is lost on restart.
If RAM is a concern, set ``max_size`` for LRU eviction.
"""

from __future__ import annotations

from collections import OrderedDict

from agora.middlewares.dedup.stores.backend import BackendDedupStore
from agora.state import MembershipKeyStore, MemoryBackend


class InMemoryStore(BackendDedupStore):
    """In-memory dedup store with optional LRU eviction.

    Parameters
    ----------
    max_size:
        Maximum number of keys to keep. When exceeded, the oldest keys are
        evicted. ``None`` keeps all keys.
    namespace:
        Namespace prefix used inside the backing ``MemoryBackend``.
    """

    def __init__(
        self,
        max_size: int | None = None,
        *,
        namespace: str = "dedup",
    ) -> None:
        self._max_size = max_size
        self._lru: OrderedDict[str, None] | None = OrderedDict() if max_size is not None else None
        self._backend = MemoryBackend()
        super().__init__(MembershipKeyStore(self._backend, namespace=namespace))

    async def exists(self, key: str) -> bool:
        if self._lru is not None:
            return self._exists_in_lru(key)
        return await super().exists(key)

    async def add(self, key: str) -> None:
        if self._lru is not None:
            self._remember_with_lru(key, ttl_seconds=self._default_ttl_seconds)
            return
        await super().add(key)

    async def mark_if_new(self, key: str, *, ttl_seconds: int | None = None) -> bool:
        if self._lru is not None:
            if self._exists_in_lru(key):
                return False
            self._remember_with_lru(key, ttl_seconds=ttl_seconds)
            return True
        return await super().mark_if_new(key)

    def __len__(self) -> int:
        if self._lru is not None:
            self._purge_expired_lru_keys()
            return len(self._lru)
        return self._store.count()

    def _exists_in_lru(self, key: str) -> bool:
        assert self._lru is not None
        if key not in self._lru:
            return False
        if not self._store.contains(key):
            self._lru.pop(key, None)
            return False
        self._lru.move_to_end(key)
        return True

    def _purge_expired_lru_keys(self) -> None:
        assert self._lru is not None
        for key in list(self._lru):
            if not self._store.contains(key):
                self._lru.pop(key, None)

    def _remember_with_lru(self, key: str, *, ttl_seconds: int | None) -> None:
        self._store.add(key, ttl_s=ttl_seconds)
        assert self._lru is not None
        self._lru[key] = None
        self._lru.move_to_end(key)
        if self._max_size and len(self._lru) > self._max_size:
            oldest, _ = self._lru.popitem(last=False)
            self._store.delete(oldest)
