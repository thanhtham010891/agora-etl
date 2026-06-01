"""
agora/dedup/stores/base.py
==========================
Abstract dedup store — defines the storage interface for seen keys.

Separate from DedupStrategy: the store says "have I seen this key?"
while the strategy says "are these two keys a match?".
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

K = TypeVar("K")


class DedupStore(ABC, Generic[K]):
    """Abstract key-value store for tracking seen dedup keys.

    Implementations:
    - InMemoryStore     — single-process, no persistence
    - agora_plugins.redis.RedisStore — distributed, optional TTL
    - agora_bloom.BloomFilterStore — probabilistic, memory-efficient
    """

    @abstractmethod
    async def exists(self, key: K) -> bool:
        """Return True if *key* has been seen before."""

    @abstractmethod
    async def add(self, key: K) -> None:
        """Mark *key* as seen."""

    async def mark_if_new(self, key: K, *, ttl_seconds: int | None = None) -> bool:
        """Mark *key* as seen if it has not been seen before.

        Returns:
            True if the key was newly recorded.
            False if the key already existed.

        Notes:
            The default implementation calls ``exists()`` then ``add()`` and is
            NOT atomic. Under concurrent execution (buffered lane) two coroutines
            can both see ``exists() == False`` and both call ``add()``, producing
            a duplicate. Stores that need atomicity MUST override this method
            (see ``BackendDedupStore`` which delegates to ``MembershipKeyStore.mark_if_new``).
            Do NOT use the default implementation in buffered pipelines without
            an external lock.
        """
        if await self.exists(key):
            return False
        await self.add(key)
        return True

    async def close(self) -> None:
        """Release any held resources."""
