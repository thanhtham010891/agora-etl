"""
agora/dedup/stores/embedding.py
=================================
``EmbeddingStore`` — semantic deduplication using vector embeddings.

Unlike exact-key stores (``InMemoryStore``, ``RedisStore``), this store
compares records by **semantic similarity**: two records are duplicates
if their embeddings are within ``similarity_threshold`` cosine distance,
even if their slugs or exact text differ.

Example: "Phở Bà Đặng" and "Quán Phở Bà Đặng (Chi nhánh 2)" would be
exact-key misses but semantic duplicates at threshold ≥ 0.90.

Backend options
---------------
``"memory"`` (default):
    In-process dict. No persistence. Suitable for single-run dedup.

Storage format
--------------
Each stored entry is: ``{"key": str, "embedding": list[float]}``.
On ``exists(key)``, the key is embedded and compared against all stored
embeddings.  The first match above ``similarity_threshold`` is a hit.

Performance note
----------------
For large datasets (> 100k records), use a dedicated vector DB
(pgvector, Qdrant, Weaviate) instead.  This store is intentionally
simple and dependency-light for the common case.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import logstruct

from agora.middlewares.dedup.stores.base import DedupStore
from agora.utils.math import cosine_similarity as _cosine_similarity

if TYPE_CHECKING:
    from agora.ai.providers.base import AIProvider

logger = logstruct.getLogger(__name__)

# _cosine_similarity imported from agora.utils.math


class EmbeddingStore(DedupStore[str]):
    """Semantic dedup store using vector embeddings + cosine similarity.

    Parameters
    ----------
    provider:
        Any ``AIProvider`` that implements ``embed()``.
        Use ``GeminiProvider`` or ``OpenAIProvider`` — not ``AnthropicProvider``.
    similarity_threshold:
        Records with cosine similarity >= this value are considered duplicates.
        Recommended range: 0.88-0.95.
        - 0.95+ : very strict (near-exact match only)
        - 0.90  : good balance (catches paraphrases)
        - 0.85  : lenient (may over-deduplicate)
    The Redis-backed variant lives in the ``agora-etl-redis`` plugin as
    ``RedisEmbeddingStore``.
    """

    def __init__(
        self,
        provider: AIProvider,
        *,
        similarity_threshold: float = 0.92,
    ) -> None:
        self._provider = provider
        self._threshold = similarity_threshold
        self._memory: list[tuple[str, list[float]]] = []
        self._lock = asyncio.Lock()  # guards _memory for concurrent mark_if_new calls

    # ------------------------------------------------------------------ #
    # DedupStore implementation                                            #
    # ------------------------------------------------------------------ #

    async def exists(self, key: str) -> bool:
        """Return True if *key* is semantically similar to a seen key."""
        embedding = (await self._provider.embed(key)).embedding
        return self._memory_exists(embedding)

    async def add(self, key: str) -> None:
        """Mark *key* as seen (stores its embedding)."""
        embedding = (await self._provider.embed(key)).embedding
        self._memory.append((key, embedding))
        logger.debug("embedding_store_add", backend="memory", total=len(self._memory))

    async def mark_if_new(self, key: str, *, ttl_seconds: int | None = None) -> bool:
        """Atomic check-and-add using an asyncio lock.

        Embeds *key* once, then under the lock checks similarity and appends
        only if no match is found — preventing the check-then-act race that
        the base class default would have in buffered (concurrent) lanes.
        """
        embedding = (await self._provider.embed(key)).embedding
        async with self._lock:
            if self._memory_exists(embedding):
                return False
            self._memory.append((key, embedding))
            logger.debug("embedding_store_add", backend="memory", total=len(self._memory))
            return True

    async def close(self) -> None:
        return None

    # ------------------------------------------------------------------ #
    # Memory backend                                                       #
    # ------------------------------------------------------------------ #

    def _memory_exists(self, query_embedding: list[float]) -> bool:
        for _, stored_embedding in self._memory:
            sim = _cosine_similarity(query_embedding, stored_embedding)
            if sim >= self._threshold:
                logger.debug("embedding_dedup_hit", similarity=round(sim, 4))
                return True
        return False
