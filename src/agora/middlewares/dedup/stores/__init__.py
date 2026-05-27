# agora.dedup.stores
"""
Dedup store implementations — storage backends for tracking seen keys.

Registry
--------
``dedup_store_registry`` provides plugin-style access::

    from agora.middlewares.dedup.stores import dedup_store_registry

    cls = dedup_store_registry.get_or_raise("memory")
    store = cls(max_size=10_000)
"""

from typing import Any

from agora.core.registry import Registry
from agora.middlewares.dedup.stores.base import DedupStore
from agora.middlewares.dedup.stores.memory import InMemoryStore
from agora.middlewares.dedup.stores.sqlite import SQLiteDedupStore

# ======================================================================
# Dedup Store Registry
# ======================================================================

dedup_store_registry: Registry[type[DedupStore[str]]] = Registry(name="dedup_store")

# Register built-in stores
dedup_store_registry.register("memory", InMemoryStore)
dedup_store_registry.register("sqlite", SQLiteDedupStore)


def _register_lazy_stores() -> None:
    """Register optional stores as factories.

    EmbeddingStore requires an explicit ``provider`` kwarg (an ``AIProvider``
    instance).  Callers must inject it — there is no implicit registry lookup.
    This keeps the AI provider dependency visible rather than hidden::

        from agora_ai_gemini import GeminiProvider
        from agora.middlewares.dedup.stores import dedup_store_registry

        store = dedup_store_registry.create("embedding", provider=GeminiProvider(...))
    """

    def _embedding_factory(**kwargs: Any) -> Any:
        if "provider" not in kwargs:
            raise TypeError(
                "EmbeddingStore requires an 'provider' kwarg (an AIProvider instance). "
                "Pass it explicitly: dedup_store_registry.create('embedding', provider=my_provider)"
            )
        from agora.middlewares.dedup.stores.embedding import EmbeddingStore

        return EmbeddingStore(**kwargs)

    dedup_store_registry.register_factory("embedding", _embedding_factory)


_register_lazy_stores()
dedup_store_registry.load_entrypoints("agora.middlewares.dedup.stores")

__all__ = ["DedupStore", "InMemoryStore", "SQLiteDedupStore", "dedup_store_registry"]
