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

from collections.abc import Iterator
from typing import Any

from agora.core.registry import Registry, RegistryItemInfo
from agora.middlewares.dedup.stores.base import DedupStore
from agora.middlewares.dedup.stores.memory import InMemoryStore
from agora.middlewares.dedup.stores.sqlite import SQLiteDedupStore

_DEDUP_STORE_ENTRYPOINT_GROUP = "agora.middlewares.dedup.stores"


class _LazyDedupStoreRegistry(Registry[type[DedupStore[str]]]):
    """Registry that defers third-party dedup store discovery until first lookup.

    Plugin dedup stores import ``DedupStore`` from
    ``agora.middlewares.dedup.stores.base``. Importing that leaf module first
    requires Python to initialize the parent package
    ``agora.middlewares.dedup.stores``.

    If this package eagerly loads entry points during import, plugin discovery
    re-enters the same plugin module while it is only partially initialized,
    which surfaces as a circular-import style ``AttributeError``. Lazy loading
    keeps leaf-module imports side-effect free while preserving the public
    registry lookup contract.
    """

    def __init__(self) -> None:
        super().__init__(name="dedup_store")
        self._entrypoints_loaded = False
        self._entrypoints_loading = False

    def _ensure_entrypoints_loaded(self) -> None:
        if self._entrypoints_loaded or self._entrypoints_loading:
            return
        self.load_entrypoints(_DEDUP_STORE_ENTRYPOINT_GROUP)

    def load_entrypoints(self, group: str) -> None:
        if group != _DEDUP_STORE_ENTRYPOINT_GROUP:
            super().load_entrypoints(group)
            return
        if self._entrypoints_loaded or self._entrypoints_loading:
            return
        self._entrypoints_loading = True
        try:
            super().load_entrypoints(group)
        finally:
            self._entrypoints_loading = False
            self._entrypoints_loaded = True

    def get(self, key: str) -> type[DedupStore[str]] | None:
        self._ensure_entrypoints_loaded()
        return super().get(key)

    def get_or_raise(self, key: str) -> type[DedupStore[str]]:
        self._ensure_entrypoints_loaded()
        return super().get_or_raise(key)

    def create(self, key: str, **kwargs: Any) -> type[DedupStore[str]]:
        self._ensure_entrypoints_loaded()
        return super().create(key, **kwargs)

    def has(self, key: str) -> bool:
        self._ensure_entrypoints_loaded()
        return super().has(key)

    def all_keys(self) -> list[str]:
        self._ensure_entrypoints_loaded()
        return super().all_keys()

    def items(self) -> Iterator[tuple[str, type[DedupStore[str]]]]:
        self._ensure_entrypoints_loaded()
        return super().items()

    def all_items(self) -> Iterator[tuple[str, str]]:
        self._ensure_entrypoints_loaded()
        return super().all_items()

    def describe_items(self) -> list[RegistryItemInfo]:
        self._ensure_entrypoints_loaded()
        return super().describe_items()


# ======================================================================
# Dedup Store Registry
# ======================================================================

dedup_store_registry: Registry[type[DedupStore[str]]] = _LazyDedupStoreRegistry()

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

__all__ = ["DedupStore", "InMemoryStore", "SQLiteDedupStore", "dedup_store_registry"]
