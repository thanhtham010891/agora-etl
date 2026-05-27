"""
agora/ai/cache.py
==================
``LLMCache`` — pluggable response cache for LLM calls.

Why cache?
----------
Each LLM call costs money and latency. ETL pipelines often re-process
the same records (reruns, backfills). Caching by content hash ensures
identical records never hit the API twice.

Cache key = SHA-256(prompt + sorted kwargs). Collision risk: negligible.

Implementations
---------------
- ``InMemoryLLMCache``  — single-process, lost on restart
- ``SQLiteLLMCache``    — persists across restarts, zero dependencies
- plugin caches         — registered via ``agora.ai.caches`` entry points

Design notes
------------
The cache now reuses ``agora.state`` instead of hand-rolling a separate
storage layer. This keeps checkpoint, HTTP cache, dedup, and AI cache
on the same backend/capability model.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from agora.core.constants import LLM_CACHE_DEFAULT_TTL_S
from agora.core.errors import ConfigError
from agora.core.registry import Registry
from agora.state import MemoryBackend, SQLiteBackend, StateBackend, TTLKeyValueStore
from agora.state.registry import state_backend_registry

_LLM_NAMESPACE = "llm_cache"

if TYPE_CHECKING:
    from pathlib import Path


def make_cache_key(prompt: str, kwargs: dict[str, Any]) -> str:
    """Stable SHA-256 key from prompt + kwargs.

    Kwargs are sorted before hashing so argument order doesn't matter.
    """
    payload = json.dumps({"prompt": prompt, **dict(sorted(kwargs.items()))}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


@runtime_checkable
class LLMCache(Protocol):
    """Structural protocol for LLM response caches."""

    async def get(self, key: str) -> str | None:
        """Return cached value for *key*, or None if not found / expired."""
        ...

    async def set(self, key: str, value: str, ttl: int = LLM_CACHE_DEFAULT_TTL_S) -> None:
        """Store *value* under *key* with *ttl* seconds expiry."""
        ...

    async def close(self) -> None:
        """Release any held resources (connections, file handles)."""
        ...


class _BackendLLMCache:
    """Async protocol adapter over the shared state cache capability."""

    def __init__(self, store: TTLKeyValueStore) -> None:
        self._store = store

    async def get(self, key: str) -> str | None:
        value = self._store.get(key)
        if value is None:
            return None
        if not isinstance(value, str):
            raise TypeError(f"LLM cache entry for {key!r} must be a string, got {type(value)!r}")
        return value

    async def set(self, key: str, value: str, ttl: int = LLM_CACHE_DEFAULT_TTL_S) -> None:
        self._store.set(key, value, ttl_s=ttl)

    async def close(self) -> None:
        self._store.close()


class StateBackendLLMCache(_BackendLLMCache):
    """LLM cache backed by a shared ``agora.state`` backend."""

    def __init__(
        self,
        backend: StateBackend,
        *,
        namespace: str = _LLM_NAMESPACE,
        default_ttl_s: int = LLM_CACHE_DEFAULT_TTL_S,
    ) -> None:
        super().__init__(
            TTLKeyValueStore(
                backend=backend,
                namespace=namespace,
                default_ttl_s=default_ttl_s,
            )
        )


class InMemoryLLMCache(_BackendLLMCache):
    """In-memory LLM cache.

    This implementation keeps the previous LRU behavior while storing values
    through the shared state capability.
    """

    def __init__(self, max_size: int = 1_000) -> None:
        self._max_size = max_size
        self._store = TTLKeyValueStore(
            backend=MemoryBackend(),
            namespace=_LLM_NAMESPACE,
            default_ttl_s=LLM_CACHE_DEFAULT_TTL_S,
        )
        self._order: OrderedDict[str, float] = OrderedDict()
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> str | None:
        async with self._lock:
            value = self._store.get(key)
            if value is None:
                self._order.pop(key, None)
                return None
            if not isinstance(value, str):
                raise TypeError(
                    f"LLM cache entry for {key!r} must be a string, got {type(value)!r}"
                )
            self._order.pop(key, None)
            self._order[key] = time.monotonic()
            return value

    async def set(self, key: str, value: str, ttl: int = LLM_CACHE_DEFAULT_TTL_S) -> None:
        async with self._lock:
            if key not in self._order and len(self._order) >= self._max_size:
                oldest_key, _ = self._order.popitem(last=False)
                self._store.delete(oldest_key)
            self._store.set(key, value, ttl_s=ttl)
            self._order.pop(key, None)
            self._order[key] = time.monotonic()

    async def close(self) -> None:
        async with self._lock:
            self._order.clear()
            self._store.clear()
            self._store.close()


class SQLiteLLMCache:
    """SQLite-backed LLM cache. Persists across restarts.

    All SQLite I/O is dispatched via asyncio.to_thread to avoid blocking
    the event loop.
    """

    def __init__(self, path: str | Path = ".agora_llm_cache.db") -> None:
        self._store = TTLKeyValueStore(
            backend=SQLiteBackend(path=path),
            namespace=_LLM_NAMESPACE,
            default_ttl_s=LLM_CACHE_DEFAULT_TTL_S,
        )
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> str | None:
        async with self._lock:
            value = await asyncio.to_thread(self._store.get, key)
        if value is None:
            return None
        if not isinstance(value, str):
            raise TypeError(f"LLM cache entry for {key!r} must be a string, got {type(value)!r}")
        return value

    async def set(self, key: str, value: str, ttl: int = LLM_CACHE_DEFAULT_TTL_S) -> None:
        async with self._lock:
            await asyncio.to_thread(self._store.set, key, value, ttl_s=ttl)

    async def close(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self._store.close)


def _build_state_backend(cfg: dict[str, Any]) -> StateBackend:
    """Build a state backend instance from ``state_backend_registry`` config."""
    if not isinstance(cfg, dict):
        raise ConfigError(f"Expected dict for AI cache backend config, got {type(cfg).__name__}")

    backend_cfg = dict(cfg)
    backend_type = backend_cfg.pop("type", None)
    if backend_type is None:
        raise ConfigError(f"Missing 'type' in AI cache backend config: {cfg}")

    if backend_type == "redis" and "key_prefix" in backend_cfg and "prefix" not in backend_cfg:
        backend_cfg["prefix"] = backend_cfg.pop("key_prefix")

    try:
        return state_backend_registry.create(backend_type, **backend_cfg)  # type: ignore[return-value]
    except TypeError as exc:
        raise ConfigError(
            f"Failed to instantiate AI cache state backend '{backend_type}': {exc}"
        ) from exc


def build_llm_cache(
    cfg: LLMCache | dict[str, Any],
) -> LLMCache:
    """Build an ``LLMCache`` from a config dict or pass through an existing cache.

    Supported shapes:

    - ``{"type": "memory", "max_size": 1000}``
    - ``{"type": "sqlite", "path": ".cache/llm.db"}``
    - ``{"type": "redis", "url": "...", "key_prefix": "agora:llm:"}`` via ``agora-etl-redis``
    - ``{"backend": {"type": "sqlite", "path": ".cache/llm.db"}}``
    - ``{"type": "backend", "backend": {...}, "namespace": "llm_cache"}``
    """
    if isinstance(cfg, LLMCache):
        return cfg
    if not isinstance(cfg, dict):
        raise ConfigError(f"Expected dict for AI cache config, got {type(cfg).__name__}")

    cache_cfg = dict(cfg)
    cache_type = cache_cfg.pop("type", None)
    if cache_type is None and "backend" in cache_cfg:
        cache_type = "backend"
    if cache_type is None:
        raise ConfigError(f"Missing 'type' in AI cache config: {cfg}")

    try:
        return ai_cache_registry.create(cache_type, **cache_cfg)  # type: ignore[return-value]
    except TypeError as exc:
        raise ConfigError(f"Failed to instantiate AI cache '{cache_type}': {exc}") from exc


ai_cache_registry: Registry[type[LLMCache]] = Registry(name="ai_cache")
ai_cache_registry.register_factory("memory", InMemoryLLMCache)  # type: ignore[arg-type]
ai_cache_registry.register_factory("sqlite", SQLiteLLMCache)  # type: ignore[arg-type]


def _backend_cache_factory(
    *,
    backend: dict[str, Any],
    namespace: str = _LLM_NAMESPACE,
    default_ttl_s: int = LLM_CACHE_DEFAULT_TTL_S,
) -> LLMCache:
    return StateBackendLLMCache(
        _build_state_backend(backend),
        namespace=namespace,
        default_ttl_s=default_ttl_s,
    )


ai_cache_registry.register_factory("backend", _backend_cache_factory)  # type: ignore[arg-type]
ai_cache_registry.load_entrypoints("agora.ai.caches")
