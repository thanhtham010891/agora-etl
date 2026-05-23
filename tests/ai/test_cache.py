from __future__ import annotations

import asyncio
import threading
import time
from typing import TYPE_CHECKING

import pytest

from agora.ai.cache import InMemoryLLMCache, SQLiteLLMCache, build_llm_cache, make_cache_key

if TYPE_CHECKING:
    from pathlib import Path


def test_make_cache_key_is_stable_for_sorted_kwargs() -> None:
    left = make_cache_key("prompt", {"b": 2, "a": 1})
    right = make_cache_key("prompt", {"a": 1, "b": 2})
    assert left == right


@pytest.mark.asyncio
async def test_inmemory_llm_cache_preserves_lru_eviction() -> None:
    cache = InMemoryLLMCache(max_size=2)

    await cache.set("a", "alpha")
    await cache.set("b", "bravo")
    assert await cache.get("a") == "alpha"

    await cache.set("c", "charlie")

    assert await cache.get("a") == "alpha"
    assert await cache.get("b") is None
    assert await cache.get("c") == "charlie"


@pytest.mark.asyncio
async def test_inmemory_llm_cache_respects_ttl() -> None:
    cache = InMemoryLLMCache()

    await cache.set("soon", "gone", ttl=0)

    assert await cache.get("soon") is None


@pytest.mark.asyncio
async def test_sqlite_llm_cache_persists_values(tmp_path: Path) -> None:
    path = tmp_path / "llm-cache.db"
    first = SQLiteLLMCache(path)
    second = SQLiteLLMCache(path)

    try:
        await first.set("answer", "42")
        assert await second.get("answer") == "42"
    finally:
        await first.close()
        await second.close()


@pytest.mark.asyncio
async def test_build_llm_cache_supports_state_backend_config(tmp_path: Path) -> None:
    config = {
        "backend": {
            "type": "sqlite",
            "path": tmp_path / "llm-state.db",
        }
    }
    first = build_llm_cache(config)
    second = build_llm_cache(config)

    try:
        await first.set("shared", "value")
        assert await second.get("shared") == "value"
    finally:
        await first.close()
        await second.close()


@pytest.mark.asyncio
async def test_sqlite_llm_cache_serializes_threaded_store_access(tmp_path: Path) -> None:
    path = tmp_path / "serialized-cache.db"
    cache = SQLiteLLMCache(path)
    active = 0
    max_active = 0
    counter_lock = threading.Lock()

    def _tracked_set(key: str, value: str, ttl_s: int | None = None) -> None:
        del key, value, ttl_s
        nonlocal active, max_active
        with counter_lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.02)
        with counter_lock:
            active -= 1

    cache._store.set = _tracked_set  # type: ignore[method-assign,assignment]

    try:
        await asyncio.gather(*(cache.set(f"k{i}", f"v{i}") for i in range(5)))
    finally:
        await cache.close()

    assert max_active == 1
