"""
Tests for DedupMiddleware + InMemoryStore + FuzzyMatchStrategy.
"""

from __future__ import annotations

import pytest

from agora import IterableSource, Pipeline
from agora.core.types import DedupStoreFailurePolicy
from agora.middlewares.dedup import DedupMiddleware
from agora.middlewares.dedup.stores.base import DedupStore
from agora.middlewares.dedup.stores.memory import InMemoryStore
from agora.middlewares.dedup.stores.sqlite import SQLiteDedupStore
from agora.middlewares.dedup.strategies.fuzzy import FuzzyMatchStrategy, _jaro_winkler
from agora.sinks.io.stdout import StdoutSink

# ======================================================================
# InMemoryStore
# ======================================================================


@pytest.mark.asyncio
async def test_memory_store_add_and_exists():
    store = InMemoryStore()
    assert not await store.exists("abc")
    await store.add("abc")
    assert await store.exists("abc")


@pytest.mark.asyncio
async def test_memory_store_lru_eviction():
    store = InMemoryStore(max_size=2)
    await store.add("a")
    await store.add("b")
    await store.add("c")  # evicts "a"
    assert not await store.exists("a")
    assert await store.exists("b")
    assert await store.exists("c")


@pytest.mark.asyncio
async def test_memory_store_mark_if_new_reports_duplicates():
    store = InMemoryStore()
    assert await store.mark_if_new("abc")
    assert not await store.mark_if_new("abc")
    assert await store.exists("abc")


@pytest.mark.asyncio
async def test_sqlite_dedup_store_persists_keys_across_instances(tmp_path):
    path = tmp_path / "dedup.db"

    first = SQLiteDedupStore(path)
    try:
        assert await first.mark_if_new("abc")
    finally:
        await first.close()

    second = SQLiteDedupStore(path)
    try:
        assert not await second.mark_if_new("abc")
        assert await second.exists("abc")
    finally:
        await second.close()


# ======================================================================
# FuzzyMatchStrategy
# ======================================================================


def test_jaro_winkler_identical():
    assert _jaro_winkler("hello", "hello") == 1.0


def test_jaro_winkler_empty():
    assert _jaro_winkler("", "") == 1.0
    assert _jaro_winkler("abc", "") == 0.0


def test_jaro_winkler_similar():
    score = _jaro_winkler("pho 24", "phở 24")
    assert score > 0.8


def test_fuzzy_strategy_exact_match():
    s = FuzzyMatchStrategy(threshold=0.85)
    assert s.is_duplicate("Pho 24", "Pho 24")


def test_fuzzy_strategy_similar():
    s = FuzzyMatchStrategy(threshold=0.82)
    assert s.is_duplicate("pho 24 hanoi", "pho24 hanoi")


def test_fuzzy_strategy_different():
    s = FuzzyMatchStrategy(threshold=0.9)
    assert not s.is_duplicate("McDonalds", "KFC")


# ======================================================================
# DedupMiddleware in pipeline
# ======================================================================


@pytest.mark.asyncio
async def test_dedup_exact_drops_duplicates():
    records = ["a", "b", "a", "c", "b"]
    source = IterableSource(records)
    pipeline = (
        Pipeline(source)
        .pipe(DedupMiddleware(key=lambda x: x, store=InMemoryStore()))
        .build(StdoutSink())
    )
    summary = await pipeline.run()
    assert summary.records_consumed == 5
    assert summary.records_written == 3  # a, b, c
    assert summary.records_dropped == 2


@pytest.mark.asyncio
async def test_dedup_fuzzy_drops_similar():
    records = ["Pho 24 Hanoi", "Pho24 Hanoi", "KFC Downtown"]
    source = IterableSource(records)
    pipeline = (
        Pipeline(source)
        .pipe(
            DedupMiddleware(
                key=lambda x: x.lower(),
                store=InMemoryStore(),
                strategy=FuzzyMatchStrategy(threshold=0.85),
            )
        )
        .build(StdoutSink())
    )
    summary = await pipeline.run()
    assert summary.records_consumed == 3
    assert summary.records_written == 2  # "Pho24 Hanoi" is a fuzzy dup
    assert summary.records_dropped == 1


@pytest.mark.asyncio
async def test_dedup_exact_prefers_mark_if_new_capability():
    class _AtomicStore(DedupStore[str]):
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []
            self._seen: set[str] = set()

        async def exists(self, key: str) -> bool:
            self.calls.append(("exists", key))
            raise AssertionError("exact dedup path should not call exists()")

        async def add(self, key: str) -> None:
            self.calls.append(("add", key))
            raise AssertionError("exact dedup path should not call add()")

        async def mark_if_new(self, key: str, *, ttl_seconds: int | None = None) -> bool:
            del ttl_seconds
            self.calls.append(("mark_if_new", key))
            if key in self._seen:
                return False
            self._seen.add(key)
            return True

    class _CollectSink:
        sink_name = "collect"

        def __init__(self) -> None:
            self.records: list[str] = []

        async def open(self) -> None:
            return None

        async def write(self, record: str) -> None:
            self.records.append(record)

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    store = _AtomicStore()
    sink = _CollectSink()
    summary = await (
        Pipeline(IterableSource(["a", "b", "a"]))
        .pipe(DedupMiddleware(key=lambda x: x, store=store))
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == ["a", "b"]
    assert summary.records_dropped == 1
    assert store.calls == [
        ("mark_if_new", "a"),
        ("mark_if_new", "b"),
        ("mark_if_new", "a"),
    ]


@pytest.mark.asyncio
async def test_dedup_store_fail_open_passes_record_through():
    class _FailingStore(DedupStore[str]):
        async def exists(self, key: str) -> bool:
            raise RuntimeError("dedup backend down")

        async def add(self, key: str) -> None:
            raise RuntimeError("dedup backend down")

        async def mark_if_new(self, key: str, *, ttl_seconds: int | None = None) -> bool:
            del ttl_seconds
            raise RuntimeError("dedup backend down")

    class _CollectSink:
        sink_name = "collect"

        def __init__(self) -> None:
            self.records: list[str] = []

        async def open(self) -> None:
            return None

        async def write(self, record: str) -> None:
            self.records.append(record)

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    sink = _CollectSink()
    summary = await (
        Pipeline(IterableSource(["a"]))
        .pipe(
            DedupMiddleware(
                key=lambda x: x,
                store=_FailingStore(),
                store_failure_policy=DedupStoreFailurePolicy.FAIL_OPEN,
            )
        )
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == ["a"]
    assert summary.records_written == 1
    assert summary.records_dropped == 0
