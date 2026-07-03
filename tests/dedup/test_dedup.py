"""
Tests for DedupMiddleware + InMemoryStore + FuzzyMatchStrategy.
"""

from __future__ import annotations

import asyncio

import pytest

from agora import IterableSource, Pipeline
from agora.core.data_plane import DataPlane, SourceDataPlaneSpec
from agora.core.source import BaseSource
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
async def test_memory_store_lru_path_preserves_ttl_expiration():
    store = InMemoryStore(max_size=10)

    assert await store.mark_if_new("ephemeral", ttl_seconds=1) is True
    await asyncio.sleep(1.1)
    assert await store.mark_if_new("ephemeral", ttl_seconds=1) is True
    assert await store.exists("ephemeral") is True


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
async def test_dedup_exact_marks_store_only_after_successful_delivery():
    class _TrackingStore(DedupStore[str]):
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []
            self._seen: set[str] = set()

        async def exists(self, key: str) -> bool:
            self.calls.append(("exists", key))
            return key in self._seen

        async def add(self, key: str) -> None:
            self.calls.append(("add", key))
            self._seen.add(key)

        async def mark_if_new(self, key: str, *, ttl_seconds: int | None = None) -> bool:
            del ttl_seconds
            self.calls.append(("mark_if_new", key))
            raise AssertionError("dedup middleware must not mark keys before sink success")

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

    store = _TrackingStore()
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
        ("exists", "a"),
        ("add", "a"),
        ("exists", "b"),
        ("add", "b"),
        ("exists", "a"),
    ]


@pytest.mark.asyncio
async def test_dedup_exact_does_not_persist_key_when_sink_fails():
    class _FailingSink:
        sink_name = "failing"

        async def open(self) -> None:
            return None

        async def write(self, record: dict[str, str]) -> None:
            del record
            raise RuntimeError("sink down")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    class _CollectSink:
        sink_name = "collect"

        def __init__(self) -> None:
            self.records: list[dict[str, str]] = []

        async def open(self) -> None:
            return None

        async def write(self, record: dict[str, str]) -> None:
            self.records.append(record)

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    record = {"id": "A"}
    middleware = DedupMiddleware(key=lambda row: row["id"], store=InMemoryStore())

    with pytest.raises(RuntimeError, match="sink down"):
        await (
            Pipeline(IterableSource([record]))
            .pipe(middleware)
            .build(_FailingSink())  # type: ignore[arg-type]
            .run()
        )

    sink = _CollectSink()
    summary = await (
        Pipeline(IterableSource([record]))
        .pipe(middleware)
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [record]
    assert summary.records_written == 1
    assert summary.records_dropped == 0


@pytest.mark.asyncio
async def test_dedup_respects_explicit_empty_store_instance():
    store = InMemoryStore(max_size=1)
    middleware = DedupMiddleware(key=lambda value: value, store=store)
    sink = StdoutSink()

    await Pipeline(IterableSource(["a", "b"])).pipe(middleware).build(sink).run()

    assert await store.exists("a") is False
    assert await store.exists("b") is True


@pytest.mark.asyncio
async def test_dedup_batch_source_marks_store_after_successful_batch_delivery():
    class _BatchSource(BaseSource[int]):
        source_name = "batch_dedup_source"

        def __init__(self, batches: list[list[int]]) -> None:
            self._batches = batches
            self._checkpoint: int | None = None

        def data_plane_spec(self) -> SourceDataPlaneSpec:
            return SourceDataPlaneSpec(
                source_name=self.source_name,
                emitted_plane=DataPlane.PYTHON_BATCHES,
                supports_batch_emit=True,
                emits_arrow_batches=False,
            )

        async def stream_batches(self):  # type: ignore[override]
            for index, batch in enumerate(self._batches):
                self._checkpoint = index
                yield batch

        async def stream(self):
            for batch in self._batches:
                for record in batch:
                    yield record

        def current_checkpoint(self) -> int | None:
            return self._checkpoint

    class _CollectSink:
        sink_name = "collect"

        def __init__(self) -> None:
            self.records: list[int] = []

        async def open(self) -> None:
            return None

        async def write(self, record: int) -> None:
            self.records.append(record)

        async def write_batch(self, records: list[int]) -> None:
            self.records.extend(records)

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    store = InMemoryStore()
    sink = _CollectSink()

    summary = await (
        Pipeline(_BatchSource([[1, 2]]))
        .pipe(DedupMiddleware(key=str, store=store))
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [1, 2]
    assert summary.records_written == 2
    assert await store.exists("1") is True
    assert await store.exists("2") is True


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
