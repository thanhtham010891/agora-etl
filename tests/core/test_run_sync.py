"""
tests/core/test_run_sync.py
============================
Tests for BoundPipeline.run_sync().

Coverage:
- Plain sync context: runs successfully without asyncio.run() by caller
- Returns correct PipelineRunSummary
- Already-running event loop: runs in background thread, does not deadlock
- Repeated calls on the same instance work
- max_records param is respected
- Exceptions from pipeline propagate to the caller
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from agora import IterableSource, Pipeline
from agora.core.source import BaseSource

# ======================================================================
# Fixtures
# ======================================================================


class _CollectSink:
    sink_name = "collect"

    def __init__(self) -> None:
        self.records: list[Any] = []

    async def open(self) -> None:
        pass

    async def write(self, record: Any) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


class _ErrorSource(BaseSource[int]):
    source_name = "error_source"

    async def stream(self):  # type: ignore[override]
        yield 1
        raise RuntimeError("source exploded")


# ======================================================================
# Tests — plain sync context
# ======================================================================


def test_run_sync_returns_summary() -> None:
    sink = _CollectSink()
    summary = Pipeline(IterableSource([1, 2, 3])).build(sink).run_sync()
    assert summary.records_written == 3
    assert sink.records == [1, 2, 3]


def test_run_sync_respects_max_records() -> None:
    sink = _CollectSink()
    summary = Pipeline(IterableSource(list(range(100)))).build(sink).run_sync(max_records=5)
    assert summary.records_written == 5
    assert len(sink.records) == 5


def test_run_sync_empty_source() -> None:
    sink = _CollectSink()
    summary = Pipeline(IterableSource([])).build(sink).run_sync()
    assert summary.records_written == 0


def test_run_sync_repeated_calls() -> None:
    """Calling run_sync() multiple times on the same instance must work."""
    sink = _CollectSink()
    bound = Pipeline(IterableSource([10, 20])).build(sink)
    s1 = bound.run_sync()
    s2 = bound.run_sync()
    assert s1.records_written == 2
    assert s2.records_written == 2


def test_run_sync_propagates_source_exception() -> None:
    sink = _CollectSink()
    with pytest.raises(RuntimeError, match="source exploded"):
        Pipeline(_ErrorSource()).build(sink).run_sync()


# ======================================================================
# Tests — already-running event loop (background thread path)
# ======================================================================


@pytest.mark.asyncio
async def test_run_sync_inside_running_loop() -> None:
    """run_sync() called from inside an async context must not deadlock."""
    sink = _CollectSink()
    # asyncio loop is already running here (pytest-asyncio manages it).
    summary = Pipeline(IterableSource([1, 2, 3])).build(sink).run_sync()
    assert summary.records_written == 3
    assert sink.records == [1, 2, 3]


@pytest.mark.asyncio
async def test_run_sync_inside_running_loop_respects_max_records() -> None:
    sink = _CollectSink()
    summary = Pipeline(IterableSource(list(range(50)))).build(sink).run_sync(max_records=10)
    assert summary.records_written == 10


@pytest.mark.asyncio
async def test_run_sync_inside_running_loop_propagates_exception() -> None:
    sink = _CollectSink()
    with pytest.raises(RuntimeError, match="source exploded"):
        Pipeline(_ErrorSource()).build(sink).run_sync()


@pytest.mark.asyncio
async def test_run_sync_concurrent_calls_in_loop() -> None:
    """Two run_sync() calls from different async tasks must not interfere."""
    sink1 = _CollectSink()
    sink2 = _CollectSink()
    bound1 = Pipeline(IterableSource([1, 2, 3])).build(sink1)
    bound2 = Pipeline(IterableSource([4, 5, 6])).build(sink2)

    s1, s2 = await asyncio.gather(
        asyncio.to_thread(bound1.run_sync),
        asyncio.to_thread(bound2.run_sync),
    )
    assert s1.records_written == 3
    assert s2.records_written == 3
    assert sink1.records == [1, 2, 3]
    assert sink2.records == [4, 5, 6]
