"""tests/core/test_dlq_source.py
=================================
Unit tests for DLQSource — verifies stream() filtering by max_attempts.

Requirements: 2.14
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from agora.core.dlq import DLQRecord, DLQSource
from agora.core.source import BaseSource

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_record(attempt: int = 0, max_attempts: int | None = None, **overrides) -> DLQRecord:
    """Create a minimal DLQRecord with configurable retry fields."""
    defaults = {
        "pipeline_id": "pipe_1",
        "run_id": "run_abc",
        "stage": "sink",
        "error_type": "ValueError",
        "error_message": "something went wrong",
        "record": {"key": "value"},
        "attempt": attempt,
        "max_attempts": max_attempts,
    }
    defaults.update(overrides)
    return DLQRecord(**defaults)


class _InMemoryDLQSource(DLQSource):
    """Concrete DLQSource backed by an in-memory list — for testing only."""

    source_name = "in_memory_dlq"

    def __init__(self, records: list[DLQRecord]) -> None:
        self._records = records

    async def _iter_records(self) -> AsyncGenerator[DLQRecord, None]:
        for record in self._records:
            yield record


async def _collect(source: DLQSource) -> list[DLQRecord]:
    """Drain stream() into a list."""
    results: list[DLQRecord] = []
    async for record in source.stream():
        results.append(record)
    return results


# ---------------------------------------------------------------------------
# DLQSource class structure
# ---------------------------------------------------------------------------


class TestDLQSourceStructure:
    def test_dlq_source_is_subclass_of_base_source(self):
        assert issubclass(DLQSource, BaseSource)

    def test_dlq_source_name(self):
        assert DLQSource.source_name == "dlq_source"

    def test_iter_records_is_abstract(self):
        assert getattr(DLQSource._iter_records, "__isabstractmethod__", False)

    def test_stream_is_not_abstract(self):
        assert not getattr(DLQSource.stream, "__isabstractmethod__", False)

    def test_cannot_instantiate_dlq_source_directly(self):
        with pytest.raises(TypeError):
            DLQSource()  # type: ignore[abstract]

    def test_concrete_subclass_can_be_instantiated(self):
        source = _InMemoryDLQSource([])
        assert source is not None


# ---------------------------------------------------------------------------
# stream() filtering: max_attempts=None (unlimited)
# ---------------------------------------------------------------------------


class TestStreamUnlimitedRetries:
    @pytest.mark.asyncio
    async def test_record_with_max_attempts_none_is_always_yielded(self):
        """max_attempts=None means unlimited — record always passes filter."""
        records = [_make_record(attempt=0, max_attempts=None)]
        source = _InMemoryDLQSource(records)
        result = await _collect(source)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_record_with_max_attempts_none_and_high_attempt_is_yielded(self):
        """Even attempt=999 passes when max_attempts=None."""
        records = [_make_record(attempt=999, max_attempts=None)]
        source = _InMemoryDLQSource(records)
        result = await _collect(source)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_multiple_unlimited_records_all_yielded(self):
        records = [
            _make_record(attempt=0, max_attempts=None),
            _make_record(attempt=5, max_attempts=None),
            _make_record(attempt=100, max_attempts=None),
        ]
        source = _InMemoryDLQSource(records)
        result = await _collect(source)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# stream() filtering: attempt < max_attempts
# ---------------------------------------------------------------------------


class TestStreamWithMaxAttempts:
    @pytest.mark.asyncio
    async def test_record_with_attempt_zero_and_max_attempts_three_is_yielded(self):
        """attempt=0 < max_attempts=3 → eligible."""
        records = [_make_record(attempt=0, max_attempts=3)]
        source = _InMemoryDLQSource(records)
        result = await _collect(source)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_record_with_attempt_equal_to_max_attempts_is_filtered(self):
        """attempt=3 == max_attempts=3 → exhausted, filtered out."""
        records = [_make_record(attempt=3, max_attempts=3)]
        source = _InMemoryDLQSource(records)
        result = await _collect(source)
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_record_with_attempt_exceeding_max_attempts_is_filtered(self):
        """attempt=5 > max_attempts=3 → exhausted, filtered out."""
        records = [_make_record(attempt=5, max_attempts=3)]
        source = _InMemoryDLQSource(records)
        result = await _collect(source)
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_record_with_attempt_one_below_max_is_yielded(self):
        """attempt=2 < max_attempts=3 → still eligible."""
        records = [_make_record(attempt=2, max_attempts=3)]
        source = _InMemoryDLQSource(records)
        result = await _collect(source)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_max_attempts_one_attempt_zero_is_yielded(self):
        """attempt=0 < max_attempts=1 → first (and only) attempt allowed."""
        records = [_make_record(attempt=0, max_attempts=1)]
        source = _InMemoryDLQSource(records)
        result = await _collect(source)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_max_attempts_one_attempt_one_is_filtered(self):
        """attempt=1 == max_attempts=1 → exhausted."""
        records = [_make_record(attempt=1, max_attempts=1)]
        source = _InMemoryDLQSource(records)
        result = await _collect(source)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# stream() filtering: mixed records
# ---------------------------------------------------------------------------


class TestStreamMixedRecords:
    @pytest.mark.asyncio
    async def test_mixed_eligible_and_exhausted_records(self):
        """Only eligible records pass through; exhausted ones are dropped."""
        records = [
            _make_record(attempt=0, max_attempts=3),  # eligible
            _make_record(attempt=3, max_attempts=3),  # exhausted
            _make_record(attempt=1, max_attempts=None),  # unlimited — eligible
            _make_record(attempt=5, max_attempts=5),  # exhausted
            _make_record(attempt=2, max_attempts=5),  # eligible
        ]
        source = _InMemoryDLQSource(records)
        result = await _collect(source)
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_empty_source_yields_nothing(self):
        source = _InMemoryDLQSource([])
        result = await _collect(source)
        assert result == []

    @pytest.mark.asyncio
    async def test_all_exhausted_yields_nothing(self):
        records = [
            _make_record(attempt=3, max_attempts=3),
            _make_record(attempt=10, max_attempts=5),
        ]
        source = _InMemoryDLQSource(records)
        result = await _collect(source)
        assert result == []

    @pytest.mark.asyncio
    async def test_stream_preserves_record_identity(self):
        """Records yielded by stream() are the exact same objects from _iter_records()."""
        r1 = _make_record(attempt=0, max_attempts=3)
        r2 = _make_record(attempt=1, max_attempts=None)
        source = _InMemoryDLQSource([r1, r2])
        result = await _collect(source)
        assert result[0] is r1
        assert result[1] is r2

    @pytest.mark.asyncio
    async def test_stream_preserves_order(self):
        """Records are yielded in the same order as _iter_records()."""
        records = [
            _make_record(attempt=0, max_attempts=5, run_id="first"),
            _make_record(attempt=1, max_attempts=5, run_id="second"),
            _make_record(attempt=2, max_attempts=5, run_id="third"),
        ]
        source = _InMemoryDLQSource(records)
        result = await _collect(source)
        assert [r.run_id for r in result] == ["first", "second", "third"]
