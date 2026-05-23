"""tests/core/test_dlq_record.py
================================
Unit tests for DLQRecord — verifies new `attempt` and `max_attempts` fields
are present with correct defaults and that all existing fields are preserved.

Requirements: 2.15, 3.15, 3.16
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from agora.core.dlq import DLQRecord

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _minimal_record(**overrides) -> DLQRecord:
    """Create a DLQRecord with only the required positional fields."""
    defaults = {
        "pipeline_id": "pipe_1",
        "run_id": "run_abc",
        "stage": "sink",
        "error_type": "ValueError",
        "error_message": "something went wrong",
        "record": {"key": "value"},
    }
    defaults.update(overrides)
    return DLQRecord(**defaults)


# ---------------------------------------------------------------------------
# New fields: attempt and max_attempts
# ---------------------------------------------------------------------------


class TestAttemptField:
    def test_attempt_field_exists(self):
        record = _minimal_record()
        assert hasattr(record, "attempt")

    def test_attempt_default_is_zero(self):
        record = _minimal_record()
        assert record.attempt == 0

    def test_attempt_can_be_set_explicitly(self):
        record = _minimal_record(attempt=3)
        assert record.attempt == 3

    def test_attempt_is_int(self):
        record = _minimal_record()
        assert isinstance(record.attempt, int)


class TestMaxAttemptsField:
    def test_max_attempts_field_exists(self):
        record = _minimal_record()
        assert hasattr(record, "max_attempts")

    def test_max_attempts_default_is_none(self):
        record = _minimal_record()
        assert record.max_attempts is None

    def test_max_attempts_can_be_set_to_int(self):
        record = _minimal_record(max_attempts=5)
        assert record.max_attempts == 5

    def test_max_attempts_can_be_set_to_none_explicitly(self):
        record = _minimal_record(max_attempts=None)
        assert record.max_attempts is None


# ---------------------------------------------------------------------------
# Backward compatibility: existing fields preserved
# ---------------------------------------------------------------------------


class TestExistingFieldsPreserved:
    def test_required_fields_present(self):
        record = _minimal_record()
        assert record.pipeline_id == "pipe_1"
        assert record.run_id == "run_abc"
        assert record.stage == "sink"
        assert record.error_type == "ValueError"
        assert record.error_message == "something went wrong"
        assert record.record == {"key": "value"}

    def test_optional_fields_default_to_none(self):
        record = _minimal_record()
        assert record.source is None
        assert record.checkpoint is None
        assert record.middleware is None
        assert record.sink is None
        assert record.original_record is None
        assert record.processed_record is None

    def test_created_at_defaults_to_now(self):
        before = datetime.now(UTC)
        record = _minimal_record()
        after = datetime.now(UTC)
        assert before <= record.created_at <= after

    def test_optional_fields_can_be_set(self):
        now = datetime.now(UTC)
        record = DLQRecord(
            pipeline_id="p",
            run_id="r",
            stage="middleware",
            error_type="RuntimeError",
            error_message="oops",
            record=42,
            source="kafka_source",
            checkpoint={"offset": 100},
            middleware="dedup",
            sink="postgres_sink",
            created_at=now,
        )
        assert record.source == "kafka_source"
        assert record.checkpoint == {"offset": 100}
        assert record.middleware == "dedup"
        assert record.sink == "postgres_sink"
        assert record.created_at == now
        assert record.original_record is None
        assert record.processed_record is None

    def test_record_is_frozen(self):
        record = _minimal_record()
        with pytest.raises((AttributeError, TypeError)):
            record.attempt = 99  # type: ignore[misc]

    def test_instantiation_without_new_fields_still_works(self):
        """Existing call sites that don't pass attempt/max_attempts must not break."""
        record = DLQRecord(
            pipeline_id="pipe",
            run_id="run",
            stage="sink",
            error_type="IOError",
            error_message="disk full",
            record={"data": 1},
        )
        assert record.attempt == 0
        assert record.max_attempts is None


# ---------------------------------------------------------------------------
# Combined: new + existing fields together
# ---------------------------------------------------------------------------


class TestFullRecord:
    def test_all_fields_set_correctly(self):
        now = datetime.now(UTC)
        record = DLQRecord(
            pipeline_id="pipeline_x",
            run_id="run_y",
            stage="transform",
            error_type="KeyError",
            error_message="missing field",
            record={"raw": "data"},
            source="csv_source",
            checkpoint=None,
            middleware="transform_mw",
            sink=None,
            created_at=now,
            attempt=2,
            max_attempts=10,
            original_record={"raw": "data"},
            processed_record={"normalized": True},
        )
        assert record.pipeline_id == "pipeline_x"
        assert record.run_id == "run_y"
        assert record.stage == "transform"
        assert record.error_type == "KeyError"
        assert record.error_message == "missing field"
        assert record.record == {"raw": "data"}
        assert record.source == "csv_source"
        assert record.checkpoint is None
        assert record.middleware == "transform_mw"
        assert record.sink is None
        assert record.created_at == now
        assert record.attempt == 2
        assert record.max_attempts == 10
        assert record.original_record == {"raw": "data"}
        assert record.processed_record == {"normalized": True}

    def test_replay_payload_prefers_original_record_for_pipeline_mode(self):
        record = DLQRecord(
            pipeline_id="pipeline_x",
            run_id="run_y",
            stage="sink_write",
            error_type="KeyError",
            error_message="missing field",
            record={"raw": "compat"},
            original_record={"raw": "source"},
            processed_record={"normalized": True},
        )
        assert record.replay_payload(mode="pipeline") == {"raw": "source"}

    def test_replay_payload_prefers_processed_record_for_sink_mode(self):
        record = DLQRecord(
            pipeline_id="pipeline_x",
            run_id="run_y",
            stage="sink_write",
            error_type="KeyError",
            error_message="missing field",
            record={"raw": "compat"},
            original_record={"raw": "source"},
            processed_record={"normalized": True},
        )
        assert record.replay_payload(mode="sink") == {"normalized": True}


# ---------------------------------------------------------------------------
# DLQSink.replay() default method — Requirements: 2.13
# ---------------------------------------------------------------------------


class TestDLQSinkReplay:
    """Tests for the default DLQSink.replay() implementation."""

    @pytest.mark.asyncio
    async def test_replay_increments_attempt(self):
        """replay() returns a new DLQRecord with attempt + 1."""
        from agora.core.dlq import DLQSink

        class _ConcreteDLQSink(DLQSink):
            sink_name = "test_dlq"

            async def open(self) -> None:
                pass

            async def write(self, record) -> None:
                pass

            async def flush(self) -> None:
                pass

            async def close(self) -> None:
                pass

        sink = _ConcreteDLQSink()
        record = _minimal_record(attempt=0)
        replayed = await sink.replay(record)
        assert replayed.attempt == 1

    @pytest.mark.asyncio
    async def test_replay_preserves_all_other_fields(self):
        """replay() preserves all fields except attempt."""
        from agora.core.dlq import DLQSink

        class _ConcreteDLQSink(DLQSink):
            sink_name = "test_dlq"

            async def open(self) -> None:
                pass

            async def write(self, record) -> None:
                pass

            async def flush(self) -> None:
                pass

            async def close(self) -> None:
                pass

        sink = _ConcreteDLQSink()
        now = datetime.now(UTC)
        record = DLQRecord(
            pipeline_id="pipe_x",
            run_id="run_y",
            stage="sink",
            error_type="IOError",
            error_message="disk full",
            record={"data": 42},
            source="kafka",
            checkpoint={"offset": 5},
            middleware="dedup",
            sink="postgres",
            created_at=now,
            attempt=2,
            max_attempts=5,
        )
        replayed = await sink.replay(record)

        assert replayed.attempt == 3
        assert replayed.pipeline_id == "pipe_x"
        assert replayed.run_id == "run_y"
        assert replayed.stage == "sink"
        assert replayed.error_type == "IOError"
        assert replayed.error_message == "disk full"
        assert replayed.record == {"data": 42}
        assert replayed.source == "kafka"
        assert replayed.checkpoint == {"offset": 5}
        assert replayed.middleware == "dedup"
        assert replayed.sink == "postgres"
        assert replayed.created_at == now
        assert replayed.max_attempts == 5
        assert replayed.original_record is None
        assert replayed.processed_record is None

    @pytest.mark.asyncio
    async def test_replay_returns_new_record_object(self):
        """replay() returns a new DLQRecord, not the same object."""
        from agora.core.dlq import DLQSink

        class _ConcreteDLQSink(DLQSink):
            sink_name = "test_dlq"

            async def open(self) -> None:
                pass

            async def write(self, record) -> None:
                pass

            async def flush(self) -> None:
                pass

            async def close(self) -> None:
                pass

        sink = _ConcreteDLQSink()
        record = _minimal_record(attempt=1)
        replayed = await sink.replay(record)
        assert replayed is not record

    @pytest.mark.asyncio
    async def test_replay_chaining_increments_correctly(self):
        """Calling replay() multiple times increments attempt each time."""
        from agora.core.dlq import DLQSink

        class _ConcreteDLQSink(DLQSink):
            sink_name = "test_dlq"

            async def open(self) -> None:
                pass

            async def write(self, record) -> None:
                pass

            async def flush(self) -> None:
                pass

            async def close(self) -> None:
                pass

        sink = _ConcreteDLQSink()
        record = _minimal_record(attempt=0)
        r1 = await sink.replay(record)
        r2 = await sink.replay(r1)
        r3 = await sink.replay(r2)
        assert r1.attempt == 1
        assert r2.attempt == 2
        assert r3.attempt == 3

    def test_replay_is_not_abstract(self):
        """DLQSink.replay() has a default implementation — subclasses don't need to override it."""
        from agora.core.dlq import DLQSink

        # replay should not be abstract
        assert not getattr(DLQSink.replay, "__isabstractmethod__", False)
        # and it should be defined directly on DLQSink
        assert "replay" in DLQSink.__dict__
