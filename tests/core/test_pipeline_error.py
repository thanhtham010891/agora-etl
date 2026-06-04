"""
tests/core/test_pipeline_error.py
===================================
Tests for enriched PipelineError (P0-05).

Coverage:
- PipelineError carries all context fields
- __str__ includes context fields
- with_context() composes partial enrichment correctly
- Executor wraps terminal errors with pipeline/run/stage/source context
- Original exception is preserved via __cause__
- Non-error runs return summary normally (no regression)
"""

from __future__ import annotations

from typing import Any

import pytest

from agora import IterableSource, Pipeline
from agora.core.errors import PipelineError
from agora.core.source import BaseSource

# ======================================================================
# PipelineError unit tests
# ======================================================================


def test_pipeline_error_stores_context_fields() -> None:
    err = PipelineError(
        "something failed",
        pipeline_id="my_pipeline",
        run_id="run-123",
        stage="sink",
        source_name="kafka_source",
        sink_name="postgres_sink",
        checkpoint={"offset": 42},
    )
    assert err.pipeline_id == "my_pipeline"
    assert err.run_id == "run-123"
    assert err.stage == "sink"
    assert err.source_name == "kafka_source"
    assert err.sink_name == "postgres_sink"
    assert err.checkpoint == {"offset": 42}


def test_pipeline_error_str_includes_context() -> None:
    err = PipelineError(
        "boom",
        pipeline_id="p1",
        stage="middleware",
        source_name="src",
    )
    s = str(err)
    assert "boom" in s
    assert "p1" in s
    assert "middleware" in s
    assert "src" in s


def test_pipeline_error_str_no_context_is_plain() -> None:
    err = PipelineError("plain error")
    assert str(err) == "plain error"


def test_pipeline_error_with_context_merges_fields() -> None:
    original = PipelineError("base", pipeline_id="p1", stage="source_stream")
    enriched = original.with_context(run_id="r1", stage="sink")
    assert enriched.pipeline_id == "p1"  # preserved
    assert enriched.run_id == "r1"  # added
    assert enriched.stage == "sink"  # overwritten


def test_pipeline_error_with_context_does_not_mutate_original() -> None:
    original = PipelineError("base", pipeline_id="p1")
    original.with_context(pipeline_id="p2")
    assert original.pipeline_id == "p1"


def test_pipeline_error_default_fields_are_none() -> None:
    err = PipelineError("x")
    assert err.pipeline_id is None
    assert err.run_id is None
    assert err.stage is None
    assert err.source_name is None
    assert err.sink_name is None
    assert err.checkpoint is None


# ======================================================================
# Executor integration — PipelineError raised on failure
# ======================================================================


class _ExplodingSource(BaseSource[int]):
    source_name = "exploding_source"

    async def stream(self):  # type: ignore[override]
        yield 1
        raise ValueError("deliberate source failure")


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


@pytest.mark.asyncio
async def test_executor_raises_pipeline_error_with_context() -> None:
    """PipelineError raised inside a pipeline is enriched with runtime context."""

    class _PipelineErrorSource(BaseSource[int]):
        source_name = "pe_source"

        async def stream(self):  # type: ignore[override]
            yield 1
            raise PipelineError("deliberate pipeline error")

    sink = _CollectSink()
    with pytest.raises(PipelineError) as exc_info:
        await Pipeline(_PipelineErrorSource(), id="test_pipe").build(sink).run()
    err = exc_info.value
    assert err.pipeline_id == "test_pipe"
    assert err.run_id is not None
    assert err.source_name == "pe_source"
    assert err.stage is not None


@pytest.mark.asyncio
async def test_executor_pipeline_error_preserves_original_cause() -> None:
    """PipelineError.with_context() preserves __cause__ chain."""
    cause = ValueError("root cause")

    class _CauseSource(BaseSource[int]):
        source_name = "cause_src"

        async def stream(self):  # type: ignore[override]
            yield 1
            raise PipelineError("wrapped") from cause

    sink = _CollectSink()
    with pytest.raises(PipelineError) as exc_info:
        await Pipeline(_CauseSource(), id="cause_test").build(sink).run()
    err = exc_info.value
    assert err.pipeline_id == "cause_test"


@pytest.mark.asyncio
async def test_executor_non_pipeline_error_propagates_unchanged() -> None:
    """Plain ValueError from a source is not wrapped — propagates as-is."""
    sink = _CollectSink()
    with pytest.raises(ValueError, match="deliberate source failure"):
        await Pipeline(_ExplodingSource(), id="raw_err_test").build(sink).run()


@pytest.mark.asyncio
async def test_executor_successful_run_returns_summary() -> None:
    """Regression: successful runs must not raise PipelineError."""
    sink = _CollectSink()
    summary = await Pipeline(IterableSource([1, 2, 3]), id="ok_pipe").build(sink).run()
    assert summary.records_written == 3


@pytest.mark.asyncio
async def test_pipeline_error_message_includes_original_message() -> None:
    """PipelineError raised by pipeline code keeps its message after enrichment."""

    class _MsgSource(BaseSource[int]):
        source_name = "msg_src"

        async def stream(self):  # type: ignore[override]
            yield 1
            raise PipelineError("custom pipeline failure message")

    sink = _CollectSink()
    with pytest.raises(PipelineError) as exc_info:
        await Pipeline(_MsgSource(), id="msg_test").build(sink).run()
    assert "custom pipeline failure message" in str(exc_info.value)
