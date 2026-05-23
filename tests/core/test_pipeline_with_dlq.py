"""
Tests for Pipeline.with_dlq() immutability.

Validates: Requirements 2.11, 2.12, 3.13, 3.14

Verifies:
- Pipeline.from_source(src).with_dlq(sink) returns a new BoundPipeline object
- The returned BoundPipeline has _dlq_sink set correctly
- No intermediate object is mutated (the Pipeline builder itself is unchanged)
- Chaining with_dlq() then other methods works correctly
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from agora import IterableSource, Pipeline
from agora.core.pipeline import BoundPipeline
from agora.core.types import DLQFailurePolicy

if TYPE_CHECKING:
    from agora.core.dlq import DLQRecord


# ======================================================================
# Minimal DLQ sink fixture
# ======================================================================


class _CollectDLQSink:
    sink_name = "collect_dlq"

    def __init__(self) -> None:
        self.records: list[DLQRecord] = []

    async def open(self) -> None:
        return None

    async def write(self, record: DLQRecord) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


# ======================================================================
# Return type tests
# ======================================================================


def test_pipeline_with_dlq_returns_bound_pipeline():
    """Pipeline(src).build(dlq=sink) must return a BoundPipeline."""
    src = IterableSource([1, 2, 3])
    dlq_sink = _CollectDLQSink()

    result = Pipeline(src).build(dlq=dlq_sink)  # type: ignore[arg-type]

    assert isinstance(result, BoundPipeline)


def test_pipeline_with_dlq_sets_dlq_sink_correctly():
    """The returned BoundPipeline must have _dlq_sink set to the provided sink."""
    src = IterableSource([1, 2, 3])
    dlq_sink = _CollectDLQSink()

    result = Pipeline(src).build(dlq=dlq_sink)  # type: ignore[arg-type]

    assert result._dlq_sink is dlq_sink


def test_pipeline_with_dlq_sets_failure_policy_default():
    """Default dlq_failure_policy must be DLQFailurePolicy.LOG_ONLY."""
    src = IterableSource([])
    dlq_sink = _CollectDLQSink()

    result = Pipeline(src).build(dlq=dlq_sink)  # type: ignore[arg-type]

    assert result._dlq_failure_policy == DLQFailurePolicy.LOG_ONLY


def test_pipeline_with_dlq_sets_custom_failure_policy():
    """Custom dlq_failure_policy must be propagated to the returned BoundPipeline."""
    src = IterableSource([])
    dlq_sink = _CollectDLQSink()

    result = Pipeline(src).build(
        dlq=dlq_sink,  # type: ignore[arg-type]
        dlq_failure_policy=DLQFailurePolicy.RAISE,
    )

    assert result._dlq_failure_policy == DLQFailurePolicy.RAISE


# ======================================================================
# Immutability tests — Pipeline builder must not be mutated
# ======================================================================


def test_pipeline_builder_not_mutated_after_with_dlq():
    """Calling build(dlq=...) on a Pipeline must not mutate the Pipeline builder."""
    src = IterableSource([1, 2, 3])
    pipeline = Pipeline(src)

    # Capture state before
    middlewares_before = list(pipeline._middlewares)
    pipeline_id_before = pipeline._pipeline_id

    # Call build with dlq
    pipeline.build(dlq=_CollectDLQSink())  # type: ignore[arg-type]

    # Pipeline builder state must be unchanged
    assert pipeline._middlewares == middlewares_before
    assert pipeline._pipeline_id == pipeline_id_before


def test_pipeline_with_dlq_returns_new_object_not_same_as_build():
    """build(dlq=...) must return a new BoundPipeline, not the same object as build()."""
    src = IterableSource([])
    pipeline = Pipeline(src)
    dlq_sink = _CollectDLQSink()

    bound = pipeline.build()
    with_dlq_result = pipeline.build(dlq=dlq_sink)  # type: ignore[arg-type]

    # Must be different objects
    assert with_dlq_result is not bound


def test_pipeline_with_dlq_does_not_mutate_intermediate_build():
    """build(dlq=...) must not mutate any intermediate BoundPipeline created by build()."""
    src = IterableSource([])
    pipeline = Pipeline(src)
    dlq_sink = _CollectDLQSink()

    # Build a BoundPipeline first
    intermediate = pipeline.build()
    assert intermediate._dlq_sink is None  # no DLQ yet

    # Now call build with dlq — should not affect the intermediate object
    result = pipeline.build(dlq=dlq_sink)  # type: ignore[arg-type]

    # The intermediate BoundPipeline must remain unmodified
    assert intermediate._dlq_sink is None
    # The result must have the DLQ set
    assert result._dlq_sink is dlq_sink


def test_multiple_with_dlq_calls_are_independent():
    """Multiple build(dlq=...) calls on the same Pipeline must produce independent BoundPipelines."""
    src = IterableSource([])
    pipeline = Pipeline(src)
    dlq_sink_a = _CollectDLQSink()
    dlq_sink_b = _CollectDLQSink()

    result_a = pipeline.build(dlq=dlq_sink_a)  # type: ignore[arg-type]
    result_b = pipeline.build(dlq=dlq_sink_b)  # type: ignore[arg-type]

    assert result_a is not result_b
    assert result_a._dlq_sink is dlq_sink_a
    assert result_b._dlq_sink is dlq_sink_b


# ======================================================================
# Chaining tests — methods after with_dlq() must work correctly
# ======================================================================


def test_chaining_with_checkpoint_store_after_with_dlq():
    """Building with both dlq and checkpoint must return a correct BoundPipeline."""
    from agora import InMemoryCheckpointStore

    src = IterableSource([])
    dlq_sink = _CollectDLQSink()
    store = InMemoryCheckpointStore()

    result = Pipeline(src).build(
        dlq=dlq_sink,  # type: ignore[arg-type]
        checkpoint=store,
    )

    assert isinstance(result, BoundPipeline)
    assert result._dlq_sink is dlq_sink
    assert result._checkpoint_store is store


def test_chaining_with_sink_after_with_dlq():
    """Calling with_sink() after build(dlq=...) must return a new BoundPipeline."""
    from agora.sinks.io.stdout import StdoutSink

    src = IterableSource([])
    dlq_sink = _CollectDLQSink()
    new_sink = StdoutSink()

    result = (
        Pipeline(src)
        .build(dlq=dlq_sink)  # type: ignore[arg-type]
        .with_sink(new_sink)
    )

    assert isinstance(result, BoundPipeline)
    assert result._dlq_sink is dlq_sink


def test_chaining_with_dlq_after_with_dlq_replaces_sink():
    """Building two pipelines with different DLQ sinks must produce independent BoundPipelines."""
    src = IterableSource([])
    dlq_sink_a = _CollectDLQSink()
    dlq_sink_b = _CollectDLQSink()

    first = Pipeline(src).build(dlq=dlq_sink_a)  # type: ignore[arg-type]
    second = Pipeline(src).build(dlq=dlq_sink_b)  # type: ignore[arg-type]

    # first must have sink_a
    assert first._dlq_sink is dlq_sink_a
    # second must have sink_b
    assert second._dlq_sink is dlq_sink_b
    # they must be different objects
    assert first is not second


# ======================================================================
# End-to-end functional test — with_dlq() pipeline actually runs
# ======================================================================


@pytest.mark.asyncio
async def test_pipeline_with_dlq_runs_correctly():
    """A pipeline built with dlq= must run and route errors to DLQ."""
    from agora.core.middleware import Middleware

    class _BoomMiddleware(Middleware[int, int]):
        name = "boom"

        async def process(self, record: int, ctx) -> int | None:
            raise RuntimeError("boom")

    dlq_sink = _CollectDLQSink()

    summary = await (
        Pipeline(IterableSource([1, 2, 3]))
        .pipe(_BoomMiddleware())
        .build(dlq=dlq_sink)  # type: ignore[arg-type]
        .run()
    )

    assert summary.records_errored == 3
    assert len(dlq_sink.records) == 3
    assert all(r.stage == "middleware" for r in dlq_sink.records)
