"""
tests/ai/test_ai_batch.py
==========================
Tests for AIBatchMiddleware.

See .claude/ai_layer.md for detailed execution flow diagrams.

Pattern used throughout
-----------------------
We create process() tasks, let the event loop run (either via gather or
on_stop drain), then collect results.  We intentionally avoid wall-clock
sleeps because they make tests fragile on slow CI machines.

Key behaviours tested
---------------------
- Timeout-based flush: on_stop drains records that didn't fill a batch.
- Size-based flush: asyncio.create_task(_flush_pending) fires before gather.
- output_fields filtering.
- Error passthrough on invalid JSON / length mismatch.
- Token metrics tracked via ctx.

Important: Why asyncio.sleep(0) in on_stop
-------------------------------------------
When on_stop() is called, there may be process() calls that have enqueued
records but haven't awaited their futures yet. sleep(0) yields control to
the event loop, allowing those tasks to reach the await point before we
drain the queue. Without it, we might drain before all records are enqueued,
leaving some futures unresolved.

See .claude/ai_layer.md#AIBatchMiddleware for full explanation.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from agora import IterableSource, Pipeline
from agora.ai.providers.base import CompletionResponse
from agora.core.context import PipelineContext
from agora.core.metrics import PipelineMetrics
from agora.core.source import BaseSource
from agora.middlewares.ai.batch import AIBatchMiddleware
from agora.middlewares.dedup.middleware import DedupMiddleware
from agora.middlewares.dedup.stores.memory import InMemoryStore
from agora.sinks.io.stdout import StdoutSink

# ======================================================================
# Helpers
# ======================================================================


def _make_ctx() -> PipelineContext:
    return PipelineContext(pipeline_id="test_batch", metrics=PipelineMetrics())


def _make_provider(response_list: list[dict]) -> MagicMock:
    provider = MagicMock()
    provider.model = "test-model"
    provider.complete = AsyncMock(
        return_value=CompletionResponse(
            content=json.dumps(response_list),
            model="test-model",
            input_tokens=50,
            output_tokens=20,
        )
    )
    return provider


def _make_mw(
    response_list: list[dict],
    batch_size: int = 20,
    output_fields: list[str] | None = None,
    flush_timeout_ms: int = 99_999,  # effectively never — rely on on_stop or size flush
) -> tuple[AIBatchMiddleware, MagicMock]:
    provider = _make_provider(response_list)
    mw = AIBatchMiddleware(
        provider=provider,
        prompt_fn=lambda records: json.dumps(records),
        output_fields=output_fields,
        batch_size=batch_size,
        flush_timeout_ms=flush_timeout_ms,
    )
    return mw, provider


# ======================================================================
# Basic: on_stop drains partial batch and resolves futures
# ======================================================================


@pytest.mark.asyncio
async def test_on_stop_drains_and_merges_results():
    """on_stop flushes records that didn't reach batch_size."""
    records = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}, {"id": 3, "name": "C"}]
    enrichments = [{"summary": "S1"}, {"summary": "S2"}, {"summary": "S3"}]
    mw, _ = _make_mw(enrichments, batch_size=50)
    ctx = _make_ctx()

    await mw.on_start(ctx)
    try:
        tasks = [asyncio.create_task(mw.process(r, ctx)) for r in records]
        # on_stop yields once (sleep(0)) so tasks can enqueue, then drains
        await mw.on_stop(ctx)
        results = await asyncio.gather(*tasks)
    finally:
        pass  # on_stop already called

    assert len(results) == 3
    for i, r in enumerate(results):
        assert r["summary"] == f"S{i + 1}"
        assert r["id"] == i + 1  # original fields preserved


@pytest.mark.asyncio
async def test_output_fields_filter():
    """output_fields restricts which enrichment keys are merged."""
    records = [{"id": 1}]
    enrichments = [{"summary": "ok", "price": "$$", "_internal": "skip"}]
    mw, _ = _make_mw(enrichments, batch_size=50, output_fields=["summary", "price"])
    ctx = _make_ctx()

    await mw.on_start(ctx)
    tasks = [asyncio.create_task(mw.process(r, ctx)) for r in records]
    await mw.on_stop(ctx)
    results = await asyncio.gather(*tasks)

    assert "summary" in results[0]
    assert "price" in results[0]
    assert "_internal" not in results[0]


# ======================================================================
# Size-based flush: batch_size records → immediate flush task
# ======================================================================


@pytest.mark.asyncio
async def test_size_based_flush():
    """Exactly batch_size records queued → flush triggered by size, not timeout."""
    n = 4
    records = [{"id": i} for i in range(n)]
    enrichments = [{"tag": f"t{i}"} for i in range(n)]
    mw, provider = _make_mw(enrichments, batch_size=n, flush_timeout_ms=99_999)
    ctx = _make_ctx()

    await mw.on_start(ctx)
    try:
        tasks = [asyncio.create_task(mw.process(r, ctx)) for r in records]
        # gather yields so the _flush_pending background task can run
        results = await asyncio.gather(*tasks)
    finally:
        await mw.on_stop(ctx)

    provider.complete.assert_called_once()
    assert len(results) == n
    assert all(r["tag"] == f"t{i}" for i, r in enumerate(results))


# ======================================================================
# Error handling: passthrough on invalid JSON
# ======================================================================


@pytest.mark.asyncio
async def test_passthrough_on_invalid_json():
    """Bad LLM response → all records pass through unchanged."""
    provider = MagicMock()
    provider.model = "test"
    provider.complete = AsyncMock(
        return_value=CompletionResponse(
            content="not json", model="test", input_tokens=1, output_tokens=1
        )
    )
    mw = AIBatchMiddleware(
        provider=provider,
        prompt_fn=lambda r: "...",
        batch_size=50,
        flush_timeout_ms=99_999,
        on_error="passthrough",
    )
    ctx = _make_ctx()
    original = {"id": 42, "name": "Place"}

    await mw.on_start(ctx)
    tasks = [asyncio.create_task(mw.process(original, ctx))]
    await mw.on_stop(ctx)
    results = await asyncio.gather(*tasks)

    assert results[0] == original


@pytest.mark.asyncio
async def test_passthrough_on_length_mismatch():
    """LLM returns wrong count → all records pass through unchanged."""
    provider = MagicMock()
    provider.model = "test"
    # 1 result for 2 records
    provider.complete = AsyncMock(
        return_value=CompletionResponse(
            content='[{"summary": "only one"}]',
            model="test",
            input_tokens=5,
            output_tokens=5,
        )
    )
    mw = AIBatchMiddleware(
        provider=provider,
        prompt_fn=lambda r: json.dumps(r),
        batch_size=50,
        flush_timeout_ms=99_999,
    )
    ctx = _make_ctx()
    records = [{"id": 1}, {"id": 2}]

    await mw.on_start(ctx)
    tasks = [asyncio.create_task(mw.process(r, ctx)) for r in records]
    await mw.on_stop(ctx)
    results = await asyncio.gather(*tasks)

    assert results[0]["id"] == 1
    assert results[1]["id"] == 2


# ======================================================================
# Metrics: tokens tracked via ctx
# ======================================================================


@pytest.mark.asyncio
async def test_tracks_tokens_in_metrics():
    """LLM tokens are recorded in ctx.metrics after a batch call."""
    records = [{"id": 1}]
    enrichments = [{"summary": "ok"}]
    mw, _ = _make_mw(enrichments, batch_size=50)
    ctx = _make_ctx()

    await mw.on_start(ctx)
    tasks = [asyncio.create_task(mw.process(r, ctx)) for r in records]
    await mw.on_stop(ctx)
    await asyncio.gather(*tasks)

    ai_m = ctx.metrics.middleware(mw.name).ai
    assert ai_m.llm_calls == 1
    assert ai_m.input_tokens == 50
    assert ai_m.output_tokens == 20


# ======================================================================
# Multiple batches: more records than batch_size
# ======================================================================


@pytest.mark.asyncio
async def test_multiple_batches():
    """Records > batch_size are split across multiple LLM calls."""
    n = 6
    batch_size = 2
    records = [{"id": i} for i in range(n)]
    # provider is called multiple times; each call returns 2-item list
    provider = MagicMock()
    provider.model = "test"
    provider.complete = AsyncMock(
        side_effect=[
            CompletionResponse(
                content=json.dumps([{"tag": f"t{i}"}, {"tag": f"t{i + 1}"}]),
                model="test",
                input_tokens=10,
                output_tokens=5,
            )
            for i in range(0, n, batch_size)
        ]
    )
    mw = AIBatchMiddleware(
        provider=provider,
        prompt_fn=lambda r: json.dumps(r),
        batch_size=batch_size,
        flush_timeout_ms=99_999,
    )
    ctx = _make_ctx()

    await mw.on_start(ctx)
    try:
        tasks = [asyncio.create_task(mw.process(r, ctx)) for r in records]
        results = await asyncio.gather(*tasks)
    finally:
        await mw.on_stop(ctx)

    assert len(results) == n
    assert provider.complete.call_count == n // batch_size


@pytest.mark.asyncio
async def test_batches_when_used_via_pipeline_runner():
    """Pipeline.run() should keep enough records in flight for real batching."""
    enrichments = [{"tag": "a"}, {"tag": "b"}, {"tag": "c"}]
    provider = _make_provider(enrichments)
    mw = AIBatchMiddleware(
        provider=provider,
        prompt_fn=lambda records: json.dumps(records),
        batch_size=3,
        flush_timeout_ms=99_999,
    )

    summary = await (
        Pipeline(IterableSource([{"id": 1}, {"id": 2}, {"id": 3}]))
        .pipe(mw)
        .build(StdoutSink())
        .run()
    )

    provider.complete.assert_called_once()
    assert summary.records_consumed == 3
    assert summary.records_written == 3


@pytest.mark.asyncio
async def test_pipeline_runner_flushes_partial_batch_at_end_of_stream():
    provider = _make_provider([{"tag": "done"}])
    summary = await (
        Pipeline(IterableSource([{"id": 1}]))
        .pipe(
            AIBatchMiddleware(
                provider=provider,
                prompt_fn=lambda records: json.dumps(records),
                batch_size=2,
                flush_timeout_ms=99_999,
            )
        )
        .build(StdoutSink())
        .run()
    )

    provider.complete.assert_called_once()
    assert summary.records_written == 1


@pytest.mark.asyncio
async def test_pipeline_runner_flushes_buffered_records_before_reraising_source_error():
    provider = _make_provider([{"tag": "done"}])

    class FailingSource(BaseSource[dict]):
        async def stream(self):
            yield {"id": 1}
            raise RuntimeError("boom-source")

    class CollectSink:
        sink_name = "collect"

        def __init__(self) -> None:
            self.items: list[dict] = []

        async def open(self) -> None:
            pass

        async def write(self, record):
            self.items.append(record)

        async def flush(self):
            pass

        async def close(self):
            pass

    sink = CollectSink()
    pipeline = (
        Pipeline(FailingSource())
        .pipe(
            AIBatchMiddleware(
                provider=provider,
                prompt_fn=lambda records: json.dumps(records),
                batch_size=2,
                flush_timeout_ms=99_999,
            )
        )
        .build(sink)  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="boom-source"):
        await pipeline.run()

    assert sink.items == [{"id": 1, "tag": "done"}]
    provider.complete.assert_called_once()


@pytest.mark.asyncio
async def test_dedup_before_batch_drops_duplicates_without_hanging():
    provider = _make_provider([{"tag": "done"}])
    summary = await asyncio.wait_for(
        (
            Pipeline(IterableSource(["a", "a"]))
            .pipe(DedupMiddleware(key=lambda x: x, store=InMemoryStore()))
            .pipe(
                AIBatchMiddleware(
                    provider=provider,
                    prompt_fn=lambda records: json.dumps(records),
                    batch_size=2,
                    flush_timeout_ms=99_999,
                )
            )
            .build(StdoutSink())
            .run()
        ),
        timeout=1.0,
    )

    provider.complete.assert_called_once()
    assert summary.records_written == 1
    assert summary.records_dropped == 1
