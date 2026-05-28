# Testing Pipelines

_When to read this: you want to write fast, deterministic tests for your pipeline logic without hitting real databases, files, or external APIs._

## pytest-asyncio setup

Pipelines are async. Add this to your `conftest.py` or `pyproject.toml` once and forget it:

```python
# conftest.py
import pytest

pytest.ini_options = {"asyncio_mode": "auto"}
```

Or in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
```

All test functions marked `async def` will run under asyncio automatically.

## IterableSource for test data

The simplest way to feed known records into a pipeline is `IterableSource`. It wraps any Python iterable and streams it as pipeline records.

```python
from agora.sources.iterable import IterableSource

records = [
    {"id": 1, "name": "Alice", "score": 95},
    {"id": 2, "name": "Bob",   "score": 42},
    {"id": 3, "name": "Carol", "score": 78},
]

source = IterableSource(records, source_name="test_records")
```

Use this instead of pointing tests at real files or APIs. It keeps tests fast and removes external dependencies.

## CollectSink: capturing output

Write a small sink that collects records into a list. This is the standard pattern for asserting what a pipeline produced:

```python
from agora.core.sink import BaseSink

class CollectSink(BaseSink[dict]):
    sink_name = "collect"

    def __init__(self) -> None:
        self.records: list[dict] = []

    async def write(self, record: dict) -> None:
        self.records.append(record)
```

Wire it into a pipeline and inspect `sink.records` after the run:

```python
async def test_score_filter():
    source = IterableSource([
        {"id": 1, "score": 95},
        {"id": 2, "score": 42},
        {"id": 3, "score": 78},
    ])
    sink = CollectSink()

    pipeline = (
        Pipeline(source, id="test")
        .pipe(FilterMiddleware(lambda r: r["score"] >= 70))
        .build(sink)
    )
    await pipeline.run()

    assert len(sink.records) == 2
    assert sink.records[0]["id"] == 1
    assert sink.records[1]["id"] == 3
```

## Testing middleware in isolation

You don't always need a full pipeline to test a middleware. If your middleware is a pure transformation, test it directly:

```python
from agora.core.middleware import Middleware

class UppercaseMiddleware(Middleware):
    async def process(self, record: dict) -> dict | None:
        return {k: v.upper() if isinstance(v, str) else v for k, v in record.items()}


async def test_uppercase_middleware():
    mw = UppercaseMiddleware()
    result = await mw.process({"name": "alice", "score": 42})
    assert result == {"name": "ALICE", "score": 42}


async def test_uppercase_middleware_passthrough_none():
    mw = UppercaseMiddleware()
    # Simulate a filter: return None to drop the record
    result = await mw.process({"name": "", "score": 0})
    # depends on your logic — assert accordingly
    assert result is not None
```

For middlewares with side effects (DB lookups, API calls), inject a mock or a fake at construction time rather than patching globals.

## Testing with DLQ

When a pipeline has a dead-letter queue configured, failed records go to the DLQ sink instead of stopping the pipeline. Use `CollectSink` as the DLQ sink to assert what ended up there:

```python
async def test_bad_records_go_to_dlq():
    source = IterableSource([
        {"id": 1, "value": "good"},
        {"id": 2, "value": None},   # will fail validation
        {"id": 3, "value": "good"},
    ])
    output = CollectSink()
    dlq = CollectSink()

    pipeline = (
        Pipeline(source, id="test")
        .pipe(ValidateMiddleware(schema=MySchema))
        .build(output, dlq=dlq)
    )
    await pipeline.run()

    assert len(output.records) == 2
    assert len(dlq.records) == 1
    assert dlq.records[0].record["id"] == 2
```

The DLQ sink receives the original record as it was before the failure, not a wrapped error object. Assert on the record fields directly.

## Testing checkpointing: verify resume behavior

To test that a pipeline resumes correctly, run it twice against the same `InMemoryCheckpointStore` and verify the second run starts from the right position.

```python
from agora.core.checkpoint import InMemoryCheckpointStore


class CountingSource:
    """Source that records which offsets it streamed."""
    source_name = "counting"
    supports_checkpoint = True

    def __init__(self, records: list[dict]) -> None:
        self._records = records
        self._offset = 0
        self.streamed_from: list[int] = []

    def current_checkpoint(self):
        return {"offset": self._offset}

    async def prepare_resume(self, checkpoint) -> None:
        if checkpoint is not None and isinstance(checkpoint.value, dict):
            self._offset = checkpoint.value.get("offset", 0)

    async def stream(self):
        self.streamed_from.append(self._offset)
        for record in self._records[self._offset:]:
            self._offset += 1
            yield record


async def test_checkpoint_resume():
    store = InMemoryCheckpointStore()
    records = [{"id": i} for i in range(10)]

    source = CountingSource(records)
    sink = CollectSink()

    # First run: process all 10, checkpoint every 5
    pipeline = (
        Pipeline(source, id="test")
        .build(sink, checkpoint=store, checkpoint_key="test:counting", checkpoint_every=5)
    )
    await pipeline.run()
    assert len(sink.records) == 10

    # Second run: should resume from offset 10 (end), nothing to process
    source2 = CountingSource(records)
    sink2 = CollectSink()
    pipeline2 = (
        Pipeline(source2, id="test")
        .build(sink2, checkpoint=store, checkpoint_key="test:counting", checkpoint_every=5)
    )
    await pipeline2.run()

    assert source2.streamed_from[0] == 10  # resumed from end
    assert len(sink2.records) == 0
```

Use `InMemoryCheckpointStore` in tests — it's fast and requires no cleanup. Only use `SQLiteCheckpointStore` in tests that specifically verify persistence across process restarts (rare).

## Asserting on PipelineRunSummary

`pipeline.run()` returns a `PipelineRunSummary`. Use it to assert counts without inspecting the sink:

```python
async def test_summary_counts():
    source = IterableSource([
        {"id": 1, "active": True},
        {"id": 2, "active": False},
        {"id": 3, "active": True},
    ])
    sink = CollectSink()

    pipeline = (
        Pipeline(source, id="test")
        .pipe(FilterMiddleware(lambda r: r["active"]))
        .build(sink)
    )
    summary = await pipeline.run()

    assert summary.records_consumed == 3
    assert summary.records_written == 2
    assert summary.records_dropped == 1
    assert summary.records_errored == 0
```

`records_consumed` is what the source emitted. `records_written` is what reached the sink. The difference is what middleware dropped or errored. If these numbers don't add up, something in your middleware chain is silently swallowing records.
