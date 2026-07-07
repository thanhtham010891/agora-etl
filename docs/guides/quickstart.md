# Quickstart

_When to read this: you're new to agora and want a pipeline running in under 5 minutes._

## Install

```bash
pip install agora-etl
```

## Write your first pipeline

A pipeline has three parts: a source that emits records, zero or more middlewares that transform them, and a sink that persists them. Here's the smallest possible example — reading from an in-memory list and printing to stdout:

```python
import asyncio
from agora import IterableSource, Pipeline

records = [
    {"id": 1, "name": "Ho Chi Minh City", "country": "VN"},
    {"id": 2, "name": "Hanoi",            "country": "VN"},
    {"id": 3, "name": "Da Nang",          "country": "VN"},
]

async def main() -> None:
    summary = await (
        Pipeline(IterableSource(records))
        .build()  # no sink = StdoutSink
        .run()
    )
    print(summary)

asyncio.run(main())
```

`build()` with no arguments defaults to `StdoutSink`. That's enough to verify the pipeline runs.

## Add a transformation and a real sink

Real pipelines transform records before writing them. Subclass `Middleware` to do that, and subclass `BaseSink` to write somewhere useful:

```python
import asyncio
from dataclasses import dataclass

from agora import IterableSource, Middleware, Pipeline
from agora.core.sink import BaseSink

# --- domain types ---

@dataclass
class RawCity:
    id: int
    name: str
    country: str

@dataclass
class City:
    id: int
    name: str
    country: str
    slug: str

# --- middleware ---

class SlugMiddleware(Middleware[RawCity, City]):
    name = "slugify"

    async def process(self, record: RawCity, ctx) -> City:
        return City(
            id=record.id,
            name=record.name,
            country=record.country,
            slug=record.name.lower().replace(" ", "-"),
        )

# --- sink ---

class MemorySink(BaseSink[City]):
    sink_name = "memory"

    def __init__(self) -> None:
        self.rows: list[City] = []

    async def write(self, record: City) -> None:
        self.rows.append(record)

# --- pipeline ---

async def main() -> None:
    raw = [
        RawCity(1, "Ho Chi Minh City", "VN"),
        RawCity(2, "Hanoi", "VN"),
        RawCity(3, "Da Nang", "VN"),
    ]

    sink = MemorySink()

    summary = await (
        Pipeline(IterableSource(raw))
        .pipe(SlugMiddleware())
        .filter(lambda c: c.country == "VN")
        .build(sink)
        .run()
    )

    print(summary)
    # PipelineRunSummary(consumed=3, written=3, dropped=0, errors=0, elapsed=0.0s)

    for city in sink.rows:
        print(city.slug)
    # ho-chi-minh-city
    # hanoi
    # da-nang

asyncio.run(main())
```

A few things to notice:

- `.pipe()` and `.filter()` return a new `Pipeline` — the builder is immutable, so you can branch from a shared base.
- `.filter()` is shorthand for `.pipe(FilterMiddleware(...))`. Use it when you just need a predicate.
- `.build(sink)` locks in the sink and returns a `BoundPipeline`. Nothing runs until you call `.run()`.
- `.run()` returns a `PipelineRunSummary` with counts for consumed, written, dropped, and errored records plus elapsed time.

## Run it

```bash
python pipeline.py
```

No CLI, no config files, no daemon. It's just Python.

## Common starting points

When moving beyond the in-memory quickstart, start from the doc that
matches the system boundary:

- [Kafka plugin](../plugins/kafka.md) for topic consumer or topic-to-topic flows
- [PostgreSQL plugin](../plugins/postgresql.md) for incremental extracts and relational loads
- [Redis plugin](../plugins/redis.md) for Redis Streams, shared state, and replay
- [ArrowProcessBatchMiddleware](../middleware/arrow-process-batch.md) for columnar process-isolated transforms
- [Sources](../source/index.md) for file readers such as `CsvSource`, `JsonLinesSource`, and `ArrowCsvSource`

## Next steps

- [Composing pipelines](pipelines.md) — fan-out, routing, batching, backpressure
- [Handling failures](failure-handling.md) — DLQ, retry, sink failure policies
- [Sources](../source/index.md) — file, HTTP polling, DLQ replay, and custom sources
