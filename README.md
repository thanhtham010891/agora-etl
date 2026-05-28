# Agora ETL Framework

**Async-first ETL framework for Python.**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
![Python](https://img.shields.io/badge/python-3.11%20|%203.12%20|%203.13-blue)
[![PyPI](https://img.shields.io/pypi/v/agora-etl)](https://pypi.org/project/agora-etl/)

---

## Overview

agora-etl is a Python async ETL framework built around a `Source → Middleware chain → Sink(s)` model.

- **Source** emits records one at a time via an async generator
- **Middleware chain** transforms, filters, validates, or enriches each record
- **Sink** persists records to a destination

The pipeline builder is immutable — every `.pipe()` and `.filter()` returns a new instance. Calling `.build()` produces a runnable `BoundPipeline`. The framework handles checkpointing, dead-letter queues, retries, backpressure, and long-running scheduled workers so you can focus on the transformation logic.

---

## Install

```bash
pip install agora-etl                  # core only
pip install "agora-etl[file]"          # + Parquet support and faster JSONL
```

## Quick start

Minimal pipeline — filter and print:

```python
import asyncio
from dataclasses import dataclass
from agora import Pipeline, IterableSource
from agora.sinks.io.stdout import StdoutSink

@dataclass
class Event:
    id: int
    name: str
    score: float

async def main():
    summary = await (
        Pipeline(IterableSource([
            Event(id=1, name="alice", score=0.9),
            Event(id=2, name="bob",   score=0.4),
            Event(id=3, name="carol", score=0.85),
        ]))
        .filter(lambda r: r.score > 0.8)
        .build(StdoutSink())
        .run()
    )
    print(f"written={summary.records_written}  dropped={summary.records_dropped}")

asyncio.run(main())
```

With DLQ — failed records are captured, pipeline continues:

```python
import asyncio
from agora import Pipeline, IterableSource
from agora.core.middleware import Middleware
from agora.core.dlq import SQLiteDLQSink
from agora.sinks.io.stdout import StdoutSink

class EnrichMiddleware(Middleware[dict, dict]):
    name = "enrich"

    async def process(self, record: dict, ctx) -> dict | None:
        if record.get("value") is None:
            raise ValueError(f"missing value on record {record['id']}")
        return {**record, "value": record["value"].upper()}

async def main():
    summary = await (
        Pipeline(IterableSource([
            {"id": 1, "value": "hello"},
            {"id": 2, "value": None},    # will fail → goes to DLQ
            {"id": 3, "value": "world"},
        ]))
        .pipe(EnrichMiddleware())
        .build(
            StdoutSink(),
            dlq=SQLiteDLQSink(".dlq.db"),
        )
        .run()
    )
    print(f"written={summary.records_written}  errored={summary.records_errored}")
    print(f"run_id={summary.run_id}")
    # written=2  errored=1
    # run_id=<uuid> — use this to query .dlq.db: SELECT * FROM dlq_records WHERE run_id='...'

asyncio.run(main())
```

The `middleware_error` log lines are expected — they confirm record id=2 was caught and routed to the DLQ. The pipeline completed normally.

Or scaffold a project:

```bash
agora new my-pipeline
cd my-pipeline
agora run pipelines.example
```

---

## Documentation

- [Quickstart](docs/guides/quickstart.md)
- [Pipelines](docs/guides/pipelines.md)
- [Failure Handling](docs/guides/failure-handling.md)
- [Checkpointing](docs/guides/checkpointing.md)
- [Scheduling](docs/guides/scheduling.md)
- [Testing](docs/guides/testing.md)
- [Observability](docs/guides/observability.md)
- [Sources](docs/sources.md) · [Sinks](docs/sinks.md) · [Middlewares](docs/middlewares.md)
- [Architecture](docs/architecture.md)
- [CLI](docs/cli.md)
- [Plugins](docs/plugins/index.md)
- [Configuration](docs/configuration.md)

---

## Built-in components

**Sources**

| Component | Description |
|---|---|
| `JsonLinesSource` | Stream records from a JSONL file |
| `CsvSource` | Stream records from a CSV file |
| `ParquetSource` | Stream records from a Parquet file (`[file]` extra) |
| `HTTPSource` | Abstract base for HTTP polling sources |

**Sinks**

| Component | Description |
|---|---|
| `JsonLinesSink` | Write records as JSONL |
| `CsvSink` | Write records as CSV |
| `ParquetSink` | Write records to Parquet (`[file]` extra) |
| `WebhookSink` | POST records to an HTTP endpoint |
| `StdoutSink` | Print records to stdout |
| `LogSink` | Emit records via the structured logger |

**Middlewares**

| Component | Description |
|---|---|
| `MapMiddleware` | Apply a function to each record |
| `FilterMiddleware` | Drop records that do not match a predicate |
| `RetryMiddleware` | Retry a middleware on exception with backoff |
| `ValidateMiddleware` | Validate records against a Pydantic model |
| `EnrichMiddleware` | Enrich records with data from an async callable |
| `DedupMiddleware` | Drop duplicate records by a computed key |
| `AIEnrichMiddleware` | Add fields using an LLM |
| `AIClassifyMiddleware` | Classify records into a fixed set of categories |
| `AIExtractMiddleware` | Extract structured fields from unstructured text |
| `AIBatchMiddleware` | Batch multiple records into a single LLM call |

---

## CLI

```bash
agora new <name>       # scaffold a new project
agora run <module>     # run a pipeline once
agora worker           # start the worker pool
agora dlq replay       # replay failed records
agora plugins list     # list registered plugins
agora version          # print version
```

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
