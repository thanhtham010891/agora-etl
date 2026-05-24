# Agora ETL Framework

**Async-first ETL framework for Python.**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
[![PyPI](https://img.shields.io/pypi/v/agora-etl)](https://pypi.org/project/agora-etl/)
---

## Documentation

- [Docs Home](docs/index.md)
- [Getting Started](docs/getting-started.md)
- [Configuration](docs/configuration.md)
- [Architecture](docs/architecture.md)
- [Sources](docs/sources.md)
- [Sinks](docs/sinks.md)
- [Middlewares](docs/middlewares.md)
- [Runner](docs/runner.md)
- [CLI](docs/cli.md)
- [Plugins](docs/plugins.md)

## Examples

- [ETL from CSV file](examples/etl-csv/)
- [ETL from JSON Lines file](examples/etl-json/)
- [ETL from Parquet file](examples/etl-parquet/)
- [ETL from HTTP API](examples/etl-http/)

---

## Overview

Agora is an async-first ETL framework for Python. It provides a composable pipeline model — source, middleware chain, sink — with built-in support for fault tolerance, observability, checkpointing, and AI enrichment.

**Key features:**

- Fluent, immutable pipeline builder
- Built-in dead-letter queue (DLQ) with replay
- Resumable pipelines via checkpointing
- Adaptive backpressure for fast-source / slow-sink scenarios
- AI enrichment middlewares (enrich, classify, extract, translate, batch)
- Fuzzy and exact deduplication
- Scheduled pipelines with health monitoring
- Plugin system via Python entry-points
- CLI for scaffolding, running, and managing pipelines

---

## Requirements

- Python 3.11+

---

## Install

```bash
pip install agora-etl                # core only
pip install "agora-etl[file]"          # + Parquet / CSV / JSON Lines support
pip install "agora-etl[all,dev]"       # everything + dev tools
```

---

## Quick start

```bash
agora new my-pipeline
cd my-pipeline
agora run pipelines.example
```

---

## Core concepts

A pipeline has three parts:

```
Source  →  Middleware chain  →  Sink(s)
```

- **Source** — emits records one at a time
- **Middleware** — transforms, filters, enriches, or validates each record
- **Sink** — persists records to a destination

`Pipeline` is immutable. Every `.pipe()` and `.filter()` call returns a new instance, making it safe to branch:

```python
from agora import Middleware, Pipeline, PipelineContext
from agora.sources.file.jsonlines import JsonLinesSource
from agora.sinks.file.jsonlines import JsonLinesSink
from agora.core.dlq import SQLiteDLQSink
from agora.core.checkpoint import SQLiteCheckpointStore


class NormalizeMiddleware(Middleware[RawRecord, CleanRecord]):
    name = "normalize"

    async def process(self, record: RawRecord, ctx: PipelineContext) -> CleanRecord | None:
        if not record.name:
            return None
        return CleanRecord(id=record.id, name=record.name.strip())


summary = await (
    Pipeline(JsonLinesSource("data/records.jsonl", row_mapper=RawRecord.from_dict))
    .pipe(NormalizeMiddleware())
    .filter(lambda r: r.score > 0.8)
    .build(
        JsonLinesSink("output/clean.jsonl"),
        dlq=SQLiteDLQSink(".dlq.db"),
        checkpoint=SQLiteCheckpointStore(".checkpoint.db"),
        checkpoint_every=100,
        batch_size=50,
    )
    .run(max_records=10_000)
)

print(f"written={summary.records_written}  dropped={summary.records_dropped}  errors={summary.records_errored}")
```

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
| `MapMiddleware` | Apply a synchronous function to each record |
| `FilterMiddleware` | Drop records that do not match a predicate |
| `RetryMiddleware` | Retry a middleware on exception |
| `ValidateMiddleware` | Validate records against a Pydantic model |
| `EnrichMiddleware` | Enrich records with data from an async callable |
| `DedupMiddleware` | Drop duplicate records by a computed key |

**AI middlewares** (require an `AIProvider` plugin)

| Component | Description |
|---|---|
| `AIEnrichMiddleware` | Add fields to each record using an LLM |
| `AIClassifyMiddleware` | Classify records into a fixed set of categories |
| `AIExtractMiddleware` | Extract structured fields from unstructured text |
| `AIValidateMiddleware` | Validate records using an LLM |
| `AITranslateMiddleware` | Translate text fields to a target language |
| `AIBatchMiddleware` | Batch multiple records into a single LLM call |

---

## CLI

```bash
agora new <name>          # scaffold a new project
agora run <module>        # run a pipeline once
agora worker              # start the worker pool
agora pipelines list      # list pipeline modules
agora plugins list        # list registered plugins
agora dlq list            # inspect dead-letter queue
agora dlq replay          # replay failed records
agora config show         # show resolved settings
agora version             # print version
```

---

## Configuration

Agora reads config from environment variables or an `agora.env` file:

```env
AGORA_LOG_LEVEL=INFO
AGORA_HEALTH_HOST=127.0.0.1
AGORA_HEALTH_PORT=8080
AGORA_HEALTH_AUTH_TOKEN=my-secret
```

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
