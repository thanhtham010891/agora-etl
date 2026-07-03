# Agora ETL

**Async-first ETL framework for Python.**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)
[![PyPI](https://img.shields.io/pypi/v/agora-etl)](https://pypi.org/project/agora-etl/)

`agora-etl` is the core Agora runtime: a builder-first ETL framework organized
around a `Source → Middleware chain → Sink(s)` model.

The core package owns runtime semantics, public contracts, CLI diagnostics,
checkpointing, DLQ behavior, scheduling primitives, health/metrics surfaces,
and plugin discovery. Backend integrations such as Redis, Kafka, PostgreSQL,
cron scheduling, distributed coordination, and Anthropic live in the official
[`agora-etl-plugins`](docs/plugins/index.md) package.

Keep the package story simple:

- `agora-etl` owns runtime semantics and public framework contracts
- `agora-etl-plugins` owns official backend implementations and backend
  runbooks
- `agora-etl-rs` accelerates selected hot paths through the core acceleration
  boundary

## Install

```bash
pip install agora-etl
```

Optional core extras:

```bash
pip install "agora-etl[file]"       # pyarrow + faster JSONL/file paths
pip install "agora-etl[rs]"         # optional Rust acceleration boundary
pip install "agora-etl[benchmark]"  # local benchmarking helpers
```

Official integrations:

```bash
pip install "agora-etl-plugins[redis]"
pip install "agora-etl-plugins[kafka]"
pip install "agora-etl-plugins[postgres]"
pip install "agora-etl-plugins[all]"
```

`agora-etl-plugins 0.4.x` targets `agora-etl>=0.4.1,<1`.

## Quickstart

```python
import asyncio

from agora import DeliveryConfig, IterableSource, Pipeline
from agora.core.dlq import SQLiteDLQSink


records = [
    {"id": 1, "city": "Ho Chi Minh City", "confidence": 0.92},
    {"id": 2, "city": "Da Nang", "confidence": 0.41},
    {"id": 3, "city": "Hanoi", "confidence": 0.88},
]


async def main() -> None:
    summary = await (
        Pipeline(IterableSource(records))
        .filter(lambda row: row["confidence"] >= 0.5, name="confidence_gate")
        .build(
            config=DeliveryConfig(
                dlq=SQLiteDLQSink(".agora_dlq.db"),
                batch_size=100,
            ),
        )
        .run()
    )
    print(summary)


asyncio.run(main())
```

`build()` without an explicit sink uses the default stdout sink. The example:

- emits in-memory records
- filters low-confidence rows
- writes accepted records to stdout
- routes failed records to a local SQLite DLQ
- returns a `PipelineRunSummary` with counts, timing, and runtime signals

## Core capabilities

| Area | What core provides |
|---|---|
| Pipeline builder | Immutable `Pipeline`, `BoundPipeline`, `.pipe()`, `.build()`, `.fan_out()`, `.route()`, and `.explain()`. |
| Runtime lanes | Linear, buffered, Python batch, and Arrow batch execution with one shared data-plane vocabulary. |
| Reliability | Checkpoint stores, DLQ sinks, replay semantics, retry policies, sink failure policies, and conservative checkpoint advancement. |
| Workers | `Schedule`, `ScheduledPipeline`, `WorkerPool`, graceful shutdown, health endpoints, and distributed-coordination hooks. |
| Observability | Run summaries, runtime metrics, tracing, Prometheus rendering helpers, health snapshots, and doctor diagnostics. |
| Extensibility | Public plugin entry-point groups, manifest compatibility diagnostics, registries, and stable core facades. |
| AI runtime support | Provider contracts, AI middleware, cache contracts, budget/cost governance, and provider capability guards. |

## Runtime guarantees

Agora's public runtime contract is documented in
[Runtime Guarantees](docs/guides/runtime-guarantees.md). The high-level model:

- source order is preserved at the sink boundary
- checkpoint advancement waits for handled outcomes
- sink delivery is fail-closed by default
- DLQ replay acknowledges only after durable replay success
- batch, Arrow, and process-isolated paths keep the same correctness contract
- at-least-once delivery is the model; exactly-once behavior requires external
  idempotency or transactional systems

## Execution lanes and data planes

Agora selects the execution lane from explicit source, middleware, and sink
contracts:

- `python_rows`
- `python_batches`
- `arrow_batches`

Use `.explain()` before a run to inspect lane selection and sink downgrade
decisions:

```python
from agora import Pipeline
from agora.sinks.file.csv import CsvSink


bound = Pipeline(source).build(CsvSink(path="out.csv", row_mapper=lambda row: row))
plan = bound.explain(max_records=1_000)

print(plan)
print(plan.to_dict())
```

`PipelineExplain` includes the selected lane, source data plane, writer input
plane, middleware compatibility, sink plane, and downgrade markers.

## Core vs plugins

Keep the package boundary clear:

| Package | Owns |
|---|---|
| `agora-etl` | Runtime semantics, public contracts, CLI, docs, health, metrics, checkpointing, DLQ behavior, and plugin discovery. |
| `agora-etl-plugins` | Official Redis, Kafka, PostgreSQL, cron, distributed coordination, Anthropic, backend DLQ, and backend-specific observability surfaces. |
| `agora-etl-rs` | Optional acceleration primitives selected through the core acceleration boundary. |

If a capability depends on Redis, Kafka, PostgreSQL, cron parsing, distributed
lease ownership, or a hosted AI provider, it belongs in a plugin package rather
than core.

## CLI

The package installs the `agora` command:

```bash
agora --help
agora doctor
agora plugins list
agora dlq replay --help
```

Use `agora doctor` and `agora plugins list --json` in release gates and
operator diagnostics when installed package behavior matters.

## Documentation map

- [Quickstart](docs/guides/quickstart.md)
- [Pipelines](docs/guides/pipelines.md)
- [Runtime Guarantees](docs/guides/runtime-guarantees.md)
- [Failure Handling](docs/guides/failure-handling.md)
- [Recovery Matrix](docs/guides/recovery-matrix.md)
- [Checkpointing](docs/guides/checkpointing.md)
- [Scheduling and Workers](docs/guides/scheduling.md)
- [Observability](docs/guides/observability.md)
- [Performance](docs/guides/performance.md)
- [Plugins](docs/plugins/index.md)
- [Architecture](docs/architecture.md)
- [CLI](docs/cli.md)
- [Change Log](docs/change-log/index.md)

Reference sections:

- [Sources](docs/source/index.md)
- [Sinks](docs/sink/index.md)
- [Middlewares](docs/middleware/index.md)
- [Schema](docs/schema.md)
- [State](docs/state.md)

## Development

```bash
python -m venv .venv
.venv/bin/pip install -e ".[file,dev,benchmark]"
make ci
```

Useful focused checks:

```bash
make docs-check
make test-core
make contracts
make preservation
```

The GitHub CI matrix runs on Python `3.11`, `3.12`, and `3.13`.

## Security

Security reporting instructions live in [SECURITY.md](SECURITY.md). Do not
include exploitable details, credentials, payloads, production hostnames, or
private stack traces in public issues.

## License

Apache 2.0 — see [LICENSE](LICENSE).
