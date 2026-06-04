# Agora ETL Framework

**Async-first ETL framework for Python.**

[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
![Python](https://img.shields.io/badge/python-3.11%20|%203.12%20|%203.13-blue)
[![PyPI](https://img.shields.io/pypi/v/agora-etl)](https://pypi.org/project/agora-etl/)

`agora-etl` is the core Agora runtime: a builder-first ETL framework organized
around `Source -> Middleware chain -> Sink(s)`.

It stays focused on runtime semantics and extension contracts:

- immutable pipeline composition
- per-record, batched, and Arrow-aware execution
- checkpointing and replay
- DLQ and sink failure policies
- backpressure, tracing, metrics, and health surfaces
- scheduled workers and config-driven runs

Official backend integrations such as Redis, Kafka, PostgreSQL, cron
scheduling, and distributed coordination live in
[`agora-etl-plugins`](docs/plugins/index.md). Optional acceleration belongs in
`agora-etl-rs`.

## Install

```bash
pip install agora-etl
pip install "agora-etl[file]"          # pyarrow + faster JSONL paths
pip install "agora-etl-plugins[all]"   # official first-party integrations
```

## Quick Start

```python
import asyncio

from agora import DeliveryConfig, IterableSource, Pipeline
from agora.core.dlq import SQLiteDLQSink
from agora.sinks.file.jsonlines import JsonLinesSink

records = [
    {"id": 1, "city": "Ho Chi Minh City", "confidence": 0.92},
    {"id": 2, "city": "Da Nang",          "confidence": 0.41},
    {"id": 3, "city": "Hanoi",            "confidence": 0.88},
]


async def main() -> None:
    summary = await (
        Pipeline(IterableSource(records))
        .filter(lambda row: row["confidence"] >= 0.5, name="confidence_gate")
        .build(
            JsonLinesSink(path="cities.jsonl"),
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

What this does:

- reads from an in-memory source
- filters low-confidence rows
- writes the survivors to `cities.jsonl`
- sends failed records to a local SQLite DLQ
- returns a `PipelineRunSummary` with counts, timing, and runtime signals

## Why `0.3.0` Matters

Agora now exposes execution shape as a first-class contract.

Three shared data planes:

- `python_rows`
- `python_batches`
- `arrow_batches`

That vocabulary now shows up consistently across sources, middleware, writer
planning, sink boundaries, runtime summaries, and tracing.

You can inspect the plan before the run starts:

```python
from agora import Pipeline
from agora.sinks.file.csv import CsvSink

bound = Pipeline(source).build(CsvSink(path="out.csv", row_mapper=lambda row: row))

plan = bound.explain(max_records=1_000)
print(plan)
print(plan.to_dict())
```

`PipelineExplain` surfaces:

- selected execution lane
- source and writer data planes
- middleware compatibility matrix
- per-sink selected plane and downgrade markers

## Execution Shapes

Agora supports more than one runtime shape, but keeps them explicit:

- row pipelines: standard `Middleware` / `MapMiddleware` / `FilterMiddleware`
- Python batch pipelines: `stream_batches()` plus `BatchMiddleware`
- Arrow pipelines: `pyarrow.RecordBatch` plus Arrow-native middleware and sinks

The important rule is that one middleware chain must stay internally
compatible:

- Python-row chain is valid
- all-Arrow chain is valid
- mixed Arrow plus Python-row/list-batch chain is rejected during planning

For Arrow fan-out, Agora chooses the best path per sink:

- Arrow-capable sinks receive the original `RecordBatch`
- non-Arrow sinks downgrade only at the sink boundary

## Builder Model

The pipeline builder is immutable.

```python
base = (
    Pipeline(source)
    .pipe(NormalizeMiddleware())
    .filter(lambda row: row["confidence"] >= 0.5)
)

csv_pipeline = base.build(CsvSink(path="clean.csv", row_mapper=lambda row: row))
jsonl_pipeline = base.build(JsonLinesSink(path="clean.jsonl"))
```

Common composition patterns:

- `.build(sink)` for one destination
- `.fan_out([...])` for writing the same record to many sinks
- `.route(router)` for sending each record to one matching sink
- `.run(max_records=N)` for bounded runs, now enforced at the source boundary

Delivery options are passed via `DeliveryConfig`, including:

- `dlq`
- `checkpoint`
- `batch_size`
- `sink_failure_policy`
- `sink_concurrency`
- `backpressure`
- `tracer`

## Public Data-Plane API

The execution-shape contract is now public API:

```python
from agora import DataPlane, SinkDataPlaneSpec, SourceDataPlaneSpec
```

```python
source_spec = source.data_plane_spec()
sink_spec = sink.data_plane_spec()
```

Legacy data-plane booleans still work in `0.3.x`, but they are now a
compatibility bridge and emit `DeprecationWarning` when Agora has to infer a
non-row plane from them.

Preferred direction:

- sources override `data_plane_spec()`
- sinks advertise `accepted_data_planes` and `native_data_planes`

## Core vs Plugins

Keep this mental model:

- `agora-etl`: runtime semantics, pipeline contracts, CLI, docs
- `agora-etl-plugins`: official integrations
- `agora-etl-rs`: optional acceleration that preserves the same semantics

If a capability depends on Redis, Kafka, PostgreSQL, cron parsing, or
distributed lease ownership, it likely belongs in the plugin bundle rather
than the core package.

## Documentation Map

- [Quickstart](docs/guides/quickstart.md)
- [Pipelines](docs/guides/pipelines.md)
- [Failure Handling](docs/guides/failure-handling.md)
- [Checkpointing](docs/guides/checkpointing.md)
- [Observability](docs/guides/observability.md)
- [Scheduling](docs/guides/scheduling.md)
- [Plugins](docs/plugins/index.md)
- [Architecture](docs/architecture.md)
- [CLI](docs/cli.md)
- [Change Log](docs/change-log/index.md)

Reference sections:

- [Sources](docs/source/index.md)
- [Sinks](docs/sink/index.md)
- [Middlewares](docs/middleware/index.md)

## License

Apache 2.0 - see [LICENSE](LICENSE).
