# Plugins

_When to read this: you want to understand which capabilities live in the official plugin ecosystem and when a plugin is the right boundary instead of core Agora._

Agora plugins extend the core runtime with integrations that are better kept
outside `agora-etl` itself. In the `0.4.x` line, this is a production boundary:
core owns the runtime contract, while `agora-etl-plugins` owns official backend
integrations and their backend-specific validation matrix.

This section focuses on the public plugin story:

- what the official first-party plugin package includes
- which plugin families are production-ready dataset surfaces
- when to use each plugin family
- what kind of system problem each family solves
- how to build your own plugin package

Advanced runtime/wedge helpers may still be exported from a family root, but
those surfaces should be treated as advanced composed flows rather than as
first-stop backend primitives.

## Start here

- Want the official first-party integrations: [Official Bundle](official-bundle.md)
- Need production compatibility and release gates: [Production Readiness](production-readiness.md)
- Need Redis-backed state, streams, or replay: [Redis](redis.md)
- Need topic-based pipelines: [Kafka](kafka.md)
- Need relational extract/load workflows: [PostgreSQL](postgresql.md)
- Need analytical warehouse tables: [BigQuery](bigquery.md)
- Need object-store datasets: [S3](s3.md)
- Need calendar scheduling: [Scheduling](scheduling.md)
- Need multi-worker lease ownership: [Distributed Coordination](distributed.md)
- Need Anthropic completion or structured output support: [Anthropic](anthropic.md)
- Want to build your own package: [Developing Plugins](developing.md)
- Need to know which extension points are stable: [Plugin Contract](contract.md)
- Need to understand manifest versioning: [Manifest Contract](manifest.md)
- Need the doc authority map: [Source Of Truth Map](../source-of-truth.md)

## What counts as a plugin?

Agora discovers plugin packages through Python entry-points. A plugin may
provide:

- sources
- sinks
- middlewares
- AI providers
- caches
- state backends
- metrics exporters
- runner integrations

This keeps the core framework smaller and lets integrations evolve on their own
release cadence.

These plugin contracts also form the supported backend layer that operator
surfaces should build on. Runtime semantics still belong in the core.

## Official first-party package

The public first-party plugin distribution is
[`agora-etl-plugins`](https://pypi.org/project/agora-etl-plugins/).

Current official coverage includes Redis, Kafka, PostgreSQL, BigQuery, S3,
Anthropic completion support, cron scheduling, and distributed worker
coordination. The published plugin `0.4.x` line targets `agora-etl>=0.4.1,<1`;
these docs are aligned with the current `agora-etl` `0.4.x` production line.

Install examples:

```bash
pip install "agora-etl-plugins[redis]"
pip install "agora-etl-plugins[kafka]"
pip install "agora-etl-plugins[postgres]"
pip install "agora-etl-plugins[bigquery]"
pip install "agora-etl-plugins[s3]"
pip install "agora-etl-plugins[anthropic]"
pip install "agora-etl-plugins[all]"
```

## Common pipeline shapes

| Pipeline shape | Plugin family | Start with |
|---|---|---|
| Redis Streams in, relational table out | Redis + PostgreSQL | [Redis](redis.md), [PostgreSQL](postgresql.md) |
| Warehouse query in, object dataset out | BigQuery + S3 | [BigQuery](bigquery.md), [S3](s3.md) |
| Kafka topic in, Kafka topic out | Kafka | [Kafka](kafka.md) |
| Periodic sync every hour or every weekday | Scheduling | [Scheduling](scheduling.md) |
| Same schedules deployed on multiple workers | Distributed coordination | [Distributed Coordination](distributed.md) |
| Shared replay or dead-letter inspection | Redis, Kafka, or PostgreSQL | [Redis](redis.md), [Kafka](kafka.md), [PostgreSQL](postgresql.md) |
| Kafka source into Redis or PostgreSQL sink with wedge/runtime metrics | Kafka + Redis/PostgreSQL composed flow | [Redis](redis.md), [PostgreSQL](postgresql.md) |

## Production maturity at a glance

| Family | Production role | Boundary |
|---|---|---|
| Redis | Flagship backend | Streams, sink, state, DLQ, exact dedup, AI cache, observability, and optional Kafka-to-Redis composed flows. |
| Kafka | Flagship backend | Topic source/sink, Kafka DLQ, Avro/JSON Schema/Protobuf registry helpers, security, tracing, and transactional hooks. |
| PostgreSQL | Flagship backend | Source, sink, schema adapter, DLQ, HA read routing, `COPY`, `COPY + MERGE`, and optional Kafka-to-Postgres composed flows. |
| BigQuery | Production-ready dataset backend | Table/query source, batch-oriented load-job table sink, and bounded append-only Storage Write sink for analytical ETL workloads, with a required local live verification gate before public support claims. |
| S3 | Production-ready dataset backend | Lexically ordered prefix source plus partitioned JSONL/CSV/Parquet dataset sink, validated locally against MinIO before public support claims. |
| Distributed coordination | Production coordination | Redis-backed leases, fencing tokens, fail-safe behavior, and optional Redlock quorum. |
| Scheduling | Official helper | Cron parsing and next-run calculation for worker schedules. |
| Anthropic | Official AI provider | Completion and structured output. Embeddings are deliberately out of scope. |

Each flagship family page begins with a maturity card that names:

- current support label
- in-scope contract
- explicit out-of-scope boundary
- required validation gate
- operator-facing hooks

## Quick examples

### Redis stream to PostgreSQL table

```python
from agora import DeliveryConfig, Pipeline
from agora_plugins.postgres import PostgresSink
from agora_plugins.redis import RedisStreamSource


source = RedisStreamSource(
    url="redis://localhost:6379",
    stream="orders:raw",
    group="orders-projection",
    consumer="worker-1",
    deserializer=lambda fields: {
        "order_id": int(fields["order_id"]),
        "status": fields["status"],
    },
)

sink = PostgresSink(
    dsn="postgresql://app:secret@localhost:5432/app",
    table="order_projection",
    row_mapper=lambda record: record,
    conflict_key="order_id",
)

summary = await (
    Pipeline(source)
    .build(sink, config=DeliveryConfig(batch_size=100))
    .run(max_records=1_000)
)
```

### Kafka topic to topic enrichment

```python
import json

from agora import DeliveryConfig, Pipeline
from agora_plugins.kafka import KafkaSink, KafkaSource


summary = await (
    Pipeline(
        KafkaSource(
            topics=["orders.raw"],
            bootstrap_servers="localhost:9092",
            group_id="orders-cleaner",
            deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
    )
    .build(
        KafkaSink(
            topic="orders.cleaned",
            bootstrap_servers="localhost:9092",
            serializer=lambda record: json.dumps(record).encode("utf-8"),
        ),
        config=DeliveryConfig(batch_size=100),
    )
    .run(max_records=1_000)
)
```

### BigQuery table to S3 dataset

```python
from agora import DeliveryConfig, Pipeline
from agora_plugins.bigquery import BigQuerySource
from agora_plugins.s3 import S3Sink


summary = await (
    Pipeline(
        BigQuerySource(
            table="analytics.orders",
            checkpoint_column="order_id",
            order_by=["order_id"],
            row_mapper=lambda row: row,
        )
    )
    .build(
        S3Sink(
            bucket="warehouse-exports",
            prefix="orders_projection",
            format="parquet",
            row_mapper=lambda record: record,
            partition_path_fn=lambda record: f"dt={record['order_date']}",
        ),
        config=DeliveryConfig(batch_size=500),
    )
    .run(max_records=10_000)
)
```

### Cron-scheduled worker with shared lease ownership

```python
from agora.runner import Schedule, ScheduledPipeline, WorkerPool
from agora_plugins.distributed import RedisWorkerCoordinator


def get_worker() -> WorkerPool:
    pool = WorkerPool(
        coordinator=RedisWorkerCoordinator(redis_url="redis://localhost:6379"),
    )
    pool.register(
        ScheduledPipeline(
            factory=build_daily_pipeline,
            schedule=Schedule.cron("0 2 * * *"),
            pipeline_id="daily-sync",
        )
    )
    return pool
```

### BigQuery

Choose BigQuery when extraction or delivery belongs on analytical tables and
query jobs rather than on event streams or OLTP writes.

See: [BigQuery](bigquery.md)

### S3

Choose S3 when the pipeline boundary is a dataset prefix, partitioned files,
or object-store based interchange with JSONL, CSV, or Parquet.

See: [S3](s3.md)

## How to think about plugins

Use a plugin when:

- The capability depends on an external system
- The integration has its own dependency footprint
- The feature should evolve independently from the core runtime
- Operators may want multiple interchangeable backends

Use the family pages in this section when the question is less about
"what is a plugin?" and more about "which backend story matches my pipeline?"

Keep work in the core when it is really part of Agora's execution model,
pipeline semantics, or stable framework contract.

## Discovery model

After installation, plugin components are available through Agora registries
and the CLI.

Examples:

```bash
agora plugins list
```

```python
from agora.sources import source_registry
from agora.sinks import sink_registry

source = source_registry.create("my_source", url="https://api.example.com")
sink = sink_registry.create("my_sink", dsn="postgresql://example/db")
```

For the full plugin contract and entry-point groups, see
[Plugin Contract](contract.md) and [Developing Plugins](developing.md).
