# PostgreSQL Plugins

_When to read this: you need PostgreSQL as a source, sink, DLQ backend, or schema-aware operational store in the pipeline._

Use the PostgreSQL family when the pipeline needs to read from relational SQL,
write into operational tables, or keep replay state in a database operators
already manage.

## Install

```bash
pip install "agora-etl-plugins[postgres]"
```

That extra covers both the PostgreSQL source/sink path and the PostgreSQL-backed
DLQ components.

## When PostgreSQL is a good fit

- extract rows from a SQL query
- load rows into operational tables
- choose between SQL inserts, `COPY`, or staged `COPY + MERGE`
- keep DLQ records in PostgreSQL instead of local files
- evolve a target table alongside schema-aware middleware

## What the PostgreSQL family includes

### PostgreSQL source

`PostgresSource` streams rows from a query in batches.

It supports:

- query parameters
- row mapping into pipeline records
- single-field or composite checkpoints
- resumable extraction when the query is written around checkpoint parameters
- fail-closed or log-and-continue row mapping behavior

Use it when you want Agora to pull directly from a relational source of truth.

### PostgreSQL sink

`PostgresSink` is the main write path.

It supports three write modes:

- `sql`: batch insert or upsert through SQL statements
- `copy`: fast bulk load when you do not need upsert behavior
- `copy_merge`: stage with `COPY`, then merge into the target table

This gives you a practical tradeoff surface:

- `sql` for straightforward operational writes
- `copy` for raw append-heavy throughput
- `copy_merge` when you want bulk loading with conflict-aware final writes

| Mode | Best for | Notes |
|---|---|---|
| `sql` | moderate upsert workloads | simplest operational default |
| `copy` | append-only bulk loads | fastest when no upsert is needed |
| `copy_merge` | large upsert-heavy loads | stage first, then merge into the target table |

### Schema adapter

`PostgresSchemaAdapter` wraps a sink and applies table changes from runtime
schema information.

Use it when:

- schema middleware already produces table metadata
- the target table should auto-create or add missing columns
- you want the pipeline to stay close to the shape of incoming records

Schema-qualified table names such as `analytics.users` are supported for both
write SQL and schema introspection paths.

### PostgreSQL DLQ

`PostgresDLQSink` and `PostgresDLQSource` keep dead-letter records in a table.

This works well when:

- operators already live in PostgreSQL
- replay needs SQL-level visibility
- failure records should participate in existing backup, access, or audit paths

## Quickstart

This example reads active customers from PostgreSQL and upserts the transformed
rows into another table.

```python
from agora import DeliveryConfig, Pipeline
from agora_plugins.postgres import PostgresSink, PostgresSource


source = PostgresSource(
    dsn="postgresql://app:secret@localhost:5432/app",
    query="""
        SELECT id, email, updated_at
        FROM customers
        WHERE (
            updated_at > %(cursor)s
            OR (updated_at = %(cursor)s AND id > %(last_id)s)
        )
        ORDER BY updated_at, id
    """,
    params={"cursor": "2026-01-01T00:00:00+00:00", "last_id": 0},
    checkpoint_fields=["updated_at", "id"],
    checkpoint_params={"updated_at": "cursor", "id": "last_id"},
    row_mapper=lambda row: {
        "customer_id": row["id"],
        "email": row["email"].lower(),
        "synced_at": row["updated_at"],
    },
)

sink = PostgresSink(
    dsn="postgresql://app:secret@localhost:5432/app",
    table="customer_projection",
    row_mapper=lambda record: record,
    conflict_key="customer_id",
    insert_mode="copy_merge",
    batch_size=500,
)

summary = await (
    Pipeline(source)
    .build(sink, config=DeliveryConfig(batch_size=100))
    .run(max_records=10_000)
)
```

What this shows:

- `PostgresSource` can resume from checkpoint-aware queries
- composite checkpoints work when one cursor field is not enough
- `copy_merge` is the bulk-write option when append-only `COPY` is not enough
- `conflict_key` defines the upsert identity

If you want the incremental extract shape pre-wired:

```bash
agora new my-extractor --preset postgres-incremental
cd my-extractor
pip install -e '.[dev]'
```

That scaffold gives a runnable cursor-based extractor, test, and project
layout to extend.

## Incremental extract pattern

This is the narrow pattern most teams want from PostgreSQL first: query only
rows newer than the last checkpoint and normalize them before writing
downstream.

```python
from __future__ import annotations

import os

from agora import DeliveryConfig, MapMiddleware, Pipeline
from agora_plugins.postgres import PostgresSource
from agora.sinks.io.stdout import StdoutSink


def normalise(record: dict) -> dict:
    return {
        key: (value.isoformat() if hasattr(value, "isoformat") else value)
        for key, value in record.items()
    }


source = PostgresSource(
    dsn=os.environ["DATABASE_URL"],
    query="""
        SELECT *
        FROM events
        WHERE updated_at > :cursor
        ORDER BY updated_at
    """,
    cursor_column="updated_at",
)

summary = await (
    Pipeline(source, id="postgres_incremental")
    .pipe(MapMiddleware(normalise, name="normalise"))
    .build(
        StdoutSink(),
        config=DeliveryConfig(batch_size=1_000, checkpoint_every=10),
    )
    .run()
)
```

What this pattern shows:

- `PostgresSource` can own the resume cursor directly
- normalization can stay in ordinary middleware before the sink
- checkpoint state advances after successful batch delivery, not after query read

## Schema-aware sink example

If you are already using `SchemaMiddleware`, wrap the sink with
`PostgresSchemaAdapter` so missing tables or columns can be created from the
runtime schema shape.

```python
from agora import DeliveryConfig, Pipeline
from agora.schema import SchemaMiddleware
from agora_plugins.postgres import PostgresSchemaAdapter, PostgresSink
from agora_plugins.redis import RedisStreamSource


source = RedisStreamSource(
    url="redis://localhost:6379",
    stream="customers:raw",
    group="customer-sync",
    consumer="worker-1",
    deserializer=lambda fields: {
        "customer_id": int(fields["customer_id"]),
        "email": fields["email"],
        "status": fields["status"],
    },
)

sink = PostgresSchemaAdapter(
    PostgresSink(
        dsn="postgresql://app:secret@localhost:5432/app",
        table="public.customer_projection",
        row_mapper=lambda record: record,
        conflict_key="customer_id",
    )
)

summary = await (
    Pipeline(source)
    .pipe(SchemaMiddleware(table="public.customer_projection"))
    .build(sink, config=DeliveryConfig(batch_size=100))
    .run(max_records=5_000)
)
```

Use this when the record shape is still evolving but the relational target
should stay close to what the pipeline emits.

## DLQ example

```python
from agora import DeliveryConfig, Pipeline
from agora_plugins.postgres import PostgresDLQSink, PostgresSink, PostgresSource


dlq = PostgresDLQSink(
    dsn="postgresql://app:secret@localhost:5432/app",
    table="ops.agora_dlq",
)

summary = await (
    Pipeline(
        PostgresSource(
            dsn="postgresql://app:secret@localhost:5432/app",
            query="SELECT id, payload FROM inbox ORDER BY id",
            row_mapper=lambda row: row,
        )
    )
    .build(
        PostgresSink(
            dsn="postgresql://app:secret@localhost:5432/app",
            table="processed_events",
            row_mapper=lambda record: record,
            conflict_key="id",
        ),
        config=DeliveryConfig(dlq=dlq, batch_size=100),
    )
    .run()
)
```

Use PostgreSQL DLQ when operators already inspect failures through SQL tooling
and you want replay records in the same operational estate.

## Common patterns

### Database extract pipeline

- source from PostgreSQL
- transform in middleware
- publish elsewhere as files, Kafka events, or API calls

### Operational load pipeline

- ingest from file or stream
- map into target rows
- write with upsert or `copy_merge`

### SQL-backed recovery workflow

- store failures in PostgreSQL DLQ
- inspect them with normal SQL tooling
- replay only after the downstream issue is fixed

## Boundaries

PostgreSQL is a strong fit when records are naturally relational and operators
already trust SQL as the place to inspect data.

It is a weaker fit when:

- the workload is purely event-stream oriented
- the team wants a message bus rather than a table sink
- data shape changes too quickly for a relational target to stay comfortable
