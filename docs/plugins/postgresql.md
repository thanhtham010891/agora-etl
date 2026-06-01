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

## Sample

This example reads active customers from PostgreSQL and upserts the transformed
rows into another table.

```python
from agora import Pipeline
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

pipeline = Pipeline(source).build(sink)
```

What this shows:

- `PostgresSource` can resume from checkpoint-aware queries
- composite checkpoints work when one cursor field is not enough
- `copy_merge` is the bulk-write option when append-only `COPY` is not enough
- `conflict_key` defines the upsert identity

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
