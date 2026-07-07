# PostgreSQL Plugins

_When to read this: the pipeline needs PostgreSQL extraction, table writes,
schema adaptation, SQL-native DLQ/replay, HA read routing, or Kafka-to-Postgres
composed flows._

The PostgreSQL family in `agora-etl-plugins 0.4.x` is a production-ready
flagship backend. It includes checkpoint-aware SQL sources, pooled table sinks,
SQL/`COPY`/`COPY + MERGE` write modes, schema-aware table adaptation,
PostgreSQL-backed DLQ, observability reports, replica staleness controls, and
Kafka-to-Postgres composed flows.

## Maturity card

| Field | Value |
|---|---|
| Support label | Production-ready flagship |
| In scope | Checkpoint-aware source reads, SQL/`COPY`/`COPY + MERGE` sink modes, schema adapter, PostgreSQL DLQ/replay, HA read routing, and sink observability. |
| Out of scope | General migration tooling, warehouse-style mutation breadth outside the documented sink modes, and treating Kafka-to-Postgres runtime helpers as the default PostgreSQL onboarding lane. |
| Required validation gate | `make test-release-gate-postgres` |
| Operator hooks | `metrics_snapshot()`, `health_snapshot()`, `acceptance_report()`, `render_prometheus_metrics()`, plus replica-staleness controls on source reads. |

## Install

```bash
pip install "agora-etl-plugins[postgres]"
```

The extra installs `psycopg[binary]` and `psycopg_pool`.

## Public surface

| Component | Kind | Use it for |
|---|---|---|
| `PostgresSource` | Source | Streaming query results with optional checkpoint-aware resume. |
| `PostgresSink` | Sink | Writing rows with SQL, `COPY`, or staged `COPY + MERGE`. |
| `PostgresSchemaAdapter` | Sink wrapper | Auto-create and additive auto-alter from runtime `SchemaMiddleware` metadata. |
| `PostgresDLQSink` | DLQ sink | Persisting dead-letter records to a table. |
| `PostgresDLQSource` | DLQ source | Reading bounded replay windows from a DLQ table. |
| `PostgresConfig`, `PostgresPluginConfig` | Config | Env/model-backed connection, TLS, auth, pool, and statement settings. |
| `PostgresConnectionConfig`, `PostgresTLSConfig`, `PostgresAuthConfig` | Config | Explicit connection wiring for source/sink/DLQ. |
| `PostgresWriteSafetyPolicy` | Sink safety | Strict or align-to-target write behavior. |
| `PostgresSinkWriteError`, `PostgresPoisonRecordInfo` | Error model | Classifying schema drift, constraint violations, type mismatch, and unknown failures. |
| `PostgresPrometheusExporter` | Observability | Prometheus rendering for source/sink/DLQ metrics. |
| `KafkaPostgresRuntime` and builders | Composed flow helper | Advanced Kafka-to-Postgres wedge runtime, poison DLQ config, metrics, and acceptance reports. Start with the PostgreSQL primitives above unless the pipeline is explicitly a Kafka-to-Postgres composed flow. |

`KafkaPostgres*` surfaces are exported for advanced composed pipelines, but
they are not the default PostgreSQL onboarding story.

Entry-points installed by the package:

| Group | Key | Target |
|---|---|---|
| `agora.sources` | `postgres` | `PostgresSource` |
| `agora.sources` | `postgres_dlq_source` | `PostgresDLQSource` |
| `agora.sinks` | `postgres` | `PostgresSink` |
| `agora.sinks` | `postgres_schema_adapter` | `PostgresSchemaAdapter` |
| `agora.sinks` | `postgres_dlq` | `PostgresDLQSink` |

## PostgresSource

`PostgresSource` streams rows from a SQL query and maps each row into a
pipeline record.

Important constructor options:

| Option | Meaning |
|---|---|
| `dsn` or `connection` | Connection source. `connection` accepts `PostgresConnectionConfig`. |
| `query`, `params` | SQL text and base parameters. |
| `row_mapper` | Sync/async callable. May accept context when its signature supports it. |
| `batch_size` | `fetchmany()` size. |
| `checkpoint_field` + `checkpoint_param` | Single-cursor resume. |
| `checkpoint_fields` + `checkpoint_params` | Composite-cursor resume. |
| `on_record_error` | Fail closed or log/drop/continue row mapping failures. |
| `statement_timeout_ms` | Applies a PostgreSQL statement timeout for reads. |
| `transaction_read_only`, `transaction_isolation_level` | Read transaction controls. |
| `read_routing` | `dsn`, `primary`, `standby`, `prefer_standby`, or `any`. |
| `max_replica_replay_lag_s`, `on_replica_stale` | Standby staleness guard and fallback behavior. |
| `fetch_strategy` | `client` or `server_side`. |
| `server_side_cursor_name`, `server_side_cursor_withhold` | Server-side cursor controls. |

Checkpointing is enabled only when checkpoint field/parameter mapping is
provided. Without it, the source reports full-rerun recovery semantics.

## PostgresSink

`PostgresSink` buffers mapped rows and flushes to PostgreSQL.

Important constructor options:

| Option | Meaning |
|---|---|
| `table` | Target table; schema-qualified names are supported. |
| `row_mapper` | Converts pipeline records to row dictionaries. |
| `conflict_key` | One key or a list of keys used for upsert identity. |
| `batch_size` | Buffer size before flush. |
| `upsert=True` | Upsert by `conflict_key`. |
| `insert_mode` | `sql`, `copy`, or `copy_merge`. |
| `pool_size` | Sink-owned write connection pool size. |
| `max_rows_per_statement` | Optional row chunk limit. |
| `max_parameters_per_statement=32000` | Parameter safety limit for SQL mode. |
| `write_safety_policy` | Strict row shape or align rows to target table columns. |
| `poison_record_sink` | Optional DLQ sink for failed flush buffers. |
| `pool_acquire_timeout_s`, `pool_health_check`, `pool_max_lifetime_s`, `pool_max_idle_s` | Pool controls. |
| `allow_quoted_identifiers` | Opt-in for quoted identifier support. |

Write modes:

| Mode | Best for | Constraints |
|---|---|---|
| `sql` | General insert/upsert | Honors parameter limits and chunking. |
| `copy` | Append-only bulk loads | Requires `upsert=False`. |
| `copy_merge` | Large upsert-heavy loads | Stages with `COPY`, then merges into target table. |

When `upsert=True`, `open()` validates that target table constraints exactly
match `conflict_key`. If `PostgresSchemaAdapter` wraps the sink, that preflight
is deferred until after schema DDL runs.

## Quickstart

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
    pool_size=4,
)

summary = await (
    Pipeline(source)
    .build(sink, config=DeliveryConfig(batch_size=100))
    .run(max_records=10_000)
)
```

## Incremental extract pattern

Use `checkpoint_field/checkpoint_param` for simple cursor queries:

```python
from agora import DeliveryConfig, MapMiddleware, Pipeline
from agora.sinks.io.stdout import StdoutSink
from agora_plugins.postgres import PostgresSource


def normalise(record: dict) -> dict:
    return {
        key: (value.isoformat() if hasattr(value, "isoformat") else value)
        for key, value in record.items()
    }


source = PostgresSource(
    dsn="postgresql://app:secret@localhost:5432/app",
    query="""
        SELECT *
        FROM events
        WHERE updated_at > %(cursor)s
        ORDER BY updated_at
    """,
    params={"cursor": "2026-01-01T00:00:00+00:00"},
    checkpoint_field="updated_at",
    checkpoint_param="cursor",
    row_mapper=lambda row: row,
    fetch_strategy="server_side",
    statement_timeout_ms=30_000,
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

## Read routing and replica staleness

`PostgresSource` can ask PostgreSQL/libpq for read routing through
`target_session_attrs`-style behavior:

```python
source = PostgresSource(
    dsn="postgresql://app:secret@postgres-ha/app",
    query="SELECT id, updated_at FROM events WHERE updated_at > %(cursor)s",
    params={"cursor": "2026-01-01T00:00:00+00:00"},
    row_mapper=lambda row: row,
    checkpoint_field="updated_at",
    checkpoint_param="cursor",
    read_routing="prefer_standby",
    max_replica_replay_lag_s=2.0,
    on_replica_stale="route_primary",
)
```

Use `route_primary` only when primary fallback is acceptable for the workload.
Use `fail_closed` when stale standby reads are safer than surprising primary
traffic.

## Schema adapter

`PostgresSchemaAdapter` wraps a sink and applies runtime schema metadata from
`SchemaMiddleware`.

```python
from agora import DeliveryConfig, Pipeline
from agora.schema import SchemaMiddleware
from agora_plugins.postgres import PostgresSchemaAdapter, PostgresSink
from agora_plugins.redis import RedisStreamSource


sink = PostgresSchemaAdapter(
    PostgresSink(
        dsn="postgresql://app:secret@localhost:5432/app",
        table="public.customer_projection",
        row_mapper=lambda record: record,
        conflict_key="customer_id",
    ),
    auto_create=True,
    auto_alter=True,
    schema_lock_timeout_ms=5_000,
    schema_advisory_lock=True,
)

summary = await (
    Pipeline(
        RedisStreamSource(
            url="redis://localhost:6379",
            stream="customers:raw",
            group="customer-sync",
            consumer="worker-1",
        )
    )
    .pipe(SchemaMiddleware(table="public.customer_projection"))
    .build(sink, config=DeliveryConfig(batch_size=100))
    .run(max_records=5_000)
)
```

The adapter can create tables and add missing columns. For existing tables with
rows, new non-null schema columns are added nullable because no default/backfill
value exists. Use migration tooling instead when schema changes require review,
backfill, or destructive changes.

## DLQ and poison writes

Use `PostgresDLQSink` when failed records should be inspectable through SQL and
participate in database backup, retention, and access controls.

```python
from agora import DeliveryConfig, Pipeline
from agora_plugins.postgres import PostgresDLQSink, PostgresSink, PostgresSource


dlq = PostgresDLQSink(
    dsn="postgresql://app:secret@localhost:5432/app",
    table="ops.agora_dlq",
)

sink = PostgresSink(
    dsn="postgresql://app:secret@localhost:5432/app",
    table="processed_events",
    row_mapper=lambda record: record,
    conflict_key="id",
    poison_record_sink=dlq,
    poison_record_pipeline_id="orders-postgres",
)

summary = await (
    Pipeline(
        PostgresSource(
            dsn="postgresql://app:secret@localhost:5432/app",
            query="SELECT id, payload FROM inbox ORDER BY id",
            row_mapper=lambda row: row,
        )
    )
    .build(sink, config=DeliveryConfig(batch_size=100))
    .run()
)
```

Sink write failures are classified as schema drift, constraint violation, type
mismatch, or unknown. When a poison DLQ sink is configured, failed buffered rows
can be routed with that classification metadata.

## Observability

PostgreSQL components expose:

- source health snapshots and recovery-contract snapshots
- source/sink/DLQ metrics snapshots
- sink latency histograms for connect, pool acquire, and flush
- Prometheus text rendering through `PostgresPrometheusExporter`
- acceptance reports with threshold objects

## Production checklist

- Back every `conflict_key` with a real unique constraint or primary key.
- Use `copy_merge` for large upsert-heavy loads and `copy` only for
  append-only loads with `upsert=False`.
- Keep source queries ordered by checkpoint fields.
- Use `server_side` fetch for large reads that should not materialize the whole
  result client-side.
- Prefer `sslmode="verify-full"` unless deployment policy owns the override.
- Use `read_routing` and `max_replica_replay_lag_s` deliberately in HA setups.
- Use `PostgresSchemaAdapter` only when automatic additive schema changes are
  acceptable.
- Use PostgreSQL DLQ when operators need SQL filtering, retention, backup, and
  audit over failed records.

## Boundaries

PostgreSQL is the right fit when records are naturally relational, operators
debug through SQL, or replay state belongs in a database table.

It is the wrong fit when the workload is primarily event transport, when table
shape changes faster than migrations can govern it, or when the pipeline needs
a broker rather than an operational store.
