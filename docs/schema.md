# Schema

_When to read this: your records are semi-structured and you need to infer, persist, or constrain schema changes over time._

Agora ships an optional schema toolkit for pipelines that ingest semi-structured
records and need to track how those records change over time.

Use it when you want to:

- infer a table shape from sample records
- keep a persisted schema across runs
- enforce a schema-evolution policy in the middleware chain
- generate a Pydantic model from an observed schema

If your input schema is already fixed and known ahead of time, start with
`ValidateMiddleware` instead. Schema tools are more useful when the shape can
change between batches or over time.

## The main pieces

| Tool | What it does |
|---|---|
| `infer_schema()` | Infer a `Schema` from a list of records |
| `SchemaMiddleware` | Infer and evolve schema while the pipeline runs |
| `SchemaContract` | Decide how changes are handled |
| `BackendSchemaStore` | Persist schemas in a shared state backend |
| `InMemorySchemaStore` | Keep schemas only for the current process |
| `schema.to_pydantic_model()` | Generate a Pydantic model from the inferred schema |

## Inferring a schema from sample data

`infer_schema()` works with dicts, Pydantic models, and dataclass-like objects.

```python
from agora.schema import infer_schema

records = [
    {"id": 1, "name": "Alice", "score": 95.5},
    {"id": 2, "name": "Bob", "score": 87},
]

schema = infer_schema(records, table="students")

print(schema.table)          # students
print(schema.column_names()) # ['id', 'name', 'score']
```

Agora infers one of these core data types:

- `string`
- `integer`
- `float`
- `boolean`
- `timestamp`
- `json`
- `bytes`
- `null`

Type widening is intentionally simple:

- `null -> any type`
- `integer -> float`
- incompatible mixed types fall back to `string`

All inferred columns are nullable by default so sparse semi-structured payloads
remain writable.

## Tracking schema during a pipeline run

`SchemaMiddleware` is a passthrough middleware. Records continue downstream, but
the middleware observes their shape and keeps the latest `Schema` in
`ctx.extras["schema"]`.

```python
from agora import IterableSource, Pipeline
from agora.schema import SchemaMiddleware
from agora.sinks.io.stdout import StdoutSink

pipeline = (
    Pipeline(
        IterableSource(
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob", "score": 98},
            ]
        ),
        id="users",
    )
    .pipe(SchemaMiddleware(table="public.users"))
    .build(StdoutSink())
)
```

At runtime:

1. `on_start()` loads any previously saved schema from the configured store.
2. `process()` observes each record and applies the active contract.
3. `on_stop()` persists the latest schema and publishes schema metrics into
   `summary.metrics.by_middleware["schema"].schema`.

## Choosing a schema contract

`SchemaContract` controls what happens when the next record does not match the
current schema.

| Contract | Behavior |
|---|---|
| `EVOLVE` | Add new columns and widen compatible types |
| `FREEZE` | Reject schema changes |
| `DISCARD_COLUMN` | Strip unknown columns before the record continues |
| `DISCARD_ROW` | Drop the whole record when it no longer matches |

### `EVOLVE`

Best default when new columns may appear over time.

```python
from agora.schema import SchemaContract, SchemaMiddleware

SchemaMiddleware(table="users", contract=SchemaContract.EVOLVE)
```

Behavior:

- new columns are added to the schema
- `integer -> float` widens to `float`
- incompatible type conflicts widen to `string`

### `FREEZE`

Use this when the schema is already approved and any change should fail the run.

```python
from agora.schema import SchemaContract, SchemaMiddleware

SchemaMiddleware(table="users", contract=SchemaContract.FREEZE)
```

Behavior:

- the offending record is dropped
- the middleware remembers the schema error
- `on_stop()` raises `SchemaEvolutionError`

That deferred raise matters in long-running runs: downstream records may already
have been written before the run finishes with a schema error. Use `FREEZE` only
when that fail-at-shutdown behavior matches your operating model.

### `DISCARD_COLUMN`

Use this when you want to keep writing known fields and silently ignore new
columns.

```python
from agora.schema import SchemaContract, SchemaMiddleware

SchemaMiddleware(table="users", contract=SchemaContract.DISCARD_COLUMN)
```

Behavior:

- columns missing from the active schema are removed from the forwarded record
- the persisted schema does not grow to include those columns
- known columns can still widen or fall back to `string` if their types change

### `DISCARD_ROW`

Use this when partial writes are not acceptable and unexpected shape changes
should be dropped entirely.

```python
from agora.schema import SchemaContract, SchemaMiddleware

SchemaMiddleware(table="users", contract=SchemaContract.DISCARD_ROW)
```

Behavior:

- a record is dropped when it introduces a new column
- a record is also dropped when an existing column changes type
- the active schema is left unchanged

## Persisting schema across runs

To keep schema history between runs, give `SchemaMiddleware` a store.

### In-memory store

Good for tests and local experiments.

```python
from agora.schema import InMemorySchemaStore, SchemaMiddleware

SchemaMiddleware(
    table="users",
    store=InMemorySchemaStore(),
)
```

### Backend-backed store

`BackendSchemaStore` works with any `StateBackend`, including plugin backends.

```python
from agora.state import SQLiteBackend
from agora.schema import BackendSchemaStore, SchemaMiddleware

SchemaMiddleware(
    table="users",
    store=BackendSchemaStore(SQLiteBackend(".agora_schemas.db")),
)
```

The storage key is derived from both `pipeline_id` and `table`, so the same
table name in two different pipelines is stored separately.

Store load and save failures are logged as warnings and the pipeline continues.
If you need strict persistence semantics, treat those warnings as operational
alerts.

## Generating a Pydantic model

An inferred `Schema` can be turned into a Pydantic model for later validation or
typing work.

```python
from agora.schema import infer_schema

schema = infer_schema(
    [
        {"user-id": 42, "class": "vip"},
    ],
    table="raw-users",
)

UserRecord = schema.to_pydantic_model()
record = UserRecord.model_validate({"user-id": 42, "class": "vip"})

print(record.user_id)
print(record.class_)
```

Agora keeps original column names as aliases, so fields like `user-id` and
`class` still round-trip correctly with `model_dump(by_alias=True)`.

## Working with downstream sinks

Core Agora schema helpers are sink-agnostic. They tell you what the records look
like; destination-specific table creation or migration lives in plugin packages.

A common pattern is:

1. use `SchemaMiddleware` to observe and persist the schema
2. read `ctx.extras["schema"]` or the stored schema
3. hand that schema to a sink adapter from a plugin package

For example, the PostgreSQL plugin provides schema-aware adapters that can use
the inferred schema when writing to a table.

## Common limits and surprises

- `SchemaMiddleware` only understands record-like objects that expose fields as
  a dict, `model_dump()`, or `__dict__`. Opaque objects infer no columns.
- Nested dicts and lists are inferred as `json`. Agora does not recursively
  expand nested schemas in core.
- There is no built-in schema compaction, rename detection, or column removal
  workflow in core. Evolution is additive plus type widening.
- `SchemaMiddleware` is not part of the generic middleware registry. Import it
  from `agora.schema`.
