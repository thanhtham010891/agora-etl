# Sinks

Sinks receive processed records and persist them. With `.build()` and `.fan_out()`, every sink sees every record. With `.route()`, each record goes to exactly one sink based on the routing predicate.

## Which sink to use

| Situation | Sink |
|---|---|
| Debugging or inspecting records locally | `StdoutSink` |
| Structured log output | `LogSink` |
| Append records to a JSONL file | `JsonLinesSink` |
| Write records to a CSV file | `CsvSink` |
| Write records to a Parquet file | `ParquetSink` |
| POST records to an HTTP endpoint | `WebhookSink` |
| Write to a database or custom store | Subclass `BaseSink` |
| Write to multiple destinations | `.fan_out()` |
| Route records to different sinks by value | `SinkRouter` |

## Built-in sinks

### StdoutSink

Print records to stdout. Use this during development to verify what the pipeline is producing before wiring a real sink.

```python
from agora.sinks.io.stdout import StdoutSink

sink = StdoutSink(prefix="[record] ")
```

### LogSink

Emit records via the structured logger.

```python
from agora.sinks.io.log import LogSink

sink = LogSink(level="info")
```

### JsonLinesSink

Write records as newline-delimited JSON (JSONL). Prefers `orjson` when available and falls back to stdlib `json`. The `agora-etl[file]` extra includes the faster path.

```python
from agora.sinks.file.jsonlines import JsonLinesSink

sink = JsonLinesSink(
    path="output/records.jsonl",
    serializer=lambda r: r.model_dump(),   # optional; defaults to model_dump / __dict__
    append=False,
    flush_every=100,
    encoding="utf-8",
)
```

### CsvSink

Write records as CSV using stdlib only. Keeps its file handle and writer open for the sink lifecycle — repeated flushes do not reopen the file on every batch.

```python
from agora.sinks.file.csv import CsvSink

sink = CsvSink(
    path="output/records.csv",
    row_mapper=lambda r: {"id": r.id, "name": r.name, "score": r.score},
    fieldnames=["id", "name", "score"],   # explicit column order
    append=False,
    flush_every=100,
    delimiter=",",
)
```

### ParquetSink

Write records incrementally to a Parquet file via PyArrow.

Requires: `pip install "agora-etl[file]"`

The schema is inferred from the first written batch and reused for all later flushes. Missing fields in later rows are written as `null`. New columns introduced after the first batch are not added automatically — if your records have variable shapes, normalize them in a middleware before they reach this sink.

```python
from agora.sinks.file.parquet import ParquetSink

sink = ParquetSink(
    path="output/records.parquet",
    row_mapper=lambda r: {"id": r.id, "name": r.name, "score": float(r.score)},
    batch_size=1000,
    compression="snappy",
)
```

### WebhookSink

POST records to an HTTP endpoint. Supports batch mode and automatic retry on 429/5xx responses.

```python
from agora.sinks.http.webhook import WebhookSink

sink = WebhookSink(
    url="https://api.example.com/ingest",
    headers={"Authorization": "Bearer my-token"},
    batch_mode=True,
    flush_every=50,
    max_retries=3,
)
```

## Custom sink

Subclass `BaseSink[T]` and implement `write()`:

```python
from agora.core.sink import BaseSink

class MyDatabaseSink(BaseSink[MyRecord]):
    sink_name = "my_database"

    async def open(self) -> None:
        self._conn = await connect(self._dsn)

    async def write(self, record: MyRecord) -> None:
        await self._conn.execute(
            "INSERT INTO records (id, name) VALUES ($1, $2)",
            record.id, record.name,
        )

    async def close(self) -> None:
        await self._conn.close()
```

For bulk inserts, override `write_batch()` and set `batch_writable_native = True` on the class:

```python
class MyDatabaseSink(BaseSink[MyRecord]):
    sink_name = "my_database"
    batch_writable_native = True

    async def write_batch(self, records: list[MyRecord]) -> None:
        await self._conn.executemany(
            "INSERT INTO records (id, name) VALUES ($1, $2)",
            [(r.id, r.name) for r in records],
        )
```

`batch_writable_native = True` tells the runtime to call `write_batch()` instead of individual `write()` calls. When there is only one sink in the pipeline, the runtime also takes a direct fast path that skips fan-out bookkeeping — so setting this flag on a single-destination sink is worth doing for any sink that benefits from bulk operations.

## Fan-out

Write each record to multiple sinks:

```python
summary = await (
    Pipeline(src)
    .fan_out([file_sink, webhook_sink], batch_size=50)
    .run()
)
```

## Routing

Route records to different sinks based on a predicate:

```python
from agora.core.sink import SinkRouter

router = (
    SinkRouter()
    .route(lambda r: r.region == "APAC", apac_sink)
    .route(lambda r: r.region == "EMEA", emea_sink)
    .default(fallback_sink)
)

summary = await Pipeline(src).route(router).run()
```

## Common mistakes

**Introducing new columns after the first ParquetSink flush.** The Parquet schema is locked after the first batch. If your `row_mapper` returns a dict with a new key on record 1001, that column is silently ignored. Normalize your output shape in a middleware before records reach `ParquetSink`.

**Not setting `batch_writable_native = True` when overriding `write_batch()`.** Implementing `write_batch()` alone is not enough — the runtime checks the class flag to decide which path to take. Without the flag, `write()` is called per record even if `write_batch()` exists.

**Expecting fan-out to be atomic.** `.fan_out()` writes to each sink independently. If the second sink fails after the first has already written, the first write is not rolled back. Design sinks to be idempotent if you need safe retries across a fan-out.
