# Sinks

Sinks receive processed records and persist them. The pipeline fans out to all registered sinks — every sink sees every record.

## Built-in sinks

### JsonLinesSink

Write records as newline-delimited JSON (JSONL).

Prefers `orjson` when available and falls back to stdlib `json`. The
`agora-etl[file]` extra includes that faster path.

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

Write records as CSV. Uses stdlib only.

`CsvSink` keeps its file handle and writer open for the sink lifecycle, so
repeated flushes do not reopen the file on every batch.

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

The Parquet schema is inferred from the first written batch and then reused for
later flushes. Missing fields in later rows are written as `null`. New columns
introduced after the first batch are not added automatically.

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

POST records to an HTTP endpoint. Supports batch mode and retry on 429/5xx.

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

### StdoutSink

Print records to stdout. Useful for development and debugging.

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

Override `write_batch()` for bulk inserts:

```python
    async def write_batch(self, records: list[MyRecord]) -> None:
        await self._conn.executemany(
            "INSERT INTO records (id, name) VALUES ($1, $2)",
            [(r.id, r.name) for r in records],
        )
```

Set `batch_writable_native = True` on the class to tell the runtime to prefer `write_batch()` over individual `write()` calls.

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
