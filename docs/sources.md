# Sources

_When to read this: you need to choose or build the upstream side of a pipeline, especially around batching, Arrow, and resume support._

Sources emit records via an async generator. The pipeline consumes them one at a time.

## Which source to use

| Situation | Source |
|---|---|
| Reading a JSONL file | `JsonLinesSource` |
| Reading a CSV or TSV file | `CsvSource` |
| Reading a Parquet file | `ParquetSource` |
| CSV/JSONL with zero per-row allocation (Arrow) | `ArrowCsvSource` / `ArrowJsonLinesSource` |
| Polling an HTTP API | `HTTPSource` (subclass) |
| Custom data origin | Subclass `BaseSource` |
| Need resume after restart | Use any source with `supports_checkpoint = True` |

## Checkpoint support at a glance

Checkpointing is opt-in per source. A source must explicitly set `supports_checkpoint = True`, implement `current_checkpoint()`, and restore state in `prepare_resume()`.

For the full per-source contract (resume keys, granularity, known limitations), see the [Recovery Support Matrix](guides/recovery-matrix.md). Quick reference:

| Source | Checkpoint support | Resume position |
|---|---|---|
| `JsonLinesSource` | Yes | line number |
| `CsvSource` | Yes | row number |
| `ParquetSource` | Yes | row number |
| `HTTPSource` | Not by default | custom, source-specific |

Do not assume a source is resumable unless its docs say so.

## Built-in sources

### JsonLinesSource

Stream records from a JSONL (newline-delimited JSON) file. Uses stdlib `json` by default and switches to `orjson` automatically when the `agora-etl[file]` extra is installed.

```python
from agora.sources.file.jsonlines import JsonLinesSource
from agora import SourceRecordFailurePolicy

source = JsonLinesSource(
    path="data/events.jsonl",
    row_mapper=lambda row: Event(**row),
    encoding="utf-8",
    batch_size=1000,
    on_record_error=SourceRecordFailurePolicy.LOG_AND_CONTINUE,
)
```

Supports checkpointing — resumes from the last processed line number after a restart.

Set `emit_batches=True` to route through the batch execution lane (see
[Batch execution lane](#batch-execution-lane) below) — the source yields
`list` batches of `emit_batch_size` records instead of one record at a time,
which avoids per-record runtime overhead.

### CsvSource

Stream records from a CSV or TSV file using the stdlib `csv` module.

```python
from agora.sources.file.csv import CsvSource

source = CsvSource(
    path="data/products.csv",
    row_mapper=lambda row: Product(
        id=row["id"],
        name=row["name"],
        price=float(row["price"]),
    ),
    delimiter=",",
    has_header=True,
    encoding="utf-8-sig",   # strips Excel BOM
)
```

Supports checkpointing by row number.

Like `JsonLinesSource`, `CsvSource` accepts `emit_batches=True` (and
`emit_batch_size`, default `5000`) to participate in the
[batch execution lane](#batch-execution-lane).

### ParquetSource

Stream records from a Parquet file using PyArrow. Reads in batches internally to avoid loading the entire file into memory, but exposes row-oriented dicts to `row_mapper`.

Requires: `pip install "agora-etl[file]"`

```python
from agora.sources.file.parquet import ParquetSource

source = ParquetSource(
    path="data/sales.parquet",
    row_mapper=lambda row: SalesRecord(**row),
    batch_size=1000,
)
```

Supports checkpointing by row number.

Pass `use_arrow_batches=True` to emit `pa.RecordBatch` objects directly through
the batch lane with zero per-row Python allocation (`row_mapper` is bypassed).
When paired with an Arrow-native sink like `ParquetSink`, the runtime takes the
Arrow fast path. See [Batch execution lane](#batch-execution-lane).

### Batch execution lane

File sources can opt into the batch execution lane, which processes records in
batches instead of one at a time — eliminating per-record runtime orchestration
when the source and middleware chain are naturally batch-oriented.

| Source | Flag | Batch shape | row_mapper |
|---|---|---|---|
| `CsvSource` / `JsonLinesSource` | `emit_batches=True` | `list[T]` (mapped records) | applied per row |
| `ParquetSource` | `use_arrow_batches=True` | `pa.RecordBatch` | bypassed |
| `ArrowCsvSource` / `ArrowJsonLinesSource` | always on | `pa.RecordBatch` | bypassed |

```python
from agora import DeliveryConfig
from agora.sources.file.csv import CsvSource

source = CsvSource(path="data/products.csv", row_mapper=lambda r: r, emit_batches=True)
summary = await Pipeline(source).build(sink, config=DeliveryConfig(batch_size=5000)).run()
```

Checkpointing, DLQ routing, and ordering guarantees are all preserved — the
checkpoint advances once per batch, after the batch is durably written. To
transform records on the batch lane without falling back to per-record
dispatch, use `BatchMapMiddleware` / `BatchFilterMiddleware` (see
[middlewares](middlewares.md)).

### ArrowCsvSource / ArrowJsonLinesSource

Arrow-native file readers that emit `pa.RecordBatch` objects directly — no
`row_mapper`, no per-row Python dict allocation. Use these when you want
columnar processing and your downstream logic can stay Arrow-native.

Requires: `pip install "agora-etl[file]"`

```python
from agora.sources.file.csv import ArrowCsvSource
from agora.sources.file.jsonlines import ArrowJsonLinesSource
# or simply:
from agora import ArrowCsvSource, ArrowJsonLinesSource

csv_src  = ArrowCsvSource(path="data/products.csv", batch_size=65_536)
jsonl_src = ArrowJsonLinesSource(path="data/events.jsonl", batch_size=65_536)
```

Both sources set `emits_arrow_batches=True` and `supports_batch_emit=True`, so the runtime automatically selects the Arrow execution lane. Pair them with `ArrowMapMiddleware`/`ArrowFilterMiddleware` and an Arrow-native sink (e.g. `ParquetSink`) to keep data columnar end-to-end:

```python
import pyarrow.compute as pc
import pyarrow as pa
from agora import ArrowCsvSource, ArrowMapMiddleware, ArrowFilterMiddleware, Pipeline
from agora.sinks.file.parquet import ParquetSink

def scale_price(batch):
    idx = batch.schema.get_field_index("price")
    return batch.set_column(idx, "price",
        pc.multiply(pc.cast(batch.column(idx), pa.float64()), 100.0))

summary = await (
    Pipeline(ArrowCsvSource(path="data/products.csv"))
    .pipe(ArrowMapMiddleware(scale_price))
    .pipe(ArrowFilterMiddleware(lambda b: pc.greater(b.column("price"), 0.0)))
    .build(ParquetSink(path="out.parquet"))
    .run()
)
```

The important constraint is not just the source. To stay on the Arrow fast
path, you also need Arrow-native middleware and an Arrow-native sink. If you
insert a regular `MapMiddleware` or `FilterMiddleware`, the runtime materialises
rows and continues on the batch lane without the fully columnar write path.

**Constraints:**
- `row_mapper` is not called — the batch is passed as-is to the middleware/sink.
- Checkpointing tracks row count but does not support mid-file resume (no row-skip on restart).
- `pyarrow.csv` infers column types automatically (numbers become int/float, not strings). Use `pyarrow.csv.ConvertOptions` in a subclass if you need explicit schema control.
- Only works with Arrow-native middleware (`ArrowBatchMiddleware` subclasses) and Arrow-native sinks. Regular `MapMiddleware`/`FilterMiddleware` force `to_pylist()` and lose the throughput advantage.

### HTTPSource

Abstract base for HTTP polling sources. Handles rate limiting, retries, circuit breaking, and response caching. Override `fetch_batch()` only — the base class owns the rest.

```python
from agora.sources.http.http import HTTPSource, StopFetching, CircuitBreakerConfig

class PostsSource(HTTPSource[Post]):
    source_name = "posts_api"

    def __init__(self) -> None:
        super().__init__(
            base_url="https://api.example.com",
            requests_per_second=5.0,
            max_retries=3,
            cache_ttl_seconds=3600,
            circuit_breaker=CircuitBreakerConfig(failure_threshold=5),
        )
        self._page = 1

    async def fetch_batch(self):
        resp = await self.get("/posts", params={"page": self._page})
        items = resp.json()["items"]
        if not items:
            raise StopFetching
        for item in items:
            yield Post(**item)
        self._page += 1
```

Available request methods: `self.get()`, `self.post()`. Both are rate-limited, retried, and optionally cached.

`HTTPSource` does not define a checkpoint contract. If you need resumability, implement it explicitly in the subclass using a cursor, page token, or watermark, then set `supports_checkpoint = True` and implement `current_checkpoint()` and `prepare_resume()`.

## Custom source

Subclass `BaseSource[T]` and implement `stream()`:

```python
from agora import BaseSource, SourceRecordError
from collections.abc import AsyncGenerator

class MySource(BaseSource[MyRecord]):
    source_name = "my_source"

    async def open(self) -> None:
        self._client = await create_client()

    async def close(self) -> None:
        await self._client.close()

    async def stream(self) -> AsyncGenerator[MyRecord, None]:
        async for raw in self._client.fetch():
            try:
                yield MyRecord.from_dict(raw)
            except Exception as exc:
                raise SourceRecordError(exc, record=raw)
```

Raise `SourceRecordError` to route a single bad record to the DLQ without stopping the pipeline. Do not raise a plain exception from `stream()` unless you want the entire run to abort.

## Checkpointable source

Set `supports_checkpoint = True` and implement both hooks:

```python
class MyCheckpointableSource(BaseSource[MyRecord]):
    source_name = "my_source"
    supports_checkpoint = True

    def current_checkpoint(self) -> dict | None:
        return {"cursor": self._last_cursor}

    async def prepare_resume(self, checkpoint) -> None:
        if checkpoint:
            self._last_cursor = checkpoint.value["cursor"]
```

## Common mistakes

**Assuming HTTPSource is resumable.** `HTTPSource` does not implement checkpoint hooks. If your HTTP source needs to resume from a page token or cursor after a restart, you must implement `current_checkpoint()` and `prepare_resume()` in your subclass and set `supports_checkpoint = True`. Without this, a restart replays from the beginning.

**Raising a plain exception instead of `SourceRecordError` for bad records.** A plain exception raised from `stream()` aborts the entire run. `SourceRecordError` routes only that record to the DLQ and lets the pipeline continue. Use `SourceRecordError` for per-record parse or validation failures; let genuine infrastructure errors propagate as plain exceptions.

**Forgetting `supports_checkpoint = True`.** Implementing `current_checkpoint()` and `prepare_resume()` is not enough on its own. The runtime checks `supports_checkpoint` before touching the checkpoint store. If the flag is missing, the source runs normally but the checkpoint is never saved or restored.
