# Sources

Sources emit records via an async generator. The pipeline consumes them one at a time.

## Built-in sources

### JsonLinesSource

Stream records from a JSONL (newline-delimited JSON) file.

```python
from agora.sources.file.jsonlines import JsonLinesSource
from agora.core.types import SourceRecordFailurePolicy

source = JsonLinesSource(
    path="data/events.jsonl",
    row_mapper=lambda row: Event(**row),
    encoding="utf-8",
    batch_size=1000,
    on_record_error=SourceRecordFailurePolicy.LOG_AND_CONTINUE,
)
```

Supports checkpointing — resumes from the last processed line number after a restart.

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

### ParquetSource

Stream records from a Parquet file using PyArrow. Reads in batches to avoid loading the entire file into memory.

Requires: `pip install agora-etl[file]`

```python
from agora.sources.file.parquet import ParquetSource

source = ParquetSource(
    path="data/sales.parquet",
    row_mapper=lambda row: SalesRecord(**row),
    batch_size=1000,
)
```

Supports checkpointing by row number.

### HTTPSource

Abstract base for HTTP polling sources. Handles rate limiting, retries, circuit breaking, and response caching. Override `fetch_batch()` only.

```python
from agora.sources.http.http import HTTPSource, StopFetching
from agora.sources._internal.circuit_breaker import CircuitBreakerConfig

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

## Custom source

Subclass `BaseSource[T]` and implement `stream()`:

```python
from agora.core.source import BaseSource, SourceRecordError
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

Raise `SourceRecordError` to route a single bad record to the DLQ without stopping the pipeline.

## Checkpointable source

Implement `current_checkpoint()` and `prepare_resume()` to support resumable pipelines:

```python
class MyCheckpointableSource(BaseSource[MyRecord]):
    source_name = "my_source"

    def current_checkpoint(self) -> dict | None:
        return {"cursor": self._last_cursor}

    async def prepare_resume(self, checkpoint) -> None:
        if checkpoint:
            self._last_cursor = checkpoint.value["cursor"]
```
