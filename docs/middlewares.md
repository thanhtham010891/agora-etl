# Middlewares

Middlewares are the transformation layer between source and sink. Each middleware receives a record, optionally transforms it, and returns the result — or `None` to drop the record.

## Middleware execution order

Middlewares run in the order you register them with `.pipe()`. The chain is linear: each middleware receives the output of the previous one.

```
record → middleware_1 → middleware_2 → middleware_3 → writer
```

If a middleware returns `None`, the record is dropped immediately. No subsequent middleware in the chain sees it, and it does not reach the sink.

If a middleware raises an exception, the chain stops for that record. The runtime calls `on_error()` on the raising middleware (default: log the error), then routes the record to the DLQ if one is configured. The pipeline continues with the next record — a single middleware failure does not abort the run.

`on_start()` is called on all middlewares in order before the pipeline loop begins. If one fails during startup, the runtime calls `on_stop()` on all already-started middlewares before propagating the error. `on_stop()` is called in reverse order after the loop ends, even if the run failed.

## Built-in middlewares

| Middleware | When to use |
|---|---|
| `MapMiddleware` | Apply a sync or async function to every record |
| `FilterMiddleware` | Drop records that don't match a predicate |
| `RetryMiddleware` | Wrap another middleware with exponential backoff retry |
| `ValidateMiddleware` | Validate records against a Pydantic model |
| `EnrichMiddleware` | Add fields via an async callable |
| `DedupMiddleware` | Drop duplicate records by a computed key |
| `RouteMiddleware` | Dispatch to different sub-middlewares by a key function |

### MapMiddleware

Apply a sync or async callable to each record. The shorthand `.filter()` on the pipeline uses `FilterMiddleware` internally.

```python
from agora import MapMiddleware

pipeline.pipe(MapMiddleware(lambda r: r.model_copy(update={"score": r.score * 100})))
```

`MapMiddleware` accepts both sync and async functions — it detects which at construction time.

### FilterMiddleware

Drop records that do not match a predicate. The pipeline's `.filter()` shorthand wraps this:

```python
pipeline.filter(lambda r: r.score > 0.5)

# equivalent to:
from agora import FilterMiddleware
pipeline.pipe(FilterMiddleware(lambda r: r.score > 0.5))
```

### RetryMiddleware

Wrap any middleware with exponential backoff retry. Retries on any exception by default; narrow the `exceptions` tuple to retry only specific error types.

```python
from agora import RetryMiddleware

pipeline.pipe(RetryMiddleware(
    inner=my_middleware,
    max_retries=3,
    backoff_base=2.0,
    exceptions=(httpx.HTTPError,),
))
```

The retry name in logs and metrics is `retry(<inner.name>)`.

### ValidateMiddleware

Validate records against a Pydantic model. Invalid records are dropped or routed to the DLQ depending on configuration.

```python
from agora.middlewares.validate import ValidateMiddleware

pipeline.pipe(ValidateMiddleware(schema=MyModel))
```

### EnrichMiddleware

Add or update fields on each record using an async callable.

```python
from agora.middlewares.enrich import EnrichMiddleware

async def fetch_metadata(record):
    meta = await metadata_api.get(record.id)
    return record.model_copy(update={"tags": meta.tags})

pipeline.pipe(EnrichMiddleware(enricher=fetch_metadata))
```

### DedupMiddleware

Drop duplicate records by a computed key. Exact dedup is the default; fuzzy dedup by string similarity is also available.

```python
from agora.middlewares.dedup import DedupMiddleware

# Exact dedup
pipeline.pipe(DedupMiddleware(key=lambda r: r.id))
```

```python
from agora.middlewares.dedup.stores.memory import InMemoryStore
from agora.middlewares.dedup.strategies.fuzzy import FuzzyMatchStrategy

# Fuzzy dedup by name similarity (Jaro-Winkler)
pipeline.pipe(DedupMiddleware(
    key=lambda r: r.name.lower(),
    store=InMemoryStore(),
    strategy=FuzzyMatchStrategy(threshold=0.85),
    max_fuzzy_keys=100_000,
))
```

Fuzzy dedup is O(n) per record up to `max_fuzzy_keys`. For large-scale fuzzy dedup across processes, use a Redis-backed store from `agora-etl-plugins`.

### RouteMiddleware

Dispatch each record to a different sub-middleware based on a key function. Useful when a single pipeline handles records from multiple sources that need different transformations.

```python
from agora.core.middleware import RouteMiddleware

pipeline.pipe(
    RouteMiddleware(key=lambda r: r.source)
    .route("source_a", NormalizerA())
    .route("source_b", NormalizerB())
    .default(FallbackNormalizer())
)
```

Records with no matching route and no default are dropped, and a warning is logged.

## AI middlewares

All AI middlewares require an `AIProvider`. By default, `on_error="passthrough"` — the original record passes through unchanged when an LLM call fails. Set `on_error="raise"` to stop the pipeline on LLM errors instead.

The three `on_error` values:

| Value | Behavior |
|---|---|
| `"passthrough"` | Original record passes through unchanged (default) |
| `"drop"` | Record is dropped and counted as `records_dropped` |
| `"raise"` | Exception propagates — routes to DLQ if configured, stops pipeline if not |

### AIEnrichMiddleware

Add fields to each record using an LLM. Use `LLMCache` to avoid re-calling the API for records you've already processed.

```python
from agora.middlewares.ai.enrich import AIEnrichMiddleware

pipeline.pipe(AIEnrichMiddleware(
    provider=my_provider,
    prompt_template="Summarize this product: {name}. Return JSON: {\"summary\": \"...\"}",
    output_fields=["summary"],
    cache=LLMCache(".cache/llm.db"),
))
```

### AIClassifyMiddleware

Classify each record into one of a fixed set of categories.

```python
from agora.middlewares.ai.classify import AIClassifyMiddleware

pipeline.pipe(AIClassifyMiddleware(
    provider=my_provider,
    source_fields=["name", "description"],
    categories=["restaurant", "hotel", "attraction", "cafe"],
    output_field="category",
))
```

### AIExtractMiddleware

Extract structured fields from unstructured text.

```python
from agora.middlewares.ai.extract import AIExtractMiddleware

pipeline.pipe(AIExtractMiddleware(
    provider=my_provider,
    source_field="raw_text",
    output_fields=["price", "currency", "quantity"],
))
```

### AIValidateMiddleware

Validate records using an LLM and drop or flag invalid ones.

```python
from agora.middlewares.ai.validate import AIValidateMiddleware

pipeline.pipe(AIValidateMiddleware(
    provider=my_provider,
    prompt_template="Is this a valid address: {address}? Return JSON: {\"valid\": true/false}",
))
```

### AITranslateMiddleware

Translate text fields to a target language.

```python
from agora.middlewares.ai.translate import AITranslateMiddleware

pipeline.pipe(AITranslateMiddleware(
    provider=my_provider,
    source_field="description",
    target_language="English",
    output_field="description_en",
))
```

### AIBatchMiddleware

Amortize LLM costs by batching multiple records into a single API call. This is the only built-in middleware that activates buffered execution mode — the runtime will process records concurrently up to `batch_size` to keep the LLM call full.

The LLM response must be a JSON array of the same length as the input batch.

`flush_timeout_ms` controls how long the middleware waits for a full batch before sending a partial one. When the source slows down or the pipeline is near the end of its records, the buffer may not fill to `batch_size`. After `flush_timeout_ms` milliseconds, whatever is in the buffer is sent as-is. Set it lower (e.g. 200ms) for latency-sensitive pipelines, higher (e.g. 2000ms) to maximize batch fill rate.

```python
from agora.middlewares.ai.batch import AIBatchMiddleware

pipeline.pipe(AIBatchMiddleware(
    provider=my_provider,
    prompt_fn=lambda records: (
        f"Enrich {len(records)} records. "
        f"Return a JSON array of same length. Input: {json.dumps(records)}"
    ),
    output_fields=["summary", "tags"],
    batch_size=20,
    flush_timeout_ms=500,
))
```

## Custom middleware

Subclass `Middleware[T, U]` and implement `process()`:

```python
from agora.core.middleware import Middleware
from agora.core.context import PipelineContext

class NormalizeMiddleware(Middleware[RawRecord, CleanRecord]):
    name = "normalize"

    async def process(self, record: RawRecord, ctx: PipelineContext) -> CleanRecord | None:
        if not record.name:
            return None   # drop the record
        return CleanRecord(
            id=record.id,
            name=record.name.strip().lower(),
        )
```

Return `None` to drop the record. Raise an exception to route it to the DLQ.

Use `on_start()` and `on_stop()` for setup and teardown:

```python
    async def on_start(self, ctx: PipelineContext) -> None:
        self._client = await create_client()

    async def on_stop(self, ctx: PipelineContext) -> None:
        await self._client.close()
```

## Custom AI middleware

Subclass `AIMiddleware[T]` to get access to the provider, prompt rendering, response caching, and error handling helpers:

```python
from agora.middlewares.ai.base import AIMiddleware

class SentimentMiddleware(AIMiddleware[Review]):
    name = "sentiment"

    async def process(self, record: Review, ctx: PipelineContext) -> Review | None:
        try:
            prompt = self._render_prompt(
                "Analyze sentiment of: {text}. Return JSON: {\"sentiment\": \"positive|negative|neutral\"}",
                record,
            )
            resp = await self._cached_complete(prompt, ctx=ctx)
            data = self._parse_json(resp.content)
            return record.model_copy(update=data)
        except Exception as exc:
            return await self._handle_error(exc, record, ctx)
```

Always pass `ctx=ctx` to `_cached_complete()` — it is required for AI metrics tracking.
