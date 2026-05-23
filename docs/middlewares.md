# Middlewares

Middlewares are the composable transformation layer between source and sink. Each middleware receives a record, optionally transforms it, and returns the result — or `None` to drop the record.

## Built-in middlewares

### MapMiddleware

Apply a synchronous function to each record:

```python
from agora import MapMiddleware

pipeline.pipe(MapMiddleware(lambda r: r.model_copy(update={"score": r.score * 100})))
```

### FilterMiddleware

Drop records that do not match a predicate. Shorthand: `.filter()` on the pipeline:

```python
pipeline.filter(lambda r: r.score > 0.5)
```

### RetryMiddleware

Retry a middleware on exception with exponential backoff:

```python
from agora import RetryMiddleware

pipeline.pipe(RetryMiddleware(inner=my_middleware, max_retries=3))
```

### ValidateMiddleware

Validate records against a Pydantic model. Invalid records are dropped or routed to the DLQ depending on configuration:

```python
from agora.middlewares.validate import ValidateMiddleware

pipeline.pipe(ValidateMiddleware(schema=MyModel))
```

### EnrichMiddleware

Enrich records with data from an async callable:

```python
from agora.middlewares.enrich import EnrichMiddleware

async def fetch_metadata(record):
    meta = await metadata_api.get(record.id)
    return record.model_copy(update={"tags": meta.tags})

pipeline.pipe(EnrichMiddleware(enricher=fetch_metadata))
```

### DedupMiddleware

Drop duplicate records by a computed key:

```python
from agora.middlewares.dedup import DedupMiddleware
from agora.middlewares.dedup.stores.memory import InMemoryStore
from agora.middlewares.dedup.strategies.fuzzy import FuzzyMatchStrategy

# Exact dedup (default)
pipeline.pipe(DedupMiddleware(key=lambda r: r.id))

# Fuzzy dedup by name similarity
pipeline.pipe(DedupMiddleware(
    key=lambda r: r.name.lower(),
    store=InMemoryStore(),
    strategy=FuzzyMatchStrategy(threshold=0.85),
    max_fuzzy_keys=100_000,
))
```

Fuzzy dedup uses Jaro-Winkler similarity. Performance is O(n) per record up to `max_fuzzy_keys`. For large-scale fuzzy dedup, use a vector store plugin.

For distributed dedup across processes, use a Redis-backed store (available as a separate package).

## AI middlewares

All AI middlewares require an `AIProvider`. Failures default to `on_error="passthrough"` — the original record passes through unchanged so the pipeline never stops due to LLM errors.

### AIEnrichMiddleware

Add fields to each record using an LLM:

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

Classify each record into one of a fixed set of categories:

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

Extract structured fields from unstructured text:

```python
from agora.middlewares.ai.extract import AIExtractMiddleware

pipeline.pipe(AIExtractMiddleware(
    provider=my_provider,
    source_field="raw_text",
    output_fields=["price", "currency", "quantity"],
))
```

### AIValidateMiddleware

Validate records using an LLM and drop or flag invalid ones:

```python
from agora.middlewares.ai.validate import AIValidateMiddleware

pipeline.pipe(AIValidateMiddleware(
    provider=my_provider,
    prompt_template="Is this a valid address: {address}? Return JSON: {\"valid\": true/false}",
))
```

### AITranslateMiddleware

Translate text fields to a target language:

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

Amortize LLM costs by batching multiple records into a single API call. The LLM response must be a JSON array of the same length as the input batch.

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

Override `on_start()` and `on_stop()` for setup and teardown:

```python
    async def on_start(self, ctx: PipelineContext) -> None:
        self._client = await create_client()

    async def on_stop(self, ctx: PipelineContext) -> None:
        await self._client.close()
```

## Custom AI middleware

Subclass `AIMiddleware[T]`:

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
