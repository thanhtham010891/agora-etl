# Middleware Reference

_When to read this: you want the middleware catalog without opening one large page._

Middlewares run in registration order. They may:

- transform a record or batch and pass it downstream
- return `None` to drop a record
- raise to route that record or batch through the configured failure path

For runtime-wide guarantees around ordering, checkpointing, and failure
handling, see [Runtime Guarantees](../guides/runtime-guarantees.md).

## Per-record middlewares

- [MapMiddleware](map.md): transform one record into another record
- [FilterMiddleware](filter.md): keep or drop a record by predicate
- [RetryMiddleware](retry.md): retry another middleware before failure escapes
- [ValidateMiddleware](validate.md): validate or coerce records before they continue
- [EnrichMiddleware](enrich.md): fetch extra data or merge side lookups into each record
- [DedupMiddleware](dedup.md): drop duplicate records by key or fuzzy strategy
- [RouteMiddleware](route.md): dispatch records into different middleware branches

## Batch middlewares

- [BatchMapMiddleware](batch-map.md): transform a whole Python list batch in one call
- [BatchFilterMiddleware](batch-filter.md): filter a whole Python list batch in one call
- [ProcessBatchMiddleware](process-batch.md): run a Python-object batch transform in a process pool

## Arrow-native middlewares

- [ArrowBatchMiddleware](arrow-batch.md): base class for custom Arrow-native transforms
- [ArrowMapMiddleware](arrow-map.md): vectorized Arrow transform in the main runtime process
- [ArrowFilterMiddleware](arrow-filter.md): vectorized Arrow row filtering
- [ArrowProcessBatchMiddleware](arrow-process-batch.md): process-isolated Arrow-native batch transform

## Schema and AI

- [SchemaMiddleware](schema.md): track and constrain shape evolution
- [AI cache helpers](ai-cache-helpers.md): choose a cache for AI middlewares
- [AIEnrichMiddleware](ai-enrich.md): merge provider-generated fields back into each record
- [AIClassifyMiddleware](ai-classify.md): assign one label from a known category set
- [AIExtractMiddleware](ai-extract.md): pull structured values out of one text field
- [AIValidateMiddleware](ai-validate.md): ask an LLM to judge record quality
- [AITranslateMiddleware](ai-translate.md): translate selected fields into another language
- [AIBatchMiddleware](ai-batch.md): amortize provider cost by grouping many records in one call

## Writing a custom middleware

Subclass `Middleware[T, U]` and implement `process()`:

```python
from agora import Middleware, PipelineContext


class NormalizeMiddleware(Middleware[RawRecord, CleanRecord]):
    name = "normalize"

    async def process(self, record: RawRecord, ctx: PipelineContext) -> CleanRecord | None:
        if not record.name:
            return None
        return CleanRecord(
            id=record.id,
            name=record.name.strip().lower(),
        )
```

Use `on_start()` and `on_stop()` for setup and teardown, and raise to route
the failing record or batch through the configured failure policy.

## Writing a custom AI middleware

Subclass `AIMiddleware[T]` when the middleware needs prompt rendering,
provider calls, caching, and structured AI error handling:

```python
from agora.middlewares.ai.base import AIMiddleware


class SentimentMiddleware(AIMiddleware[Review]):
    name = "sentiment"

    async def process(self, record: Review, ctx: PipelineContext) -> Review | None:
        prompt = self._render_prompt(
            "Analyze sentiment of: {text}. Return JSON: {\"sentiment\": \"positive|negative|neutral\"}",
            record,
        )
        resp = await self._cached_complete(prompt, ctx=ctx)
        data = self._parse_json(resp.content)
        return record.model_copy(update=data)
```

Always pass `ctx=ctx` to `_cached_complete()` so AI metrics and tracing stay
correct.
