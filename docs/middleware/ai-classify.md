# AIClassifyMiddleware

_Use this when: each record should be assigned one label from a known category set._

`AIClassifyMiddleware` classifies each record into one of the configured
categories.

## What it does

- builds text from selected record fields
- chooses one category
- writes the chosen label back into the record
- can also store a confidence score

## Two modes

- LLM mode: more flexible, usually slower and more expensive; requires a
  completion-capable provider
- embedding mode: cheaper and faster, but requires an embedding-capable
  provider

## When it is a good fit

- product, location, or event categorization
- routing records into known business buckets
- lightweight taxonomy assignment before sink writes

## Example

```python
from agora.middlewares.ai.classify import AIClassifyMiddleware


pipeline.pipe(
    AIClassifyMiddleware(
        provider=my_provider,
        source_fields=["name", "description"],
        categories=["restaurant", "hotel", "attraction", "cafe"],
        output_field="ai_category",
        confidence_field="ai_confidence",
    )
)
```

## Embedding mode

Use embedding mode when the provider supports embeddings and the category list
is stable:

```python
AIClassifyMiddleware(
    provider=my_provider,
    source_fields=["name", "description"],
    categories=["restaurant", "hotel", "attraction", "cafe"],
    output_field="ai_category",
    use_embeddings=True,
)
```

Completion-only providers such as Anthropic can still be used in ordinary LLM
mode, but they are rejected for `use_embeddings=True`.

## When to choose something else

- use [AIEnrichMiddleware](ai-enrich.md) when multiple output fields matter
- use [RouteMiddleware](route.md) when the category already exists in the record
  and only branching behavior is needed
