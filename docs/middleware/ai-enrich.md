# AIEnrichMiddleware

_Use this when: an LLM should add, rewrite, or infer fields on each record._

`AIEnrichMiddleware` calls a provider once per record, parses a JSON response,
and merges the returned fields back into the record.

## What it does

- renders a prompt from the current record
- calls the configured AI provider
- parses the response as JSON
- merges selected fields back into the record

## When it is a good fit

- summaries, tags, categories, or descriptions
- filling in missing structured fields from unstructured context
- record-by-record enrichment where the provider decision depends on the whole
  record

## How it behaves

- works with dicts, dataclasses, and Pydantic models
- `output_fields` can whitelist only some response keys
- caching is optional but recommended for repeated runs
- `on_error` controls whether failures pass through, drop, or raise

## Example

```python
from agora.ai import SQLiteLLMCache
from agora.middlewares.ai.enrich import AIEnrichMiddleware


pipeline.pipe(
    AIEnrichMiddleware(
        provider=my_provider,
        prompt_template=(
            "Summarize this product: {name}. "
            'Return JSON: {"summary": "...", "price_level": "..."}'
        ),
        output_fields=["summary", "price_level"],
        cache=SQLiteLLMCache(".cache/llm.db"),
    )
)
```

## When to choose something else

- use [AIExtractMiddleware](ai-extract.md) when the main job is extracting
  structured fields from one text field
- use [AIClassifyMiddleware](ai-classify.md) when the output is one label from a
  fixed category set
- use [AIBatchMiddleware](ai-batch.md) when provider cost dominates and records
  can be processed in grouped prompts

