# AITranslateMiddleware

_Use this when: selected record fields should be translated into another language._

`AITranslateMiddleware` translates one or more fields and writes the translated
values back into the record.

## What it does

- gathers the requested source fields
- builds one translation prompt for those fields
- expects a JSON object keyed by the same field names
- writes translated values either in place or under a prefix

## When it is a good fit

- localizing names or descriptions
- keeping original text and translated text side by side
- normalizing multilingual inputs before indexing or analytics

## Example

```python
from agora.middlewares.ai.translate import AITranslateMiddleware


pipeline.pipe(
    AITranslateMiddleware(
        provider=my_provider,
        fields=["name", "description"],
        target_lang="vi",
        source_lang="auto",
        output_prefix="vi_",
    )
)
```

## Field naming

- `output_prefix=""` overwrites the original fields
- `output_prefix="vi_"` writes new fields such as `vi_name` and
  `vi_description`

## When to choose something else

- use [AIEnrichMiddleware](ai-enrich.md) when the provider should add new facts,
  not only translate
- use [AIBatchMiddleware](ai-batch.md) when many similarly shaped records should
  amortize provider cost through grouped prompts

