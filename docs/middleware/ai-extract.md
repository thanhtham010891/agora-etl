# AIExtractMiddleware

_Use this when: one text field contains multiple structured facts that should be pulled out into explicit fields._

`AIExtractMiddleware` reads one source field, asks the provider to extract a set
of named outputs, and merges those outputs into the record.

## What it does

- reads a chosen text field from the record
- builds an extraction prompt from the requested fields
- expects one JSON object back
- merges the extracted values into the record

## When it is a good fit

- extracting phone numbers, amenities, hours, price ranges, or named entities
- turning free-form descriptions into structured columns
- enrichment where the important input is one main text field

## Example

```python
from agora.middlewares.ai.extract import AIExtractMiddleware


pipeline.pipe(
    AIExtractMiddleware(
        provider=my_provider,
        source_field="raw_description",
        extract={
            "phone": "Phone number as string, null if not found",
            "amenities": "List of all mentioned amenities",
            "price_min_vnd": "Minimum price in VND as integer, null if not found",
        },
    )
)
```

## How it behaves

- empty source text passes through unchanged
- only requested fields are merged back
- `null` values can be returned when the provider cannot find a field
- `on_error` controls passthrough, drop, or raise behavior

## When to choose something else

- use [AIEnrichMiddleware](ai-enrich.md) when the prompt is broader than
  field-by-field extraction
- use [AIValidateMiddleware](ai-validate.md) when the goal is quality judgment,
  not extraction

