# AIValidateMiddleware

_Use this when: data quality judgment is hard to encode as normal rules and an LLM should decide whether a record looks valid._

`AIValidateMiddleware` is an LLM-based quality gate.

## What it does

- sends the record and quality criteria to the provider
- expects a structured verdict with `valid`, `issues`, and `confidence`
- handles invalid records according to `on_invalid`

## When it is a good fit

- spam or fake-data detection
- quality checks that depend on fuzzy judgment
- human-like validation rules that are awkward to encode as ordinary code

## Invalid-record policies

- `flag`: keep the record and attach validation metadata
- `drop`: remove the record
- `raise`: raise `DataQualityError`

## Example

```python
from agora.middlewares.ai.validate import AIValidateMiddleware


pipeline.pipe(
    AIValidateMiddleware(
        provider=my_provider,
        criteria="""
        Validate this record:
        - name must look real
        - rating must be between 1.0 and 5.0 when present
        - coordinates must be inside Vietnam
        """,
        min_confidence=0.85,
        on_invalid="flag",
    )
)
```

## Practical advice

- keep criteria explicit and narrow
- review flagged records before using `drop` or `raise` as a hard production
  gate
- cache repeated validation workloads when the same records may reappear

## When to choose something else

- use [ValidateMiddleware](validate.md) for deterministic schema validation
- use [FilterMiddleware](filter.md) for simple rule-based keep/drop logic

