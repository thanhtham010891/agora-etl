# ValidateMiddleware

_Use this when: records should satisfy a schema or custom validation rule before continuing downstream._

`ValidateMiddleware` is the main per-record data-shape gate in Agora.

## What it does

- validates a record against a Pydantic model, or
- runs a custom validator callable
- returns the validated or coerced value
- drops, logs, or raises on invalid records depending on `on_error`

## When it is a good fit

- records arrive as loose dicts and should become typed models
- downstream sinks expect a stable schema
- malformed data should be removed before enrichment or writing

## How it behaves

- `schema=...` uses Pydantic validation
- `validator=...` gives full custom control
- `on_error="drop"` silently removes invalid records
- `on_error="log"` logs and drops invalid records
- `on_error="raise"` makes validation failure terminal for that record flow

## Example: Pydantic validation

```python
from agora.middlewares.validate import ValidateMiddleware


pipeline.pipe(ValidateMiddleware(schema=MyModel))
```

## Example: custom validator

```python
from agora.middlewares.validate import ValidateMiddleware


def validate_order(record: dict) -> dict | None:
    if not record.get("order_id"):
        return None
    return {**record, "amount": float(record["amount"])}


pipeline.pipe(ValidateMiddleware(validator=validate_order, on_error="log"))
```

## When to choose something else

- use [SchemaMiddleware](schema.md) when the goal is to observe or evolve schema
  over time, not only validate a single record
- use [FilterMiddleware](filter.md) for simple keep/drop predicates that do not
  need validation semantics

