# BatchFilterMiddleware

_Use this when: the source emits Python list batches and filtering should stay on that batch lane._

`BatchFilterMiddleware` is the list-batch equivalent of `FilterMiddleware`.

## What it does

- accepts a sync or async predicate
- evaluates each record inside the list batch
- returns the batch with unmatched elements replaced by `None`

## When it is a good fit

- batch-capable file sources
- early filtering before heavy downstream work
- workloads where per-record middleware dispatch would be avoidable overhead

## How it behaves

- drop semantics stay per record, even though the stage runs batch-native
- the runtime preserves batch ordering
- async predicates are evaluated across the batch

## Example

```python
from agora import BatchFilterMiddleware, CsvSource, Pipeline


pipeline = (
    Pipeline(CsvSource(path="data.csv", row_mapper=lambda row: row, emit_batches=True))
    .pipe(BatchFilterMiddleware(lambda record: int(record["score"]) > 50, name="high_score"))
)
```

## When to choose something else

- use [FilterMiddleware](filter.md) for ordinary record-by-record pipelines
- use [ArrowFilterMiddleware](arrow-filter.md) for Arrow-native vectorized masks

