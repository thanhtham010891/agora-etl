# BatchMapMiddleware

_Use this when: the source already emits Python list batches and the transform should stay on that batch lane._

`BatchMapMiddleware` keeps the pipeline on the list-batch path instead of
falling back to one middleware call per record.

## What it does

- accepts a sync or async callable
- applies that callable across the records in a list batch
- preserves `MapMiddleware` semantics: each element becomes a new element or
  `None`

## When it is a good fit

- `CsvSource(..., emit_batches=True)` or `JsonLinesSource(..., emit_batches=True)`
- list-batch transforms where reducing orchestration overhead matters
- workloads that are still most naturally expressed with Python objects, not
  Arrow batches

## How it behaves

- the batch shape stays `list[record]`
- each element is still handled independently for drop semantics
- async callables are gathered across the batch

## Example

```python
from agora import BatchMapMiddleware, CsvSource, Pipeline


pipeline = (
    Pipeline(CsvSource(path="data.csv", row_mapper=lambda row: row, emit_batches=True))
    .pipe(
        BatchMapMiddleware(
            lambda record: {**record, "score": int(record["score"]) * 100},
            name="scale_score",
        )
    )
)
```

## When to choose something else

- use [MapMiddleware](map.md) when the source is record-oriented
- use [ProcessBatchMiddleware](process-batch.md) when the batch transform is
  CPU-heavy or should be isolated in worker processes
- use [ArrowMapMiddleware](arrow-map.md) when the pipeline can stay columnar

