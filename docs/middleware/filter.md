# FilterMiddleware

_Use this when: a per-record predicate should decide which records stay in the flow._

`FilterMiddleware` is the simplest way to drop records before they reach more
expensive middleware or sinks.

## What it does

- accepts a sync or async predicate
- keeps the record when the predicate is truthy
- drops the record when the predicate is falsy

## When it is a good fit

- discarding invalid or incomplete records early
- threshold filtering such as `score > 0.8`
- trimming the workload before enrichment, AI, or sink writes

## How it behaves

- records that fail the predicate are counted as dropped
- downstream middleware and sinks never see dropped records
- ordering of kept records is preserved

## Example

```python
from agora import FilterMiddleware, Pipeline


pipeline = (
    Pipeline(source)
    .pipe(FilterMiddleware(lambda r: r["status"] == "active", name="active_only"))
)
```

## When to choose something else

- use [MapMiddleware](map.md) when the stage transforms values
- use [BatchFilterMiddleware](batch-filter.md) when the source already emits
  list batches
- use [ArrowFilterMiddleware](arrow-filter.md) when the pipeline is Arrow-native

