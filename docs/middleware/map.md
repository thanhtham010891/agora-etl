# MapMiddleware

_Use this when: each record needs a straightforward per-record transform._

`MapMiddleware` is the default "change one record into another record" stage in
Agora. It runs one callable per record and forwards the returned value
downstream.

## What it does

- accepts a sync or async callable
- receives one record at a time
- returns the transformed record
- drops the record if the callable returns `None`

## When it is a good fit

- field cleanup such as trimming strings or coercing types
- small derived fields such as slugs, normalized IDs, or computed scores
- lightweight async lookups that still belong on the per-record lane

## How it behaves

- records stay in source order
- exceptions follow normal middleware failure handling
- in buffered or batch-capable pipelines, simple sync map functions can still
  use the runtime's fast path without leaving the per-record programming model

## Example

```python
from agora import MapMiddleware, Pipeline


def normalise(record: dict) -> dict | None:
    if not record.get("name"):
        return None
    return {
        **record,
        "name": record["name"].strip().lower(),
        "score": float(record["score"]),
    }


pipeline = (
    Pipeline(source)
    .pipe(MapMiddleware(normalise, name="normalise"))
)
```

## When to choose something else

- use [FilterMiddleware](filter.md) when the stage only decides keep vs drop
- use [BatchMapMiddleware](batch-map.md) when the source already emits list
  batches and reducing per-record dispatch matters
- use [ArrowMapMiddleware](arrow-map.md) when the data is already in
  `pyarrow.RecordBatch` form and the transform is vectorizable

