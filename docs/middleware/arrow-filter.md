# ArrowFilterMiddleware

_Use this when: row-dropping can be expressed as a vectorized Arrow boolean mask._

`ArrowFilterMiddleware` is the Arrow-native filtering stage for columnar
pipelines.

## What it does

- receives a `pa.RecordBatch`
- builds a `pa.BooleanArray` mask from that batch
- returns a filtered `pa.RecordBatch`

## When it is a good fit

- filtering by numeric thresholds
- filtering by string equality or set membership
- trimming data before an expensive Arrow or process-isolated transform

## How it behaves

- keeps the pipeline on the Arrow lane
- drops rows inside the batch rather than dropping the whole batch object
- if the filtered result has zero rows, the downstream stage sees an empty batch

## Example

```python
import pyarrow.compute as pc

from agora import ArrowFilterMiddleware


pipeline.pipe(
    ArrowFilterMiddleware(
        lambda batch: pc.greater(batch.column("price"), 0.0),
        name="positive_price_only",
    )
)
```

## When to choose something else

- use [FilterMiddleware](filter.md) for ordinary Python-record pipelines
- use [BatchFilterMiddleware](batch-filter.md) for list-batch pipelines
- use [ArrowMapMiddleware](arrow-map.md) when the stage transforms columns
  instead of filtering rows

