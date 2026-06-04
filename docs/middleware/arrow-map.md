# ArrowMapMiddleware

_Use this when: the source already emits Arrow batches and the transform can stay fully vectorized in-process._

`ArrowMapMiddleware` is the easiest way to do a columnar transform without
materializing Python row objects.

## What it does

- accepts a `pa.RecordBatch -> pa.RecordBatch` callable
- runs on the Arrow execution lane
- keeps the data columnar through the middleware stage

## When it is a good fit

- casting columns
- arithmetic on numeric fields
- string normalization with `pyarrow.compute`
- adding derived columns while staying in-process

## How it behaves

- the transform must return another `pa.RecordBatch`
- row count may stay the same or change if the transform does so intentionally,
  but pure row-dropping is usually clearer in `ArrowFilterMiddleware`
- no process hop, so it is lighter than `ArrowProcessBatchMiddleware`

## Example

```python
import pyarrow as pa
import pyarrow.compute as pc

from agora import ArrowMapMiddleware


def scale_price(batch: pa.RecordBatch) -> pa.RecordBatch:
    idx = batch.schema.get_field_index("price")
    scaled = pc.multiply(pc.cast(batch.column(idx), pa.float64()), 100.0)
    return batch.set_column(idx, "price", scaled)


pipeline.pipe(ArrowMapMiddleware(scale_price, name="scale_price"))
```

## When to choose something else

- use [ArrowFilterMiddleware](arrow-filter.md) when the stage only builds a
  boolean mask and drops rows
- use [ArrowProcessBatchMiddleware](arrow-process-batch.md) when the transform
  is CPU-heavy or should run outside the main runtime process

