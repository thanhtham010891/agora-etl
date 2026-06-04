# ArrowBatchMiddleware

_Use this when: a custom middleware should consume and return `pyarrow.RecordBatch` directly._

`ArrowBatchMiddleware` is the base class for custom Arrow-native stages.

## What it does

- defines the contract for Arrow-native batch processing
- keeps the pipeline on the Arrow execution lane
- avoids row materialization when the surrounding source, middleware, and sink
  are all Arrow-compatible

## Compatibility rule

An Arrow middleware chain must stay consistently Arrow-native.

- valid: Arrow source + only Arrow middleware
- valid: Arrow source + only Python-row middleware, with one materialization before the chain
- invalid: mixing Arrow middleware with Python-row or list-batch middleware in the same chain

That invalid mixed-chain shape now fails during planning with `PipelineError`.

## When it is a good fit

- the transform is naturally vectorized
- `pyarrow.compute` or Arrow-native logic is the right tool
- a built-in Arrow middleware is not expressive enough

## What a custom implementation looks like

Override `process_arrow_batch(batch, ctx)` and return another
`pa.RecordBatch`.

```python
import pyarrow as pa
import pyarrow.compute as pc

from agora import ArrowBatchMiddleware


class NormaliseScore(ArrowBatchMiddleware):
    name = "normalise_score"

    async def process_arrow_batch(self, batch, ctx):
        idx = batch.schema.get_field_index("score")
        normed = pc.divide(pc.cast(batch.column(idx), pa.float64()), 100.0)
        return batch.set_column(idx, "score", normed)
```

## When to choose something else

- use [ArrowMapMiddleware](arrow-map.md) for straightforward "batch in, batch
  out" transforms without creating a subclass
- use [ArrowFilterMiddleware](arrow-filter.md) when the stage only drops rows
- use [ArrowProcessBatchMiddleware](arrow-process-batch.md) when the Arrow
  transform is heavy enough to justify worker processes
