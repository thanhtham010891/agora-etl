# EnrichMiddleware

_Use this when: a record needs more data from an API, database, cache, or service call._

`EnrichMiddleware` is the per-record integration stage for side lookups.

## What it does

- calls an enricher for each record
- lets the enricher return an updated record
- passes the original record through unchanged when the enricher returns `None`
- supports sync callables, async callables, and callable objects with state

## When it is a good fit

- API lookups such as geocoding or metadata fetches
- cache-backed enrichment
- database lookups keyed by one field in the record

## How it behaves

- `on_error="passthrough"` keeps the original record on enrichment failure
- `on_error="drop"` removes the record
- `on_error="raise"` lets the failure propagate
- stateful enrichers can define `on_start()` and `on_stop()` hooks

## Example

```python
from agora.middlewares.enrich import EnrichMiddleware


async def fetch_metadata(record, ctx):
    meta = await metadata_api.get(record["id"])
    return {**record, "tags": meta["tags"], "region": meta["region"]}


pipeline.pipe(EnrichMiddleware(enricher=fetch_metadata))
```

## Practical advice

- pair it with [RetryMiddleware](retry.md) for flaky upstream services
- keep enrichment idempotent when possible
- prefer [ProcessBatchMiddleware](process-batch.md) or
  [ArrowProcessBatchMiddleware](arrow-process-batch.md) when the bottleneck is
  CPU-heavy computation rather than remote I/O

