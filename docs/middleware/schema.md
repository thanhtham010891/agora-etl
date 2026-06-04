# SchemaMiddleware

_Use this when: record shape evolves over time and the pipeline should observe, constrain, or persist that evolution._

`SchemaMiddleware` lives in `agora.schema`, but it behaves like a normal
middleware in the pipeline chain.

## What it does

- observes incoming records
- infers or evolves a schema incrementally
- publishes the current schema into `ctx.extras`
- optionally loads and saves schema state through a store

## When it is a good fit

- incoming payload shape changes over time
- downstream systems need schema visibility
- the pipeline should enforce a schema contract such as evolve vs stricter modes

## How it behaves

- the record itself normally passes through unchanged
- records may be dropped when the configured schema contract rejects them
- schema information is available to later stages such as schema-aware sinks
- on shutdown, the latest schema can be persisted

## Example

```python
from agora.schema import SchemaContract, SchemaMiddleware


pipeline.pipe(
    SchemaMiddleware(
        table="public.users",
        contract=SchemaContract.EVOLVE,
    )
)
```

## How it differs from ValidateMiddleware

- `ValidateMiddleware` checks whether one record is valid now
- `SchemaMiddleware` tracks how the overall record shape changes over time

See [Schema](../schema.md) for contracts, stores, adapters, and generated
schema metadata.

