# RouteMiddleware

_Use this when: one pipeline ingests mixed record families that need different middleware branches._

`RouteMiddleware` dispatches each record to one sub-middleware based on a key
function.

## What it does

- computes a route key from the record
- selects a registered middleware branch for that key
- optionally falls back to a default branch
- drops unmatched records when no default branch exists

## When it is a good fit

- one source produces multiple event types
- different upstream systems share a pipeline but need different normalization
- the branch decision belongs inside the middleware chain rather than at the
  sink level

## How it behaves

- branch middleware gets normal startup and shutdown hooks
- metrics are recorded against the branch middleware, not only the router
- unmatched records log a warning and are dropped if there is no default

## Example

```python
from agora import RouteMiddleware


pipeline.pipe(
    RouteMiddleware(key=lambda record: record["kind"], name="kind_router")
    .route("order", OrderNormalizer())
    .route("refund", RefundNormalizer())
    .default(FallbackNormalizer())
)
```

## When to choose something else

- use `.route()` at the sink layer when the decision is "which destination
  should receive this record?"
- use ordinary `MapMiddleware` or `FilterMiddleware` when all records share the
  same transform logic

