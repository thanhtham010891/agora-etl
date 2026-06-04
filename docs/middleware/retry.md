# RetryMiddleware

_Use this when: another middleware can fail transiently and should retry before the record reaches DLQ or terminal failure handling._

`RetryMiddleware` wraps one inner middleware and re-runs it with exponential
backoff when selected exceptions are raised.

## What it does

- wraps one middleware
- retries only the configured exception types
- backs off between attempts using `backoff_base ** attempt`
- re-raises the final failure when retries are exhausted

## When it is a good fit

- flaky HTTP enrichment
- temporary database or cache connectivity errors
- provider calls that often recover after a short wait

## How it behaves

- the wrapped middleware keeps its own `on_start()` and `on_stop()` lifecycle
- a successful retry returns the recovered record as if the failure never
  happened
- a permanently failing record continues through the normal failure path after
  retries are exhausted

## Example

```python
import httpx

from agora import RetryMiddleware


pipeline.pipe(
    RetryMiddleware(
        inner=my_middleware,
        max_retries=3,
        backoff_base=2.0,
        exceptions=(httpx.HTTPError,),
    )
)
```

## When to choose something else

- use a DLQ when failures should be inspected and replayed later instead of
  retried inline
- use [ProcessBatchMiddleware](process-batch.md) or
  [ArrowProcessBatchMiddleware](arrow-process-batch.md) when the issue is CPU
  isolation, not transient failure

