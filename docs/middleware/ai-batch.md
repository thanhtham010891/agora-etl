# AIBatchMiddleware

_Use this when: AI cost or latency dominates and multiple records can be processed in one grouped provider call._

`AIBatchMiddleware` buffers records, builds one prompt for many of them, and
matches the provider response back to the original records by index.

## What it does

- buffers records up to `batch_size`
- flushes either on size or `flush_timeout_ms`
- sends one provider call for the whole group
- expects a JSON array with the same length as the input batch

## When it is a good fit

- enrichment patterns that repeat across many similar records
- provider latency dominates the pipeline
- the prompt can naturally describe a list of records at once

## How it behaves

- uses a background flush task started by `on_start()`
- drains pending work on `on_stop()`
- falls back to per-record error handling if the grouped call fails

## Example

```python
import json

from agora.middlewares.ai.batch import AIBatchMiddleware


pipeline.pipe(
    AIBatchMiddleware(
        provider=my_provider,
        prompt_fn=lambda records: (
            f"Enrich these {len(records)} records. "
            "Return a JSON array with keys: summary, tags.\n"
            f"Input: {json.dumps(records, ensure_ascii=False)}"
        ),
        output_fields=["summary", "tags"],
        batch_size=20,
        flush_timeout_ms=500,
    )
)
```

## When to choose something else

- use the ordinary AI middleware classes when per-record prompts are easier to
  reason about
- use [ArrowProcessBatchMiddleware](arrow-process-batch.md) or
  [ProcessBatchMiddleware](process-batch.md) when the bottleneck is compute
  rather than provider round-trip cost
