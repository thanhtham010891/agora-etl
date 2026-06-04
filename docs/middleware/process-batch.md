# ProcessBatchMiddleware

_Use this when: a Python-object list-batch transform is CPU-heavy, blocking, or should not share the main runtime process._

`ProcessBatchMiddleware` runs a synchronous `list[...] -> list[...]` transform
in a managed process pool while checkpointing, DLQ routing, and sink commits
stay in the main runtime process.

## What it does

- receives one Python `list` batch at a time
- sends that batch to a worker process
- runs the transform there
- returns the transformed batch to the main runtime for downstream handling

## When it is a good fit

- the source already emits list batches
- the transform is CPU-heavy Python code
- the transform uses blocking native libraries
- the pipeline still wants Python record objects rather than Arrow batches

## How it behaves

- one input batch maps to one worker invocation
- `fn` must be pickleable and importable by worker processes
- records inside the batch must be pickleable
- checkpoint advancement still waits for downstream write or DLQ handling
- timeout recycles the active worker-pool generation
- in ordered pipelined mode, unresolved sibling batches from the recycled
  generation fail instead of committing from stale worker state

## Example

```python
from agora import CsvSource, Pipeline, ProcessBatchMiddleware


def transform(batch: list[dict]) -> list[dict | None]:
    output: list[dict | None] = []
    for record in batch:
        if int(record["score"]) < 0:
            output.append(None)
            continue
        output.append({**record, "score": int(record["score"]) * 2})
    return output


pipeline = (
    Pipeline(CsvSource(path="data.csv", row_mapper=lambda row: row, emit_batches=True))
    .pipe(
        ProcessBatchMiddleware(
            fn=transform,
            max_workers=4,
            max_in_flight_batches=4,
            timeout_s=60,
            name="score_process",
        )
    )
)
```

## Practical limits in 0.3.x

- requires a batch-capable source
- pipelined execution only activates when `max_workers > 1` and
  `max_in_flight_batches > 1`
- pipelined commits currently require `ordered=True`
- this middleware is for Python object batches, not Arrow batches

## When to choose something else

- use [BatchMapMiddleware](batch-map.md) when the transform is not heavy enough
  to justify a process hop
- use [ArrowProcessBatchMiddleware](arrow-process-batch.md) when the pipeline
  can stay columnar

