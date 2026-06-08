# Checkpointing

_When to read this: you have a pipeline that reads large files or long streams and you need it to resume from where it left off after a crash or restart._

## What checkpointing does

Without checkpointing, every pipeline run starts from the beginning of its source. If your source is a 10 GB Parquet file and the pipeline crashes at record 800,000, the next run re-reads from record 0.

Checkpointing solves this by periodically saving the source's current position to a store. On the next run, the source loads that position and skips ahead. The position is source-specific — for a file source it might be a byte offset or row index; for a streaming source it might be a cursor or sequence number.

Checkpointing is opt-in. If your source is cheap to re-read (a small API call, an in-memory list), skip it.

## Which sources support it

A source supports checkpointing when it implements the `CheckpointableSource` protocol and sets `supports_checkpoint = True`. The built-in sources that do this are `CsvSource`, `JsonLinesSource`, and `ParquetSource`. For the full per-source resume contract (resume keys, granularity, edge cases), see the [Recovery Support Matrix](recovery-matrix.md).

You can verify at runtime:

```python
from agora.core.checkpoint import is_checkpoint_capable

source = CsvSource("events.csv")
print(is_checkpoint_capable(source))  # True
```

If you pass a checkpoint store to a pipeline whose source returns `False` here, the pipeline logs a warning and runs without checkpointing — it does not raise.

## Inspecting resume state from the CLI

Once a pipeline has written at least one checkpoint, the CLI can explain what
that saved value means in operational terms:

```bash
agora checkpoint inspect nightly_events --store sqlite:/var/lib/myapp/checkpoints.db
```

The output includes:

- the raw saved value
- whether the source actually supports resume
- the resume key (`row_number`, `line_number`, cursor, and so on)
- the granularity of that key
- the built-in resume behavior that the next run will use

For automation or incident tooling, add `--json`:

```bash
agora checkpoint inspect nightly_events \
  --store sqlite:/var/lib/myapp/checkpoints.db \
  --json
```

For large built-in file sources (`CsvSource`, `JsonLinesSource`,
`ParquetSource`), `agora checkpoint inspect` also warns when the saved offset is
high enough that restart cost may be noticeable. Those sources currently
re-read from the beginning and skip forward to the saved position. The warning
includes the saved offset so operators can estimate how much of the file will
be scanned again before new output appears.

To clear saved progress explicitly:

```bash
agora checkpoint reset nightly_events \
  --store sqlite:/var/lib/myapp/checkpoints.db \
  --yes
```

`agora checkpoint reset --json` is available for scripts that need a structured
confirmation payload.

When a pipeline has failed and there are DLQ records to inspect, use:

```bash
agora diagnose nightly_events \
  --checkpoint-store sqlite:/var/lib/myapp/checkpoints.db \
  --dlq-path /var/lib/myapp/dlq.db
```

`agora diagnose` shows the same resume contract alongside the latest failure
context so operators can tell both _where_ the run stopped and _what_ a resume
will actually do.

For machine-readable incident summaries:

```bash
agora diagnose nightly_events \
  --checkpoint-store sqlite:/var/lib/myapp/checkpoints.db \
  --dlq-path /var/lib/myapp/dlq.db \
  --json
```

## Choosing a store

There are two stores out of the box.

`InMemoryCheckpointStore` keeps the checkpoint in process memory. It survives middleware failures within a run but is lost when the process exits. Use it in tests or for local one-off runs where you want resume behavior within a session but don't need it across restarts.

`SQLiteCheckpointStore` writes to a local SQLite file. It survives process restarts, crashes, and deploys. Use this in production for any pipeline that reads files or long streams.

```python
from agora import InMemoryCheckpointStore, SQLiteCheckpointStore

# Production: persists across restarts
store = SQLiteCheckpointStore(path=".agora_checkpoint.db")

# Tests / local dev: in-memory only
store = InMemoryCheckpointStore()
```

The default path for `SQLiteCheckpointStore` is `.agora_checkpoint.db` in the working directory. In production, point it at a stable path outside your deployment directory so it survives redeploys:

```python
store = SQLiteCheckpointStore(path="/var/lib/myapp/checkpoints.db")
```

## Wiring it into a pipeline

Pass the store and a checkpoint key when building the pipeline. The key is the lookup key in the store — use something stable and unique per pipeline:

```python
from agora import DeliveryConfig
from agora import SQLiteCheckpointStore

pipeline = (
    Pipeline(CsvSource("events.csv"), id="nightly_events")
    .build(
        DatabaseSink(db),
        config=DeliveryConfig(
            checkpoint=SQLiteCheckpointStore("/var/lib/myapp/checkpoints.db"),
            checkpoint_key="nightly_events:events_csv",
            checkpoint_every=500,
        ),
    )
)
```

`checkpoint_every=500` means the position is saved every 500 records. Lower values give finer resume granularity at the cost of more I/O. For most file sources, 500–1000 is a reasonable default. If your records are large and writes are slow, go higher.

## Failure policy

By default, a checkpoint store failure (disk full, corrupted DB) raises and stops the pipeline. You can relax this:

```python
from agora import DeliveryConfig
from agora import CheckpointFailurePolicy, SQLiteCheckpointStore

pipeline = (
    Pipeline(CsvSource("events.csv"), id="nightly_events")
    .build(
        DatabaseSink(db),
        config=DeliveryConfig(
            checkpoint=SQLiteCheckpointStore("/var/lib/myapp/checkpoints.db"),
            checkpoint_key="nightly_events:events_csv",
            checkpoint_every=500,
            checkpoint_failure_policy=CheckpointFailurePolicy.LOG_AND_CONTINUE,
        ),
    )
)
```

With `LOG_AND_CONTINUE`, checkpoint failures are logged but the pipeline keeps running. The tradeoff: if the store is broken and the pipeline crashes later, the next run starts from the last successfully saved position (which may be far back) or from the beginning.

Use `FAIL_CLOSED` (the default) when correctness matters more than availability. Use `LOG_AND_CONTINUE` when you'd rather process duplicate records than stop entirely.

## How resume works on restart

On each run, the pipeline calls `checkpoint_store.load(key)` before streaming begins. If a checkpoint exists, it calls `source.prepare_resume(checkpoint)`, which positions the source at the saved offset. The source then streams from that point forward.

If no checkpoint exists (first run, or the store was cleared), `prepare_resume` receives `None` and the source starts from the beginning.

This means the first run after a crash re-processes records from the last checkpoint, not from the crash point. If `checkpoint_every=500` and the crash happened at record 1,247, the next run starts at record 1,000. Records 1,000–1,247 are processed again. Design your sink to be idempotent, or use a dedup middleware, if re-processing is a problem.

## Writing a custom checkpointable source

Implement the `CheckpointableSource` protocol. The four things you must provide are `source_name`, `supports_checkpoint`, `current_checkpoint()`, and `prepare_resume()`.

```python
from collections.abc import AsyncGenerator
from agora import BaseSource
from agora.core.checkpoint import CheckpointValue, Checkpoint


class PaginatedAPISource(BaseSource[dict]):
    source_name = "paginated_api"
    supports_checkpoint = True

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url
        self._cursor: str | None = None

    def current_checkpoint(self) -> CheckpointValue:
        # Return whatever position the source is currently at.
        # Must be JSON-serializable (str, int, float, dict, list, None).
        return {"cursor": self._cursor}

    async def prepare_resume(self, checkpoint: Checkpoint | None) -> None:
        # Called once before streaming starts.
        # Set internal state so stream() picks up from the right place.
        if checkpoint is not None and isinstance(checkpoint.value, dict):
            self._cursor = checkpoint.value.get("cursor")

    async def stream(self) -> AsyncGenerator[dict, None]:
        cursor = self._cursor
        while True:
            params = {"cursor": cursor} if cursor else {}
            page = await fetch_page(self._base_url, params)
            for record in page["items"]:
                self._cursor = record["id"]
                yield record
            cursor = page.get("next_cursor")
            if not cursor:
                break
```

`current_checkpoint()` is called by the runtime every `checkpoint_every` records. Whatever you return here is what gets stored and handed back to `prepare_resume()` on the next run. Keep it small and serializable.
