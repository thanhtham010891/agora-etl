# Recovery Support Matrix

_When to read this: you need to know if a built-in source resumes after a restart, and what "resume" actually means for that source._

This page lists every built-in source with its checkpoint capability, resume granularity, and known limitations. It is the companion to [Checkpointing](checkpointing.md) — that guide explains the mechanics; this page tells you which sources participate.

For the runtime promises this matrix relies on (when checkpoints advance, what gets restored, lifecycle order), see [Runtime Guarantees](runtime-guarantees.md).

## Built-in sources

| Source | Checkpointable | Resume position | Notes |
|---|---|---|---|
| `JsonLinesSource` | Yes | Line number (`line_number`) | Skips lines `<= resume_line_number` on read. |
| `CsvSource` | Yes | Row number (`row_number`) | Skips rows `<= resume_row_number`. Header is not counted. |
| `ParquetSource` | Yes | Row number (`row_number`) | Skips rows `<= resume_row_number` across PyArrow batches. |
| `HTTPSource` | No (by default) | Subclass-specific | Base class does not implement checkpoint hooks. Subclasses must opt in. |
| `IterableSource` | No | Not supported | In-memory testing source. Re-emits from start every run. |

A source is checkpointable when it satisfies all three conditions:

1. `supports_checkpoint = True`.
2. `current_checkpoint()` returns a JSON-serializable position that is not `None` while records are flowing.
3. `prepare_resume(checkpoint)` restores internal state from a previously saved checkpoint.

`FileSource` (the abstract base) sets `supports_checkpoint = True` by default, which is why all three file sources resume out of the box. `BaseSource` defaults to `False`, which is why `HTTPSource` and `IterableSource` do not.

You can verify at runtime:

```python
from agora.core.checkpoint import is_checkpoint_capable

print(is_checkpoint_capable(my_source))  # True or False
```

## CLI visibility

The runtime contract above is also surfaced directly in the CLI so operators do
not have to infer resume behavior from source names alone.

Use:

```bash
agora checkpoint inspect <pipeline_id> --store sqlite:/path/to/checkpoints.db
```

This prints the saved checkpoint plus:

- recovery support status
- resume key
- granularity
- resume behavior
- resume cost model

For built-in file sources, the command also warns when the saved row or line
offset is high enough that resume cost may be noticeable because restart still
re-reads from the beginning and skips forward sequentially. The warning carries
the saved offset so operators can estimate how much data will be re-scanned.

Add `--json` when this needs to feed automation or dashboards:

```bash
agora checkpoint inspect <pipeline_id> \
  --store sqlite:/path/to/checkpoints.db \
  --json
```

To remove the saved resume position entirely:

```bash
agora checkpoint reset <pipeline_id> --store sqlite:/path/to/checkpoints.db --yes
```

Use:

```bash
agora diagnose <pipeline_id> \
  --checkpoint-store sqlite:/path/to/checkpoints.db \
  --dlq-path /path/to/dlq.db
```

This shows the same recovery contract next to the latest checkpoint or failure
record. If the checkpoint is missing but the DLQ still has records, `diagnose`
falls back to the latest DLQ source name so the operator still gets a recovery
hint instead of a blank section.

`agora diagnose --json` returns the same contract as structured JSON, including
checkpoint state, recovery hints, DLQ summary, and replay guidance.

## Per-source contracts

### JsonLinesSource

- **Resume key:** `{"line_number": <int>}`.
- **Granularity:** the line number of the last record _yielded_ to the pipeline (not the last record _written_ to the sink). The runtime decides when to persist this via `checkpoint_every`.
- **First-run behavior:** `prepare_resume(None)` resets `_resume_line_number = 0` — streams from the first line.
- **Blank lines:** counted toward the line number but skipped (no record yielded).
- **Parse errors:** under `SourceRecordFailurePolicy.FAIL_CLOSED` (default) raise `SourceRecordError` with the current checkpoint attached. Under `LOG_AND_CONTINUE`, the error is logged and the line is dropped — the line counter still advances.

### CsvSource

- **Resume key:** `{"row_number": <int>}`.
- **Granularity:** data row number (header row is consumed by `DictReader` and does not count).
- **First-run behavior:** same as `JsonLinesSource`.
- **Blank rows:** when `skip_blank_lines=True` (default), a row where every value is empty is counted toward the row number but skipped.
- **Parse errors:** same policy split as `JsonLinesSource`.

### ParquetSource

- **Resume key:** `{"row_number": <int>}`.
- **Granularity:** row number across the entire file. PyArrow batch boundaries are an internal optimization — they do not affect resume semantics.
- **First-run behavior:** same as the other file sources.
- **Performance note:** resume currently re-reads and discards rows up to the saved offset. There is no row-group seek. For large Parquet files with high `resume_row_number`, the cost of restart is proportional to that offset.

### HTTPSource

- **Default:** does not implement checkpoint hooks. A restart replays from the beginning of `fetch_batch()`.
- **To enable resume in a subclass:**
  1. Set `supports_checkpoint = True`.
  2. Override `current_checkpoint()` to return your cursor, page token, or watermark.
  3. Override `prepare_resume(checkpoint)` to restore that state.

  ```python
  class PostsSource(HTTPSource[Post]):
      source_name = "posts_api"
      supports_checkpoint = True

      def __init__(self) -> None:
          super().__init__(base_url="https://api.example.com")
          self._cursor: str | None = None

      def current_checkpoint(self):
          return {"cursor": self._cursor}

      async def prepare_resume(self, checkpoint):
          if checkpoint and isinstance(checkpoint.value, dict):
              self._cursor = checkpoint.value.get("cursor")
  ```

  The runtime calls `current_checkpoint()` after every record yielded; persistence is gated by `checkpoint_every`.

### IterableSource

- **Not checkpointable.** Designed for tests and one-off in-memory runs.
- A pipeline that wires a checkpoint store with `IterableSource` logs `pipeline_checkpoint_unsupported_source` and runs without checkpointing — it does not raise.

## What "resume" promises

When a checkpointable source restarts with a stored checkpoint:

- `prepare_resume(checkpoint)` is called once, before `stream()` starts.
- The source positions itself so the next yielded record is the one after the saved position.
- Records before the saved position are skipped on the source side. They are not delivered to middlewares or sinks.
- Records between the last saved checkpoint and the crash point are re-processed. Make your sink idempotent or use a dedup middleware if this matters.

This matches the at-least-once delivery model from [Runtime Guarantees](runtime-guarantees.md). The runtime does not promise exactly-once across restarts.

## What "resume" does not promise

- **No mid-batch resume for ParquetSource.** PyArrow's batch iterator is consumed sequentially; the source uses row-level skipping, not row-group seeking. Resume is correct, but not zero-cost on large files.
- **No cross-source coordination.** Each source has its own checkpoint key. The runtime makes no attempt to align resume positions across sources in a multi-source job.
- **No automatic checkpoint expiry or compaction.** The checkpoint store keeps the latest saved value per key indefinitely. Manage retention at the store level if needed.
- **No checkpoint validation against source content.** If you swap the underlying file or feed without resetting the checkpoint key, the saved offset will be applied to the new content as-is.

## Checkpoint failure handling

Two failure surfaces — both controlled by `CheckpointFailurePolicy`:

- **`load()` failure** (during pipeline startup): under `FAIL_CLOSED` (default), the run aborts. Under `LOG_AND_CONTINUE`, the failure is logged and the source starts from the beginning as if no checkpoint existed.
- **`save()` failure** (during the run): under `FAIL_CLOSED`, the run aborts. Under `LOG_AND_CONTINUE`, the failure is logged, the run continues, and `mark_saved` still advances internally so the runtime does not retry-storm the broken store on every record.

Corrupted checkpoint payloads (missing required fields, wrong type) raise `TypeError` with a descriptive message. This was the `0.1.7` fix for the silent `KeyError`.

## Adding resume support to a custom source

The full protocol is in [Checkpointing](checkpointing.md#writing-a-custom-checkpointable-source). The minimum:

```python
class MySource(BaseSource[MyRecord]):
    source_name = "my_source"
    supports_checkpoint = True

    def current_checkpoint(self) -> dict | None:
        return {"cursor": self._cursor} if self._cursor else None

    async def prepare_resume(self, checkpoint) -> None:
        if checkpoint and isinstance(checkpoint.value, dict):
            self._cursor = checkpoint.value.get("cursor")
```

If you add resume support to a custom source in your own project, document the
resume key and granularity alongside the source. Builders need that contract in
writing to operate the pipeline safely.
