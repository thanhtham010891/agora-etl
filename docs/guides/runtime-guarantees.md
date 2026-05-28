# Runtime Guarantees

_When to read this: you need to know what the Agora runtime promises so you can build reliable pipelines without reading the source._

This page is the single source of truth for what the core runtime guarantees and what it intentionally does not. Other docs link here when they discuss execution behavior.

If a behavior is not on this page, treat it as implementation detail. It may change between minor releases without a deprecation cycle. Behaviors on this page are part of the public contract — we will not change them without writing it down here first.

## What is guaranteed

### Source order

Records are committed to sinks in source order. This holds in both linear and buffered execution modes.

In buffered mode, the runtime may run a slow stage (e.g. `AIBatchMiddleware`) concurrently across multiple records, but it drains results back into source order before they reach the sink. A fast record produced from a later source position will not commit ahead of a slower record produced from an earlier source position.

### Sink fail-closed by default

`SinkFailurePolicy.FAIL_CLOSED` is the default. When the sink raises during `write()` or `write_batch()` and the failed record is not routed to a DLQ, the run stops and the original sink exception propagates from `pipeline.run()`.

The runtime never silently skips a failed record under `FAIL_CLOSED`.

### Checkpoint advancement is conservative

The source checkpoint advances only through records that were durably handled under the active failure policy.

| Outcome | Checkpoint advances? |
|---|---|
| Sink wrote successfully | Yes |
| Middleware dropped record (returned None) | Yes |
| Middleware raised, record routed to DLQ | Yes |
| Sink raised, record routed to DLQ | Yes |
| Sink raised, no DLQ, `FAIL_CLOSED` | No (run aborts before advancing) |
| Sink raised, no DLQ, `LOG_AND_CONTINUE` | Yes (matches "record was handled") |

The checkpoint never advances past a record whose write failed and was not handled.

### DLQ routing on middleware failure

When a middleware raises during `process()`, the chain stops for that record and the runtime calls the middleware's `on_error()` hook. If a DLQ sink is configured, the failed record is written to the DLQ as a `DLQRecord` with `stage="middleware"` and the middleware name attached.

If no DLQ is configured, the error is counted in `PipelineRunSummary.records_errored` and the record is discarded. The pipeline continues with the next record either way.

### DLQ failure policy is honored

`DLQFailurePolicy.LOG_ONLY` (the default) logs DLQ write failures and continues. The original error is already counted; a failed DLQ write is a secondary failure.

`DLQFailurePolicy.RAISE` propagates DLQ write failures and stops the run. Use this when you need a hard guarantee that no failed record is silently lost.

### DLQ replay acknowledges only after success

When replaying a DLQ via `SQLiteDLQSource`, a record is acknowledged (removed from the DLQ) only after replay produces one successful sink write. Records that fail replay remain in the DLQ.

`SQLiteDLQSource` skips records whose `attempt` count has reached `max_attempts`. Records below the ceiling are yielded for replay.

### Cancellation is cooperative and ordered

When the run task receives `asyncio.CancelledError` or `KeyboardInterrupt`, the runtime:

1. Marks the run as interrupted.
2. Stops middlewares via `chain.stop_all()`.
3. Flushes and closes the writer.
4. Flushes and closes the DLQ sink if open.
5. Closes the checkpoint store.

Shutdown errors during an interrupted run are logged but suppressed — the original cancellation propagates instead of being masked by a cleanup failure.

A buffered pipeline aborts pending buffered work on cancellation. It does not commit later records out of order to "catch up."

### Lifecycle ordering

Within a single run, the runtime invokes lifecycle hooks in this order:

1. Source `open()`.
2. Checkpoint store `load()` (if configured and the source supports checkpointing).
3. Source `prepare_resume()` (if the source supports checkpointing).
4. Middleware chain `start_all()`.
5. Writer `open()` and DLQ sink `open()` (DLQ closes if writer fails after DLQ opens).
6. Records stream and dispatch.
7. On termination: middleware `stop_all()`, DLQ flush + close, writer flush + close, checkpoint store `close()`.

A failure during `_open_sinks` rolls back any sinks that already opened.

## What is intentionally not guaranteed

### Exactly-once delivery across sinks

Agora delivers at-least-once. If your sink is not idempotent and the process crashes between the sink write and the next checkpoint save, the next run can re-process records since the last checkpoint. Use a dedup middleware or an idempotent sink if duplicates matter.

### Transactional coupling between sink and checkpoint store

The runtime saves the checkpoint after the sink reports success. There is no two-phase commit between the two stores. A crash in the gap re-processes the affected records on the next run.

### Safe execution of untrusted config

Config import references execute trusted project code. Do not load configs from sources you do not trust.

### Public-edge hardening for the built-in health server

The built-in health server is suitable for private network boundaries and internal monitoring. It is not hardened against public-edge traffic. Use a reverse proxy or place it on an internal network.

### Cross-source ordering

Order is guaranteed within a single source. The runtime makes no claims about the relative order of records from different sources.

### Specific resume granularity

Each source defines its own resume granularity (line number, row number, cursor, etc.). See the [Recovery Support Matrix](recovery-matrix.md) for the per-source contract. The runtime guarantees that whatever the source declares is what gets restored — not that a particular source uses any particular granularity.

## Backpressure and adaptive buffering

`Backpressure.adaptive(...)` monitors writer flush latency and checkpoint save latency to scale the in-flight record limit up or down. It is a throughput tuning mechanism — it does not relax any of the guarantees above.

Without `Backpressure`, the buffer is unbounded by default. Use `max_buffer_size` to put a hard cap on it.

## When this page changes

This contract is part of the public release story. Changes follow these rules:

- Adding a new guarantee is a minor-version change. The new guarantee must ship with at least one preservation test.
- Tightening an existing guarantee (making it stricter) is a minor-version change.
- Loosening or removing a guarantee is a breaking change. It belongs in a major-version bump and must appear in the change log with rationale.

Each guarantee on this page has a backing test in `tests/preservation/test_runtime_guarantees.py`. If a test fails, the guarantee is broken — fix the code, not the test.
