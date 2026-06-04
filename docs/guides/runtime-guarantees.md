# Runtime Guarantees

_When to read this: you need to know what the Agora runtime promises so you can build reliable pipelines without reading the source._

This page is the contract for the core `agora-etl` runtime. Other docs link
here when they talk about execution behavior, failure handling, checkpointing,
or replay.

If a behavior is not on this page, treat it as implementation detail. It may
change between minor releases without a deprecation cycle. Behaviors on this
page are part of the public contract and should be safe to build operational
assumptions around.

For the hook-by-hook order of startup, streaming, shutdown, scheduled runs, and
workers, see [Lifecycle](lifecycle.md). This page focuses on the promises that
remain true across those phases.

## Scope of this contract

This page covers the core runtime:

- `Pipeline`
- `BoundPipeline`
- core execution lanes
- checkpoint restore and persistence
- DLQ behavior
- cancellation and shutdown

Plugin-specific behaviors may extend this contract, but they do not weaken the
core guarantees described here unless their docs say so explicitly.

## Terms used on this page

These terms are used precisely:

- **consumed**: the source emitted the record
- **written**: the sink reported a successful write for that record
- **dropped**: the record was intentionally removed from the flow, usually by middleware returning `None`
- **errored**: the record hit an exception in source, middleware, or sink handling
- **handled**: the runtime reached a defined terminal outcome for that record under the active policy, such as written, dropped, or routed to the DLQ

The distinction between **emitted** and **handled** matters because checkpoint
persistence follows handled outcomes, not merely source output.

## Guaranteed startup behavior

### Checkpoint restore happens before source open

If a checkpoint store is configured and the source supports checkpointing,
Agora loads the saved checkpoint and calls `source.prepare_resume(checkpoint)`
before the source context is entered.

This means custom sources should treat `prepare_resume()` as the place where the
resume cursor is restored.

### Middleware startup is ordered and rollback-safe

Middleware `on_start()` hooks run in registration order.

If startup fails partway through:

- the failing middleware is asked to stop
- already-started middlewares are stopped in reverse order
- no records are streamed
- sink open does not begin

### Writer/DLQ open is all-or-nothing at the run boundary

Before streaming begins, Agora opens:

1. the writer
2. the DLQ sink, if configured

If open fails:

- already-opened sinks are closed
- source streaming never begins
- the run fails before any record is consumed

## Guaranteed processing behavior

### Middleware execution order is left to right

Within one record flow, middleware executes in the exact order it was
registered.

If a middleware returns `None`:

- downstream middlewares do not see that record
- the sink does not see that record
- the record counts as dropped
- checkpoint advancement still follows the active checkpoint policy for handled records

If a middleware raises:

- `on_error()` is called on that middleware
- the rest of the chain is skipped for that record
- the runtime applies DLQ routing if configured
- the pipeline continues with the next record unless a later failure policy stops the run

### Source order is preserved at the sink boundary

Records are committed to sinks in source order.

This remains true in:

- linear execution
- buffered execution
- batch execution

Buffered execution may process work concurrently internally, but later source
positions do not commit ahead of earlier ones.

### Lane changes do not weaken correctness guarantees

Batch and Arrow paths may change how data is grouped or represented in memory,
but they do not change the core guarantees around ordering, checkpoint gating,
or fail-closed delivery defaults.

### Process-isolated batch middleware keeps the same commit contract

`ProcessBatchMiddleware` runs the batch transform in a separate worker process,
but the commit boundary stays in the main runtime.

That means:

- checkpoint advancement still waits for the downstream write / DLQ outcome
- a raised or timed-out process batch still fails the whole batch
- later batches do not commit ahead of earlier ones
- a timed-out worker pool is recycled before the next batch is accepted
- in ordered pipelined mode, unresolved sibling batches from that recycled
  worker-pool generation are failed rather than committed from stale worker
  state
- on pipeline cancellation or abort, Agora terminates the active process-pool
  generation instead of waiting indefinitely for worker code to return

So process isolation changes where compute happens, not when a batch is
considered durably handled.

## Guaranteed sink and delivery behavior

### Sink failure is fail-closed by default

`SinkFailurePolicy.FAIL_CLOSED` is the default.

Under `FAIL_CLOSED`:

- if sink delivery fails
- and the failure is not successfully routed to the DLQ
- the run stops
- the original sink exception propagates from `pipeline.run()`

The runtime never silently skips an unhandled sink failure under
`FAIL_CLOSED`.

### Sink failure policy controls whether a failed record is terminal

| Situation | `FAIL_CLOSED` | `LOG_AND_CONTINUE` |
|---|---|---|
| Sink write succeeds | record is written | record is written |
| Sink write fails and DLQ routing succeeds | record is counted errored, run continues | record is counted errored, run continues |
| Sink write fails and DLQ routing does not happen | run stops | record is counted errored, run continues |

### Batch writes preserve per-record outcome handling

When the runtime writes a batch:

- it still expects one delivery outcome per input record
- partial failures are resolved record by record
- checkpoint persistence and DLQ routing remain aligned to those per-record outcomes

That means a successful record in the same write batch as a failed record can
still be committed and checkpointed under the active policy.

## Guaranteed checkpoint behavior

### Checkpoint advancement is conservative

The source checkpoint advances only through records that were handled under the
active failure policy.

| Outcome | Checkpoint advances? |
|---|---|
| Sink wrote successfully | Yes |
| Middleware dropped record (returned `None`) | Yes |
| Middleware raised, record routed to DLQ | Yes |
| Sink raised, record routed to DLQ | Yes |
| Sink raised, no DLQ, `FAIL_CLOSED` | No |
| Sink raised, no DLQ, `LOG_AND_CONTINUE` | Yes |

The checkpoint never advances past a record whose sink failure was both:

- unhandled
- run-terminal

### Checkpoint save cadence is explicit

Checkpoint persistence happens only when all of these are true:

1. checkpointing is enabled
2. the source supports checkpointing
3. the source reports a checkpoint value
4. the `checkpoint_every` threshold is reached

`checkpoint_every` is therefore a durability tuning knob, not just a
performance knob.

### Checkpoint load failure policy is honored

On startup, checkpoint load and `prepare_resume()` failures follow
`CheckpointFailurePolicy`:

| Policy | Behavior |
|---|---|
| `FAIL_CLOSED` | abort the run |
| `LOG_AND_CONTINUE` | log the failure and continue from scratch |

### Checkpoint save failure policy is honored

During a run, checkpoint save failures follow the same policy:

| Policy | Behavior |
|---|---|
| `FAIL_CLOSED` | abort the run |
| `LOG_AND_CONTINUE` | log the failure and continue processing |

Under `LOG_AND_CONTINUE`, the runtime marks the failed save slot as handled so
it does not retry-storm the same checkpoint write on every following record.

### Non-checkpointable sources degrade explicitly

If you pass a checkpoint store to a source that does not explicitly opt into
checkpointing:

- Agora logs `pipeline_checkpoint_unsupported_source`
- the pipeline continues
- no checkpoint load/save occurs for that run

## Guaranteed DLQ behavior

### Middleware and sink failures produce structured DLQ records

When DLQ routing is enabled, Agora writes a `DLQRecord` containing:

- pipeline ID
- run ID
- stage
- error type and message
- source name
- current checkpoint
- original record
- processed record when relevant
- middleware or sink metadata when available

### DLQ failure policy is independent and explicit

`DLQFailurePolicy.LOG_ONLY` is the default.

| Policy | Behavior when DLQ write fails |
|---|---|
| `LOG_ONLY` | log the DLQ failure and continue |
| `RAISE` | propagate the DLQ failure and stop the run |

This policy applies to the DLQ write itself. It does not retroactively make the
original record successful.

### DLQ replay acknowledges only after replay succeeds

When replaying from a DLQ-backed source such as `SQLiteDLQSource`, a record is
acknowledged only after replay produces one successful sink write.

If replay fails again:

- the record stays in the DLQ
- it can be replayed again later

## Guaranteed shutdown behavior

### Shutdown preserves the original terminal reason

If a run is cancelled or already failed, cleanup errors are logged and
suppressed so they do not replace the original cancellation or run failure.

This is important operationally: if the real reason was "sink write failed" or
"task was cancelled", Agora preserves that reason instead of masking it behind a
secondary close/flush error.

### Shutdown order is stable

On termination of an active run:

1. source context exits, so `source.close()` runs
2. middleware `on_stop()` hooks run in reverse order
3. DLQ flushes and closes
4. writer flushes and closes
5. checkpoint store closes

### Buffered work does not break ordering during shutdown

A buffered pipeline may abandon pending concurrent work during cancellation, but
it does not commit later results out of order in an attempt to finish faster.

## What is intentionally not guaranteed

### Exactly-once delivery across sinks

Agora delivers at-least-once. If your sink is not idempotent and the process
crashes between a successful sink write and the next persisted checkpoint, the
next run can re-process records since the last saved checkpoint.

Use idempotent sinks, dedup middleware, or application-level keys when
duplicates matter.

### Transactional coupling between sink and checkpoint store

The runtime saves the checkpoint after the sink reports success. There is no
two-phase commit between the sink and the checkpoint store.

### Cross-source ordering

Order is guaranteed within one source stream. The runtime makes no claims about
the relative order of records coming from different sources or different worker
processes.

### A specific resume granularity for every source

Each source defines its own resume value and granularity. The runtime guarantees
that the source's declared checkpoint value is restored as-is. It does not
guarantee that every source resumes by line number, offset, page token, or any
other particular shape.

See the [Recovery Support Matrix](recovery-matrix.md) for per-source behavior.

### Safe execution of untrusted project code or config

Config import references execute trusted project code. Do not load configs,
plugins, or worker modules from sources you do not trust.

### Public-edge hardening for the built-in health server

The built-in health server is intended for private network boundaries and
internal monitoring. It is not a hardened public-edge gateway.

## Backpressure and buffering

`Backpressure.adaptive(...)` monitors writer flush latency and checkpoint save
latency to tune in-flight limits. This is a throughput control mechanism only.

It does not relax:

- source-order guarantees
- checkpoint gating
- sink failure policy
- DLQ semantics

## When this page changes

This contract is part of the public release story. Changes follow these rules:

- Adding a new guarantee is a minor-version change. The new guarantee must ship with at least one preservation test.
- Tightening an existing guarantee (making it stricter) is a minor-version change.
- Loosening or removing a guarantee is a breaking change. It belongs in a major-version bump and must appear in the change log with rationale.

Each guarantee on this page has a backing test in `tests/preservation/test_runtime_guarantees.py`. If a test fails, the guarantee is broken — fix the code, not the test.
