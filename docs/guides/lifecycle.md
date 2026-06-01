# Lifecycle

_When to read this: you need to know exactly when Agora opens resources, restores checkpoints, runs hooks, and shuts things down._

This page explains the lifecycle of an Agora pipeline at three levels:

- the builder lifecycle (`Pipeline` -> `BoundPipeline`)
- the single-run runtime lifecycle (`pipeline.run()`)
- the long-lived worker lifecycle (`ScheduledPipeline` and `WorkerPool`)

If you are debugging resource leaks, startup errors, duplicate processing after
restart, or shutdown behavior, start here.

## The builder lifecycle

Agora has two user-facing pipeline objects:

| Object | What it represents |
|---|---|
| `Pipeline` | An immutable builder: source + middleware definitions |
| `BoundPipeline` | A runnable pipeline: source + middleware chain + writer + delivery config |

The important rule is that `Pipeline` is immutable.

```python
base = Pipeline(source, id="orders").pipe(normalize)
with_filter = base.filter(lambda r: r["active"])

# base is unchanged
```

That means:

- every `.pipe()` and `.filter()` call returns a new `Pipeline`
- `.build()`, `.fan_out()`, and `.route()` produce a `BoundPipeline`
- each `.run()` call creates a fresh executor around the same prepared pipeline definition

The builder stage does not open network connections, read files, or start the
source. Runtime side effects begin only when you call `await bound.run()`.

## Single-run lifecycle

This is the runtime order for one `pipeline.run()` call.

### 1. Run state is created

Agora creates a new run-scoped context with:

- a fresh `run_id`
- empty metrics
- the configured tracer
- the pipeline logger context

This state is per run. It is not reused across later runs.

### 2. Checkpoint restore happens before the source opens

If a checkpoint store is configured and the source explicitly supports
checkpointing, Agora:

1. loads the last saved checkpoint from the store
2. calls `source.prepare_resume(checkpoint)`

This happens before `source.open()`.

That ordering matters for custom sources: `prepare_resume()` should restore the
cursor or internal offset that `open()` and `stream()` will later use.

If the source does not advertise `supports_checkpoint = True`, Agora logs a
warning and continues without checkpointing.

### 3. Middleware startup runs in registration order

Agora calls `on_start(ctx)` on each middleware from left to right.

If middleware startup fails partway through:

- the failing middleware gets `on_stop(ctx)`
- already-started middlewares are rolled back in reverse order
- the run stops before the source opens or any sink writes happen

### 4. Writer opens before the source starts streaming

After middleware startup, Agora opens the delivery side:

1. writer open
2. DLQ sink open, if configured

If this stage fails:

- any already-opened writer is closed
- any already-opened DLQ sink is closed
- the source still has not opened yet

This keeps startup failure deterministic: either the pipeline is ready to run,
or startup fails before records start moving.

### 5. Source opens and streams records

Agora enters the source async context, which calls:

1. `source.open()`
2. `source.stream()` or `source.stream_batches()`
3. `source.close()` when streaming ends

The runtime selects the execution lane first, then runs the stream loop:

- linear lane
- buffered lane
- batch lane

Arrow fast paths are still part of that same run lifecycle. They change how
data moves internally, not the high-level lifecycle order.

### 6. Each record follows the delivery lifecycle

For a per-record run, the lifecycle is:

1. source emits a record
2. middleware chain processes it left to right
3. writer attempts delivery
4. if needed, DLQ handles the failure
5. checkpoint may advance
6. success hooks may run

For batch lanes, the same ideas apply, but delivery and checkpoint persistence
can happen in grouped writes rather than one record at a time.

## Lifecycle of a single record

The most useful mental model is not just "pipeline starts and stops", but what
happens to one record.

### Record accepted by the source

The source yields a raw record and may also expose:

- a checkpoint value
- a post-delivery success hook

### Middleware chain

Each middleware sees the output of the previous one.

- returning a record passes it onward
- returning `None` drops the record
- raising an exception stops the chain for that record

On exception, Agora calls the middleware's `on_error()` hook and then applies
the configured DLQ/failure path.

### Writer and sink delivery

The writer delivers to the configured sink topology:

- single sink via `.build()`
- all sinks via `.fan_out()`
- one routed sink via `.route()`

Depending on `SinkFailurePolicy`, a sink error may:

- stop the run
- be routed to the DLQ
- be logged and skipped

### Checkpoint persistence

Checkpoint progression is driven by handled records, not merely emitted records.

Agora only persists a checkpoint after the active delivery path says the record
has been handled under the configured policy.

## Shutdown lifecycle

The shutdown order is different from startup, and that is intentional.

### Normal completion

On a clean run:

1. the source context exits, so `source.close()` runs
2. middlewares stop in reverse order
3. DLQ flushes and closes
4. writer flushes and closes
5. checkpoint store closes

Source close happens before middleware shutdown because the source context wraps
the streaming phase itself.

### Cancellation

On `asyncio.CancelledError` or a graceful stop signal higher up:

- Agora marks the run as interrupted
- the original cancellation is preserved
- cleanup still runs in the usual shutdown sequence
- cleanup errors are logged and suppressed so they do not replace the original cancellation

Buffered work is not allowed to "race ahead" and commit later records out of
order during shutdown.

## ScheduledPipeline lifecycle

`ScheduledPipeline` adds a run loop around `BoundPipeline`.

### Factory lifecycle

The factory is called before every run:

```python
scheduled = ScheduledPipeline(
    factory=build_pipeline,
    schedule=Schedule.every(hours=1),
    pipeline_id="orders_hourly",
)
```

This is not optional ceremony. It ensures each run gets a fresh pipeline object
instead of reusing one that may still hold file handles, buffered writers, or
run-scoped state from the previous iteration.

### Run loop lifecycle

For each scheduled iteration:

1. optional `pre_run_hook()` decides whether this run should happen
2. the factory builds a fresh `BoundPipeline`
3. the pipeline runs once
4. a `RunRecord` is appended to history
5. `on_run_complete` and observers are notified
6. the schedule waits until the next run

If `pre_run_hook()` returns `False`, the run is skipped without incrementing the
run counter or being treated as an error.

### Error lifecycle

When a scheduled run fails:

- the run is recorded with an error
- `consecutive_errors` increments
- the configured backoff policy decides the retry delay
- the scheduler stops only after `max_consecutive_errors` is reached

## WorkerPool lifecycle

`WorkerPool` manages multiple `ScheduledPipeline` instances plus optional
health/metrics serving.

### Startup

When `WorkerPool.run()` starts:

1. pipelines are registered
2. a shared metrics collector is prepared
3. optional distributed coordinator starts
4. metrics observers are wired into each scheduled pipeline
5. signal handlers are installed
6. scheduled pipelines start as asyncio tasks
7. optional `HealthServer` starts

### Graceful stop

On shutdown request:

1. each scheduled pipeline receives `stop()`
2. sleeping schedulers wake early
3. the health server is signalled to stop
4. the worker waits up to `graceful_shutdown_timeout`
5. remaining tasks are force-cancelled if they exceed the timeout
6. distributed leases are released if a coordinator is active

This is why a single Ctrl+C is usually safe: the worker first asks active runs
to finish cleanly before escalating to cancellation.

## DLQ replay lifecycle

Replay is just another pipeline run with a DLQ-backed source.

The important lifecycle rule is:

- a replayed DLQ record is acknowledged only after one successful sink write

If replay fails again, the record stays in the DLQ for another attempt.

## Common lifecycle mistakes

- Reusing a single `BoundPipeline` instance across scheduled runs instead of a factory.
- Assuming `source.open()` runs before checkpoint restore. It does not.
- Putting required cleanup only in middleware destructors instead of `on_stop()`.
- Assuming a graceful worker shutdown interrupts the current record immediately. It usually waits for the in-flight run to clean up first.
