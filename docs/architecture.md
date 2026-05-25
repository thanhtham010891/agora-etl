# Architecture

This document describes how the agora runtime executes a pipeline.

## Overview

```
Pipeline (builder)
    │
    ▼
BoundPipeline (runner)
    │
    ├── Source          — emits records via async generator
    ├── MiddlewareChain — processes each record in sequence
    ├── Writer          — delivers records to one or more sinks
    ├── DLQSink         — captures failed records (optional)
    └── CheckpointStore — persists source position (optional)
```

`Pipeline` is a fluent, immutable builder. Calling `.build()` produces a `BoundPipeline` which owns the execution loop.

## Execution modes

### Linear mode

The default. Records flow through the middleware chain one at a time:

```
source.stream()
  → chain.process(record)
  → writer.write(result)
```

Used when no middleware declares `min_concurrency > 1`.

### Buffered mode

Activated when a middleware (e.g. `AIBatchMiddleware`) sets `min_concurrency > 1`. The runtime submits records to the buffered stage concurrently up to the configured limit, then drains results in source order to preserve ordering guarantees.

```
source.stream()
  → chain.process_range(0, split_index, record)   # sync prefix
  → buffered_stage.submit(record)                 # concurrent
  → chain.process_range(split_index+1, end, result)  # sync suffix
  → writer.write(result)
```

Buffered execution preserves output order, but it is not "fire and forget".
If a fail-closed sink error or run cancellation occurs, Agora aborts pending
buffered work instead of continuing to commit later records out of order.

## Runtime guarantees

Agora intentionally makes a small number of core guarantees:

- linear execution commits records in source order
- buffered execution may process concurrently, but commits downstream writes in source order
- fail-closed sink or checkpoint failures stop the run instead of silently advancing later records
- source checkpoints advance only through records that were successfully handled under the active failure policy
- DLQ replay acknowledges a record only after replay produces one successful write

These guarantees are the behaviors preservation tests are meant to lock down.

## Backpressure

When `backpressure=Backpressure.adaptive(...)` is set, the runtime monitors writer flush latency and checkpoint save latency to dynamically scale the in-flight record limit up or down. This prevents fast sources from overwhelming slow sinks.

Fixed backpressure is also available:

```python
Backpressure.fixed(max_buffer_size=200)
```

Adaptive backpressure is a tuning mechanism, not a delivery guarantee. It may
raise or lower concurrency as writer flush latency and checkpoint latency
change, but it does not relax the ordering and fail-closed rules above.

## Dead-letter queue

When a record fails (middleware error, sink error), the runtime writes a `DLQRecord` to the configured `dlq` sink. The DLQ record preserves:

- the original record
- the processed record (if the failure occurred at the sink)
- the pipeline and run identifiers
- the error type and message
- the source checkpoint at the time of failure

Failed records can be replayed via `agora dlq replay`.

Replay semantics are intentionally narrow:

- `pipeline` replay re-enters the middleware chain from the original payload when one is available
- `sink` replay bypasses middleware and re-drives only the sink writer path from the processed payload
- records are acknowledged only after replay produces one successful write
- records that fail replay or are dropped during replay remain in the DLQ

## Checkpointing

Checkpointing is explicit, not automatic for every source. A source must opt in
to resume support by exposing `supports_checkpoint = True` and implementing the
checkpoint hooks. The runtime calls `checkpoint_store.save()` every
`checkpoint_every` records. On the next run, `source.prepare_resume(checkpoint)`
is called before streaming begins.

Built-in checkpointable sources: `CsvSource`, `ParquetSource`, `JsonLinesSource`.

Sources that do not explicitly opt into checkpoint support still run normally,
but Agora does not advertise resume behavior for them and does not touch the
checkpoint store on their behalf.

Checkpoint persistence is fail-closed by default. If a checkpoint save fails,
the run raises unless `checkpoint_failure_policy=LOG_AND_CONTINUE` is set
explicitly.

## Non-guarantees

Agora does not claim a few stronger properties that depend on external systems:

- exactly-once delivery across arbitrary sinks
- transactional coupling between sink writes and external checkpoint stores
- safe execution of untrusted declarative configs
- public-edge hardening for the built-in health server

## Plugin system

Agora discovers plugins via Python entry-points when the relevant registries are
loaded. Third-party packages register themselves under the `agora.*`
entry-point groups. The core registries (`source_registry`, `sink_registry`,
etc.) expose those registrations to code, config assembly, and CLI workflows.

See [Plugins](plugins/index.md) for details.

## Operational constraints

Agora is a framework runtime, not a full control plane. A few boundaries are
intentional:

- config import references execute trusted project code
- the built-in health server is lightweight and best kept behind private network boundaries
- plugin manifest compatibility hints are diagnostics, not a full dependency solver

## Tracing

The runtime emits spans for each pipeline stage. Three tracers are available:

| Tracer | Description |
|---|---|
| `NoopTracer` | Default — no overhead |
| `InMemoryTracer` | Stores spans in memory — useful for testing |
| `OpenTelemetryTracer` | Exports to any OTLP-compatible backend |

## State backends

Checkpoints, DLQ records, and the HTTP response cache all use the same `StateBackend` abstraction:

| Backend | Use case |
|---|---|
| `MemoryBackend` | Tests and single-run pipelines |
| `SQLiteBackend` | Default for local / single-process deployments |

Third-party backends (Redis, Postgres) are available as separate packages.
