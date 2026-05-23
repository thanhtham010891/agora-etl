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

## Backpressure

When `backpressure=Backpressure.adaptive(...)` is set, the runtime monitors writer flush latency and checkpoint save latency to dynamically scale the in-flight record limit up or down. This prevents fast sources from overwhelming slow sinks.

Fixed backpressure is also available:

```python
Backpressure.fixed(max_buffer_size=200)
```

## Dead-letter queue

When a record fails (middleware error, sink error), the runtime writes a `DLQRecord` to the configured `dlq` sink. The DLQ record preserves:

- the original record
- the processed record (if the failure occurred at the sink)
- the pipeline and run identifiers
- the error type and message
- the source checkpoint at the time of failure

Failed records can be replayed via `agora dlq replay`.

## Checkpointing

Sources that implement `current_checkpoint()` can be resumed after a restart. The runtime calls `checkpoint_store.save()` every `checkpoint_every` records. On the next run, `source.prepare_resume(checkpoint)` is called before streaming begins.

Built-in checkpointable sources: `CsvSource`, `ParquetSource`, `JsonLinesSource`.

## Plugin system

Agora discovers plugins via Python entry-points at import time. Third-party packages register themselves under the `agora.*` entry-point groups. The core registries (`source_registry`, `sink_registry`, etc.) load these automatically.

See [plugins.md](plugins.md) for details.

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
