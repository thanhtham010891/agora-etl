# Architecture

_When to read this: you want the mental model behind Agora's runtime pieces, lane selection, and how records move through the system._

## Public surfaces first

Agora now has a deliberate public import hierarchy inside `agora-etl`:

- `agora` is the builder-first convenience facade for most application code
- `agora.core` is the stable framework-contract facade
- `agora.core.<domain>` packages such as `source`, `sink`, `middleware`,
  `checkpoint`, `context`, `metrics`, `tracing`, `session`, `explain`, and
  `container` are the public domain boundaries inside the core
- `agora.core.runtime` is an advanced coordination facade for runtime-adjacent
  tooling, not the default import boundary for ordinary builders or plugins
- underscore-prefixed support modules under those packages remain internal

The practical rule is simple: prefer package `__init__` facades and documented
top-level modules, not helper files.

## Orchestration layers

The orchestration path is now intentionally split into three layers:

1. **Builder layer** — `Pipeline`, `BoundPipeline`, and the fluent methods such
   as `.pipe()`, `.build()`, `.fan_out()`, `.route()`, `.run()`, and
   `.explain()`
2. **Executor facade** — `agora.core.executor`, `agora.core.session`, and
   `agora.core.explain`, which turn a built pipeline into one concrete run
3. **Runtime support** — `agora.core.runtime`, which owns delivery, buffering,
   lane planning, process-batch coordination, checkpoint persistence, and other
   lower-level machinery behind the executor

That split matters for maintainability:

- application code should normally stop at `agora` or `agora.core`
- plugin code should usually depend on `agora.core.<domain>` contracts
- runtime internals are free to keep evolving behind the executor facade

## The five components

Every pipeline is composed of five parts. Understanding what each one owns makes the rest of the runtime predictable.

**Source** emits records via an async generator. It owns the cursor into the data — file position, page number, Kafka offset, whatever makes sense for that source. The runtime never pulls faster than the source yields.

**MiddlewareChain** is the ordered list of middlewares you registered with
`.pipe()`. Records flow through it left to right. If any middleware returns
`None`, the record is dropped and does not continue. If any middleware raises,
the runtime attempts DLQ routing (if configured) and the chain stops for that
record. Later records do not advance checkpoint state past that failure unless
the record reaches a handled terminal outcome.

**Writer** delivers processed records to one or more sinks. It handles fan-out,
batching, sink concurrency, and native batch write result normalization. You
do not construct it directly — `.build()`, `.fan_out()`, and `.route()`
assemble the writer boundary for you.

**DLQSink** captures failed records. A DLQ record preserves the original payload, the processed payload (if the failure happened at the sink), the pipeline and run IDs, the error type and message, and the source checkpoint at the time of failure. Failed records can be replayed with `agora dlq replay`.

**CheckpointStore** persists the source's position so a pipeline can resume after a restart. The runtime calls `checkpoint_store.save()` every `checkpoint_every` records. On the next run, `source.prepare_resume(checkpoint)` is called before streaming begins. Not all sources support checkpointing — see [Sources](source/index.md) for which ones do.

For the exact hook order of startup, streaming, and shutdown, see
[Lifecycle](guides/lifecycle.md).

## Execution lanes

The runtime selects one of three execution lanes based on the pipeline's source and middleware chain.

**Linear lane** is the default. Records move through the chain one at a time:

```
source.stream() → chain.process(record) → writer.write(result)
```

This is the right lane for most pipelines. Simple, predictable, easy to reason about under failure.

**Buffered lane** activates when a middleware in the chain exposes a `submit` method **and** declares `min_concurrency > 1` — in practice, `AIBatchMiddleware`. The runtime splits the chain at that middleware and runs the buffered stage concurrently up to the configured limit, then drains results in source order before passing them to the suffix of the chain and the writer.

```
source.stream()
  → chain.process_range(0, split_index, record)   # sync prefix
  → buffered_stage.submit(record)                 # concurrent
  → chain.process_range(split_index+1, end, result)  # sync suffix
  → writer.write(result)
```

A submit-capable middleware with `min_concurrency == 1` runs on the linear lane — there is no concurrency benefit to pay the per-record task overhead for.

The key point: buffered mode helps when one middleware stage is slow and
concurrent, but it does not change the ordering or failure semantics. Output
order is still source order. A sink failure or cancellation still aborts
pending buffered work rather than committing later records out of order.

**Batch lane** activates when the source advertises a non-row data plane (e.g. `CsvSource(emit_batches=True)`, `ArrowCsvSource`, `ParquetSource(use_arrow_batches=True)`). The runtime calls `source.stream_batches()` instead of `stream()` and processes whole batches at once:

```
source.stream_batches() → chain.process_batch(batch) → writer.write_batch(results)
```

Checkpointing, DLQ routing, and ordering guarantees are all preserved — the checkpoint advances once per batch, after the batch is durably written.

`ProcessBatchMiddleware` is a special case of the batch lane: the transform
executes in a dedicated worker process, and when `max_workers > 1` plus
`max_in_flight_batches > 1`, Agora can keep multiple process batches in flight
while still committing them back in source order. If a process batch times out,
Agora recycles that process pool before accepting the next sequential commit so
later batches do not inherit stuck worker state. In `0.3.x`, this pipelined
mode is supported only with `ordered=True`.

`ArrowProcessBatchMiddleware` is the Arrow-native sibling of that pattern:
the source emits `pa.RecordBatch`, the middleware sends the batch across the
worker boundary as Arrow IPC bytes, and the sink can stay Arrow-native on the
way out. This keeps the orchestration semantics the same while avoiding
materializing Python row objects in the process path.

**Arrow fast path** is a sub-case of the batch lane. When the source emits `pa.RecordBatch` objects (`data_plane_spec().emitted_plane == arrow_batches`) **and** every middleware in the chain is Arrow-native **and** at least one sink exposes `write_arrow_batch()`, the runtime keeps data columnar through the chain and into every Arrow-capable sink:

```
source.stream_batches()              # yields pa.RecordBatch
  → chain.process_arrow_batch()      # each stage: RecordBatch → RecordBatch
  → sink.write_arrow_batch()         # for Arrow-native sinks
  → sink.write_batch()/write()       # row fallback only for non-Arrow sinks
```

There are now three distinct Arrow-source cases:

- no Arrow middleware in the chain: the runtime may materialize rows once
  before the chain and continue as a Python-row pipeline
- every middleware in the chain is Arrow-native: the Arrow chain stays active
- the chain mixes Arrow middleware with Python-row/list-batch middleware:
  planning fails with `PipelineError` because the chain is internally
  inconsistent

Agora now models those choices with one shared data-plane vocabulary:

- `python_rows` — one record at a time
- `python_batches` — `list[...]` batches of Python row objects
- `arrow_batches` — `pyarrow.RecordBatch`

That vocabulary is exposed on the public contracts too:

- `source.data_plane_spec()` / `source.emitted_data_plane`
- `sink.data_plane_spec()`
- `from agora import DataPlane, SourceDataPlaneSpec, SinkDataPlaneSpec`

The planner resolves two important boundaries up front:

- the plane emitted by the source
- the plane that will actually enter the writer after any required materialization

In `0.4.0`, the planner requires explicit data-plane contracts. Legacy source
and sink bool flags are no longer consulted when selecting a non-row lane.

## How To Predict The Selected Lane

Use this as the quick mental model:

| Source shape | Middleware chain | Sink shape | Selected lane | Fast path notes |
| --- | --- | --- | --- | --- |
| `stream()` only | regular middleware or no middleware | any sink | `linear` | default path |
| `stream()` only | any stage with `submit()` and `min_concurrency > 1` | any sink | `buffered` | preserves source order while running the buffered stage concurrently |
| `stream_batches()` with `data_plane_spec().emitted_plane == PYTHON_BATCHES` | regular `BatchMiddleware` or no middleware | batch-writable sink | `batch` | avoids per-record runtime orchestration |
| Arrow batch source (`data_plane_spec().emitted_plane == ARROW_BATCHES`) | all stages Arrow-native | Arrow-native sink | `batch` + Arrow fast path | `arrow_fast_path_active=true`, `arrow_chain_active=true` |
| Arrow batch source (`data_plane_spec().emitted_plane == ARROW_BATCHES`) | no Arrow stages, only Python-row middleware | any sink | `batch` | materialises rows once before the chain |
| Arrow batch source (`data_plane_spec().emitted_plane == ARROW_BATCHES`) | all stages Arrow-native | mixed fan-out sinks | `batch` + partial Arrow fast path | Arrow sinks get `write_arrow_batch()`, other sinks get row fallback at the sink boundary |
| Arrow batch source (`data_plane_spec().emitted_plane == ARROW_BATCHES`) | mixed Arrow + regular middleware | any sink | invalid | planner raises `PipelineError` before the run starts |

To confirm the decision at runtime, inspect:

- `summary.runtime.execution_lane`
- `summary.runtime.source_data_plane`
- `summary.runtime.writer_input_data_plane`
- `summary.runtime.direct_flush_active`
- `summary.runtime.arrow_fast_path_active`
- `summary.runtime.arrow_chain_active`
- `summary.runtime.writer_downgraded_sink_count`

If tracing is enabled, the same decision appears in span attributes:

- `pipeline.run`: `planned_lane`, `direct_flush_eligible`,
  `arrow_fast_path_eligible`, `arrow_chain_eligible`,
  `source_data_plane`, `writer_input_data_plane`, `downgraded_sink_count`
- `source.stream`: `lane`, `batch_source`, `buffered_stage_count`,
  `source_data_plane`, `writer_input_data_plane`

## Runtime guarantees

The full contract — what is guaranteed, what is intentionally not — lives in [Runtime Guarantees](guides/runtime-guarantees.md). The high points:

- Records are committed to sinks in source order in both linear and buffered modes.
- The source checkpoint advances only through records that were durably handled under the active failure policy.
- DLQ replay acknowledges a record only after replay produces one successful write.
- At-least-once delivery is the model. There is no exactly-once guarantee and no transactional coupling between sink writes and the checkpoint store.

For the per-source resume contract (which sources support checkpointing, what their resume position means), see the [Recovery Support Matrix](guides/recovery-matrix.md).

## Observability layers

Observability now follows the same facade-first split as orchestration:

- `agora.core.context` owns run-scoped context and trace/log helpers
- `agora.core.metrics` owns per-run summary types such as
  `PipelineRunSummary`
- `agora.core.tracing` owns tracer implementations and span behavior
- `agora.metrics.collector` and `agora.metrics.exporters` sit at the edge for
  cumulative stats and metric export
- `agora.health` exposes health/readiness endpoints on top of collector state

This keeps runtime signal generation in the core while leaving process-level
health and exporter integrations at the observability edge.

## Backpressure

When `backpressure=Backpressure.adaptive(...)` is set, the runtime monitors writer flush latency and checkpoint save latency to dynamically scale the in-flight record limit up or down. This prevents a fast source from overwhelming a slow sink.

To set a fixed buffer size without adaptive scaling, pass `max_buffer_size` directly:

```python
Backpressure.adaptive(max_buffer_size=200)
```

Adaptive backpressure is a throughput tuning mechanism. It does not relax the ordering or fail-closed guarantees above.

## State backends

Checkpoints, DLQ records, and the HTTP response cache all use the same `StateBackend` abstraction:

| Backend | When to use |
|---|---|
| `MemoryBackend` | Tests and single-run pipelines where persistence is not needed |
| `SQLiteBackend` | Default for local and single-process deployments |

Third-party backends (Redis, Postgres) are available as separate packages under `agora-etl-plugins`.

For direct use of `StateBackend`, `TTLKeyValueStore`, and `MembershipKeyStore`,
see [State](state.md).

## Tracing

Three tracers are available:

| Tracer | When to use |
|---|---|
| `NoopTracer` | Default — zero overhead |
| `InMemoryTracer` | Tests — inspect spans after a run |
| `OpenTelemetryTracer` | Production — exports to any OTLP-compatible backend |

## Plugin system

Agora discovers plugins via Python entry-points when the relevant registries are loaded. Third-party packages register themselves under the `agora.*` entry-point groups. See [Plugins](plugins/index.md) for details.
