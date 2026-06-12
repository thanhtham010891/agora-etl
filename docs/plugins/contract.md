# Plugin Contract

_When to read this: you are building a third-party plugin package and need to know which extension points are safe to build on, and what the compatibility expectations are in the current `0.3.x` public contract._

This page is the single source of truth for the plugin author contract. It is the plugin-layer equivalent of [Runtime Guarantees](../guides/runtime-guarantees.md).

## Stability labels

Each entry-point group carries one of three labels:

| Label | Meaning |
|---|---|
| `stable` | Semantic preserved through the current major line. Changes require a major-version bump and a migration note. |
| `provisional` | May change at a minor bump with a migration note in the change log. Build on these, but watch the change log. |
| `internal` | Not part of the public contract. May change at any time without notice. |

## Entry-point groups

| Group | Stability | Registry | Purpose |
|---|---|---|---|
| `agora.sources` | `stable` | `source_registry` | Custom source types |
| `agora.sinks` | `stable` | `sink_registry` | Custom sink types |
| `agora.middlewares` | `stable` | `middleware_registry` | Custom middleware types |
| `agora.runner` | `stable` | `runner_registry` | Custom runner types |
| `agora.middlewares.dedup.stores` | `stable` | `dedup_store_registry` | Dedup store backends |
| `agora.middlewares.dedup.strategies` | `stable` | `dedup_strategy_registry` | Dedup comparison strategies |
| `agora.ai.providers` | `provisional` | `ai_provider_registry` | AI provider implementations |
| `agora.ai.caches` | `provisional` | `ai_cache_registry` | LLM response cache backends |
| `agora.metrics.exporters` | `provisional` | `metrics_exporter_registry` | Metrics exporters |
| `agora.state.backends` | `provisional` | `state_backend_registry` | State backend implementations |

## What "stable" means in practice

For `stable` groups, the following are part of the contract:

- The entry-point group name will not change.
- The registry attribute name on the module will not change.
- The base class or protocol that plugins must satisfy will not have required methods removed.
- The discovery path (`load_entrypoints(group)`) will continue to work.

Adding optional methods to a base class is not a breaking change. Removing required methods is.

## What "provisional" means in practice

For `provisional` groups, the group name and registry attribute are stable, but:

- The base class or protocol shape may gain or lose required methods at a minor bump.
- The configuration contract for the plugin may change.
- A migration note will appear in the change log when this happens.

Build on provisional groups when you need the capability. Watch the change log before upgrading.

## Plugin author obligations

A plugin package that registers under any `stable` or `provisional` group must:

1. Register under the correct entry-point group in `pyproject.toml`.
2. Implement the required base class or protocol for that group.
3. Import from documented public facades such as `agora`, `agora.core`, or
   `agora.core.<domain>`, not underscore-prefixed support modules.
4. Optionally declare a `MANIFEST` at the package root for compatibility diagnostics (see [Manifest Contract](manifest.md)).

## What the runtime does with incompatible plugins

When a plugin declares a `MANIFEST` with an `agora_api_version` that does not match the active `AGORA_PLUGIN_MANIFEST_VERSION`:

- The plugin is **not** registered in the active registry.
- It is still recorded in diagnostics with `compatible=False`.
- `agora plugins list` shows it as incompatible so operators can see why it was excluded.
- `agora doctor` reports it as a compatibility warning instead of pretending the install is clean.
- The pipeline continues to start — incompatible plugins do not abort discovery.

When a plugin declares an entry-point key that collides with an existing
built-in/public key for that group:

- The plugin is **not** allowed to override the existing registration.
- Agora keeps the built-in/public key active.
- `agora doctor` reports the collision as a warning so operators can fix the install instead of relying on import order.
- `agora plugins list` keeps a diagnostics row for the skipped plugin so the active key and the rejected collision are both visible.

When a plugin has no `MANIFEST`:

- It is registered normally.
- `compatible` is reported as `None` in diagnostics.
- No warning is emitted.

See [Manifest Contract](manifest.md) for the full compatibility model.

## Operational diagnostics

Agora exposes the same plugin contract in CLI diagnostics so plugin authors and
operators do not have to reverse-engineer discovery behavior from logs.

### `agora plugins list`

`agora plugins list` covers every public entry-point group on this page, not
just sources/sinks/middlewares.

The JSON form (`agora plugins list --json`) includes:

- `category`
- `group`
- `registry`
- `stability`
- `origin`
- `compatibility`
- `manifest`
- `capabilities`
- `error` for broken entry-points or collision diagnostics that did not become active registrations

This output is intended to be stable enough for local diagnostics and contract
tests in plugin packages.

### `agora doctor`

`agora doctor` checks the same public entry-point groups and distinguishes:

- plugin load failures: reported as `FAIL`
- incompatible MANIFEST versions: reported as `WARN`
- entry-point key collisions with built-ins/public keys: reported as `WARN`
- manifestless plugins: loaded normally, but counted separately in diagnostics

## Stable base classes and protocols

## Preferred import boundaries for plugin authors

For most plugins, the safe import layers are:

- `agora` when the root facade already exports the contract you need
- `agora.core` for shared framework contracts
- `agora.core.source`, `agora.core.sink`, `agora.core.middleware`,
  `agora.core.checkpoint`, `agora.core.context`, `agora.core.metrics`, and
  `agora.core.tracing` for domain-specific contracts
- `agora.runner` for runner coordination contracts
- `agora.state` for state backend contracts

For provisional plugin groups, the following module-level contracts are also
part of the public boundary:

- `agora.ai.cache`
- `agora.ai.providers.base` for provider capability protocols and response types
- `agora.core.registry` for MANIFEST compatibility binding
- `agora.core.retry` for shared retry helpers
- `agora.metrics.exporters`
- `agora.middlewares.dedup.stores.base`

Prefer those facades over file-level modules. They are what the architecture
and public API tests freeze intentionally.

### Sources — `agora.sources`

Implement `BaseSource[T]` from `agora.core.source`. Required:

- `source_name: str` — class attribute
- `stream() -> AsyncGenerator[T, None]` — async generator

Optional lifecycle hooks: `open()`, `close()`, `prepare_resume()`, `current_checkpoint()`.

For checkpointable sources, also set `supports_checkpoint = True`. See [Recovery Support Matrix](../guides/recovery-matrix.md).

### Data-plane contract for sources and sinks

If a plugin source emits anything other than Python rows, or a plugin sink
accepts anything beyond Python rows, advertise that explicitly:

- sources: override `data_plane_spec()` and return `SourceDataPlaneSpec`
- sinks: declare `accepted_data_planes` / `native_data_planes`, or override
  `sink_capabilities()` with explicit planes

For plugin tests, prefer the public helpers:

```python
from agora import sink_data_plane_spec, source_data_plane_spec

source_spec = source_data_plane_spec(MySource())
sink_spec = sink_data_plane_spec(MySink())
```

Those helpers use the same normalization and validation path the runtime uses.
Legacy bool flags still work in `0.3.x`, but they are compatibility shims only
and emit `DeprecationWarning` when Agora has to infer the contract from them.

### Sinks — `agora.sinks`

Implement `BaseSink[T]` from `agora.core.sink`. Required:

- `sink_name: str` — class attribute
- `write(record: T) -> None` — async

Optional: `open()`, `close()`, `flush()`, `write_batch()`.

If a sink implements native batch writing, `write_batch()` may either:

- behave like `BaseSink.write_batch()` and return `None` on success
- return `list[WriteResult]` with one outcome per input record when the sink
  can report partial per-record success/failure

That second form is now part of the public batch-delivery contract and is what
the runtime uses to preserve per-record DLQ and checkpoint behavior inside one
batch flush.

### Middlewares — `agora.middlewares`

Implement `Middleware[T, U]` from `agora.core.middleware`. Required:

- `name: str` — class attribute
- `process(record: T, ctx: PipelineContext) -> U | None` — async

Optional: `on_start(ctx)`, `on_stop(ctx)`, `on_error(record, exc, ctx)`. Return
`None` to drop the record.

### Runner — `agora.runner`

Implement a runner class and register it as a factory. The runner contract is `provisional` in shape but `stable` in group name.

### Dedup stores — `agora.middlewares.dedup.stores`

Implement `DedupStore[K]` from `agora.middlewares.dedup.stores.base`. Required:

- `store_name: str`
- `exists(key: K) -> bool` — async
- `add(key: K) -> None` — async
- `mark_if_new(key: K) -> bool` — async

### Dedup strategies — `agora.middlewares.dedup.strategies`

Implement `DedupStrategy[T, K]` from `agora.middlewares.dedup.strategies.base`. Required:

- `strategy_name: str`
- `extract_key(record: T) -> K` — sync or async

## Internal paths — do not import

These module paths are internal and not part of the public contract:

- underscore-prefixed support modules under `agora.core` and
  `agora.core.<domain>` such as:
  - `agora.core.runtime._*`
  - `agora.core.sink._*`
  - `agora.core.context._*`
  - `agora.core.metrics._*`
- underscore-prefixed support modules under `agora.sources`, `agora.sinks`,
  and `agora.middlewares`
- internal helper files that are not re-exported from a documented facade

`agora.core.runtime` itself is an advanced public facade, but it is still not
the default plugin boundary. Reach for it only when building runtime-adjacent
tooling that genuinely needs coordination-level types.

Importing from internal paths may break at any minor release without notice.

## When this page changes

- Adding a new `stable` group is a minor-version change. The new group ships with at least one preservation test.
- Changing a label from `stable` to `provisional` is a breaking change — major-version bump required.
- Changing a label from `provisional` to `stable` is a minor-version change.
- Removing a `stable` group is a breaking change — major-version bump required.

Each `stable` group on this page has a backing test in `tests/preservation/test_plugin_contract.py`.
