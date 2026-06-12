# Developing Plugins

_When to read this: you are publishing a plugin package and need the concrete entry-point, registry, and packaging conventions that Agora expects._

Agora plugins are normal Python packages that register components through entry
points.

For the compatibility rules behind those entry points, read
[Plugin Contract](contract.md) alongside this page. This page is about the
mechanics; the contract page is about what stays stable.

## Entry-point groups

| Group | Purpose |
|---|---|
| `agora.sources` | Custom source types |
| `agora.sinks` | Custom sink types |
| `agora.middlewares` | Custom middleware types |
| `agora.ai.providers` | AI provider implementations |
| `agora.ai.caches` | LLM response cache backends |
| `agora.runner` | Custom runner types |
| `agora.middlewares.dedup.stores` | Dedup store backends |
| `agora.middlewares.dedup.strategies` | Dedup comparison strategies |
| `agora.metrics.exporters` | Metrics exporters |
| `agora.state.backends` | State backend implementations |

## Minimal registration example

In `pyproject.toml`:

```toml
[project.entry-points."agora.sources"]
my_source = "my_package.sources:MySource"

[project.entry-points."agora.sinks"]
my_sink = "my_package.sinks:MySink"

[project.entry-points."agora.middlewares"]
my_middleware = "my_package.middlewares:MyMiddleware"
```

After installation:

```bash
agora plugins list
```

## Config-driven usage

Registered plugins can be referenced by name in declarative pipeline configs:

```toml
[pipelines.example.source]
type = "my_source"
url = "https://api.example.com"

[[pipelines.example.middlewares]]
type = "my_middleware"
threshold = 0.9

[[pipelines.example.sinks]]
type = "my_sink"
dsn = "postgresql://example/db"
```

Or in Python:

```python
from agora import Pipeline
from agora.sources import source_registry
from agora.sinks import sink_registry

source = source_registry.create("my_source", url="https://api.example.com")
sink = sink_registry.create("my_sink", dsn="postgresql://example/db")

pipeline = Pipeline(source).build(sink)
```

Prefer registry and contract imports from public facades such as:

- `agora`
- `agora.core`
- `agora.core.source`
- `agora.core.sink`
- `agora.core.middleware`
- `agora.runner`
- `agora.state`

For group-specific provisional plugins, these advanced module-level contracts
are also part of the supported boundary:

- `agora.ai.cache` for AI cache implementations
- `agora.ai.providers.base` for AI provider capability protocols and response types
- `agora.core.registry` for MANIFEST compatibility binding
- `agora.core.retry` for shared retry policy/helpers
- `agora.metrics.exporters` for metrics exporter protocols and registries
- `agora.middlewares.dedup.stores.base` for dedup store contracts

Avoid underscore-prefixed support modules, even if they happen to work today.

## Manifest compatibility

For the full manifest contract — what `AGORA_PLUGIN_MANIFEST_VERSION` tracks,
when it bumps, and how compatibility is evaluated — see
[Manifest Contract](manifest.md).

Quick summary: declare a `MANIFEST` object at your package root with
`agora_api_version = AGORA_PLUGIN_MANIFEST_VERSION` to opt into compatibility
diagnostics. Without a `MANIFEST`, your plugin loads normally with
`compatible=None`.

```python
from agora.core.registry import AGORA_PLUGIN_MANIFEST_VERSION
```

## Authoring examples

### AI provider

```python
from agora.ai.providers.base import CompletionResponse

class MyProvider:
    model = "my-model-v1"

    async def complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> CompletionResponse:
        response = await self._client.generate(prompt)
        return CompletionResponse(
            content=response.text,
            model=self.model,
            input_tokens=response.usage.input,
            output_tokens=response.usage.output,
        )
```

If the provider also supports embeddings, implement `embed()` and
`embed_batch()` too. Completion-only providers are valid, but embedding-based
Agora features will reject them explicitly.

### Dedup store

```python
from agora.middlewares.dedup.stores.base import DedupStore

class RedisStore(DedupStore[str]):
    store_name = "redis"

    async def exists(self, key: str) -> bool:
        return await self._redis.sismember(self._set_key, key)

    async def add(self, key: str) -> None:
        await self._redis.sadd(self._set_key, key)

    async def mark_if_new(self, key: str) -> bool:
        return bool(await self._redis.sadd(self._set_key, key))

    async def close(self) -> None:
        await self._redis.aclose()
```

## Discovery hook

```python
from agora import discover_plugins

discover_plugins()
```

In most projects you do not need to call this manually. The CLI and config
assembly paths load plugin entry points for you.
