# Plugins

Agora is designed to be extended. Third-party packages register themselves via Python entry-points and are discovered when the relevant registries are loaded at runtime.

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

## Official plugins

The main first-party extension package is [`agora-etl-plugins`](https://pypi.org/project/agora-etl-plugins/). It currently groups official integrations such as:

- Redis
- cron scheduling helpers
- distributed worker coordination
- Kafka
- PostgreSQL

## Registering a plugin

In your package's `pyproject.toml`:

```toml
[project.entry-points."agora.sources"]
my_source = "my_package.sources:MySource"

[project.entry-points."agora.sinks"]
my_sink = "my_package.sinks:MySink"

[project.entry-points."agora.middlewares"]
my_middleware = "my_package.middlewares:MyMiddleware"

[project.entry-points."agora.ai.providers"]
my_provider = "my_package.providers:MyProvider"
```

After installing the package, `agora plugins list` will show the registered components.

If a plugin package also exposes a `MANIFEST`, Agora uses that metadata for CLI diagnostics and compatibility hints. The manifest version is a plugin-contract marker, not the same thing as the `agora-etl` package version.

For older plugins, `agora.core.registry.AGORA_API_VERSION` still aliases the
same manifest-contract version constant. New plugins should prefer
`AGORA_PLUGIN_MANIFEST_VERSION`. The alias is deprecated in `0.2.0`.

If a plugin advertises an incompatible manifest version, Agora leaves it out of
the active registry but still surfaces it in CLI diagnostics as incompatible so
operators can see why it was rejected.

## Using plugins in config-driven pipelines

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
dsn = "postgresql://localhost/mydb"
```

Or equivalently, in Python-driven assembly:

```python
from agora import Pipeline
from agora.sources import source_registry
from agora.sinks import sink_registry

source = source_registry.create("my_source", url="https://api.example.com")
sink = sink_registry.create("my_sink", dsn="postgresql://localhost/mydb")

pipeline = Pipeline(source).build(sink)
```

## AI providers

AI providers implement the `AIProvider` protocol:

```python
from agora.ai.providers.base import AIProvider, CompletionResponse

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

Register it:

```toml
[project.entry-points."agora.ai.providers"]
my_provider = "my_package.providers:MyProvider"
```

Use it in any AI middleware:

```python
from agora.middlewares.ai.enrich import AIEnrichMiddleware

pipeline.pipe(AIEnrichMiddleware(
    provider=MyProvider(api_key="..."),
    prompt_template="Summarize: {name}",
    output_fields=["summary"],
))
```

## Dedup store plugins

Implement the `DedupStore` protocol to add a distributed dedup backend:

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

## Discovering plugins manually

```python
from agora.core.discovery import discover_plugins

discover_plugins()   # loads all entry-points into the registries
```

This is called automatically when using `AgoraContainer.from_config()` or the CLI. In most projects you do not need to call it manually.
