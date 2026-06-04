# Agora ETL Framework

_When to read this: you want the quickest map of what Agora does and where to go next in the documentation._

agora-etl is a Python async ETL framework built around a `Source → Middleware chain → Sink(s)` model. It handles checkpointing, dead-letter queues, retries, and long-running workers so you can focus on the transformation logic.

## Start here

- **Running your first pipeline** → [guides/quickstart.md](guides/quickstart.md)
- **Backend and integration examples** → [plugins/index.md](plugins/index.md)
- **Understanding pipeline structure** → [guides/pipelines.md](guides/pipelines.md)
- **Preparing for production** → [guides/scheduling.md](guides/scheduling.md)
- **Handling failures and recovery** → [guides/failure-handling.md](guides/failure-handling.md)

## Guides

| Guide | What it covers |
|---|---|
| [Quickstart](guides/quickstart.md) | Build and run a first pipeline |
| [Pipelines](guides/pipelines.md) | Compose sources, middlewares, sinks, fan-out, and routing |
| [Lifecycle](guides/lifecycle.md) | Learn startup, run, shutdown, worker, and replay order |
| [Runtime Guarantees](guides/runtime-guarantees.md) | What the runtime promises under success, failure, and restart |
| [Failure Handling](guides/failure-handling.md) | DLQ, retry, and sink failure policies |
| [Checkpointing](guides/checkpointing.md) | Resume long-running and file-based pipelines |
| [Scheduling](guides/scheduling.md) | ScheduledPipeline, WorkerPool, and graceful shutdown |
| [Testing](guides/testing.md) | Test sources, middlewares, and whole pipelines |
| [Observability](guides/observability.md) | Run summaries, health endpoints, metrics, and tracing |
| [Configuration](configuration.md) | `AgoraSettings` and `agora/v1` TOML configs |
| [Plugins](plugins/index.md) | Official plugin families and plugin authoring |

## Reference

| Reference | What it covers |
|---|---|
| [Sources](source/index.md) | Built-in and custom source types |
| [Sinks](sink/index.md) | Built-in and custom sink types |
| [Middlewares](middleware/index.md) | Built-in, AI, and custom middlewares |
| [Schema](schema.md) | Schema inference, contracts, and persistence |
| [State](state.md) | Shared key-value backends and helper stores |
| [Architecture](architecture.md) | Execution lanes, state, plugin loading, and runtime structure |
| [CLI](cli.md) | Command reference |
| [Change Log](change-log/index.md) | Release history |
