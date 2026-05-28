# Agora ETL Framework

agora-etl is a Python async ETL framework built around a `Source → Middleware chain → Sink(s)` model. It handles checkpointing, dead-letter queues, retries, and long-running workers so you can focus on the transformation logic.

## Start here

- **Running your first pipeline** → [guides/quickstart.md](guides/quickstart.md)
- **Building a production worker** → [guides/scheduling.md](guides/scheduling.md)
- **Understanding how failures work** → [guides/failure-handling.md](guides/failure-handling.md)

## Guides

| Guide | What it covers |
|---|---|
| [Quickstart](guides/quickstart.md) | First pipeline in 5 minutes |
| [Pipelines](guides/pipelines.md) | Composing sources, middlewares, and sinks |
| [Failure Handling](guides/failure-handling.md) | DLQ, retry, failure policies |
| [Checkpointing](guides/checkpointing.md) | Resumable pipelines |
| [Scheduling](guides/scheduling.md) | Long-running workers, WorkerPool |
| [Testing](guides/testing.md) | Testing pipelines and middlewares |
| [Observability](guides/observability.md) | Health, metrics, tracing |
| [Configuration](configuration.md) | TOML-based pipeline config |
| [Plugins](plugins/index.md) | Redis, Kafka, PostgreSQL integrations |
| [Benchmark](benchmark/index.md) | Repeatable local performance checks |

## Reference

| Reference | What it covers |
|---|---|
| [Sources](sources.md) | Built-in and custom source types |
| [Sinks](sinks.md) | Built-in and custom sink types |
| [Middlewares](middlewares.md) | Built-in, AI, and custom middlewares |
| [Architecture](architecture.md) | Execution model, guarantees, state backends |
| [CLI](cli.md) | Command reference |
| [Change Log](change-log/index.md) | Release history |
