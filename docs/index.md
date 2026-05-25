# Agora ETL Documentation

Agora is an async-first ETL framework for Python built around a simple pipeline model:

```text
Source -> Middleware chain -> Sink(s)
```

It is designed for teams that want a lightweight Python-native runtime for ingestion, transformation, enrichment, checkpointing, dead-letter queues, and long-running scheduled workers.

## Why Agora

- batch imports from files, APIs, or databases
- scheduled ETL jobs with health endpoints
- pipelines with retry, validation, enrichment, and deduplication
- resumable jobs with checkpoints and DLQ replay
- plugin-based integrations for Redis, Kafka, PostgreSQL, and more

## Package Model

- `agora-etl`: the core framework
- `agora-etl-plugins`: the official plugin bundle for Redis, cron scheduling, distributed coordination, Kafka, and PostgreSQL

## Start here

- New to Agora: [Getting Started](getting-started.md)
- Want to run pipelines from TOML: [Configuration](configuration.md)
- Need command-line workflows: [CLI Reference](cli.md)
- Need repeatable local performance checks: [Benchmark](benchmark/index.md)
- Want the latest benchmark report table: [Benchmark Matrix](benchmark/matrix.md)
- Planning production workers: [Runner](runner.md)
- Understanding plugin boundaries: [Plugins](plugins/index.md)
- Browsing release history: [Change Log](change-log/index.md)

## Reference

- [Sources](sources.md)
- [Sinks](sinks.md)
- [Middlewares](middlewares.md)
- [Plugins](plugins/index.md)
- [Architecture](architecture.md)

## Common Paths

- First pipeline: read [Getting Started](getting-started.md), then run `agora new my-project`
- Declarative config: read [Configuration](configuration.md), then try `agora run --config pipelines.toml --plan`
- Long-running workers: read [Runner](runner.md), then wire a `WorkerPool`
- Plugin development: read [Plugins](plugins/index.md), then verify registration with `agora plugins list`

## Examples

The repository includes end-to-end example projects:

- `examples/etl-csv`
- `examples/etl-json`
- `examples/etl-parquet`
- `examples/etl-http`

## Production Notes

Agora is a framework, not a hosted platform. You own deployment, secret management, scheduling policy, and operational guardrails. The core runtime gives you:

- structured retries and backoff
- health and readiness endpoints
- checkpointing and DLQ replay
- pluggable state backends and integrations

For deployment-facing behavior, start with [Runner](runner.md) and [Architecture](architecture.md).
For config import safety and trusted-input boundaries, also read
[Configuration](configuration.md).
