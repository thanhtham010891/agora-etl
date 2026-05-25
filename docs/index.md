# Agora ETL Documentation

Agora is an async-first ETL framework for Python built around a simple pipeline model:

```text
Source -> Middleware chain -> Sink(s)
```

It is designed for teams that want a lightweight Python-native runtime for ingestion, transformation, enrichment, checkpointing, dead-letter queues, and long-running scheduled workers.

## What you can build with Agora

- batch imports from files, APIs, or databases
- scheduled ETL jobs with health endpoints
- pipelines with retry, validation, enrichment, and deduplication
- resumable jobs with checkpoints and DLQ replay
- plugin-based integrations for Redis, Kafka, PostgreSQL, and more

## Package overview

- `agora-etl`: the core framework
- `agora-etl-plugins`: the official plugin bundle for Redis, cron scheduling, distributed coordination, Kafka, and PostgreSQL

## Start here

- New to Agora: [Getting Started](getting-started.md)
- Want to run pipelines from TOML: [Configuration](configuration.md)
- Need command-line workflows: [CLI Reference](cli.md)
- Planning production workers: [Runner](runner.md)

## Reference guides

- [Sources](sources.md)
- [Sinks](sinks.md)
- [Middlewares](middlewares.md)
- [Plugins](plugins.md)
- [Architecture](architecture.md)

## Learning paths

### I want to run my first pipeline

1. Read [Getting Started](getting-started.md)
2. Run `agora new my-project`
3. Run `agora run pipelines.example --dry-run`

### I want declarative configs

1. Read [Configuration](configuration.md)
2. Validate a config with `agora run --config pipelines.toml --plan`
3. Run it with `agora run --config pipelines.toml`

### I want long-running workers

1. Read [Runner](runner.md)
2. Add a `worker.py` module that returns a `WorkerPool`
3. Start it with `agora worker`

### I want to extend Agora

1. Read [Plugins](plugins.md)
2. Register components with Python entry points
3. Verify registration with `agora plugins list`

## Examples

The repository includes end-to-end example projects:

- `examples/etl-csv`
- `examples/etl-json`
- `examples/etl-parquet`
- `examples/etl-http`

## Production notes

Agora is a framework, not a hosted platform. You own deployment, secret management, scheduling policy, and operational guardrails. The core runtime gives you:

- structured retries and backoff
- health and readiness endpoints
- checkpointing and DLQ replay
- pluggable state backends and integrations

For deployment-facing behavior, start with [Runner](runner.md) and [Architecture](architecture.md).
For config import safety and trusted-input boundaries, also read
[Configuration](configuration.md).
