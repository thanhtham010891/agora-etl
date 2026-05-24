# Changelog

## 0.1.2 (May 24, 2026)

### Fixes

- Fixed `Schedule.cron(...)` to load cron helpers from `agora-etl-plugins[cron]`
- Added `py.typed` to the core package so the published wheel matches its typed classifier
- Updated public docs and examples to use the current `agora_plugins.*` import paths
- Improved `agora plugins list` hints so entry-point plugins are no longer mislabeled as built-ins

## 0.1.0 (May 23, 2026)

### Features

- Fluent immutable pipeline builder (`Pipeline.pipe().filter().build().run()`)
- Async-first execution engine with adaptive backpressure
- Built-in dead-letter queue (DLQ) with SQLite backend and replay support
- Resumable pipelines via `CheckpointStore` (in-memory and SQLite backends)
- Plugin system via Python entry-points (`agora.sources`, `agora.sinks`, `agora.middlewares`)
- Built-in sources: `HTTPSource`, `JsonLinesSource`, `CsvSource`, `ParquetSource`, `IterableSource`
- Built-in sinks: `StdoutSink`, `JsonLinesSink`, `CsvSink`, `ParquetSink`, `WebhookSink`, `LogSink`
- Built-in middlewares: `MapMiddleware`, `FilterMiddleware`, `RetryMiddleware`, `ValidateMiddleware`, `EnrichMiddleware`, `DedupMiddleware`
- AI enrichment middlewares: `AIEnrichMiddleware`, `AIClassifyMiddleware`, `AIExtractMiddleware`, `AIValidateMiddleware`, `AITranslateMiddleware`, `AIBatchMiddleware`
- `ScheduledPipeline` with interval, cron, continuous, and once schedules
- `WorkerPool` for concurrent multi-pipeline execution with graceful shutdown
- Health server (`/health`, `/metrics`, `/ready`) with optional Bearer token auth
- Prometheus metrics exporter
- OpenTelemetry tracing bridge
- `AgoraContainer` for config-driven pipeline assembly
- CLI: `agora new`, `agora run`, `agora worker`, `agora dlq`, `agora plugins`, `agora config`, `agora version`
- State backends: `MemoryBackend`, `SQLiteBackend` with TTL and membership stores
