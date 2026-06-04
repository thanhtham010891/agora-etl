# Sinks

_When to read this: records are already transformed and now the pipeline needs the right destination contract._

Sinks receive processed records and persist them. Some are local development
helpers, some write files, some call external systems, and some are only for
recovery workflows.

## Built-in sinks at a glance

| Sink | Best for | Native batch support |
|---|---|---|
| [StdoutSink](stdout.md) | local debugging | no |
| [LogSink](log.md) | structured log output | no |
| [JsonLinesSink](jsonlines.md) | JSONL file output | buffered flush |
| [CsvSink](csv.md) | CSV file output | buffered flush |
| [ParquetSink](parquet.md) | Parquet file output, Arrow path | yes |
| [WebhookSink](webhook.md) | POSTing to HTTP endpoints | optional |
| [SQLiteDLQSink](sqlite-dlq.md) | local dead-letter storage | no |

## How to choose

- Use `StdoutSink` to inspect records quickly.
- Use `LogSink` when the record stream should become application logs.
- Use `JsonLinesSink` or `CsvSink` for file outputs that people or tools read directly.
- Use `ParquetSink` for large outputs, analytical storage, or Arrow-native flows.
- Use `WebhookSink` for downstream APIs, automations, or alerting systems.
- Use `SQLiteDLQSink` only as a recovery backend for failed records.

## Fan-out and routing

Sinks are not only single destinations.

- `.fan_out()` writes each record to multiple sinks.
- `.route()` sends each record to exactly one sink based on predicates.

Those patterns are explained in [Pipelines](../guides/pipelines.md).

## Custom and plugin sinks

- Want to build a sink from scratch: [Custom sink](custom.md)
- Want Kafka, Redis, or PostgreSQL sinks: see [Plugins](../plugins/index.md)

