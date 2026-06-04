# Sources

_When to read this: you need to choose the upstream side of a pipeline or understand which built-in sources support batching, Arrow, or resume._

Sources emit records into the pipeline. Some are simple and row-oriented, some
are batch-aware, and some stay fully columnar with Arrow batches.

## Built-in sources at a glance

| Source | Best for | Output shape | Checkpoint support |
|---|---|---|---|
| [IterableSource](iterable.md) | tests, tiny in-memory runs | records | no |
| [CsvSource](csv.md) | CSV or TSV files | records or `list` batches | yes |
| [JsonLinesSource](jsonlines.md) | JSONL files | records or `list` batches | yes |
| [ParquetSource](parquet.md) | Parquet files | records or `pa.RecordBatch` | yes |
| [ArrowCsvSource](arrow-csv.md) | CSV with Arrow-native throughput | `pa.RecordBatch` | row-count only |
| [ArrowJsonLinesSource](arrow-jsonlines.md) | JSONL with Arrow-native throughput | `pa.RecordBatch` | row-count only |
| [HTTPSource](http.md) | HTTP polling extractors | records | custom only |
| [SQLiteDLQSource](sqlite-dlq.md) | replaying local DLQ records | `DLQRecord` | not the main use case |

## How to choose

- Use `IterableSource` for tests, examples, and one-off in-memory inputs.
- Use `CsvSource` or `JsonLinesSource` for row-oriented file ingestion.
- Use `ParquetSource` when the file is already columnar or large enough that
  Arrow-backed batch reads matter.
- Use `ArrowCsvSource` or `ArrowJsonLinesSource` when the pipeline should stay
  columnar end to end.
- Subclass `HTTPSource` when the upstream system is a paginated or polled HTTP
  API.
- Use `SQLiteDLQSource` only for local replay workflows after failures.

## Checkpoint support at a glance

Checkpointing is opt-in per source. For the full contract, edge cases, and
resume semantics, see [Checkpointing](../guides/checkpointing.md) and the
[Recovery Matrix](../guides/recovery-matrix.md).

| Source | Resume position |
|---|---|
| `CsvSource` | row number |
| `JsonLinesSource` | line number |
| `ParquetSource` | row number |
| `ArrowCsvSource` | row-count checkpoint, no mid-file skip restore |
| `ArrowJsonLinesSource` | row-count checkpoint, no mid-file skip restore |
| `HTTPSource` | only if the subclass implements it |

## Batch and Arrow lanes

Agora has three common source shapes:

- record-by-record sources
- Python batch sources that emit `list[record]`
- Arrow-native sources that emit `pa.RecordBatch`

If the source is naturally batch-oriented, pair it with batch-aware middleware.
If the source is Arrow-native, keep the rest of the pipeline Arrow-native too.
That means Arrow middleware and an Arrow-friendly sink such as `ParquetSink`.

## Custom and plugin sources

- Want to write a source from scratch: [Custom source](custom.md)
- Want Redis, Kafka, or PostgreSQL sources: see [Plugins](../plugins/index.md)

