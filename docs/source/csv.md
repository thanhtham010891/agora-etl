# CsvSource

_Use this when: the upstream system gives CSV or TSV files and the pipeline logic is mostly row-oriented._

`CsvSource` reads a delimited text file and maps each row into a pipeline
record.

## Good fits

- CSV exports from internal tools
- TSV files from analytics or BI jobs
- row-oriented ETL where each line becomes one domain record

## Characteristics

- stdlib-based, no Arrow requirement
- supports checkpointing by row number
- can emit ordinary records or Python `list` batches
- works well with `MapMiddleware`, `FilterMiddleware`, and batch middleware

## Example

```python
from agora import Pipeline
from agora.sources.file.csv import CsvSource
from agora.sinks.io.stdout import StdoutSink

source = CsvSource(
    path="data/products.csv",
    row_mapper=lambda row: {
        "id": int(row["id"]),
        "name": row["name"].strip(),
        "price": float(row["price"]),
    },
    delimiter=",",
    has_header=True,
)

summary = await Pipeline(source).build(StdoutSink()).run()
```

## Batch lane

Set `emit_batches=True` when the rest of the pipeline is naturally
batch-oriented:

```python
source = CsvSource(
    path="data/products.csv",
    row_mapper=lambda row: row,
    emit_batches=True,
    emit_batch_size=5_000,
)
```

This emits `list[record]` batches instead of one record at a time.

## Resume behavior

`CsvSource` stores the last handled row number. On restart it skips forward to
that row before resuming.

For the exact checkpoint and recovery contract, see
[Checkpointing](../guides/checkpointing.md) and the
[Recovery Matrix](../guides/recovery-matrix.md).

