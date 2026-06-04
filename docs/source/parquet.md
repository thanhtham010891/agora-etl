# ParquetSource

_Use this when: the upstream file is already Parquet or the dataset is large enough that Arrow-backed reads matter._

`ParquetSource` reads a Parquet file in batches through PyArrow. It can feed
either ordinary Python records or Arrow batches into the runtime.

## Good fits

- analytical exports already written as Parquet
- larger file ingestion where row-by-row CSV parsing is too expensive
- pipelines that may later move to the Arrow execution lane

## Characteristics

- requires `pip install "agora-etl[file]"`
- supports checkpointing by row number
- can emit mapped records or `pa.RecordBatch`
- best companion for `ParquetSink` and Arrow middleware when staying columnar

## Record-oriented example

```python
from agora import Pipeline
from agora.sources.file.parquet import ParquetSource
from agora.sinks.io.stdout import StdoutSink

source = ParquetSource(
    path="data/sales.parquet",
    row_mapper=lambda row: {
        "id": row["id"],
        "amount": float(row["amount"]),
    },
    batch_size=1_000,
)

summary = await Pipeline(source).build(StdoutSink()).run()
```

## Arrow batch example

```python
from agora import Pipeline
from agora.sources.file.parquet import ParquetSource
from agora.sinks.file.parquet import ParquetSink

source = ParquetSource(
    path="data/sales.parquet",
    row_mapper=lambda row: row,
    use_arrow_batches=True,
)

summary = await (
    Pipeline(source)
    .build(ParquetSink(path="out.parquet", row_mapper=lambda row: row))
    .run()
)
```

When `use_arrow_batches=True`, `row_mapper` is bypassed and the source yields
Arrow `RecordBatch` objects directly.

## Resume behavior

`ParquetSource` stores a row-number checkpoint. In record mode, that gives
restart recovery through the handled row count. In Arrow batch mode, the source
still tracks row count, but the practical recovery model is batch-oriented.

If a bounded run sets `max_records`, the limiting wrapper now trims the final
Parquet batch at the source boundary instead of letting a full Arrow batch
overshoot and stopping later inside the runtime.
