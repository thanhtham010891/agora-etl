# ParquetSink

_Use this when: output size, analytical shape, or Arrow-native throughput matter._

`ParquetSink` writes records incrementally to Parquet through PyArrow. It also
has an Arrow-native write path for `pa.RecordBatch` inputs.

## Good fits

- analytical exports
- large file outputs
- Arrow-native pipelines
- pipelines that want compression and typed columns

## Characteristics

- requires `pip install "agora-etl[file]"`
- native batch sink
- supports direct `write_arrow_batch()` in the Arrow lane
- schema is inferred from the first written batch

## Record-oriented example

```python
from agora.sinks.file.parquet import ParquetSink

sink = ParquetSink(
    path="output/records.parquet",
    row_mapper=lambda record: {
        "id": record["id"],
        "score": float(record["score"]),
    },
    batch_size=1_000,
    compression="snappy",
)
```

## Arrow-native fit

`ParquetSink` is the main built-in sink for Arrow-native pipelines. Pair it
with `ArrowCsvSource`, `ArrowJsonLinesSource`, `ParquetSource(use_arrow_batches=True)`,
or `ArrowProcessBatchMiddleware` when the whole path should stay columnar.

## Important constraint

The schema is locked from the first batch. Normalize the record shape before
the sink if later records might introduce new columns.

