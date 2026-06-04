# ArrowCsvSource

_Use this when: CSV ingestion should stay columnar from the source onward._

`ArrowCsvSource` reads CSV files and emits `pyarrow.RecordBatch` objects
directly. It avoids per-row Python object allocation in the source path.

## Good fits

- large CSV ingestion with Arrow-native transforms
- vectorized filtering or mapping with `pyarrow.compute`
- process-isolated Arrow transforms through `ArrowProcessBatchMiddleware`

## Characteristics

- requires `pip install "agora-etl[file]"`
- always emits `pa.RecordBatch`
- bypasses `row_mapper`
- best results come from an all-Arrow path
- checkpointing is row-count based, not full mid-file resume

## Example

```python
import pyarrow.compute as pc

from agora import ArrowCsvSource, ArrowFilterMiddleware, Pipeline
from agora.sinks.file.parquet import ParquetSink

summary = await (
    Pipeline(ArrowCsvSource(path="data/products.csv", batch_size=65_536))
    .pipe(ArrowFilterMiddleware(lambda batch: pc.greater(batch.column("price"), 0)))
    .build(ParquetSink(path="out.parquet", row_mapper=lambda row: row))
    .run()
)
```

## Keep the Arrow path intact

To keep the columnar fast path:

- use Arrow-native middleware
- use an Arrow-friendly sink such as `ParquetSink`
- avoid regular `MapMiddleware` or `FilterMiddleware`, which materialize rows

For process-isolated Arrow transforms, see
[ArrowProcessBatchMiddleware](../middleware/arrow-process-batch.md).

