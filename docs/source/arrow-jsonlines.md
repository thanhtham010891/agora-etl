# ArrowJsonLinesSource

_Use this when: JSONL ingestion should stay Arrow-native instead of turning into per-row Python records immediately._

`ArrowJsonLinesSource` reads newline-delimited JSON and emits
`pyarrow.RecordBatch` objects directly.

## Good fits

- JSONL event archives that need vectorized transforms
- file pipelines that should stay Arrow-native end to end
- process-isolated Arrow workloads

## Characteristics

- requires `pip install "agora-etl[file]"`
- always emits `pa.RecordBatch`
- bypasses row-level mapping
- best paired with Arrow middleware and `ParquetSink`
- checkpointing is row-count based rather than precise mid-file restore

## Example

```python
import pyarrow.compute as pc

from agora import ArrowFilterMiddleware, ArrowJsonLinesSource, Pipeline
from agora.sinks.file.parquet import ParquetSink

summary = await (
    Pipeline(ArrowJsonLinesSource(path="data/events.jsonl", batch_size=65_536))
    .pipe(ArrowFilterMiddleware(lambda batch: pc.equal(batch.column("kind"), "order")))
    .build(ParquetSink(path="orders.parquet", row_mapper=lambda row: row))
    .run()
)
```

## When not to use it

If the very next step converts every row into Python objects anyway,
`JsonLinesSource` is usually the simpler choice.

