# ArrowProcessBatchMiddleware

_Use this when: an Arrow-native transform is heavy enough to justify a separate process pool._

`ArrowProcessBatchMiddleware` is the process-isolated Arrow sibling of
`ArrowMapMiddleware`.

## What it does

- receives `pa.RecordBatch`
- sends the batch across the worker boundary as Arrow IPC bytes
- runs a `pa.RecordBatch -> pa.RecordBatch` transform in a worker process
- returns the transformed batch to the main runtime

## When it is a good fit

- the source already emits Arrow batches
- the transform is vectorized and columnar
- the transform is CPU-heavy or uses native code that should not share the main
  runtime process
- downstream can stay Arrow-native

## How it behaves

- checkpoint, DLQ, and sink commit decisions stay in the main runtime process
- timeout recycles the worker-pool generation
- unresolved sibling Arrow batches from that recycled generation fail too in
  ordered pipelined mode
- the `0.3.x` contract is row-preserving: output row count must match input row
  count

## Example

```python
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from agora import (
    ArrowCsvSource,
    ArrowFilterMiddleware,
    ArrowProcessBatchMiddleware,
    DeliveryConfig,
    Pipeline,
)
from agora.sinks.file.parquet import ParquetSink


def keep_paid_orders(batch: pa.RecordBatch) -> pa.BooleanArray:
    return pc.equal(batch.column("status"), "paid")


def enrich_order_metrics(batch: pa.RecordBatch) -> pa.RecordBatch:
    amount_idx = batch.schema.get_field_index("amount")
    tax_idx = batch.schema.get_field_index("tax")
    region_idx = batch.schema.get_field_index("region")

    amount = pc.cast(batch.column(amount_idx), pa.float64())
    tax = pc.cast(batch.column(tax_idx), pa.float64())
    net_amount = pc.subtract(amount, tax)
    region = pc.utf8_upper(batch.column(region_idx))

    batch = batch.set_column(region_idx, "region", region)
    return batch.append_column("net_amount", net_amount)


pipeline = (
    Pipeline(ArrowCsvSource(path="data/orders.csv", batch_size=50_000))
    .pipe(ArrowFilterMiddleware(keep_paid_orders, name="paid_only"))
    .pipe(
        ArrowProcessBatchMiddleware(
            fn=enrich_order_metrics,
            max_workers=4,
            max_in_flight_batches=4,
            timeout_s=120,
            name="order_metrics",
        )
    )
    .build(
        ParquetSink(path="output/orders.parquet", row_mapper=lambda row: row),
        config=DeliveryConfig(batch_size=50_000, checkpoint_every=2),
    )
)
```

## Why filtering usually happens before the process hop

`ArrowFilterMiddleware` handles row-dropping before the process boundary. That
keeps `ArrowProcessBatchMiddleware` focused on row-preserving transforms, which
is the supported `0.3.x` contract.

## When to choose something else

- use [ArrowMapMiddleware](arrow-map.md) when in-process Arrow compute is
  already fast enough
- use [ProcessBatchMiddleware](process-batch.md) when the pipeline is still
  Python-object based

