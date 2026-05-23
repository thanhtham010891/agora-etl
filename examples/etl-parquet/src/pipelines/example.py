from __future__ import annotations

from pathlib import Path

from models import SalesRecord  # noqa: TC002
from normalizers import row_to_sales_record

from agora.core.pipeline import BoundPipeline, Pipeline
from agora.sinks.file.parquet import ParquetSink
from agora.sources.file.parquet import ParquetSource

INPUT_PATH = Path("data/sales.parquet")
OUTPUT_PATH = Path("output/sales_enriched.parquet")


def _record_to_row(r: SalesRecord) -> dict:
    return {
        "order_id": r.order_id,
        "product": r.product,
        "quantity": r.quantity,
        "unit_price": r.unit_price,
        "region": r.region,
        "total": r.total,
    }


async def build_pipeline() -> BoundPipeline:
    source = ParquetSource(
        path=INPUT_PATH,
        row_mapper=row_to_sales_record,
    )
    sink = ParquetSink(
        path=OUTPUT_PATH,
        row_mapper=_record_to_row,
        compression="snappy",
    )
    return (
        Pipeline(source, id="etl-parquet")
        .filter(lambda r: r.total > 0, name="positive_total")
        .build(sink, batch_size=500)
    )
