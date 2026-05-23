from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from normalizers import row_to_product

from agora.core.pipeline import BoundPipeline, Pipeline
from agora.sinks.file.csv import CsvSink
from agora.sources.file.csv import CsvSource

if TYPE_CHECKING:
    from models import Product

INPUT_PATH = Path("data/products.csv")
OUTPUT_PATH = Path("output/products_filtered.csv")


def _product_to_row(p: Product) -> dict:
    return {
        "id": p.id,
        "name": p.name,
        "category": p.category,
        "price": p.price,
        "in_stock": str(p.in_stock).lower(),
    }


async def build_pipeline() -> BoundPipeline:
    source = CsvSource(
        path=INPUT_PATH,
        row_mapper=row_to_product,
    )
    sink = CsvSink(
        path=OUTPUT_PATH,
        row_mapper=_product_to_row,
        fieldnames=["id", "name", "category", "price", "in_stock"],
    )
    return (
        Pipeline(source, id="etl-csv")
        .filter(lambda p: p.in_stock, name="in_stock_filter")
        .build(sink, batch_size=100)
    )
