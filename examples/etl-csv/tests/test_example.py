from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture
def products_csv(tmp_path: Path) -> Path:
    csv_file = tmp_path / "products.csv"
    csv_file.write_text(
        "id,name,category,price,in_stock\n"
        "1,Widget A,tools,9.99,true\n"
        "2,Widget B,tools,14.99,false\n"
        "3,Gadget X,electronics,49.99,true\n"
        "4,Gadget Y,electronics,99.99,true\n"
        "5,Doohickey,misc,4.99,false\n"
    )
    return csv_file


@pytest.mark.asyncio
async def test_csv_pipeline_filters_out_of_stock(products_csv: Path, tmp_path: Path) -> None:
    from normalizers import row_to_product
    from pipelines.example import _product_to_row

    from agora.core.pipeline import Pipeline
    from agora.sinks.file.csv import CsvSink
    from agora.sources.file.csv import CsvSource

    output = tmp_path / "out.csv"
    source = CsvSource(path=products_csv, row_mapper=row_to_product)
    sink = CsvSink(path=output, row_mapper=_product_to_row)
    pipeline = (
        Pipeline(source, id="test-csv")
        .filter(lambda p: p.in_stock, name="in_stock_filter")
        .build(sink)
    )
    summary = await pipeline.run()

    assert summary.records_consumed == 5
    assert summary.records_written == 3  # Widget A, Gadget X, Gadget Y
    assert summary.records_dropped == 2  # Widget B, Doohickey

    lines = output.read_text().splitlines()
    assert lines[0] == "id,name,category,price,in_stock"
    assert len(lines) == 4  # header + 3 records
