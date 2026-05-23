from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture
def sales_parquet(tmp_path: Path) -> Path:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        pytest.skip("pyarrow not installed")

    rows = [
        {
            "order_id": "O1",
            "product": "Widget",
            "quantity": 2,
            "unit_price": 10.0,
            "region": "APAC",
        },
        {"order_id": "O2", "product": "Gadget", "quantity": 0, "unit_price": 5.0, "region": "EMEA"},
        {
            "order_id": "O3",
            "product": "Doohickey",
            "quantity": 3,
            "unit_price": 7.5,
            "region": "APAC",
        },
        {
            "order_id": "O4",
            "product": "Thingamajig",
            "quantity": 1,
            "unit_price": 20.0,
            "region": "AMER",
        },
    ]
    path = tmp_path / "sales.parquet"
    pq.write_table(pa.Table.from_pylist(rows), str(path))
    return path


@pytest.mark.asyncio
async def test_parquet_pipeline_filters_zero_total(sales_parquet: Path, tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")

    from normalizers import row_to_sales_record
    from pipelines.example import _record_to_row

    from agora.core.pipeline import Pipeline
    from agora.sinks.file.parquet import ParquetSink
    from agora.sources.file.parquet import ParquetSource

    output = tmp_path / "out.parquet"
    source = ParquetSource(path=sales_parquet, row_mapper=row_to_sales_record)
    sink = ParquetSink(path=output, row_mapper=_record_to_row)
    pipeline = (
        Pipeline(source, id="test-parquet")
        .filter(lambda r: r.total > 0, name="positive_total")
        .build(sink)
    )
    summary = await pipeline.run()

    assert summary.records_consumed == 4
    assert summary.records_written == 3  # O1, O3, O4 (O2 has quantity=0 → total=0)
    assert summary.records_dropped == 1
