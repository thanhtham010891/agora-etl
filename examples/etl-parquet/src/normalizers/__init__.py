from __future__ import annotations

from models import SalesRecord


def row_to_sales_record(row: dict) -> SalesRecord | None:
    try:
        quantity = int(row["quantity"])
        unit_price = float(row["unit_price"])
        return SalesRecord(
            order_id=str(row["order_id"]),
            product=str(row["product"]),
            quantity=quantity,
            unit_price=unit_price,
            region=str(row["region"]),
            total=round(quantity * unit_price, 2),
        )
    except (KeyError, ValueError, TypeError):
        return None
