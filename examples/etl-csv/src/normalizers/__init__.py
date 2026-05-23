from __future__ import annotations

from models import Product


def row_to_product(row: dict) -> Product | None:
    try:
        return Product(
            id=row["id"],
            name=row["name"],
            category=row["category"],
            price=float(row["price"]),
            in_stock=row.get("in_stock", "true").lower() == "true",
        )
    except (KeyError, ValueError):
        return None
