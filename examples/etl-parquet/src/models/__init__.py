from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SalesRecord:
    order_id: str
    product: str
    quantity: int
    unit_price: float
    region: str
    total: float
