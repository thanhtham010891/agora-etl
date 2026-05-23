from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Product:
    id: str
    name: str
    category: str
    price: float
    in_stock: bool
