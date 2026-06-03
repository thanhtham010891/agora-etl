from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CleanOrderEvent:
    event_id: str
    order_id: str
    customer_id: str
    status: str
    total_cents: int
    currency: str
    occurred_at: str
    source: str
