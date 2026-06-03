from __future__ import annotations

import json
from dataclasses import asdict
from typing import TYPE_CHECKING, Any

from domain import CleanOrderEvent

if TYPE_CHECKING:
    from collections.abc import Mapping

_ALLOWED_STATUSES = {"created", "paid", "cancelled", "fulfilled"}


def normalize_order(record: Mapping[str, Any]) -> CleanOrderEvent:
    event_id = _required_string(record, "event_id")
    order_id = _required_string(record, "order_id")
    customer_id = _required_string(record, "customer_id")
    status = _required_string(record, "status").lower()
    if status not in _ALLOWED_STATUSES:
        raise ValueError(
            f"Unsupported status {status!r}. Expected one of {sorted(_ALLOWED_STATUSES)!r}."
        )

    total_cents = int(record["total_cents"])
    if total_cents < 0:
        raise ValueError("total_cents must be >= 0")

    currency = _required_string(record, "currency").upper()
    occurred_at = _required_string(record, "occurred_at")
    source = _required_string(record, "source")

    return CleanOrderEvent(
        event_id=event_id,
        order_id=order_id,
        customer_id=customer_id,
        status=status,
        total_cents=total_cents,
        currency=currency,
        occurred_at=occurred_at,
        source=source,
    )


def serialize_kafka_value(event: CleanOrderEvent) -> bytes:
    return json.dumps(asdict(event), separators=(",", ":"), sort_keys=True).encode("utf-8")


def serialize_raw_kafka_value(record: Mapping[str, Any]) -> bytes:
    return json.dumps(dict(record), separators=(",", ":"), sort_keys=True).encode("utf-8")


def deserialize_raw_kafka_value(value: bytes) -> dict[str, Any]:
    payload = json.loads(value.decode("utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("Kafka payload must decode to a JSON object.")
    return payload


def deserialize_kafka_value(value: bytes) -> CleanOrderEvent:
    payload = json.loads(value.decode("utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("Kafka payload must decode to a JSON object.")
    return normalize_order(payload)


def projection_row(event: CleanOrderEvent) -> dict[str, Any]:
    return {
        "order_id": event.order_id,
        "customer_id": event.customer_id,
        "status": event.status,
        "total_cents": event.total_cents,
        "currency": event.currency,
        "source_event_id": event.event_id,
        "source": event.source,
        "updated_at": event.occurred_at,
        "last_seen_at": event.occurred_at,
    }


def demo_seed_records() -> list[dict[str, str]]:
    return [
        {
            "event_id": "evt-1001",
            "order_id": "ord-100",
            "customer_id": "cust-001",
            "status": "created",
            "total_cents": "129900",
            "currency": "usd",
            "occurred_at": "2026-06-03T10:00:00Z",
            "source": "web",
        },
        {
            "event_id": "evt-1001",
            "order_id": "ord-100",
            "customer_id": "cust-001",
            "status": "created",
            "total_cents": "129900",
            "currency": "usd",
            "occurred_at": "2026-06-03T10:00:00Z",
            "source": "web",
        },
        {
            "event_id": "evt-1002",
            "order_id": "ord-100",
            "customer_id": "cust-001",
            "status": "paid",
            "total_cents": "129900",
            "currency": "usd",
            "occurred_at": "2026-06-03T10:01:00Z",
            "source": "payments",
        },
        {
            "event_id": "evt-bad-1",
            "order_id": "ord-999",
            "customer_id": "cust-999",
            "status": "mystery",
            "total_cents": "oops",
            "currency": "usd",
            "occurred_at": "2026-06-03T10:02:00Z",
            "source": "broken-producer",
        },
    ]


def _required_string(record: Mapping[str, Any], key: str) -> str:
    value = record.get(key)
    if value is None:
        raise ValueError(f"Missing required field {key!r}.")
    text = str(value).strip()
    if not text:
        raise ValueError(f"Field {key!r} must not be blank.")
    return text
