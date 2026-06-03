from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sample_data import build_sample_order_batch
from transforms import (
    deserialize_kafka_value,
    normalize_order,
    projection_row,
    serialize_kafka_value,
)


def test_normalize_order_coerces_and_normalizes_fields() -> None:
    event = normalize_order(
        {
            "event_id": "evt-1",
            "order_id": "ord-1",
            "customer_id": "cust-1",
            "status": "PAID",
            "total_cents": "4200",
            "currency": "usd",
            "occurred_at": "2026-06-03T10:00:00Z",
            "source": "payments",
        }
    )

    assert event.status == "paid"
    assert event.total_cents == 4200
    assert event.currency == "USD"


def test_normalize_order_rejects_unknown_status() -> None:
    with pytest.raises(ValueError, match="Unsupported status"):
        normalize_order(
            {
                "event_id": "evt-1",
                "order_id": "ord-1",
                "customer_id": "cust-1",
                "status": "mystery",
                "total_cents": "4200",
                "currency": "usd",
                "occurred_at": "2026-06-03T10:00:00Z",
                "source": "payments",
            }
        )


def test_kafka_round_trip_preserves_event_shape() -> None:
    event = normalize_order(
        {
            "event_id": "evt-1",
            "order_id": "ord-1",
            "customer_id": "cust-1",
            "status": "created",
            "total_cents": "4200",
            "currency": "usd",
            "occurred_at": "2026-06-03T10:00:00Z",
            "source": "web",
        }
    )

    decoded = deserialize_kafka_value(serialize_kafka_value(event))
    assert decoded == event


def test_projection_row_uses_order_id_as_upsert_key() -> None:
    event = normalize_order(
        {
            "event_id": "evt-2",
            "order_id": "ord-99",
            "customer_id": "cust-9",
            "status": "fulfilled",
            "total_cents": "9900",
            "currency": "usd",
            "occurred_at": "2026-06-03T10:05:00Z",
            "source": "warehouse",
        }
    )

    row = projection_row(event)
    assert row["order_id"] == "ord-99"
    assert row["source_event_id"] == "evt-2"
    assert row["status"] == "fulfilled"


def test_build_sample_order_batch_includes_duplicates_and_poison_records() -> None:
    batch = build_sample_order_batch(
        count=12,
        duplicate_every=4,
        poison_every=6,
        base_time=datetime(2026, 6, 3, 10, 0, tzinfo=UTC),
    )

    assert len(batch) == 12
    assert batch[4]["event_id"] == batch[3]["event_id"]
    assert batch[6]["status"] == "mystery"
    assert batch[6]["total_cents"] == "oops"
