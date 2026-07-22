import pytest
from order_command_center.contracts import (
    deserialize_event,
    generate_order_events,
    postgres_row,
    redis_view,
)
from order_command_center.migrate import _migration_files


def test_generated_events_are_valid_and_seed_repeatable() -> None:
    events = generate_order_events(seed=7)
    assert len(events) == 9
    assert events == generate_order_events(seed=7)
    assert len({event["producer_run_id"] for event in events}) == 1
    assert {event["order_id"] for event in events} != {
        event["order_id"] for event in generate_order_events(seed=8)
    }
    payload = (
        b'{"event_id":"x","event_type":"order.created","event_version":1,'
        b'"event_time":"2026-07-21T09:00:00Z","order_id":"order-1",'
        b'"customer_id":"customer-1","total_cents":1,"status":"created"}'
    )
    canonical_v1 = deserialize_event(payload)
    assert canonical_v1["order_id"] == "order-1"
    assert canonical_v1["fulfillment_channel"] == "standard"


def test_v2_requires_and_preserves_the_additive_fulfillment_channel() -> None:
    payload = (
        b'{"event_id":"x","event_type":"order.created","event_version":2,'
        b'"event_time":"2026-07-21T09:00:00Z","order_id":"order-1",'
        b'"customer_id":"customer-1","total_cents":1,"status":"created",'
        b'"fulfillment_channel":"pickup"}'
    )
    assert deserialize_event(payload)["fulfillment_channel"] == "pickup"
    assert "fulfillment_channel" not in generate_order_events(seed=7, event_version=1)[0]
    assert generate_order_events(seed=7, event_version=2)[0]["fulfillment_channel"] == "delivery"


def test_redis_view_uses_a_stable_order_key() -> None:
    event = generate_order_events(seed=7)[0]
    view = redis_view({"payload": event, "metadata": {}}, key_prefix="demo:orders")
    assert view["redis_key"] == f"demo:orders:current:{event['order_id']}"


def test_postgres_row_uses_kafka_coordinates_as_the_idempotency_key() -> None:
    event = generate_order_events(seed=7)[0]
    row = postgres_row(
        {"payload": event, "metadata": {"topic": "orders", "partition": 2, "offset": 9}}
    )
    assert row["kafka_delivery_key"] == "orders:2:9"
    assert row["kafka_topic"] == "orders"
    assert row["kafka_partition"] == 2
    assert row["kafka_offset"] == 9


@pytest.mark.parametrize(
    "payload",
    [
        b'{"event_id":"x","event_type":"order.created","event_version":3,'
        b'"event_time":"2026-07-21T09:00:00Z","order_id":"order-1",'
        b'"customer_id":"customer-1","total_cents":1,"status":"created"}',
        b'{"event_id":"x","event_type":"order.created","event_version":1,'
        b'"event_time":"2026-07-21T09:00:00Z","order_id":"order-1",'
        b'"customer_id":"customer-1","total_cents":1,"status":"packed"}',
        b'{"event_id":"x","event_type":"order.created","event_version":1,'
        b'"event_time":"2026-07-21T09:00:00","order_id":"order-1",'
        b'"customer_id":"customer-1","total_cents":1,"status":"created"}',
        b'{"event_id":"x","event_type":"order.created","event_version":2,'
        b'"event_time":"2026-07-21T09:00:00Z","order_id":"order-1",'
        b'"customer_id":"customer-1","total_cents":1,"status":"created"}',
    ],
)
def test_invalid_or_unsupported_events_are_rejected_at_the_source_boundary(payload: bytes) -> None:
    with pytest.raises(ValueError):
        deserialize_event(payload)


def test_migrations_define_durable_current_state_before_run_tracking() -> None:
    assert [path.name for path in _migration_files()] == [
        "001_schema.sql",
        "002_order_current_state.sql",
        "003_producer_run_id.sql",
        "004_producer_run_manifest.sql",
        "005_record_timestamps.sql",
        "006_delivery_coordinates.sql",
        "007_producer_run_state.sql",
        "008_dlq_replay_audit.sql",
        "009_dlq_replay_reconciliation.sql",
        "010_order_event_v2.sql",
    ]
