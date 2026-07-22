"""The versioned event contract shared by the producer and both projections."""

from __future__ import annotations

import json
import random
import secrets
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class OrderEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    event_type: Literal["order.created", "order.paid", "order.packed"]
    event_version: Literal[1, 2] = 1
    event_time: datetime
    producer_run_id: str = Field(default="legacy", min_length=1)
    order_id: str
    customer_id: str
    total_cents: int = Field(ge=0)
    status: Literal["created", "paid", "packed"]
    fulfillment_channel: Literal["standard", "delivery", "pickup"] | None = None

    @field_validator("event_time")
    @classmethod
    def event_time_must_include_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("event_time must be timezone-aware UTC business time.")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def event_type_must_match_status(self) -> OrderEvent:
        expected_status = self.event_type.removeprefix("order.")
        if self.status != expected_status:
            raise ValueError(
                f"status {self.status!r} does not match event_type {self.event_type!r}."
            )
        if self.event_version == 1:
            if self.fulfillment_channel not in (None, "standard"):
                raise ValueError("event_version 1 does not support a fulfillment_channel override.")
            self.fulfillment_channel = "standard"
        elif self.fulfillment_channel not in {"delivery", "pickup"}:
            raise ValueError(
                "event_version 2 requires fulfillment_channel to be 'delivery' or 'pickup'."
            )
        return self


def deserialize_event(value: bytes) -> dict[str, object]:
    """Validate bytes at the source boundary; invalid messages become poison records."""
    return OrderEvent.model_validate_json(value).model_dump(mode="json")


def generate_order_events(
    *,
    order_count: int = 3,
    seed: int | None = None,
    event_version: Literal[1, 2] = 1,
    fulfillment_channel: Literal["delivery", "pickup"] = "delivery",
) -> list[dict[str, object]]:
    """Create new order lifecycles for each producer run.

    ``seed`` is intentionally opt-in for executable tests. Normal runs use
    UUID4 and the operating system's random source, so a Kafka replay keeps
    its payload stable while each newly produced business order is distinct.
    """
    if order_count < 1:
        raise ValueError("order_count must be at least 1")

    seeded_random = random.Random(seed) if seed is not None else None

    def new_identifier(prefix: str) -> str:
        if seeded_random is None:
            return f"{prefix}_{uuid4().hex}"
        return f"{prefix}_{UUID(int=seeded_random.getrandbits(128), version=4).hex}"

    def total_cents() -> int:
        if seeded_random is None:
            return 500 + secrets.randbelow(199_501)
        return seeded_random.randint(500, 200_000)

    start = (
        datetime.now(UTC)
        if seed is None
        else datetime(2026, 1, 1, tzinfo=UTC) + timedelta(microseconds=seed)
    )
    producer_run_id = new_identifier("run")
    events: list[dict[str, object]] = []
    for order_index in range(order_count):
        order_id = new_identifier("ord")
        customer_id = new_identifier("cus")
        order_total_cents = total_cents()
        for event_index, (event_type, status) in enumerate(
            (("order.created", "created"), ("order.paid", "paid"), ("order.packed", "packed"))
        ):
            event = OrderEvent(
                event_id=new_identifier("evt"),
                event_type=event_type,
                event_version=event_version,
                event_time=start + timedelta(milliseconds=(order_index * 3) + event_index),
                producer_run_id=producer_run_id,
                order_id=order_id,
                customer_id=customer_id,
                total_cents=order_total_cents,
                status=status,
                fulfillment_channel=(fulfillment_channel if event_version == 2 else None),
            )
            payload = event.model_dump(mode="json")
            if event_version == 1:
                # A historical V1 producer never sent this additive field.
                # ``deserialize_event`` restores the canonical default for both sinks.
                payload.pop("fulfillment_channel")
            events.append(payload)
    return events


def postgres_row(record: dict[str, object]) -> dict[str, object]:
    payload = record["payload"]
    if not isinstance(payload, dict):
        raise TypeError("Kafka envelope payload must be a mapping.")
    metadata = record["metadata"]
    if not isinstance(metadata, dict):
        raise TypeError("Kafka envelope metadata must be a mapping.")
    row = dict(payload)
    try:
        topic = str(metadata["topic"])
        partition = int(metadata["partition"])
        offset = int(metadata["offset"])
    except KeyError as exc:
        raise ValueError(f"Kafka envelope metadata is missing {exc.args[0]!r}.") from exc
    row["kafka_delivery_key"] = f"{topic}:{partition}:{offset}"
    row["kafka_topic"] = topic
    row["kafka_partition"] = partition
    row["kafka_offset"] = offset
    row["kafka_metadata"] = json.dumps(metadata, default=str, sort_keys=True)
    return row


def redis_view(record: dict[str, object], *, key_prefix: str) -> dict[str, object]:
    payload = record["payload"]
    if not isinstance(payload, dict):
        raise TypeError("Kafka envelope payload must be a mapping.")
    return {
        "redis_key": f"{key_prefix}:current:{payload['order_id']}",
        "value": dict(payload),
    }
