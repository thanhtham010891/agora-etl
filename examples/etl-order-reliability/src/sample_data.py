from __future__ import annotations

from datetime import UTC, datetime, timedelta


def build_sample_order_batch(
    *,
    count: int,
    duplicate_every: int,
    poison_every: int,
    base_time: datetime | None = None,
) -> list[dict[str, str]]:
    if count <= 0:
        return []

    base_time = base_time or datetime.now(UTC).replace(second=0, microsecond=0)
    statuses = ("created", "paid", "fulfilled")
    sources = ("web", "mobile", "payments")

    records: list[dict[str, str]] = []
    previous_good: dict[str, str] | None = None

    for index in range(count):
        event_time = base_time + timedelta(seconds=index % 60)
        record = {
            "event_id": f"evt-{base_time.strftime('%Y%m%d%H%M')}-{index:06d}",
            "order_id": f"ord-{base_time.strftime('%Y%m%d%H%M')}-{index:06d}",
            "customer_id": f"cust-{index % 500:04d}",
            "status": statuses[index % len(statuses)],
            "total_cents": str(5_000 + (index % 250) * 125),
            "currency": "usd",
            "occurred_at": event_time.isoformat().replace("+00:00", "Z"),
            "source": sources[index % len(sources)],
        }

        is_poison = poison_every > 0 and index > 0 and index % poison_every == 0
        is_duplicate = duplicate_every > 0 and index > 0 and index % duplicate_every == 0

        if is_poison:
            record["status"] = "mystery"
            record["total_cents"] = "oops"
            record["source"] = "broken-producer"
            records.append(record)
            continue

        if is_duplicate and previous_good is not None:
            records.append(dict(previous_good))
            continue

        records.append(record)
        previous_good = record

    return records
