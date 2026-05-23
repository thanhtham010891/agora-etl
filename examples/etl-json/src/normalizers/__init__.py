from __future__ import annotations

from models import Event


def row_to_event(row: dict) -> Event | None:
    try:
        return Event(
            id=str(row["id"]),
            type=str(row["type"]),
            user_id=str(row["user_id"]),
            timestamp=str(row["timestamp"]),
            payload=row.get("payload") or {},
        )
    except (KeyError, TypeError):
        return None
