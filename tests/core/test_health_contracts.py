from __future__ import annotations

from datetime import UTC, datetime

from agora.core.health import ComponentHealthSnapshot


def test_component_health_snapshot_to_dict_uses_common_shape() -> None:
    checked_at = datetime(2026, 6, 23, 12, 0, tzinfo=UTC)
    snapshot = ComponentHealthSnapshot(
        component="redis_source",
        ready=False,
        connection_ready=True,
        last_error="group missing",
        checked_at=checked_at,
    )

    assert snapshot.to_dict() == {
        "ready": False,
        "component": "redis_source",
        "connection_ready": True,
        "last_error": "group missing",
        "checked_at": "2026-06-23T12:00:00+00:00",
    }


def test_component_health_snapshot_equality_ignores_checked_at() -> None:
    earlier = datetime(2026, 6, 23, 12, 0, tzinfo=UTC)
    later = datetime(2026, 6, 23, 12, 1, tzinfo=UTC)

    assert ComponentHealthSnapshot(
        component="redis_source",
        ready=True,
        connection_ready=True,
        checked_at=earlier,
    ) == ComponentHealthSnapshot(
        component="redis_source",
        ready=True,
        connection_ready=True,
        checked_at=later,
    )
