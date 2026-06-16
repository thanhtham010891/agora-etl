"""Common health snapshot contracts for Agora components and plugins."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

if TYPE_CHECKING:
    from collections.abc import Awaitable

HealthSnapshotT = TypeVar("HealthSnapshotT", bound="ComponentHealthSnapshot", covariant=True)


@dataclass(frozen=True, slots=True, kw_only=True)
class ComponentHealthSnapshot:
    """Minimum health vocabulary shared by core and plugin components."""

    ready: bool
    component: str | None = None
    connection_ready: bool | None = None
    last_error: str | None = None
    checked_at: datetime = field(
        default_factory=lambda: datetime.now(UTC),
        compare=False,
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "component": self.component,
            "connection_ready": self.connection_ready,
            "last_error": self.last_error,
            "checked_at": self.checked_at.isoformat(),
        }


class HealthCheckable(Protocol[HealthSnapshotT]):
    """Protocol for components that expose a health snapshot."""

    def health_snapshot(self) -> HealthSnapshotT | Awaitable[HealthSnapshotT]:
        """Return a machine-readable health snapshot.

        Implementations may return a snapshot directly or await backend I/O.
        """


__all__ = [
    "ComponentHealthSnapshot",
    "HealthCheckable",
]
