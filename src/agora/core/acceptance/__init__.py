"""Common acceptance-gate contracts for Agora components and plugins."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol, TypeGuard, TypeVar, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Awaitable

SnapshotT = TypeVar("SnapshotT", contravariant=True)
ReportT = TypeVar("ReportT", bound="AcceptanceReport", covariant=True)


@dataclass(frozen=True, slots=True)
class AcceptanceFinding:
    """Single machine-readable acceptance finding."""

    metric: str
    message: str
    value: Any
    threshold: Any
    component: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "metric": self.metric,
            "message": self.message,
            "value": self.value,
            "threshold": self.threshold,
        }
        if self.component is not None:
            payload["component"] = self.component
        return payload


@dataclass(frozen=True, slots=True)
class AcceptanceReport:
    """Machine-readable acceptance verdict for a component snapshot."""

    passed: bool
    thresholds: Any
    findings: tuple[AcceptanceFinding, ...] = ()
    component: str | None = None
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "passed": self.passed,
            "thresholds": (
                self.thresholds.to_dict()
                if hasattr(self.thresholds, "to_dict")
                else dict(self.thresholds)
            ),
            "findings": [finding.to_dict() for finding in self.findings],
            "evaluated_at": self.evaluated_at.isoformat(),
        }
        if self.component is not None:
            payload["component"] = self.component
        return payload


class AcceptanceGate(Protocol[SnapshotT, ReportT]):
    """Protocol for evaluating a snapshot against acceptance thresholds."""

    def evaluate(self, snapshot: SnapshotT) -> ReportT:
        """Return a machine-readable acceptance report for ``snapshot``."""


@runtime_checkable
class AcceptanceReportProvider(Protocol[ReportT]):
    """Protocol for components that expose an acceptance report directly."""

    def acceptance_report(self, thresholds: Any = None) -> ReportT | Awaitable[ReportT]:
        """Return an acceptance report, optionally using caller-provided thresholds."""


def has_acceptance_report(component: object) -> TypeGuard[AcceptanceReportProvider[Any]]:
    """Return True when *component* exposes the shared acceptance-report contract."""

    return isinstance(component, AcceptanceReportProvider)


__all__ = [
    "AcceptanceFinding",
    "AcceptanceGate",
    "AcceptanceReport",
    "AcceptanceReportProvider",
    "has_acceptance_report",
]
