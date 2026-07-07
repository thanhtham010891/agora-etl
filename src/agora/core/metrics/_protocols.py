"""Public metrics snapshot provider contracts."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, TypeGuard, TypeVar, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Awaitable

MetricsSnapshotT = TypeVar("MetricsSnapshotT", covariant=True)


@runtime_checkable
class MetricsSnapshotProvider(Protocol[MetricsSnapshotT]):
    """Protocol for components that expose machine-readable metrics snapshots."""

    def metrics_snapshot(self) -> MetricsSnapshotT | Awaitable[MetricsSnapshotT]:
        """Return a metrics snapshot directly or from backend I/O."""


def has_metrics_snapshot(component: object) -> TypeGuard[MetricsSnapshotProvider[Any]]:
    """Return True when *component* exposes the shared metrics snapshot contract."""

    return isinstance(component, MetricsSnapshotProvider)


@runtime_checkable
class PrometheusMetricsProvider(Protocol):
    """Protocol for components that render Prometheus exposition text."""

    def render_prometheus_metrics(self, namespace: str = "agora") -> str | Awaitable[str]:
        """Return Prometheus exposition text directly or from backend I/O."""


def has_prometheus_metrics(component: object) -> TypeGuard[PrometheusMetricsProvider]:
    """Return True when *component* exposes the shared Prometheus render contract."""

    return isinstance(component, PrometheusMetricsProvider)


__all__ = [
    "MetricsSnapshotProvider",
    "PrometheusMetricsProvider",
    "has_metrics_snapshot",
    "has_prometheus_metrics",
]
