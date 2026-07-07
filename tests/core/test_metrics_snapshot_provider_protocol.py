"""Test MetricsSnapshotProvider protocol compliance."""

from __future__ import annotations

from agora.core.metrics import (
    MetricsSnapshotProvider,
    PrometheusMetricsProvider,
    has_metrics_snapshot,
    has_prometheus_metrics,
)


class _MockMetricsSource:
    def __init__(self) -> None:
        self._snapshot = {"records": 3, "ready": True}

    def metrics_snapshot(self) -> dict[str, int | bool]:
        return dict(self._snapshot)


class _MockWithoutMetrics:
    def health_snapshot(self) -> dict[str, bool]:
        return {"ready": True}


class _MockPrometheusSurface:
    def render_prometheus_metrics(self, namespace: str = "agora") -> str:
        return f"# HELP {namespace}_ready Ready state\n{namespace}_ready 1\n"


def test_metrics_snapshot_provider_protocol_isinstance() -> None:
    provider = _MockMetricsSource()
    non_provider = _MockWithoutMetrics()

    assert isinstance(provider, MetricsSnapshotProvider)
    assert not isinstance(non_provider, MetricsSnapshotProvider)


def test_has_metrics_snapshot_helper_matches_protocol() -> None:
    provider = _MockMetricsSource()
    non_provider = _MockWithoutMetrics()

    assert has_metrics_snapshot(provider) is True
    assert has_metrics_snapshot(non_provider) is False


def test_metrics_snapshot_provider_returns_machine_readable_snapshot() -> None:
    provider = _MockMetricsSource()

    assert provider.metrics_snapshot() == {"records": 3, "ready": True}


def test_prometheus_metrics_provider_protocol_isinstance() -> None:
    provider = _MockPrometheusSurface()
    non_provider = _MockWithoutMetrics()

    assert isinstance(provider, PrometheusMetricsProvider)
    assert not isinstance(non_provider, PrometheusMetricsProvider)


def test_has_prometheus_metrics_helper_matches_protocol() -> None:
    provider = _MockPrometheusSurface()
    non_provider = _MockWithoutMetrics()

    assert has_prometheus_metrics(provider) is True
    assert has_prometheus_metrics(non_provider) is False


def test_prometheus_metrics_provider_returns_text_exposition() -> None:
    provider = _MockPrometheusSurface()

    assert provider.render_prometheus_metrics(namespace="agora_test") == (
        "# HELP agora_test_ready Ready state\nagora_test_ready 1\n"
    )
