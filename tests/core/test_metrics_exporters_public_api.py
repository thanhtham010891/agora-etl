from __future__ import annotations

import importlib

from agora.metrics.exporters import (
    MetricsExporter,
    PrometheusTextExporter,
    metrics_exporter_registry,
)


def test_metrics_exporters_module_reexports_public_api() -> None:
    module = importlib.import_module("agora.metrics.exporters")

    assert module.MetricsExporter is MetricsExporter
    assert module.PrometheusTextExporter is PrometheusTextExporter
    assert module.metrics_exporter_registry is metrics_exporter_registry
