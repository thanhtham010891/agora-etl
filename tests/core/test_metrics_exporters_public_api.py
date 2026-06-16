from __future__ import annotations

import importlib

from agora.metrics.exporters import (
    MetricsExporter,
    PrometheusTextExporter,
    metrics_exporter_registry,
)
from agora.metrics.prometheus import (
    append_metric_header,
    escape_label_value,
    render_scrape_time_line,
)


def test_metrics_exporters_module_reexports_public_api() -> None:
    module = importlib.import_module("agora.metrics.exporters")

    assert module.MetricsExporter is MetricsExporter
    assert module.PrometheusTextExporter is PrometheusTextExporter
    assert module.metrics_exporter_registry is metrics_exporter_registry


def test_metrics_prometheus_module_exports_public_rendering_helpers() -> None:
    module = importlib.import_module("agora.metrics.prometheus")

    assert module.append_metric_header is append_metric_header
    assert module.escape_label_value is escape_label_value
    assert module.render_scrape_time_line is render_scrape_time_line
