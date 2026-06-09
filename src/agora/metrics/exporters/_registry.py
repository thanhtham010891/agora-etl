"""Registry bootstrap for metrics exporters."""

from __future__ import annotations

from typing import TYPE_CHECKING

from agora.core.registry import Registry
from agora.metrics.exporters._prometheus import PrometheusTextExporter

if TYPE_CHECKING:
    from agora.metrics.exporters._protocols import MetricsExporter

metrics_exporter_registry: Registry[type[MetricsExporter]] = Registry(name="metrics_exporter")
metrics_exporter_registry.register_factory("prometheus", PrometheusTextExporter)  # type: ignore[arg-type]
metrics_exporter_registry.load_entrypoints("agora.metrics.exporters")
