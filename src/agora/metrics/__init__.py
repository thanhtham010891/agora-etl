"""
agora/metrics/__init__.py
==========================
agora built-in metrics collection.

Tracks pipeline run statistics without external dependencies.
Prometheus-compatible export is available through the metrics exporter registry.

Public API::

    from agora.metrics import MetricsCollector, PipelineStats

    collector = MetricsCollector()
    await collector.record_run(pipeline_id="places_ingest", summary=summary)
    stats = collector.get("places_ingest")

    # Prometheus-compatible export
    from agora.metrics.exporters import metrics_exporter_registry
    exporter = metrics_exporter_registry.create("prometheus", collector=collector)
    text = exporter.render()  # → Prometheus text format
"""

from agora.metrics.collector import MetricsCollector, PipelineStats

__all__ = ["MetricsCollector", "PipelineStats"]
