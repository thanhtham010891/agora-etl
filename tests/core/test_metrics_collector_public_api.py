from __future__ import annotations

import importlib

from agora.metrics.collector import MetricsCollector, PipelineStats


def test_metrics_collector_module_reexports_public_api() -> None:
    module = importlib.import_module("agora.metrics.collector")

    assert module.MetricsCollector is MetricsCollector
    assert module.PipelineStats is PipelineStats
