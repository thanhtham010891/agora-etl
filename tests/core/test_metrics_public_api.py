from __future__ import annotations

import importlib

from agora.core.metrics import (
    AIMetrics,
    AIMiddlewareMetrics,
    MiddlewareMetrics,
    PipelineMetrics,
    PipelineRunSummary,
    RuntimeMetrics,
)


def test_metrics_module_reexports_public_api() -> None:
    module = importlib.import_module("agora.core.metrics")

    assert module.AIMetrics is AIMetrics
    assert module.AIMiddlewareMetrics is AIMiddlewareMetrics
    assert module.MiddlewareMetrics is MiddlewareMetrics
    assert module.PipelineMetrics is PipelineMetrics
    assert module.PipelineRunSummary is PipelineRunSummary
    assert module.RuntimeMetrics is RuntimeMetrics
