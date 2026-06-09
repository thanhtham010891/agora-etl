from __future__ import annotations

import importlib

from agora.core.tracing import InMemoryTracer, NoopTracer, OpenTelemetryTracer, TraceSpan


def test_tracing_module_reexports_public_api() -> None:
    module = importlib.import_module("agora.core.tracing")

    assert module.InMemoryTracer is InMemoryTracer
    assert module.NoopTracer is NoopTracer
    assert module.OpenTelemetryTracer is OpenTelemetryTracer
    assert module.TraceSpan is TraceSpan
