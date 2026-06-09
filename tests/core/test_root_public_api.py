from __future__ import annotations

import importlib


def test_root_facade_reexports_public_api() -> None:
    module = importlib.import_module("agora")

    expected_names = {
        "Backpressure",
        "BaseSink",
        "BaseSource",
        "BoundPipeline",
        "DeliveryConfig",
        "InMemoryTracer",
        "Pipeline",
        "PipelineContext",
        "PipelineRunSummary",
        "SinkDataPlaneSpec",
        "SourceDataPlaneSpec",
        "SourceRuntimeMetrics",
        "WriteResult",
        "Writer",
        "__version__",
        "discover_plugins",
        "state_backend_registry",
    }

    for name in expected_names:
        assert hasattr(module, name), f"agora is missing export {name}"


def test_root_facade_all_matches_public_manifest() -> None:
    module = importlib.import_module("agora")

    expected_names = {
        "AgoraContainer",
        "ArrowBatchMiddleware",
        "ArrowCsvSource",
        "ArrowProcessBatchMiddleware",
        "Backpressure",
        "BatchMiddleware",
        "DeliveryConfig",
        "InMemoryTracer",
        "NoopTracer",
        "OpenTelemetryTracer",
        "Pipeline",
        "SourceRecordError",
        "SourceRuntimeMetrics",
        "StateBackend",
        "WriteResult",
        "Writer",
        "__version__",
        "discover_plugins",
        "state_backend_registry",
    }

    assert expected_names.issubset(set(module.__all__))
    assert len(module.__all__) == len(set(module.__all__))


def test_root_facade_import_paths_remain_stable() -> None:
    from agora import DeliveryConfig, Pipeline, Writer

    module = importlib.import_module("agora")

    assert module.DeliveryConfig is DeliveryConfig
    assert module.Pipeline is Pipeline
    assert module.Writer is Writer
