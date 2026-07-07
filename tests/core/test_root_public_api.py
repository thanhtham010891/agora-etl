from __future__ import annotations

import importlib
import warnings

import pytest


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
        "Registry",
        "SinkDataPlaneSpec",
        "SourceDataPlaneSpec",
        "SourceRuntimeMetrics",
        "WriteResult",
        "Writer",
        "__version__",
        "discover_plugins",
        "state_backend_registry",
    }

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
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
        "Registry",
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
    from agora import DeliveryConfig, Pipeline
    from agora.core.writer import Writer

    module = importlib.import_module("agora")

    assert module.DeliveryConfig is DeliveryConfig
    assert module.Pipeline is Pipeline
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        assert module.Writer is Writer


def test_root_facade_policy_metadata_classifies_current_surface() -> None:
    module = importlib.import_module("agora")

    public_exports = set(module.__all__) - {"__version__"}
    classified_exports = (
        module._ROOT_KEEP_AT_ROOT_EXPORTS
        | module._ROOT_REVIEW_BEFORE_KEEPING_EXPORTS
        | module._ROOT_PREFER_CORE_OVER_ROOT_EXPORTS
    )

    assert public_exports == classified_exports
    assert not (module._ROOT_KEEP_AT_ROOT_EXPORTS & module._ROOT_REVIEW_BEFORE_KEEPING_EXPORTS)
    assert not (module._ROOT_KEEP_AT_ROOT_EXPORTS & module._ROOT_PREFER_CORE_OVER_ROOT_EXPORTS)
    assert not (
        module._ROOT_REVIEW_BEFORE_KEEPING_EXPORTS & module._ROOT_PREFER_CORE_OVER_ROOT_EXPORTS
    )
    assert module._ROOT_DEPRECATION_CANDIDATES.issubset(module._ROOT_PREFER_CORE_OVER_ROOT_EXPORTS)

    assert {"Pipeline", "BoundPipeline", "DeliveryConfig"}.issubset(
        module._ROOT_KEEP_AT_ROOT_EXPORTS
    )
    assert {
        "BaseSource",
        "BaseSink",
        "Registry",
        "SourceRuntimeMetrics",
        "WriteResult",
        "Writer",
    }.issubset(module._ROOT_PREFER_CORE_OVER_ROOT_EXPORTS)
    assert module._ROOT_SOFT_DEPRECATED_EXPORTS.issubset(module._ROOT_DEPRECATION_CANDIDATES)


@pytest.mark.parametrize(
    ("export_name", "replacement"),
    [
        ("BaseSink", "agora.core.sink.BaseSink"),
        ("BaseSource", "agora.core.source.BaseSource"),
        ("CheckpointStore", "agora.core.checkpoint.CheckpointStore"),
        ("InMemoryCheckpointStore", "agora.core.checkpoint.InMemoryCheckpointStore"),
        ("Registry", "agora.core.registry.Registry"),
        ("SourceRecordError", "agora.core.source.SourceRecordError"),
        ("SourceRuntimeMetrics", "agora.core.source.SourceRuntimeMetrics"),
        ("SQLiteCheckpointStore", "agora.core.checkpoint.SQLiteCheckpointStore"),
        ("WriteResult", "agora.core.writer.WriteResult"),
        ("Writer", "agora.core.writer.Writer"),
        ("state_backend_registry", "agora.state.state_backend_registry"),
    ],
)
def test_root_soft_deprecated_exports_warn_and_resolve(export_name: str, replacement: str) -> None:
    module = importlib.import_module("agora")
    module.__dict__.pop(export_name, None)

    with pytest.deprecated_call(
        match=rf"`agora\.{export_name}` is soft-deprecated.*`{replacement}` instead\."
    ):
        value = getattr(module, export_name)

    replacement_module_name, replacement_attr = replacement.rsplit(".", 1)
    replacement_module = importlib.import_module(replacement_module_name)
    assert value is getattr(replacement_module, replacement_attr)
