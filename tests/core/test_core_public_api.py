from __future__ import annotations

import importlib
import warnings


def test_core_facade_reexports_public_api() -> None:
    module = importlib.import_module("agora.core")

    expected_names = {
        "AgoraContainer",
        "BaseSink",
        "BaseSource",
        "DataPlane",
        "DLQPayloadPolicy",
        "MetricsSnapshotProvider",
        "Pipeline",
        "PipelineContext",
        "RetryPolicy",
        "SinkDataPlaneSpec",
        "SourceDataPlaneSpec",
        "Writer",
        "discover_plugins",
        "sink_data_plane_spec",
        "source_data_plane_spec",
    }

    for name in expected_names:
        assert hasattr(module, name), f"agora.core is missing export {name}"


def test_core_facade_all_matches_public_manifest() -> None:
    module = importlib.import_module("agora.core")

    expected_names = {
        "AgoraContainer",
        "BaseSink",
        "BaseSource",
        "DataPlane",
        "DLQPayloadPolicy",
        "MetricsSnapshotProvider",
        "Pipeline",
        "PipelineContext",
        "RetryPolicy",
        "SinkDataPlaneSpec",
        "SourceDataPlaneSpec",
        "WriteResult",
        "Writer",
        "discover_plugins",
        "retry_async",
        "sink_data_plane_spec",
        "source_data_plane_spec",
    }

    assert expected_names.issubset(set(module.__all__))
    assert len(module.__all__) == len(set(module.__all__))
    assert all(not name.startswith("_") for name in module.__all__)


def test_core_facade_import_paths_remain_stable() -> None:
    from agora.core import DataPlane, Pipeline, Writer

    module = importlib.import_module("agora.core")

    assert module.DataPlane is DataPlane
    assert module.Pipeline is Pipeline
    assert module.Writer is Writer


def test_core_facade_shared_root_exports_remain_aligned() -> None:
    root_module = importlib.import_module("agora")
    core_module = importlib.import_module("agora.core")

    shared_names = {
        "AgoraContainer",
        "BaseSink",
        "BaseSource",
        "BoundPipeline",
        "Checkpoint",
        "CheckpointStore",
        "DataPlane",
        "FilterMiddleware",
        "IterableSource",
        "MapMiddleware",
        "Middleware",
        "Pipeline",
        "PipelineContext",
        "PipelineRunSummary",
        "Registry",
        "RetryPolicy",
        "SinkDataPlaneSpec",
        "SinkFanOut",
        "SinkRouter",
        "SourceDataPlaneSpec",
        "WriteResult",
        "Writer",
        "discover_plugins",
        "retry_async",
        "sink_data_plane_spec",
        "source_data_plane_spec",
    }

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        for name in shared_names:
            assert getattr(root_module, name) is getattr(core_module, name)
