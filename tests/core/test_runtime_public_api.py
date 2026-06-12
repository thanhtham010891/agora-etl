from __future__ import annotations

import importlib


def test_runtime_facade_reexports_public_api() -> None:
    module = importlib.import_module("agora.core.runtime")

    expected_names = {
        "AdaptiveBackpressureController",
        "BufferedStageSpec",
        "CheckpointState",
        "DeliveryEngine",
        "ExecutionCoordinator",
        "HotPathMetrics",
        "RuntimeLane",
        "RuntimePlan",
        "SourceRuntimeAdapter",
        "WriterTransport",
        "build_runtime_plan",
        "make_checkpoint_state",
    }

    for name in expected_names:
        assert hasattr(module, name), f"agora.core.runtime is missing export {name}"


def test_runtime_facade_does_not_export_removed_delivery_alias() -> None:
    module = importlib.import_module("agora.core.runtime")

    assert not hasattr(module, "RecordDeliveryCoordinator")


def test_runtime_facade_writer_transport_import_path_remains_stable() -> None:
    from agora.core.runtime import WriterTransport

    module = importlib.import_module("agora.core.runtime")

    assert module.WriterTransport is WriterTransport
