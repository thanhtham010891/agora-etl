from __future__ import annotations

import importlib

import pytest


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


def test_runtime_facade_deprecated_delivery_alias_warns() -> None:
    module = importlib.import_module("agora.core.runtime")

    with pytest.deprecated_call(
        match="RecordDeliveryCoordinator is deprecated; use DeliveryEngine instead."
    ):
        alias = module.RecordDeliveryCoordinator

    assert alias is module.DeliveryEngine


def test_runtime_facade_writer_transport_import_path_remains_stable() -> None:
    from agora.core.runtime import WriterTransport

    module = importlib.import_module("agora.core.runtime")

    assert module.WriterTransport is WriterTransport
