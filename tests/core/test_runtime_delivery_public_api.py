from __future__ import annotations

import importlib


def test_runtime_delivery_module_reexports_public_api() -> None:
    module = importlib.import_module("agora.core.runtime._delivery")

    expected_names = {
        "CheckpointState",
        "CommitOutcome",
        "DeliveryEngine",
        "Dropped",
        "ErroredRouted",
        "ErroredUnrouted",
        "PendingWrite",
        "ProcessedSourceRecord",
        "RecordDeliveryError",
        "RunState",
        "SourceQueueError",
        "SourceRecord",
        "Written",
        "make_checkpoint_state",
    }

    for name in expected_names:
        assert hasattr(module, name), f"agora.core.runtime._delivery is missing export {name}"


def test_runtime_delivery_import_paths_remain_stable() -> None:
    from agora.core.runtime._delivery import CheckpointState, DeliveryEngine, SourceRecord

    module = importlib.import_module("agora.core.runtime._delivery")

    assert module.CheckpointState is CheckpointState
    assert module.DeliveryEngine is DeliveryEngine
    assert module.SourceRecord is SourceRecord
