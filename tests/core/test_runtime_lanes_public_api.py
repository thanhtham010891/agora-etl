from __future__ import annotations

import importlib


def test_runtime_lanes_package_reexports_strategies() -> None:
    module = importlib.import_module("agora.core.runtime._lanes")

    expected_names = {
        "BatchLaneStrategy",
        "BufferedLaneStrategy",
        "LinearLaneStrategy",
    }

    for name in expected_names:
        assert hasattr(module, name), f"agora.core.runtime._lanes is missing export {name}"


def test_runtime_lanes_import_path_remains_stable() -> None:
    from agora.core.runtime._lanes import (
        BatchLaneStrategy,
        BufferedLaneStrategy,
        LinearLaneStrategy,
    )

    module = importlib.import_module("agora.core.runtime._lanes")

    assert module.BatchLaneStrategy is BatchLaneStrategy
    assert module.BufferedLaneStrategy is BufferedLaneStrategy
    assert module.LinearLaneStrategy is LinearLaneStrategy
