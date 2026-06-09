from __future__ import annotations

import importlib


def test_runtime_plan_module_reexports_public_api() -> None:
    module = importlib.import_module("agora.core.runtime._plan")

    expected_names = {
        "BufferedStageSpec",
        "MiddlewareExecutionPlan",
        "RuntimeLane",
        "RuntimePlan",
        "WriterExecutionPlan",
        "WriterSinkPlan",
        "build_runtime_plan",
    }

    for name in expected_names:
        assert hasattr(module, name), f"agora.core.runtime._plan is missing export {name}"


def test_runtime_plan_import_path_remains_stable() -> None:
    from agora.core.runtime._plan import RuntimeLane, RuntimePlan, build_runtime_plan

    module = importlib.import_module("agora.core.runtime._plan")

    assert module.RuntimeLane is RuntimeLane
    assert module.RuntimePlan is RuntimePlan
    assert module.build_runtime_plan is build_runtime_plan
