from __future__ import annotations

import importlib

from agora.health.responses import HealthResponseBuilder, ResponseSpec


def test_health_responses_module_reexports_public_api() -> None:
    module = importlib.import_module("agora.health.responses")

    assert module.HealthResponseBuilder is HealthResponseBuilder
    assert module.ResponseSpec is ResponseSpec
