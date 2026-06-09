from __future__ import annotations

import importlib

from agora.core.types import Backpressure, DeliveryConfig, DLQFailurePolicy, SinkFailurePolicy


def test_types_module_reexports_public_api() -> None:
    module = importlib.import_module("agora.core.types")

    assert module.Backpressure is Backpressure
    assert module.DeliveryConfig is DeliveryConfig
    assert module.DLQFailurePolicy is DLQFailurePolicy
    assert module.SinkFailurePolicy is SinkFailurePolicy
