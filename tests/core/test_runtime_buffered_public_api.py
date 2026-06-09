from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest


def test_runtime_buffered_module_reexports_compatibility_symbols() -> None:
    module = importlib.import_module("agora.core.runtime._buffered")

    expected_names = {
        "AdaptiveBackpressureController",
        "ExecutionCoordinator",
        "LinearBatchBuffer",
        "_RUST_AVAILABLE",
    }

    for name in expected_names:
        assert hasattr(module, name), f"agora.core.runtime._buffered is missing export {name}"


def test_make_linear_batch_buffer_uses_module_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("agora.core.runtime._buffered")

    class _FakeLinearBatchBuffer:
        def __init__(self, batch_size: int, metrics_flush_interval: int) -> None:
            self.batch_size = batch_size
            self.metrics_flush_interval = metrics_flush_interval

    monkeypatch.setattr(module, "_RUST_AVAILABLE", True)
    monkeypatch.setattr(module, "LinearBatchBuffer", _FakeLinearBatchBuffer)

    buffer = module.ExecutionCoordinator.make_linear_batch_buffer(7, 11)

    assert isinstance(buffer, _FakeLinearBatchBuffer)
    assert buffer.batch_size == 7
    assert buffer.metrics_flush_interval == 11
