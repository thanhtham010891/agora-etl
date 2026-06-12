from __future__ import annotations

import importlib

from agora.core.context import PipelineContext


def test_context_module_reexports_public_api() -> None:
    module = importlib.import_module("agora.core.context")

    assert module.PipelineContext is PipelineContext
    assert not hasattr(module, "_NOOP_SPAN_SCOPE")
