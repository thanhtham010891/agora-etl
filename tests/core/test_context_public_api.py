from __future__ import annotations

import importlib

from agora.core.context import _NOOP_SPAN_SCOPE, PipelineContext


def test_context_module_reexports_public_and_compat_api() -> None:
    module = importlib.import_module("agora.core.context")

    assert module.PipelineContext is PipelineContext
    assert module._NOOP_SPAN_SCOPE is _NOOP_SPAN_SCOPE
