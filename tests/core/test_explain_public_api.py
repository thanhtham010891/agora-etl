from __future__ import annotations

import importlib

from agora.core.explain import MiddlewareStageExplain, PipelineExplain, SinkWriteExplain


def test_explain_module_reexports_public_api() -> None:
    module = importlib.import_module("agora.core.explain")

    assert module.MiddlewareStageExplain is MiddlewareStageExplain
    assert module.PipelineExplain is PipelineExplain
    assert module.SinkWriteExplain is SinkWriteExplain
