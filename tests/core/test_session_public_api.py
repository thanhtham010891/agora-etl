from __future__ import annotations

import importlib

from agora.core.session import PipelineLifecycleController, PipelineRunState


def test_session_module_reexports_public_api() -> None:
    module = importlib.import_module("agora.core.session")

    assert module.PipelineLifecycleController is PipelineLifecycleController
    assert module.PipelineRunState is PipelineRunState
