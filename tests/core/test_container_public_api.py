from __future__ import annotations

import importlib

from agora.core.container import AgoraContainer


def test_container_module_reexports_public_api() -> None:
    module = importlib.import_module("agora.core.container")

    assert module.AgoraContainer is AgoraContainer
