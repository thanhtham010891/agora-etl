from __future__ import annotations

import importlib

from agora.core.writer import Writer, WriteResult


def test_writer_module_reexports_public_api() -> None:
    module = importlib.import_module("agora.core.writer")

    assert module.WriteResult is WriteResult
    assert module.Writer is Writer
