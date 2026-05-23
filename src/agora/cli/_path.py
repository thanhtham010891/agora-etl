"""
agora/cli/_path.py
==================
Internal: sys.path helpers for CLI commands.

Extracted from ``run.py`` and ``worker.py`` to eliminate duplication.
Not part of the public API.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agora.cli.context import AgoraContext


def ensure_project_on_path(ctx: AgoraContext | None = None) -> None:
    """Add project root and src/ to sys.path so project modules are importable.

    Convention: agora expects user code to live under ``src/`` (editable install)
    or at the project root.  Both are added to sys.path if not already present.
    """
    import os

    cwd = (getattr(ctx, "cwd", None) or os.getcwd()) if ctx is not None else os.getcwd()
    for p in (cwd, os.path.join(cwd, "src")):
        if p not in sys.path:
            sys.path.insert(0, p)
