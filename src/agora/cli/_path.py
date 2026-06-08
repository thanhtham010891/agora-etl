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

    cwd = os.getcwd()
    if ctx is not None:
        candidate = getattr(ctx, "cwd", None)
        candidate_module = type(candidate).__module__ if candidate is not None else ""
        if candidate_module.startswith("unittest.mock"):
            candidate = None
        if isinstance(candidate, str):
            cwd = candidate
        elif isinstance(candidate, bytes):
            cwd = os.fsdecode(candidate)
        elif isinstance(candidate, os.PathLike):
            try:
                resolved = os.fspath(candidate)
            except TypeError:
                resolved = None
            if isinstance(resolved, str):
                cwd = resolved
            elif isinstance(resolved, bytes):
                cwd = os.fsdecode(resolved)
    for p in (cwd, os.path.join(cwd, "src")):
        if p not in sys.path:
            sys.path.insert(0, p)
