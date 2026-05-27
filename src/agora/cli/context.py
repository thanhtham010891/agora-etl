"""
agora/cli/context.py
====================
AgoraContext — minimal DI container injected into every command.

agora CLI is project-agnostic, so context carries only framework-level
state.  Project-specific config is loaded inside each command.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class AgoraContext:
    """Immutable context passed to every ``BaseCommand.execute()`` call.

    Attributes
    ----------
    cwd:
        Working directory from which ``agora`` was invoked.
        Used by commands that need to locate project files.
    verbose:
        Whether ``--verbose`` / ``-v`` was passed globally.
    extra:
        Open-ended dict for framework-level extras added by commands or tests.
    """

    cwd: str = ""
    verbose: bool = False
    extra: dict[str, object] = field(default_factory=dict)

    @classmethod
    def build(cls, args_namespace: object = None) -> AgoraContext:
        """Build context from parsed global args (or defaults)."""
        import os

        verbose = getattr(args_namespace, "verbose", False)
        return cls(cwd=os.getcwd(), verbose=verbose)
