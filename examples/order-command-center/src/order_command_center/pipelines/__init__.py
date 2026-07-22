"""Independently deployable order-event projection modules.

Add a projection by creating a module here that uses ``execute_projection``.
The module owns source/sink mapping; the shared runtime owns lifecycle,
metrics, health and retry semantics.
"""

__all__ = ["base", "postgres", "redis"]
