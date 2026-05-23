"""Registry of pluggable state backends."""

from __future__ import annotations

from agora.core.registry import Registry
from agora.state.backend import MemoryBackend, SQLiteBackend, StateBackend

state_backend_registry: Registry[type[StateBackend]] = Registry(name="state_backend")
state_backend_registry.register("memory", MemoryBackend)
state_backend_registry.register("sqlite", SQLiteBackend)
state_backend_registry.load_entrypoints("agora.state.backends")

__all__ = ["state_backend_registry"]
