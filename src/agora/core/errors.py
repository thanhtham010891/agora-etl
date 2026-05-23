"""
agora/core/errors.py
====================
Centralized exception hierarchy for the agora framework.

All framework-level exceptions descend from ``AgoraError``, making it
easy for callers to catch broad categories::

    try:
        plugin = registry.get_or_raise("unknown")
    except PluginError:
        ...   # any plugin-related error
    except AgoraError:
        ...   # any agora error

Exception tree::

    AgoraError
    ├── PluginError
    │   ├── PluginNotFoundError
    │   └── PluginValidationError
    ├── RegistryError
    ├── PipelineError
    └── ConfigError
"""

from __future__ import annotations


class AgoraError(Exception):
    """Base exception for all agora framework errors."""


# ======================================================================
# Plugin errors
# ======================================================================


class PluginError(AgoraError):
    """Base for plugin-related errors."""


class PluginNotFoundError(PluginError, KeyError):
    """Raised when a plugin key is not found in a registry.

    Inherits from ``KeyError`` for backward compatibility with code
    that catches ``KeyError`` from ``Registry.get_or_raise()``.
    """

    def __init__(self, registry_name: str, key: str, available: list[str]) -> None:
        self.registry_name = registry_name
        self.key = key
        self.available = available
        super().__init__(
            f"No {registry_name} plugin registered for key '{key}'. Available: {available}"
        )


class PluginValidationError(PluginError):
    """Raised when a plugin fails protocol/type validation."""

    def __init__(self, registry_name: str, key: str, reason: str) -> None:
        self.registry_name = registry_name
        self.key = key
        self.reason = reason
        super().__init__(f"Plugin '{key}' in registry '{registry_name}' is invalid: {reason}")


# ======================================================================
# Registry errors
# ======================================================================


class RegistryError(AgoraError):
    """Raised for registry-level operational errors (e.g. discovery failures)."""


# ======================================================================
# Pipeline errors
# ======================================================================


class PipelineError(AgoraError):
    """Raised for pipeline construction or execution errors."""


# ======================================================================
# Config errors
# ======================================================================


class ConfigError(AgoraError):
    """Raised for configuration-related errors."""
