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

from typing import Any


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
    """Raised for pipeline construction or execution errors.

    Carries operational context so operators can understand failures without
    reading tracebacks alone.  All fields are optional — they are populated
    progressively as the runtime gathers information.

    Attributes
    ----------
    pipeline_id:  identifier of the pipeline that failed
    run_id:       identifier of the specific run
    stage:        execution stage where the failure occurred (e.g. "middleware",
                  "sink", "checkpoint", "source_stream")
    source_name:  name of the source that was streaming at failure time
    sink_name:    name of the sink that was writing at failure time
    checkpoint:   last known checkpoint value at failure time
    """

    def __init__(
        self,
        message: str,
        *,
        pipeline_id: str | None = None,
        run_id: str | None = None,
        stage: str | None = None,
        source_name: str | None = None,
        sink_name: str | None = None,
        checkpoint: Any | None = None,
    ) -> None:
        super().__init__(message)
        self.pipeline_id = pipeline_id
        self.run_id = run_id
        self.stage = stage
        self.source_name = source_name
        self.sink_name = sink_name
        self.checkpoint = checkpoint

    def with_context(
        self,
        *,
        pipeline_id: str | None = None,
        run_id: str | None = None,
        stage: str | None = None,
        source_name: str | None = None,
        sink_name: str | None = None,
        checkpoint: Any | None = None,
    ) -> PipelineError:
        """Return a copy of this error enriched with additional context fields.

        Only non-None values overwrite existing fields, so partial enrichment
        at multiple call sites composes safely.
        """
        return PipelineError(
            str(self),
            pipeline_id=pipeline_id if pipeline_id is not None else self.pipeline_id,
            run_id=run_id if run_id is not None else self.run_id,
            stage=stage if stage is not None else self.stage,
            source_name=source_name if source_name is not None else self.source_name,
            sink_name=sink_name if sink_name is not None else self.sink_name,
            checkpoint=checkpoint if checkpoint is not None else self.checkpoint,
        )

    def __str__(self) -> str:
        base = super().__str__()
        parts: list[str] = []
        if self.pipeline_id:
            parts.append(f"pipeline={self.pipeline_id!r}")
        if self.run_id:
            parts.append(f"run={self.run_id!r}")
        if self.stage:
            parts.append(f"stage={self.stage!r}")
        if self.source_name:
            parts.append(f"source={self.source_name!r}")
        if self.sink_name:
            parts.append(f"sink={self.sink_name!r}")
        if self.checkpoint is not None:
            parts.append(f"checkpoint={self.checkpoint!r}")
        if not parts:
            return base
        return f"{base} [{', '.join(parts)}]"


# ======================================================================
# Config errors
# ======================================================================


class ConfigError(AgoraError):
    """Raised for configuration-related errors."""
