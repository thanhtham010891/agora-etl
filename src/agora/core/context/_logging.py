"""Structured logging helpers for pipeline context."""

from __future__ import annotations

from typing import Any

import logstruct


class _BoundLogger:
    """Thin wrapper that prepends bound fields to every log call."""

    __slots__ = ("_bound", "_logger")

    def __init__(self, logger: Any, **bound: Any) -> None:
        self._logger = logger
        self._bound = bound

    def __getattr__(self, name: str) -> Any:
        original = getattr(self._logger, name)
        if callable(original):

            def _bound_call(msg: str, **kw: Any) -> Any:
                return original(msg, **{**self._bound, **kw})

            return _bound_call
        return original

    def debug(self, msg: str, **kw: Any) -> None:
        self._logger.debug(msg, **{**self._bound, **kw})

    def info(self, msg: str, **kw: Any) -> None:
        self._logger.info(msg, **{**self._bound, **kw})

    def warning(self, msg: str, **kw: Any) -> None:
        self._logger.warning(msg, **{**self._bound, **kw})

    def error(self, msg: str, **kw: Any) -> None:
        self._logger.error(msg, **{**self._bound, **kw})

    def exception(self, msg: str, **kw: Any) -> None:
        self._logger.exception(msg, **{**self._bound, **kw})


def build_bound_logger(*, pipeline_id: str, run_id: str) -> _BoundLogger:
    """Create the canonical bound logger for one pipeline run."""
    return _BoundLogger(
        logstruct.getLogger("agora.pipeline"),
        pipeline_id=pipeline_id,
        run_id=run_id,
    )
