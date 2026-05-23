"""
agora/sinks/log.py
==================
``LogSink[T]`` — emit each record as a structured log event.

No external dependencies — uses the ``logstruct`` logger already
bundled with agora.

Useful for:
- Shipping records to log aggregators (ELK, Datadog, Loki, Grafana)
  when the log pipeline handles the storage layer.
- Auditing / debugging without a real storage backend.

Usage::

    sink = LogSink(
        level="info",
        event_name="pipeline_record",
        extra_fn=lambda r: {"source": r.source, "id": r.id},
    )

    # Debug with full record dump:
    sink = LogSink(level="debug", event_name="raw_record")
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct

from agora.core.sink import BaseSink

if TYPE_CHECKING:
    from collections.abc import Callable

T = TypeVar("T")

_LEVELS = {"debug", "info", "warning", "error"}


class LogSink(BaseSink[T], Generic[T]):
    """Emit each record as a structured log event via ``logstruct``.

    Parameters
    ----------
    level:
        Log level — ``"debug"``, ``"info"``, ``"warning"``, or
        ``"error"`` (default: ``"info"``).
    event_name:
        The event string passed as the first argument to the logger
        (default: ``"pipeline_record"``).
    extra_fn:
        Optional ``(record: T) -> dict`` that extracts extra fields to
        include alongside the log event.  Defaults to a smart dump that
        calls ``model_dump()`` / ``__dict__`` / ``str()``.
    logger_name:
        Name used for the logstruct logger (default: ``"agora.sinks.log"``).
    """

    sink_name = "log"

    def __init__(
        self,
        level: str = "info",
        event_name: str = "pipeline_record",
        extra_fn: Callable[[T], dict[str, Any]] | None = None,
        logger_name: str = "agora.sinks.log",
    ) -> None:
        if level not in _LEVELS:
            raise ValueError(f"LogSink: invalid level {level!r}. Choose from {_LEVELS}")
        self._level = level
        self._event_name = event_name
        self._extra_fn = extra_fn or _default_extra
        self._logger = logstruct.getLogger(logger_name)
        self._log_fn = getattr(self._logger, level)

    async def write(self, record: T) -> None:
        extra = self._extra_fn(record)
        self._log_fn(self._event_name, **extra)


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #


def _default_extra(record: Any) -> dict[str, Any]:
    """Smart dump: model_dump() → __dict__ → str fallback."""
    if hasattr(record, "model_dump"):
        return record.model_dump()
    if hasattr(record, "__dict__"):
        return record.__dict__
    return {"record": str(record)}
