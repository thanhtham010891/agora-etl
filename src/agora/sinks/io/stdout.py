"""
agora/sinks/stdout.py
=====================
``StdoutSink`` — print records to stdout.

Replaces the ``dry_run=True`` flag in data-collector's pipeline.
Use during development to inspect what the pipeline produces without
writing to any real storage.

Usage::

    # Instead of dry_run=True:
    pipeline = Pipeline(source).pipe(normalizer).build()
    # build() defaults to StdoutSink when no sink is specified
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from agora.core.sink import BaseSink

if TYPE_CHECKING:
    from collections.abc import Callable

T = TypeVar("T")


class StdoutSink(BaseSink[T], Generic[T]):
    """Print records to stdout.

    Parameters
    ----------
    formatter:
        Callable that converts a record to a string.  Defaults to
        JSON serialization with fallback to ``repr()``.
    prefix:
        Optional string prepended to each output line.
    """

    sink_name = "stdout"

    def __init__(
        self,
        formatter: Callable[[T], str] | None = None,
        prefix: str = "▶ ",
    ) -> None:
        self._formatter = formatter or _default_format
        self._prefix = prefix

    async def write(self, record: T) -> None:
        print(self._prefix + self._formatter(record))


def _default_format(record: Any) -> str:
    """Try JSON, fall back to repr."""
    try:
        if hasattr(record, "model_dump"):
            return json.dumps(record.model_dump(), ensure_ascii=False, default=str)
        if hasattr(record, "__dict__"):
            return json.dumps(record.__dict__, ensure_ascii=False, default=str)
        return json.dumps(record, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return repr(record)
