"""Exporter protocols shared across observability edges."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class MetricsExporter(Protocol):
    @property
    def content_type(self) -> str: ...

    def render(self) -> str: ...
