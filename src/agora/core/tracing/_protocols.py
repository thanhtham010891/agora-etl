"""Minimal tracing protocols used by Agora instrumentation."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class TraceSpan(Protocol):
    """Minimal span contract used by Agora runtime instrumentation."""

    name: str

    def set_attribute(self, key: str, value: Any) -> None: ...

    def add_event(self, name: str, attributes: dict[str, Any] | None = None) -> None: ...

    def record_exception(self, exc: BaseException) -> None: ...

    def end(self) -> None: ...


@runtime_checkable
class PipelineTracer(Protocol):
    """Tracer abstraction used by pipeline runtime instrumentation."""

    def start_span(
        self,
        name: str,
        *,
        attributes: dict[str, Any] | None = None,
        parent: TraceSpan | None = None,
    ) -> TraceSpan: ...
