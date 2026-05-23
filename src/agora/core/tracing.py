"""Lightweight tracing primitives for Agora pipeline execution."""

from __future__ import annotations

from dataclasses import dataclass, field
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
    """Tracer abstraction used by ``BoundPipeline.with_tracer()``."""

    def start_span(
        self,
        name: str,
        *,
        attributes: dict[str, Any] | None = None,
        parent: TraceSpan | None = None,
    ) -> TraceSpan: ...


@dataclass
class NoopSpan:
    """Default no-op span used when tracing is disabled."""

    name: str

    def set_attribute(self, key: str, value: Any) -> None:
        del key, value

    def add_event(self, name: str, attributes: dict[str, Any] | None = None) -> None:
        del name, attributes

    def record_exception(self, exc: BaseException) -> None:
        del exc

    def end(self) -> None:
        return None


class NoopTracer:
    """Default tracer that allocates no-op spans."""

    def start_span(
        self,
        name: str,
        *,
        attributes: dict[str, Any] | None = None,
        parent: TraceSpan | None = None,
    ) -> TraceSpan:
        del attributes, parent
        return NoopSpan(name=name)


@dataclass
class RecordedSpan:
    """Simple span implementation useful for tests and lightweight debugging."""

    name: str
    parent_name: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)
    events: list[tuple[str, dict[str, Any] | None]] = field(default_factory=list)
    exceptions: list[str] = field(default_factory=list)
    ended: bool = False

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def add_event(self, name: str, attributes: dict[str, Any] | None = None) -> None:
        self.events.append((name, dict(attributes) if attributes is not None else None))

    def record_exception(self, exc: BaseException) -> None:
        self.exceptions.append(f"{type(exc).__name__}: {exc}")

    def end(self) -> None:
        self.ended = True


class InMemoryTracer:
    """Test/debug tracer that keeps completed spans in memory."""

    def __init__(self) -> None:
        self.spans: list[RecordedSpan] = []

    def start_span(
        self,
        name: str,
        *,
        attributes: dict[str, Any] | None = None,
        parent: TraceSpan | None = None,
    ) -> TraceSpan:
        span = RecordedSpan(
            name=name,
            parent_name=getattr(parent, "name", None) if parent is not None else None,
            attributes=dict(attributes or {}),
        )
        self.spans.append(span)
        return span


class OpenTelemetrySpan:
    """Adapter around an OpenTelemetry span object."""

    def __init__(self, name: str, span: Any) -> None:
        self.name = name
        self._span = span

    def set_attribute(self, key: str, value: Any) -> None:
        self._span.set_attribute(key, value)

    def add_event(self, name: str, attributes: dict[str, Any] | None = None) -> None:
        self._span.add_event(name, attributes=attributes)

    def record_exception(self, exc: BaseException) -> None:
        self._span.record_exception(exc)

    def end(self) -> None:
        self._span.end()


class OpenTelemetryTracer:
    """Optional OpenTelemetry tracer bridge.

    Requires ``opentelemetry-api`` to be installed in the environment.
    """

    def __init__(self, name: str = "agora", tracer: Any | None = None) -> None:
        try:
            from opentelemetry import trace as otel_trace
        except ImportError as exc:
            raise ImportError(
                "OpenTelemetryTracer requires opentelemetry-api to be installed."
            ) from exc
        self._otel_trace = otel_trace
        self._tracer = tracer or otel_trace.get_tracer(name)

    def start_span(
        self,
        name: str,
        *,
        attributes: dict[str, Any] | None = None,
        parent: TraceSpan | None = None,
    ) -> TraceSpan:
        context = None
        parent_span = getattr(parent, "_span", None)
        if parent_span is not None:
            context = self._otel_trace.set_span_in_context(parent_span)
        span = self._tracer.start_span(name, context=context)
        for key, value in (attributes or {}).items():
            span.set_attribute(key, value)
        return OpenTelemetrySpan(name=name, span=span)


__all__ = [
    "InMemoryTracer",
    "NoopSpan",
    "NoopTracer",
    "OpenTelemetryTracer",
    "PipelineTracer",
    "RecordedSpan",
    "TraceSpan",
]
