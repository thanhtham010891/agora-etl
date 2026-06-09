"""OpenTelemetry adapters for Agora tracing."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agora.core.tracing._protocols import TraceSpan


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
    """Optional OpenTelemetry tracer bridge."""

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
