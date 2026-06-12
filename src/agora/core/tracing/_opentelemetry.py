"""OpenTelemetry adapters for Agora tracing."""

from __future__ import annotations

from os import getenv
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


def build_configured_opentelemetry_tracer(
    *,
    name: str,
    auto_configure: bool,
) -> OpenTelemetryTracer:
    """Build an OpenTelemetry tracer and optionally auto-configure OTLP export."""
    try:
        from opentelemetry import trace as otel_trace
    except ImportError as exc:
        raise ImportError("OpenTelemetry tracing requires 'opentelemetry-api'.") from exc

    if auto_configure:
        _ensure_otel_provider_configured(otel_trace=otel_trace, service_name=name)
    return OpenTelemetryTracer(name=name)


def _ensure_otel_provider_configured(*, otel_trace: Any, service_name: str) -> None:
    provider = otel_trace.get_tracer_provider()
    if type(provider).__name__ != "ProxyTracerProvider":
        return

    try:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError as exc:
        raise ImportError(
            "OpenTelemetry auto-configuration requires 'opentelemetry-sdk' and "
            "'opentelemetry-exporter-otlp-proto-grpc', or a pre-configured global tracer provider."
        ) from exc

    resource = Resource.create(
        {
            "service.name": getenv("OTEL_SERVICE_NAME", service_name),
        }
    )
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    otel_trace.set_tracer_provider(tracer_provider)
