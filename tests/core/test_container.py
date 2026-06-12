from __future__ import annotations

import sys
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from agora.ai import ai_provider_registry
from agora.ai.cache import StateBackendLLMCache
from agora.ai.providers.base import CompletionResponse, EmbeddingResponse
from agora.core.component_factory import config_component_factory
from agora.core.container import AgoraContainer
from agora.core.container._assembly import build_tracer_from_config
from agora.core.errors import ConfigError
from agora.core.plugin import Lifecycle
from agora.core.tracing import OpenTelemetryTracer
from agora.middlewares.ai.enrich import AIEnrichMiddleware


@dataclass
class _FakeProvider:
    model: str = "fake-model"

    async def complete(self, prompt: str, **kwargs) -> CompletionResponse:
        return CompletionResponse(content=f"ok:{prompt}", model=self.model)

    async def embed(self, text: str) -> EmbeddingResponse:
        return EmbeddingResponse(embedding=[0.0, 1.0], model=self.model)

    async def embed_batch(self, texts: list[str]) -> list[EmbeddingResponse]:
        return [EmbeddingResponse(embedding=[0.0, 1.0], model=self.model) for _ in texts]


@pytest.mark.asyncio
async def test_build_ai_middleware_component_resolves_provider_and_backend_cache(tmp_path) -> None:
    provider_type = "_test_fake_ai_provider_for_container"
    ai_provider_registry.register_factory(provider_type, _FakeProvider)

    middleware = config_component_factory.build_middleware_component(
        {
            "type": "ai_enrich",
            "provider": {"type": provider_type},
            "prompt_template": "hello {name}",
            "cache": {
                "backend": {
                    "type": "sqlite",
                    "path": tmp_path / "ai-cache.db",
                }
            },
        }
    )

    assert isinstance(middleware, AIEnrichMiddleware)
    assert isinstance(middleware._provider, _FakeProvider)
    assert isinstance(middleware._cache, StateBackendLLMCache)

    await middleware._cache.set("k", "v")
    assert await middleware._cache.get("k") == "v"
    await middleware._cache.close()


@pytest.mark.asyncio
async def test_container_startup_all_fails_fast_when_singleton_factory_resolution_fails() -> None:
    container = AgoraContainer("test")

    def _boom():
        raise RuntimeError("factory exploded")

    container.register_factory("broken", _boom, singleton=True)

    with pytest.raises(RuntimeError, match="factory exploded"):
        await container.startup_all()


def test_container_build_pipeline_requires_at_least_one_sink() -> None:
    container = AgoraContainer("test")
    container.register_singleton("_pipeline_id", "missing-sink")
    container.register_singleton("source", object())
    container.register_singleton("_middlewares", [])
    container.register_singleton("_sinks", [])

    with pytest.raises(
        ConfigError,
        match=(
            r"Cannot build pipeline: no sinks are configured\. "
            r"Declarative pipelines must define at least one sink\."
        ),
    ):
        container.build_pipeline()


@dataclass
class _LifecycleProbe(Lifecycle):
    started: list[str]
    stopped: list[str]
    name: str

    async def startup(self) -> None:
        self.started.append(self.name)

    async def shutdown(self) -> None:
        self.stopped.append(self.name)


@pytest.mark.asyncio
async def test_container_startup_all_still_starts_lifecycle_singletons_in_order() -> None:
    started: list[str] = []
    stopped: list[str] = []
    container = AgoraContainer("ordered")
    container.register_singleton("first", _LifecycleProbe(started, stopped, "first"))
    container.register_singleton("second", _LifecycleProbe(started, stopped, "second"))

    await container.startup_all()
    await container.shutdown_all()

    assert started == ["first", "second"]
    assert stopped == ["second", "first"]


def test_build_tracer_from_config_auto_configures_opentelemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider_calls: list[object] = []
    tracer_names: list[str] = []
    span_processors: list[object] = []

    class ProxyTracerProvider:
        pass

    class _ConfiguredProvider:
        def __init__(self, resource=None) -> None:
            self.resource = resource
            self.processors: list[object] = []

        def add_span_processor(self, processor: object) -> None:
            self.processors.append(processor)
            span_processors.append(processor)

    class _TraceModule:
        def __init__(self) -> None:
            self.provider: object = ProxyTracerProvider()

        def get_tracer_provider(self) -> object:
            return self.provider

        def set_tracer_provider(self, provider: object) -> None:
            provider_calls.append(provider)
            self.provider = provider

        def get_tracer(self, name: str) -> object:
            tracer_names.append(name)
            return SimpleNamespace(
                start_span=lambda name, context=None: SimpleNamespace(
                    set_attribute=lambda key, value: None,
                    add_event=lambda event_name, attributes=None: None,
                    record_exception=lambda exc: None,
                    end=lambda: None,
                )
            )

        def set_span_in_context(self, parent_span: object) -> object:
            return {"parent": parent_span}

    trace_module = _TraceModule()

    class _FakeResource:
        @staticmethod
        def create(attrs: dict[str, object]) -> dict[str, object]:
            return attrs

    class _FakeBatchSpanProcessor:
        def __init__(self, exporter: object) -> None:
            self.exporter = exporter

    monkeypatch.setitem(sys.modules, "opentelemetry", SimpleNamespace(trace=trace_module))
    monkeypatch.setitem(sys.modules, "opentelemetry.trace", trace_module)
    monkeypatch.setitem(
        sys.modules, "opentelemetry.sdk.resources", SimpleNamespace(Resource=_FakeResource)
    )
    monkeypatch.setitem(
        sys.modules,
        "opentelemetry.sdk.trace",
        SimpleNamespace(TracerProvider=_ConfiguredProvider),
    )
    monkeypatch.setitem(
        sys.modules,
        "opentelemetry.sdk.trace.export",
        SimpleNamespace(BatchSpanProcessor=_FakeBatchSpanProcessor),
    )
    monkeypatch.setitem(
        sys.modules,
        "opentelemetry.exporter.otlp.proto.grpc.trace_exporter",
        SimpleNamespace(OTLPSpanExporter=lambda: "otlp-exporter"),
    )

    tracer, backend = build_tracer_from_config(
        {"enabled": True, "backend": "opentelemetry", "service_name": "orders-worker"},
        pipeline_id="orders",
    )

    assert backend == "opentelemetry"
    assert isinstance(tracer, OpenTelemetryTracer)
    assert tracer_names == ["orders-worker"]
    assert len(provider_calls) == 1
    assert provider_calls[0].resource == {"service.name": "orders-worker"}
    assert len(span_processors) == 1
    assert span_processors[0].exporter == "otlp-exporter"


def test_build_tracer_from_config_reports_missing_otel_optional_deps() -> None:
    with pytest.raises(
        ConfigError,
        match="requires either a pre-configured global tracer provider or the optional dependencies",
    ):
        build_tracer_from_config(
            {"enabled": True, "backend": "opentelemetry", "service_name": "orders-worker"},
            pipeline_id="orders",
        )
