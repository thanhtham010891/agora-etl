from __future__ import annotations

import json

from agora.health.responses import HealthResponseBuilder
from agora.metrics.collector import MetricsCollector


class _FakeExporter:
    content_type = "text/plain"

    def render(self) -> str:
        return "metric 1\n"


def test_root_redirect_response() -> None:
    builder = HealthResponseBuilder(
        collector=MetricsCollector(),
        metrics_exporter=_FakeExporter(),
    )
    response = builder.build("GET", "/")
    assert response.location == "/health"
    assert response.body == b""


def test_health_response_includes_timestamp_and_json_payload() -> None:
    collector = MetricsCollector()
    builder = HealthResponseBuilder(
        collector=collector,
        metrics_exporter=_FakeExporter(),
    )
    response = builder.build_health()
    payload = json.loads(response.body.decode("utf-8"))
    assert response.content_type == "application/json"
    assert payload["status"] == "idle"
    assert "timestamp" in payload


def test_metrics_response_uses_exporter_content_type() -> None:
    builder = HealthResponseBuilder(
        collector=MetricsCollector(),
        metrics_exporter=_FakeExporter(),
    )
    response = builder.build_metrics()
    assert response.content_type == "text/plain"
    assert response.body == b"metric 1\n"


def test_ready_response_is_service_unavailable_when_collector_is_failing() -> None:
    import asyncio

    collector = MetricsCollector()
    asyncio.run(collector.record_run("pipe", summary=None, error=RuntimeError("boom")))
    builder = HealthResponseBuilder(
        collector=collector,
        metrics_exporter=_FakeExporter(),
    )
    response = builder.build_ready()
    payload = json.loads(response.body.decode("utf-8"))
    assert payload == {"ready": False, "status": "failing"}


def test_ready_response_is_service_unavailable_when_collector_is_idle() -> None:
    collector = MetricsCollector()
    builder = HealthResponseBuilder(
        collector=collector,
        metrics_exporter=_FakeExporter(),
    )
    response = builder.build_ready()
    payload = json.loads(response.body.decode("utf-8"))
    assert b"503" in response.status_line
    assert payload == {"ready": False, "status": "idle"}


def test_ready_response_is_ok_after_successful_run() -> None:
    import asyncio

    collector = MetricsCollector()
    asyncio.run(collector.record_run("pipe", summary=None))
    builder = HealthResponseBuilder(
        collector=collector,
        metrics_exporter=_FakeExporter(),
    )
    response = builder.build_ready()
    payload = json.loads(response.body.decode("utf-8"))
    assert b"200" in response.status_line
    assert payload == {"ready": True, "status": "ok"}


def test_unknown_path_returns_not_found_response() -> None:
    builder = HealthResponseBuilder(
        collector=MetricsCollector(),
        metrics_exporter=_FakeExporter(),
    )
    response = builder.build("GET", "/missing")
    assert response.body == b"Not found"


def test_non_get_health_request_returns_method_not_allowed() -> None:
    builder = HealthResponseBuilder(
        collector=MetricsCollector(),
        metrics_exporter=_FakeExporter(),
    )
    response = builder.build("POST", "/health")
    assert b"405" in response.status_line
    assert response.body == b"Method not allowed"
