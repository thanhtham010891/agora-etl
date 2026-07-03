"""Facade builder for health-related HTTP responses."""

from __future__ import annotations

from typing import TYPE_CHECKING

from agora.health.responses._models import ResponseSpec
from agora.health.responses._support import (
    HTTP_200,
    HTTP_301,
    HTTP_404,
    HTTP_405,
    HTTP_503,
    health_payload,
    json_response,
    ready_payload,
)

if TYPE_CHECKING:
    from agora.metrics.collector import MetricsCollector
    from agora.metrics.exporters import MetricsExporter


class HealthResponseBuilder:
    """Build endpoint responses from collector and exporter state."""

    def __init__(
        self,
        *,
        collector: MetricsCollector,
        metrics_exporter: MetricsExporter,
    ) -> None:
        self._collector = collector
        self._metrics_exporter = metrics_exporter

    def build(self, method: str, path: str) -> ResponseSpec:
        if method != "GET":
            return ResponseSpec(HTTP_405, "text/plain", b"Method not allowed")
        if path in ("/", ""):
            return ResponseSpec(HTTP_301, "text/plain", b"", location="/health")
        if path == "/health":
            return self.build_health()
        if path == "/metrics":
            return self.build_metrics()
        if path == "/ready":
            return self.build_ready()
        return ResponseSpec(HTTP_404, "text/plain", b"Not found")

    def build_health(self) -> ResponseSpec:
        payload = health_payload(self._collector.to_health_dict())
        status = HTTP_200 if payload["status"] != "failing" else HTTP_503
        return json_response(status_line=status, payload=payload)

    def build_metrics(self) -> ResponseSpec:
        body = self._metrics_exporter.render().encode("utf-8")
        return ResponseSpec(HTTP_200, self._metrics_exporter.content_type, body)

    def build_ready(self) -> ResponseSpec:
        status = self._collector.overall_status
        payload = ready_payload(status)
        http_status = HTTP_200 if payload["ready"] else HTTP_503
        return json_response(status_line=http_status, payload=payload)
