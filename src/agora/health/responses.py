"""Response builders for health and metrics endpoints."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agora.metrics.collector import MetricsCollector
    from agora.metrics.exporters import MetricsExporter

_HTTP_200 = b"HTTP/1.1 200 OK\r\n"
_HTTP_301 = b"HTTP/1.1 301 Moved Permanently\r\n"
_HTTP_404 = b"HTTP/1.1 404 Not Found\r\n"
_HTTP_503 = b"HTTP/1.1 503 Service Unavailable\r\n"


@dataclass(frozen=True, slots=True)
class ResponseSpec:
    """Transport-agnostic HTTP response description."""

    status_line: bytes
    content_type: str
    body: bytes = b""
    location: str | None = None


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
        """Return the response spec for a request path."""
        if method != "GET":
            return ResponseSpec(_HTTP_404, "text/plain", b"Method not allowed")
        if path in ("/", ""):
            return ResponseSpec(_HTTP_301, "text/plain", b"", location="/health")
        if path == "/health":
            return self.build_health()
        if path == "/metrics":
            return self.build_metrics()
        if path == "/ready":
            return self.build_ready()
        return ResponseSpec(_HTTP_404, "text/plain", b"Not found")

    def build_health(self) -> ResponseSpec:
        """Build the `/health` response."""
        payload = self._collector.to_health_dict()
        payload["timestamp"] = time.time()
        body = json.dumps(payload, indent=2, default=str).encode()
        status = _HTTP_200 if payload["status"] != "failing" else _HTTP_503
        return ResponseSpec(status, "application/json", body)

    def build_metrics(self) -> ResponseSpec:
        """Build the `/metrics` response."""
        body = self._metrics_exporter.render().encode("utf-8")
        return ResponseSpec(_HTTP_200, self._metrics_exporter.content_type, body)

    def build_ready(self) -> ResponseSpec:
        """Build the `/ready` response."""
        status_str = self._collector.overall_status
        ready = status_str not in ("failing",)
        body = json.dumps({"ready": ready, "status": status_str}).encode()
        http_status = _HTTP_200 if ready else _HTTP_503
        return ResponseSpec(http_status, "application/json", body)


__all__ = ["HealthResponseBuilder", "ResponseSpec"]
