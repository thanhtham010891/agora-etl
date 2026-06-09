"""Support helpers for health and metrics HTTP responses."""

from __future__ import annotations

import json
import time
from typing import Any

from agora.health.responses._models import ResponseSpec

HTTP_200 = b"HTTP/1.1 200 OK\r\n"
HTTP_301 = b"HTTP/1.1 301 Moved Permanently\r\n"
HTTP_404 = b"HTTP/1.1 404 Not Found\r\n"
HTTP_503 = b"HTTP/1.1 503 Service Unavailable\r\n"


def json_response(
    *,
    status_line: bytes,
    payload: dict[str, Any],
) -> ResponseSpec:
    body = json.dumps(payload, indent=2, default=str).encode()
    return ResponseSpec(status_line, "application/json", body)


def health_payload(collector_payload: dict[str, Any]) -> dict[str, Any]:
    payload = dict(collector_payload)
    payload["timestamp"] = time.time()
    return payload


def ready_payload(status: str) -> dict[str, Any]:
    return {"ready": status not in ("failing",), "status": status}
