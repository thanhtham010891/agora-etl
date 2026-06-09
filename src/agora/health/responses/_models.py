"""Transport-agnostic response models for health endpoints."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ResponseSpec:
    """Transport-agnostic HTTP response description."""

    status_line: bytes
    content_type: str
    body: bytes = b""
    location: str | None = None
