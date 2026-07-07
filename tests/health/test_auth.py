"""
tests/health/test_auth.py
==========================
Unit tests for HealthServer authentication enforcement.

Tests:
- 401 on missing Authorization header when auth_token is configured
- 401 on wrong Bearer token when auth_token is configured
- 200 on correct Bearer token
- 200 when auth is disabled (auth_token=None)

Validates: Requirements 2.1, 2.2, 2.3, 3.1, 3.2, 3.3, 3.4
"""

from __future__ import annotations

import json
import warnings

import pytest

from agora.health.server import HealthServer
from tests.health._harness import body as _body
from tests.health._harness import headers as _headers
from tests.health._harness import send_request as _send_request
from tests.health._harness import status_line as _status_line

# ------------------------------------------------------------------ #
# 401 on missing Authorization header                                  #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_401_on_missing_auth_header_health() -> None:
    """GET /health without Authorization header → 401 when auth_token is configured."""
    response = await _send_request(
        b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n",
        auth_token="secret",
    )
    assert "401" in _status_line(response), (
        f"Expected 401 for missing auth header, got: {_status_line(response)!r}"
    )
    headers = _headers(response)
    assert headers["www-authenticate"] == 'Bearer realm="agora-health"'
    assert headers["cache-control"] == "no-store"
    assert headers["x-content-type-options"] == "nosniff"
    body = json.loads(_body(response))
    assert body == {"error": "Unauthorized"}, f"Unexpected body: {body}"


@pytest.mark.asyncio
async def test_401_on_missing_auth_header_metrics() -> None:
    """GET /metrics without Authorization header → 401 when auth_token is configured."""
    response = await _send_request(
        b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n",
        auth_token="secret",
    )
    assert "401" in _status_line(response), (
        f"Expected 401 for missing auth header on /metrics, got: {_status_line(response)!r}"
    )


@pytest.mark.asyncio
async def test_401_on_missing_auth_header_ready() -> None:
    """GET /ready without Authorization header → 401 when auth_token is configured."""
    response = await _send_request(
        b"GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n",
        auth_token="secret",
    )
    assert "401" in _status_line(response), (
        f"Expected 401 for missing auth header on /ready, got: {_status_line(response)!r}"
    )


@pytest.mark.asyncio
async def test_root_redirect_does_not_require_auth() -> None:
    """GET / still returns the redirect even when auth_token is configured."""
    response = await _send_request(
        b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n",
        auth_token="secret",
    )
    assert "301" in _status_line(response), (
        f"Expected 301 redirect for root path, got: {_status_line(response)!r}"
    )
    parsed_headers = _headers(response)
    assert parsed_headers["location"] == "/health"


@pytest.mark.asyncio
async def test_unknown_path_with_auth_enabled_still_returns_404() -> None:
    """Unknown paths are not protected endpoints and should stay 404."""
    response = await _send_request(
        b"GET /missing HTTP/1.1\r\nHost: localhost\r\n\r\n",
        auth_token="secret",
    )
    assert "404" in _status_line(response), (
        f"Expected 404 for unknown path, got: {_status_line(response)!r}"
    )


# ------------------------------------------------------------------ #
# 401 on wrong Bearer token                                            #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_401_on_wrong_bearer_token() -> None:
    """GET /health with wrong Bearer token → 401."""
    response = await _send_request(
        b"GET /health HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer wrong-token\r\n\r\n",
        auth_token="correct-token",
    )
    assert "401" in _status_line(response), (
        f"Expected 401 for wrong Bearer token, got: {_status_line(response)!r}"
    )
    body = json.loads(_body(response))
    assert body == {"error": "Unauthorized"}, f"Unexpected body: {body}"


@pytest.mark.asyncio
async def test_401_on_empty_bearer_token() -> None:
    """GET /health with empty Bearer token → 401."""
    response = await _send_request(
        b"GET /health HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer \r\n\r\n",
        auth_token="secret",
    )
    assert "401" in _status_line(response), (
        f"Expected 401 for empty Bearer token, got: {_status_line(response)!r}"
    )


@pytest.mark.asyncio
async def test_401_on_non_bearer_scheme() -> None:
    """GET /health with Basic auth scheme → 401 (only Bearer is accepted)."""
    response = await _send_request(
        b"GET /health HTTP/1.1\r\nHost: localhost\r\nAuthorization: Basic secret\r\n\r\n",
        auth_token="secret",
    )
    assert "401" in _status_line(response), (
        f"Expected 401 for Basic auth scheme, got: {_status_line(response)!r}"
    )


# ------------------------------------------------------------------ #
# 200 on correct Bearer token                                          #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_200_on_correct_bearer_token_health() -> None:
    """GET /health with correct Bearer token → 200 with JSON payload."""
    response = await _send_request(
        b"GET /health HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer my-secret\r\n\r\n",
        auth_token="my-secret",
    )
    assert "200" in _status_line(response), (
        f"Expected 200 for correct Bearer token, got: {_status_line(response)!r}"
    )
    headers = _headers(response)
    assert headers["cache-control"] == "no-store"
    assert headers["x-content-type-options"] == "nosniff"
    body = json.loads(_body(response))
    assert "status" in body, f"Health payload missing 'status': {body}"
    assert "pipelines" in body, f"Health payload missing 'pipelines': {body}"


@pytest.mark.asyncio
async def test_200_on_correct_bearer_token_metrics() -> None:
    """GET /metrics with correct Bearer token → 200."""
    response = await _send_request(
        b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer my-secret\r\n\r\n",
        auth_token="my-secret",
    )
    assert "200" in _status_line(response), (
        f"Expected 200 for correct Bearer token on /metrics, got: {_status_line(response)!r}"
    )


@pytest.mark.asyncio
async def test_503_idle_on_correct_bearer_token_ready() -> None:
    """GET /ready with correct Bearer token authenticates, then reports idle readiness."""
    response = await _send_request(
        b"GET /ready HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer my-secret\r\n\r\n",
        auth_token="my-secret",
    )
    assert "503" in _status_line(response), (
        f"Expected 503 idle readiness for correct Bearer token on /ready, got: {_status_line(response)!r}"
    )
    body = json.loads(_body(response))
    assert "ready" in body, f"Ready payload missing 'ready': {body}"
    assert body["ready"] is False


# ------------------------------------------------------------------ #
# 200 when auth is disabled (auth_token=None)                          #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_200_when_auth_disabled_no_header() -> None:
    """GET /health with no auth_token configured → 200 even without Authorization header."""
    response = await _send_request(
        b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n",
        auth_token=None,
    )
    assert "200" in _status_line(response), (
        f"Expected 200 when auth disabled (no header), got: {_status_line(response)!r}"
    )
    body = json.loads(_body(response))
    assert "status" in body, f"Health payload missing 'status': {body}"


def test_health_server_warns_on_non_loopback_host_without_auth() -> None:
    with pytest.warns(UserWarning, match="non-loopback host without auth_token"):
        HealthServer(port=8080, host="0.0.0.0", auth_token=None)


def test_health_server_does_not_warn_on_loopback_host_without_auth() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        HealthServer(port=8080, host="127.0.0.1", auth_token=None)

    assert not caught


@pytest.mark.asyncio
async def test_200_when_auth_disabled_with_any_header() -> None:
    """GET /health with no auth_token configured → 200 regardless of Authorization header."""
    response = await _send_request(
        b"GET /health HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer anything\r\n\r\n",
        auth_token=None,
    )
    assert "200" in _status_line(response), (
        f"Expected 200 when auth disabled (any header), got: {_status_line(response)!r}"
    )


@pytest.mark.asyncio
async def test_200_when_auth_disabled_metrics() -> None:
    """GET /metrics with no auth_token configured → 200."""
    response = await _send_request(
        b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n",
        auth_token=None,
    )
    assert "200" in _status_line(response), (
        f"Expected 200 for /metrics when auth disabled, got: {_status_line(response)!r}"
    )


@pytest.mark.asyncio
async def test_503_idle_when_auth_disabled_ready() -> None:
    """GET /ready with no auth_token configured reports idle readiness."""
    response = await _send_request(
        b"GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n",
        auth_token=None,
    )
    assert "503" in _status_line(response), (
        f"Expected 503 idle readiness for /ready when auth disabled, got: {_status_line(response)!r}"
    )
