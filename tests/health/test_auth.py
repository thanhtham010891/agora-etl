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

import asyncio
import json

import pytest

from agora.health.server import HealthServer


async def _start_server(auth_token: str | None = None) -> tuple[HealthServer, int]:
    """Start a HealthServer on a random port and return (server, port)."""
    server = HealthServer(port=0, auth_token=auth_token)
    server._stop_event = asyncio.Event()
    server._server = await asyncio.start_server(
        server._handle_connection,
        host="127.0.0.1",
        port=0,
    )
    port = server._server.sockets[0].getsockname()[1]
    return server, port


async def _send_request(port: int, request_bytes: bytes) -> str:
    """Send raw HTTP request bytes and return the full response as a string."""
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(request_bytes)
    await writer.drain()
    response = await asyncio.wait_for(reader.read(4096), timeout=2.0)
    writer.close()
    await writer.wait_closed()
    return response.decode("ascii", errors="replace")


def _status_line(response: str) -> str:
    return response.split("\r\n")[0]


def _body(response: str) -> str:
    sep = response.find("\r\n\r\n")
    return response[sep + 4 :] if sep != -1 else ""


# ------------------------------------------------------------------ #
# 401 on missing Authorization header                                  #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_401_on_missing_auth_header_health() -> None:
    """GET /health without Authorization header → 401 when auth_token is configured."""
    server, port = await _start_server(auth_token="secret")
    try:
        response = await _send_request(
            port,
            b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        assert "401" in _status_line(response), (
            f"Expected 401 for missing auth header, got: {_status_line(response)!r}"
        )
        body = json.loads(_body(response))
        assert body == {"error": "Unauthorized"}, f"Unexpected body: {body}"
    finally:
        server.stop()
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_401_on_missing_auth_header_metrics() -> None:
    """GET /metrics without Authorization header → 401 when auth_token is configured."""
    server, port = await _start_server(auth_token="secret")
    try:
        response = await _send_request(
            port,
            b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        assert "401" in _status_line(response), (
            f"Expected 401 for missing auth header on /metrics, got: {_status_line(response)!r}"
        )
    finally:
        server.stop()
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_401_on_missing_auth_header_ready() -> None:
    """GET /ready without Authorization header → 401 when auth_token is configured."""
    server, port = await _start_server(auth_token="secret")
    try:
        response = await _send_request(
            port,
            b"GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        assert "401" in _status_line(response), (
            f"Expected 401 for missing auth header on /ready, got: {_status_line(response)!r}"
        )
    finally:
        server.stop()
        await asyncio.sleep(0.05)


# ------------------------------------------------------------------ #
# 401 on wrong Bearer token                                            #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_401_on_wrong_bearer_token() -> None:
    """GET /health with wrong Bearer token → 401."""
    server, port = await _start_server(auth_token="correct-token")
    try:
        response = await _send_request(
            port,
            b"GET /health HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer wrong-token\r\n\r\n",
        )
        assert "401" in _status_line(response), (
            f"Expected 401 for wrong Bearer token, got: {_status_line(response)!r}"
        )
        body = json.loads(_body(response))
        assert body == {"error": "Unauthorized"}, f"Unexpected body: {body}"
    finally:
        server.stop()
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_401_on_empty_bearer_token() -> None:
    """GET /health with empty Bearer token → 401."""
    server, port = await _start_server(auth_token="secret")
    try:
        response = await _send_request(
            port,
            b"GET /health HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer \r\n\r\n",
        )
        assert "401" in _status_line(response), (
            f"Expected 401 for empty Bearer token, got: {_status_line(response)!r}"
        )
    finally:
        server.stop()
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_401_on_non_bearer_scheme() -> None:
    """GET /health with Basic auth scheme → 401 (only Bearer is accepted)."""
    server, port = await _start_server(auth_token="secret")
    try:
        response = await _send_request(
            port,
            b"GET /health HTTP/1.1\r\nHost: localhost\r\nAuthorization: Basic secret\r\n\r\n",
        )
        assert "401" in _status_line(response), (
            f"Expected 401 for Basic auth scheme, got: {_status_line(response)!r}"
        )
    finally:
        server.stop()
        await asyncio.sleep(0.05)


# ------------------------------------------------------------------ #
# 200 on correct Bearer token                                          #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_200_on_correct_bearer_token_health() -> None:
    """GET /health with correct Bearer token → 200 with JSON payload."""
    server, port = await _start_server(auth_token="my-secret")
    try:
        response = await _send_request(
            port,
            b"GET /health HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer my-secret\r\n\r\n",
        )
        assert "200" in _status_line(response), (
            f"Expected 200 for correct Bearer token, got: {_status_line(response)!r}"
        )
        body = json.loads(_body(response))
        assert "status" in body, f"Health payload missing 'status': {body}"
        assert "pipelines" in body, f"Health payload missing 'pipelines': {body}"
    finally:
        server.stop()
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_200_on_correct_bearer_token_metrics() -> None:
    """GET /metrics with correct Bearer token → 200."""
    server, port = await _start_server(auth_token="my-secret")
    try:
        response = await _send_request(
            port,
            b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer my-secret\r\n\r\n",
        )
        assert "200" in _status_line(response), (
            f"Expected 200 for correct Bearer token on /metrics, got: {_status_line(response)!r}"
        )
    finally:
        server.stop()
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_200_on_correct_bearer_token_ready() -> None:
    """GET /ready with correct Bearer token → 200."""
    server, port = await _start_server(auth_token="my-secret")
    try:
        response = await _send_request(
            port,
            b"GET /ready HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer my-secret\r\n\r\n",
        )
        assert "200" in _status_line(response), (
            f"Expected 200 for correct Bearer token on /ready, got: {_status_line(response)!r}"
        )
        body = json.loads(_body(response))
        assert "ready" in body, f"Ready payload missing 'ready': {body}"
    finally:
        server.stop()
        await asyncio.sleep(0.05)


# ------------------------------------------------------------------ #
# 200 when auth is disabled (auth_token=None)                          #
# ------------------------------------------------------------------ #


@pytest.mark.asyncio
async def test_200_when_auth_disabled_no_header() -> None:
    """GET /health with no auth_token configured → 200 even without Authorization header."""
    server, port = await _start_server(auth_token=None)
    try:
        response = await _send_request(
            port,
            b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        assert "200" in _status_line(response), (
            f"Expected 200 when auth disabled (no header), got: {_status_line(response)!r}"
        )
        body = json.loads(_body(response))
        assert "status" in body, f"Health payload missing 'status': {body}"
    finally:
        server.stop()
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_200_when_auth_disabled_with_any_header() -> None:
    """GET /health with no auth_token configured → 200 regardless of Authorization header."""
    server, port = await _start_server(auth_token=None)
    try:
        response = await _send_request(
            port,
            b"GET /health HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer anything\r\n\r\n",
        )
        assert "200" in _status_line(response), (
            f"Expected 200 when auth disabled (any header), got: {_status_line(response)!r}"
        )
    finally:
        server.stop()
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_200_when_auth_disabled_metrics() -> None:
    """GET /metrics with no auth_token configured → 200."""
    server, port = await _start_server(auth_token=None)
    try:
        response = await _send_request(
            port,
            b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        assert "200" in _status_line(response), (
            f"Expected 200 for /metrics when auth disabled, got: {_status_line(response)!r}"
        )
    finally:
        server.stop()
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_200_when_auth_disabled_ready() -> None:
    """GET /ready with no auth_token configured → 200."""
    server, port = await _start_server(auth_token=None)
    try:
        response = await _send_request(
            port,
            b"GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        assert "200" in _status_line(response), (
            f"Expected 200 for /ready when auth disabled, got: {_status_line(response)!r}"
        )
    finally:
        server.stop()
        await asyncio.sleep(0.05)
