"""
agora/health/server.py
=======================
``HealthServer`` — pure-asyncio HTTP server for /health and /metrics.

No dependencies beyond stdlib + agora.  Uses ``asyncio.start_server()``
with minimal HTTP/1.1 request parsing.

Endpoints
---------
    GET /           → 301 redirect to /health
    GET /health     → 200 JSON health payload
    GET /metrics    → 200 Prometheus text format
    GET /ready      → 200 {"ready": true} or 503 {"ready": false}
    *               → 404

Shutdown
--------
Call ``server.stop()`` to gracefully close the server.
It is also shutdown automatically when the event loop is cancelled.

Usage::

    from agora.health import HealthServer
    from agora.metrics import MetricsCollector

    collector = MetricsCollector()
    server = HealthServer(port=8080, collector=collector)
    await server.serve()           # blocks until stop() or Ctrl+C

Or alongside WorkerPool::

    await asyncio.gather(
        pool.run(),
        server.serve(),
    )
"""

from __future__ import annotations

import asyncio
import hmac
from typing import TYPE_CHECKING

import logstruct

from agora.health.responses import HealthResponseBuilder

if TYPE_CHECKING:
    from agora.metrics.collector import MetricsCollector

logger = logstruct.getLogger(__name__)

_HTTP_200 = b"HTTP/1.1 200 OK\r\n"
_HTTP_301 = b"HTTP/1.1 301 Moved Permanently\r\n"
_HTTP_401 = b"HTTP/1.1 401 Unauthorized\r\n"
_HTTP_404 = b"HTTP/1.1 404 Not Found\r\n"
_HTTP_503 = b"HTTP/1.1 503 Service Unavailable\r\n"
_CRLF = b"\r\n"


class HealthServer:
    """Lightweight asyncio HTTP health server.

    Parameters
    ----------
    port:
        TCP port to listen on (default: 8080).
    host:
        Bind address (default: ``"127.0.0.1"``).
    collector:
        ``MetricsCollector`` instance to expose.
        If ``None``, a new empty collector is created.
    namespace:
        Prometheus metric namespace (default: ``"agora"``).
    auth_token:
        Optional Bearer token for authentication.
        When set, all requests to ``/health``, ``/metrics``, and ``/ready``
        must include ``Authorization: Bearer <token>`` or receive HTTP 401.
        Default ``None`` disables authentication (backward-compatible).
    """

    def __init__(
        self,
        port: int = 8080,
        host: str = "127.0.0.1",
        collector: MetricsCollector | None = None,
        namespace: str = "agora",
        auth_token: str | None = None,
    ) -> None:
        self._port = port
        self._host = host
        self._namespace = namespace
        self._auth_token = auth_token
        self._server: asyncio.Server | None = None
        self._stop_event: asyncio.Event | None = None

        if collector is None:
            from agora.metrics.collector import MetricsCollector

            self._collector = MetricsCollector()
        else:
            self._collector = collector

        from agora.metrics.exporters import metrics_exporter_registry

        self._prometheus = metrics_exporter_registry.create(
            "prometheus",
            collector=self._collector,
            namespace=namespace,
        )
        self._responses = HealthResponseBuilder(
            collector=self._collector,
            metrics_exporter=self._prometheus,
        )

    def __repr__(self) -> str:
        return (
            f"HealthServer(host={self._host!r}, port={self._port}, "
            f"auth={'set' if self._auth_token else 'disabled'})"
        )

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def serve(self) -> None:
        """Start the HTTP server.  Blocks until ``stop()`` is called."""
        self._stop_event = asyncio.Event()
        self._server = await asyncio.start_server(
            self._handle_connection,
            host=self._host,
            port=self._port,
        )
        addr = (
            self._server.sockets[0].getsockname()
            if self._server.sockets
            else (self._host, self._port)
        )
        logger.info(
            "health_server_start",
            host=addr[0],
            port=addr[1],
            endpoints=["/health", "/metrics", "/ready"],
        )
        try:
            async with self._server:
                # Blocks here — exits when stop_event is set by stop()
                await self._stop_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            logger.info("health_server_stopped", port=self._port)

    def stop(self) -> None:
        """Signal the server to stop (returns immediately, non-blocking)."""
        if self._stop_event is not None:
            self._stop_event.set()  # wakes serve()
        if self._server is not None:
            self._server.close()

    @property
    def collector(self) -> MetricsCollector:
        return self._collector

    # ------------------------------------------------------------------ #
    # Request handling                                                     #
    # ------------------------------------------------------------------ #

    def _check_auth(self, raw_request: bytes) -> bool:
        """Return True if auth passes (token matches or auth disabled).

        Parses the ``Authorization`` header from raw HTTP bytes and checks
        whether it matches ``Bearer {self._auth_token}``.  Returns ``True``
        immediately when ``auth_token`` is ``None`` (auth disabled).
        """
        if self._auth_token is None:
            return True
        for line in raw_request.split(b"\r\n"):
            if line.lower().startswith(b"authorization:"):
                value = line.split(b":", 1)[1].strip().decode("ascii", errors="replace")
                expected = f"Bearer {self._auth_token}"
                return hmac.compare_digest(value, expected)
        return False

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            # Read until end-of-headers with a 10s overall timeout and 65536-byte cap.
            chunks: list[bytes] = []
            async with asyncio.timeout(10.0):
                while True:
                    chunk = await reader.read(4096)
                    if not chunk:
                        break
                    chunks.append(chunk)
                    combined = b"".join(chunks)
                    if b"\r\n\r\n" in combined or b"\n\n" in combined:
                        break
                    if len(combined) > 65536:
                        writer.close()
                        return
            raw = b"".join(chunks)
        except (TimeoutError, ConnectionResetError):
            writer.close()
            return

        method, path = _parse_request_line(raw)
        if not self._check_auth(raw):
            _write_response(writer, _HTTP_401, "application/json", b'{"error":"Unauthorized"}')
        else:
            await self._route(method, path, writer)

        try:
            await writer.drain()
        except ConnectionResetError:
            pass
        finally:
            writer.close()

    async def _route(
        self,
        method: str,
        path: str,
        writer: asyncio.StreamWriter,
    ) -> None:
        response = self._responses.build(method, path)
        if response.location is not None:
            _write_redirect(writer, response.location)
            return
        _write_response(writer, response.status_line, response.content_type, response.body)


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #


def _parse_request_line(raw: bytes) -> tuple[str, str]:
    """Parse ``METHOD /path HTTP/1.1`` from raw bytes."""
    try:
        first_line = raw.split(b"\r\n", 1)[0].decode("ascii", errors="replace")
        parts = first_line.split(" ")
        method = parts[0] if parts else "GET"
        path = parts[1].split("?")[0] if len(parts) > 1 else "/"
        return method, path
    except Exception:
        return "GET", "/"


def _write_response(
    writer: asyncio.StreamWriter,
    status_line: bytes,
    content_type: str,
    body: bytes,
) -> None:
    headers = (
        status_line
        + f"Content-Type: {content_type}\r\n".encode()
        + f"Content-Length: {len(body)}\r\n".encode()
        + b"Connection: close\r\n"
        + _CRLF
    )
    writer.write(headers + body)


def _write_redirect(writer: asyncio.StreamWriter, location: str) -> None:
    headers = (
        _HTTP_301
        + f"Location: {location}\r\n".encode()
        + b"Content-Length: 0\r\n"
        + b"Connection: close\r\n"
        + _CRLF
    )
    writer.write(headers)
