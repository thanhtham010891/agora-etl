"""
agora/health/__init__.py
=========================
agora health server — lightweight asyncio HTTP server.

Endpoints:
    GET /health   — JSON health payload (status, pipeline stats)
    GET /metrics  — Prometheus-compatible text format
    GET /         — redirect to /health

Usage::

    from agora.health import HealthServer
    from agora.metrics import MetricsCollector

    collector = MetricsCollector()
    server = HealthServer(port=8080, collector=collector)

    # Run alongside WorkerPool:
    await asyncio.gather(pool.run(), server.serve())

    # Or attach to WorkerPool directly:
    pool = WorkerPool(health_port=8080)

Endpoints are served by a pure-asyncio HTTP server (no aiohttp/fastapi dependency).
"""

from agora.health.server import HealthServer

__all__ = ["HealthServer"]
