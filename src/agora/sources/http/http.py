"""
agora/sources/http.py
=====================
``HTTPSource[T]`` — abstract async HTTP polling source.

agora owns: client lifecycle, rate-limiting, retries, circuit breaker,
            optional response caching.
Implementer owns: URL construction, request payloads, response parsing.

Example::

    class PlacesExtractor(HTTPSource[RawRecord]):
        source_name = "places_api"

        async def fetch_batch(self) -> AsyncIterator[RawRecord]:
            resp = await self.post("/search", json={"query": "coffee"})
            for item in resp.json()["results"]:
                yield RawRecord(source="places_api", data=item)

With circuit breaker + cache::

    class MyExtractor(HTTPSource[T]):
        def __init__(self):
            super().__init__(
                base_url="https://api.example.com",
                requests_per_second=2.0,
                circuit_breaker=CircuitBreakerConfig(failure_threshold=5),
                cache_ttl_seconds=3600,  # cache responses for 1h
            )
"""

from __future__ import annotations

import asyncio
import time
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct

from agora.core.retry import RetryPolicy, retry_async
from agora.core.source import BaseSource
from agora.sources._internal.circuit_breaker import (
    AsyncCircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpen,
)

try:
    import httpx

    _HTTPX_AVAILABLE = True
except ImportError:
    _HTTPX_AVAILABLE = False

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from agora.sources._internal.cache import HttpCache

T = TypeVar("T")
logger = logstruct.getLogger(__name__)


class HTTPSource(BaseSource[T], Generic[T]):
    """Abstract async HTTP source with built-in resilience.

    agora manages the httpx client lifecycle, rate-limiting, retries,
    circuit breaking, and response caching.

    Implementers override ``fetch_batch()`` only.

    Parameters
    ----------
    base_url:
        Base URL prepended to all request paths.
    requests_per_second:
        Rate limit — max requests per second (default: 1.0).
    timeout:
        Per-request timeout in seconds (default: 30).
    max_retries:
        Retry attempts on transient errors (default: 3).
    headers:
        Default headers merged into every request.
    circuit_breaker:
        ``CircuitBreakerConfig`` to enable circuit breaking.
        Pass ``None`` (default) to disable.
    cache_ttl_seconds:
        Cache HTTP responses in SQLite for this many seconds.
        Pass ``None`` (default) to disable caching.
    cache_path:
        Path to the SQLite cache file.
        Defaults to ``.cache/agora_http.db``.
    """

    source_name: str = "http"

    def __init__(
        self,
        base_url: str = "",
        requests_per_second: float = 1.0,
        timeout: int = 30,
        max_retries: int = 3,
        retry_policy: RetryPolicy[httpx.Response] | None = None,
        headers: dict[str, str] | None = None,
        circuit_breaker: CircuitBreakerConfig | None = None,
        cache_ttl_seconds: int | None = None,
        cache_path: Path | None = None,
        **client_kwargs: Any,
    ) -> None:
        if not _HTTPX_AVAILABLE:
            raise ImportError(
                "httpx is required for HTTPSource. Install it: pip install 'agora-core'"
            )
        self._base_url = base_url
        self._rps = requests_per_second
        self._min_interval = 1.0 / max(requests_per_second, 0.001)
        self._timeout = timeout
        self._max_retries = max_retries
        self._retry_policy = retry_policy or RetryPolicy[httpx.Response](
            max_attempts=max(max_retries, 1),
            initial_backoff_s=1.0,
            backoff_multiplier=2.0,
            max_backoff_s=30.0,
            retry_exceptions=(httpx.HTTPStatusError, httpx.RequestError),
            retry_if=self._should_retry_exception,
        )
        self._headers = headers or {}
        self._client_kwargs = client_kwargs
        self._client: httpx.AsyncClient | None = None
        self._last_request_at: float = 0.0

        # Circuit breaker (optional)
        self._circuit_breaker: AsyncCircuitBreaker | None = (
            AsyncCircuitBreaker(
                name=self.source_name,
                config=circuit_breaker,
            )
            if circuit_breaker is not None
            else None
        )

        # HTTP cache (optional)
        self._cache: HttpCache | None = None
        self._cache_ttl = cache_ttl_seconds
        self._cache_path = cache_path

    # ------------------------------------------------------------------ #
    # Abstract interface — implementer fills this in                       #
    # ------------------------------------------------------------------ #

    @abstractmethod
    async def fetch_batch(self) -> AsyncIterator[T]:
        """Yield one batch of records from the remote API.

        agora calls this in a loop until ``StopFetching`` is raised.
        Use ``self.get()`` / ``self.post()`` — they apply rate-limiting,
        retries, circuit breaking, and caching automatically.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------ #
    # HTTP helpers — rate-limited, retried, circuit-broken, cached         #
    # ------------------------------------------------------------------ #

    async def get(
        self,
        url: str,
        params: dict | None = None,
        headers: dict | None = None,
        use_cache: bool = True,
        **kwargs: Any,
    ) -> httpx.Response:
        """Rate-limited GET with optional caching."""
        cache_url = str(self.client.build_request("GET", url, params=params).url)
        cache_headers = {**self._headers, **(headers or {})}
        if use_cache and self._cache is not None and self._cache_ttl:
            cached = await asyncio.to_thread(
                self._cache.get,
                cache_url,
                None,
                cache_headers,
                self.source_name,
            )
            if cached is not None:
                logger.debug("http_cache_hit", url=url)
                return _CachedResponse(cached)  # type: ignore[return-value]
        resp = await self._request("GET", url, params=params, headers=headers, **kwargs)
        if use_cache and self._cache is not None:
            await asyncio.to_thread(
                self._cache.set,
                cache_url,
                resp.text,
                None,
                cache_headers,
                self.source_name,
            )
        return resp

    async def post(
        self,
        url: str,
        json: Any = None,
        headers: dict | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Rate-limited POST with retries (no caching — POST is not idempotent)."""
        return await self._request("POST", url, json=json, headers=headers, **kwargs)

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Rate-limited, retried, circuit-breaker-protected HTTP request."""
        await self._throttle()

        async def _do_request() -> httpx.Response:
            resp = await self.client.request(method, url, **kwargs)
            resp.raise_for_status()
            return resp

        async def _log_retry(attempt: int, exc: Exception, delay: float) -> None:
            status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
            logger.warning(
                "http_source_retry",
                method=method,
                url=url,
                status=status,
                attempt=attempt,
                wait_s=delay,
                error=str(exc),
            )

        if self._circuit_breaker is not None:
            try:
                return await self._circuit_breaker.call(
                    lambda: retry_async(_do_request, policy=self._retry_policy, on_retry=_log_retry)
                )
            except CircuitBreakerOpen as exc:
                logger.exception(
                    "http_source_circuit_open",
                    name=exc.name,
                    retry_in=exc.retry_in,
                )
                raise
        return await retry_async(_do_request, policy=self._retry_policy, on_retry=_log_retry)

    def _should_retry_exception(self, exc: Exception) -> bool:
        if isinstance(exc, httpx.RequestError):
            return True
        if isinstance(exc, httpx.HTTPStatusError):
            status = exc.response.status_code
            return status == 429 or status >= 500
        return False

    async def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)
        self._last_request_at = time.monotonic()

    # ------------------------------------------------------------------ #
    # Client lifecycle                                                     #
    # ------------------------------------------------------------------ #

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=self._timeout,
                headers=self._headers,
                follow_redirects=True,
                **self._client_kwargs,
            )
        return self._client

    async def open(self) -> None:
        _ = self.client  # eagerly create
        if self._cache_ttl is not None:
            from agora.sources._internal.cache import HttpCache

            self._cache = HttpCache(db_path=self._cache_path, ttl=self._cache_ttl)

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None
        if self._cache is not None:
            await asyncio.to_thread(self._cache.close)
            self._cache = None

    # ------------------------------------------------------------------ #
    # Stream — agora pipeline calls this                                   #
    # ------------------------------------------------------------------ #

    async def stream(self):  # type: ignore[override]
        """Drive fetch_batch() in a loop until StopFetching or _STOP_SENTINEL is raised."""
        while True:
            try:
                async for record in self.fetch_batch():
                    yield record
            except StopFetching:
                break


# ------------------------------------------------------------------ #
# Sentinel exceptions                                                  #
# ------------------------------------------------------------------ #


class StopFetching(Exception):  # noqa: N818
    """Raise inside ``fetch_batch()`` to signal end of pagination / data.

    Prefer raising ``StopFetching`` over returning an empty iterator so
    that the intent is explicit and the loop terminates immediately.
    """


# ------------------------------------------------------------------ #
# Internal helpers                                                     #
# ------------------------------------------------------------------ #


class _CachedResponse:
    """Minimal httpx.Response-like object returned from the HTTP cache.

    Only implements ``.text``, ``.json()``, and ``.raise_for_status()``.
    Accessing any other ``httpx.Response`` attribute (e.g. ``.headers``,
    ``.status_code``, ``.content``) will raise ``AttributeError``.
    If your ``fetch_batch()`` uses those attributes, disable caching for
    that request by passing ``use_cache=False`` to ``self.get()``.
    """

    def __init__(self, text: str) -> None:
        self._text = text

    @property
    def text(self) -> str:
        return self._text

    def json(self) -> Any:
        import json

        return json.loads(self._text)

    def raise_for_status(self) -> None:
        pass  # cached responses are always "ok"
