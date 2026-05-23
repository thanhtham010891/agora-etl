from __future__ import annotations

import httpx
import pytest

from agora.core.retry import RetryPolicy
from agora.sources.http.http import HTTPSource


class _TestHTTPSource(HTTPSource[dict]):
    async def fetch_batch(self):
        if False:
            yield {}


class _FakeHTTPClient:
    def __init__(self, responses: list[object]) -> None:
        self._responses = list(responses)
        self.calls = 0
        self.is_closed = False

    async def request(self, method: str, url: str, **kwargs):
        self.calls += 1
        result = self._responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def _status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://example.test/items")
    response = httpx.Response(status_code=status_code, request=request)
    return httpx.HTTPStatusError(
        f"status {status_code}",
        request=request,
        response=response,
    )


@pytest.mark.asyncio
async def test_http_source_retries_transient_status_and_succeeds() -> None:
    source = _TestHTTPSource(
        retry_policy=RetryPolicy[httpx.Response](
            max_attempts=3,
            initial_backoff_s=0.0,
            retry_exceptions=(httpx.HTTPStatusError, httpx.RequestError),
            retry_if=lambda exc: (
                isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code >= 500
            ),
        )
    )
    source._client = _FakeHTTPClient(  # type: ignore[assignment]
        [
            _status_error(503),
            httpx.Response(200, request=httpx.Request("GET", "https://example.test/items")),
        ]
    )

    response = await source._request("GET", "https://example.test/items")

    assert response.status_code == 200
    assert source._client.calls == 2  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_http_source_does_not_retry_non_transient_status() -> None:
    source = _TestHTTPSource(
        retry_policy=RetryPolicy[httpx.Response](
            max_attempts=3,
            initial_backoff_s=0.0,
            retry_exceptions=(httpx.HTTPStatusError, httpx.RequestError),
            retry_if=source_retryable_http_error,
        )
    )
    source._client = _FakeHTTPClient([_status_error(404)])  # type: ignore[assignment]

    with pytest.raises(httpx.HTTPStatusError):
        await source._request("GET", "https://example.test/items")

    assert source._client.calls == 1  # type: ignore[union-attr]


def source_retryable_http_error(exc: Exception) -> bool:
    if isinstance(exc, httpx.RequestError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code == 429 or exc.response.status_code >= 500
    return False
