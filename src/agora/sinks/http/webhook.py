"""
agora/sinks/webhook.py
======================
``WebhookSink[T]`` — HTTP POST each record to a webhook endpoint.

Re-uses ``httpx`` from ``agora-etl` — no new dependency needed.

Typical use-cases: Slack / Discord alerts, n8n / Make automations,
custom downstream APIs that receive data via webhook.

Usage::

    sink = WebhookSink(
        url="https://hooks.slack.com/services/...",
        payload_fn=lambda r: {"text": f"New place: {r.name}"},
        max_retries=3,
    )

    # Batch mode — POST a list of records in one request:
    sink = WebhookSink(
        url="https://api.example.com/ingest",
        payload_fn=lambda r: r.model_dump(),
        batch_mode=True,
        flush_every=50,
    )
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct

from agora.core.data_plane import DataPlane
from agora.core.sink import BaseSink, SinkCapabilities

if TYPE_CHECKING:
    from collections.abc import Callable

try:
    import httpx

    _HTTPX_AVAILABLE = True
except ImportError:
    _HTTPX_AVAILABLE = False

T = TypeVar("T")
logger = logstruct.getLogger(__name__)


class WebhookSink(BaseSink[T], Generic[T]):
    """POST each record (or batch) to an HTTP webhook endpoint.

    Parameters
    ----------
    url:
        Webhook URL to POST to.
    payload_fn:
        ``(record: T) -> Any`` — converts a record to a JSON-serializable
        payload.  In batch mode this is called per record; the list of
        payloads is then sent as a JSON array.
    headers:
        Extra HTTP headers (e.g. ``{"Authorization": "Bearer ..."}``)
        merged with ``Content-Type: application/json``.
    max_retries:
        Retry attempts on 429 / 5xx responses (default: 3).
    timeout:
        Per-request timeout in seconds (default: 30).
    batch_mode:
        If ``True``, buffer records and POST them as a JSON array.
    flush_every:
        (batch_mode only) Flush after this many records (default: 50).
    """

    sink_name = "webhook"

    def __init__(
        self,
        url: str,
        payload_fn: Callable[[T], Any] | None = None,
        headers: dict[str, str] | None = None,
        max_retries: int = 3,
        timeout: int = 30,
        batch_mode: bool = False,
        flush_every: int = 50,
    ) -> None:
        if not _HTTPX_AVAILABLE:
            raise ImportError("WebhookSink requires httpx. Install via: pip install agora-etl")
        self._url = url
        self._payload_fn = payload_fn or _default_payload
        self._headers = {"Content-Type": "application/json", **(headers or {})}
        self._max_retries = max_retries
        self._timeout = timeout
        self._batch_mode = batch_mode
        self._flush_every = flush_every
        self._buffer: list[T] = []
        self._client: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def open(self) -> None:
        self._client = httpx.AsyncClient(
            headers=self._headers,
            timeout=self._timeout,
            follow_redirects=True,
        )

    def sink_capabilities(self) -> SinkCapabilities:
        native_planes: tuple[DataPlane, ...]
        if self._batch_mode:
            native_planes = (
                DataPlane.PYTHON_ROWS,
                DataPlane.PYTHON_BATCHES,
            )
        else:
            native_planes = (DataPlane.PYTHON_ROWS,)
        return SinkCapabilities(
            accepted_data_planes=native_planes,
            native_data_planes=native_planes,
            parallel_writes_safe=False,
            ordered_writes_required=True,
        )

    async def close(self) -> None:
        await self.flush()
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------ #
    # Write                                                                #
    # ------------------------------------------------------------------ #

    async def write(self, record: T) -> None:
        if self._batch_mode:
            self._buffer.append(record)
            if len(self._buffer) >= self._flush_every:
                await self.flush()
        else:
            payload = self._payload_fn(record)
            await self._post_with_retry(payload)

    async def write_batch(self, records: list[T]) -> None:
        if self._batch_mode:
            self._buffer.extend(records)
            if len(self._buffer) >= self._flush_every:
                await self.flush()
        else:
            for record in records:
                await self.write(record)

    async def flush(self) -> None:
        if not self._buffer:
            return
        batch = list(self._buffer)
        payloads = [self._payload_fn(r) for r in batch]
        await self._post_with_retry(payloads)
        del self._buffer[: len(batch)]

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    async def _post_with_retry(self, payload: Any) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                headers=self._headers,
                timeout=self._timeout,
                follow_redirects=True,
            )
        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                resp = await self._client.post(self._url, json=payload)
                resp.raise_for_status()
                logger.debug(
                    "webhook_sink_posted",
                    url=self._url,
                    status=resp.status_code,
                )
                return
            except httpx.HTTPStatusError as exc:
                last_exc = exc
                status = exc.response.status_code
                if status == 429 or status >= 500:
                    wait = 2**attempt
                    logger.warning(
                        "webhook_sink_retry",
                        url=self._url,
                        status=status,
                        attempt=attempt,
                        wait_s=wait,
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.exception(
                        "webhook_sink_error",
                        url=self._url,
                        status=status,
                        error=str(exc),
                    )
                    raise
            except httpx.RequestError as exc:
                last_exc = exc
                logger.warning(
                    "webhook_sink_request_error",
                    url=self._url,
                    error=str(exc),
                    attempt=attempt,
                )
                await asyncio.sleep(attempt)
        raise RuntimeError(
            f"WebhookSink POST {self._url} failed after {self._max_retries} retries"
        ) from last_exc


def _default_payload(record: Any) -> Any:
    if hasattr(record, "model_dump"):
        return record.model_dump()
    if hasattr(record, "__dict__"):
        return record.__dict__
    return record
