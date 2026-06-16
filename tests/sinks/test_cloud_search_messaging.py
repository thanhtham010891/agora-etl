from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from agora.sinks.http.webhook import WebhookSink, _redact_url


async def test_webhook_single_mode_posts_each_record() -> None:
    """Single mode POSTs each record immediately."""
    sink = WebhookSink(
        url="https://example.com/webhook",
        batch_mode=False,
    )

    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    sink._client = mock_client  # type: ignore[attr-defined]

    await sink.write({"id": 1})
    await sink.write({"id": 2})

    assert mock_client.post.call_count == 2


async def test_webhook_batch_mode_buffers_until_flush_every() -> None:
    """Batch mode buffers until flush_every threshold."""
    sink = WebhookSink(
        url="https://example.com/webhook",
        batch_mode=True,
        flush_every=3,
    )

    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    sink._client = mock_client  # type: ignore[attr-defined]

    await sink.write({"id": 1})
    await sink.write({"id": 2})

    assert mock_client.post.call_count == 0
    assert len(sink._buffer) == 2  # type: ignore[attr-defined]

    await sink.write({"id": 3})

    assert mock_client.post.call_count == 1


async def test_webhook_batch_mode_flush_posts_list() -> None:
    """Batch mode flush sends list of payloads."""
    sink = WebhookSink(
        url="https://example.com/webhook",
        batch_mode=True,
    )

    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    sink._client = mock_client  # type: ignore[attr-defined]

    sink._buffer = [{"id": 1}, {"id": 2}]  # type: ignore[attr-defined]

    await sink.flush()

    call_args = mock_client.post.call_args
    assert call_args[1]["json"] == [{"id": 1}, {"id": 2}]


async def test_webhook_retry_on_429() -> None:
    """429 responses trigger retry."""
    from unittest.mock import patch

    import httpx

    sink = WebhookSink(
        url="https://example.com/webhook",
        max_retries=3,
    )

    mock_client = MagicMock()
    mock_response_429 = MagicMock()
    mock_response_429.status_code = 429
    mock_response_429.raise_for_status = MagicMock(
        side_effect=httpx.HTTPStatusError("429", request=MagicMock(), response=mock_response_429)
    )

    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.raise_for_status = MagicMock()

    mock_client.post = AsyncMock(
        side_effect=[mock_response_429, mock_response_429, mock_response_200]
    )
    sink._client = mock_client  # type: ignore[attr-defined]

    with patch("agora.sinks.http.webhook.asyncio.sleep", new_callable=AsyncMock):
        await sink.write({"id": 1})

    assert mock_client.post.call_count == 3


async def test_webhook_no_retry_on_400() -> None:
    """400 responses raise immediately without retry."""
    import httpx

    sink = WebhookSink(
        url="https://example.com/webhook",
        max_retries=3,
    )

    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.raise_for_status = MagicMock(
        side_effect=httpx.HTTPStatusError("400", request=MagicMock(), response=mock_response)
    )
    mock_client.post = AsyncMock(return_value=mock_response)
    sink._client = mock_client  # type: ignore[attr-defined]

    with pytest.raises(httpx.HTTPStatusError):
        await sink.write({"id": 1})

    assert mock_client.post.call_count == 1


async def test_webhook_raises_after_max_retries() -> None:
    """Always 503 raises RuntimeError after the initial attempt plus retries."""
    from unittest.mock import patch

    import httpx

    sink = WebhookSink(
        url="https://example.com/webhook",
        max_retries=2,
    )

    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 503
    mock_response.raise_for_status = MagicMock(
        side_effect=httpx.HTTPStatusError("503", request=MagicMock(), response=mock_response)
    )
    mock_client.post = AsyncMock(return_value=mock_response)
    sink._client = mock_client  # type: ignore[attr-defined]

    with (
        patch("agora.sinks.http.webhook.asyncio.sleep", new_callable=AsyncMock),
        pytest.raises(RuntimeError, match="failed after 2 retries"),
    ):
        await sink.write({"id": 1})

    assert mock_client.post.call_count == 3


async def test_webhook_zero_retries_still_attempts_once() -> None:
    """max_retries=0 still performs the initial request once."""
    import httpx

    sink = WebhookSink(
        url="https://example.com/webhook",
        max_retries=0,
    )

    mock_client = MagicMock()
    request = httpx.Request("POST", "https://example.com/webhook")
    mock_client.post = AsyncMock(side_effect=httpx.RequestError("boom", request=request))
    sink._client = mock_client  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="failed after 0 retries"):
        await sink.write({"id": 1})

    assert mock_client.post.call_count == 1


def test_webhook_redacts_secret_bearing_url_for_diagnostics() -> None:
    safe = _redact_url("https://user:secret@example.com/hook?token=abc#fragment")

    assert safe == "https://<redacted>@example.com/hook?<redacted>"
    assert "secret" not in safe
    assert "token=abc" not in safe
    assert "fragment" not in safe


async def test_webhook_payload_fn_called() -> None:
    """Custom payload_fn is applied."""
    sink = WebhookSink(
        url="https://example.com/webhook",
        payload_fn=lambda r: {"transformed": r["value"] * 2},
    )

    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    sink._client = mock_client  # type: ignore[attr-defined]

    await sink.write({"value": 5})

    call_args = mock_client.post.call_args
    assert call_args[1]["json"] == {"transformed": 10}
