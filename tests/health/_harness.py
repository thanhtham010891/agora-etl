from __future__ import annotations

import asyncio

from agora.health.server import HealthServer


class _MemoryStreamWriter:
    def __init__(self) -> None:
        self.buffer = bytearray()
        self.closed = False

    def write(self, data: bytes) -> None:
        self.buffer.extend(data)

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


async def send_request(
    request_bytes: bytes,
    *,
    auth_token: str | None = None,
    collector=None,
) -> str:
    """Exercise HealthServer._handle_connection without opening a real socket."""
    server = HealthServer(port=0, auth_token=auth_token, collector=collector)
    reader = asyncio.StreamReader()
    reader.feed_data(request_bytes)
    reader.feed_eof()
    writer = _MemoryStreamWriter()

    await server._handle_connection(reader, writer)

    return bytes(writer.buffer).decode("ascii", errors="replace")


def status_line(response: str) -> str:
    return response.split("\r\n")[0]


def body(response: str) -> str:
    sep = response.find("\r\n\r\n")
    return response[sep + 4 :] if sep != -1 else ""


def headers(response: str) -> dict[str, str]:
    lines = response.split("\r\n")
    parsed: dict[str, str] = {}
    for line in lines[1:]:
        if not line:
            break
        key, _sep, value = line.partition(":")
        parsed[key.lower()] = value.strip()
    return parsed
