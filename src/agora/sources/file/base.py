"""agora/sources/file/base.py — abstract FileSource."""

from __future__ import annotations

import queue
from abc import abstractmethod
from typing import TYPE_CHECKING, Generic, TypeVar

from agora.core.source import BaseSource

if TYPE_CHECKING:
    import threading
    from collections.abc import AsyncIterator

T = TypeVar("T")
_QUEUE_PUT_POLL_TIMEOUT_S = 0.05


class FileSource(BaseSource[T], Generic[T]):
    """Abstract base for file-based sources.

    Implementers override ``read_records()`` — an async generator that
    yields records from a file.  agora handles the stream() contract.

    File I/O should use ``asyncio.to_thread()`` for blocking calls.
    """

    source_name: str = "file"
    supports_prefetch: bool = True
    prefetch_limit: int = 2
    supports_checkpoint: bool = True

    @abstractmethod
    def read_records(self) -> AsyncIterator[T]:
        """Yield records from the file."""
        raise NotImplementedError

    async def stream(self) -> AsyncIterator[T]:  # type: ignore[override]
        async for record in self.read_records():
            yield record

    @staticmethod
    def _queue_put_until_stopped(
        batch_queue: queue.Queue[object],
        item: object,
        stop_event: threading.Event,
    ) -> bool:
        """Put *item* onto a bounded queue unless shutdown was requested.

        File-backed sources use a producer thread plus a bounded queue so they
        can stream records without loading entire files into memory. When the
        async consumer stops early, the producer must be able to observe that
        shutdown request instead of blocking forever on ``queue.put()``.
        """
        while not stop_event.is_set():
            try:
                batch_queue.put(item, timeout=_QUEUE_PUT_POLL_TIMEOUT_S)
                return True
            except queue.Full:
                continue
        return False
