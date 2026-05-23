"""agora/sources/file/base.py — abstract FileSource."""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Generic, TypeVar

from agora.core.source import BaseSource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

T = TypeVar("T")


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
    async def read_records(self) -> AsyncIterator[T]:
        """Yield records from the file."""
        raise NotImplementedError

    async def stream(self) -> AsyncIterator[T]:
        async for record in self.read_records():
            yield record
