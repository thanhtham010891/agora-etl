"""Structural writer protocol used by pipeline output orchestration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, TypeVar, runtime_checkable

if TYPE_CHECKING:
    from agora.core.writer._result import WriteResult

T = TypeVar("T")


@runtime_checkable
class Writer(Protocol[T]):
    """Unified write interface for pipeline sinks."""

    async def open(self) -> None: ...

    async def write(self, record: T) -> WriteResult: ...

    async def write_batch(self, records: list[T]) -> list[WriteResult]: ...

    async def flush(self) -> None: ...

    async def close(self) -> None: ...
