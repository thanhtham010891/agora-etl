"""Base middleware contract for Agora."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from agora.core.context import PipelineContext

T = TypeVar("T")
U = TypeVar("U")


class Middleware(ABC, Generic[T, U]):
    """Abstract async middleware."""

    name: str = "middleware"

    @abstractmethod
    async def process(self, record: T, ctx: PipelineContext) -> U | None:
        """Transform *record*. Return ``None`` to drop it."""

    async def on_start(self, ctx: PipelineContext) -> None:
        """Called once before the pipeline loop starts."""

    async def on_stop(self, ctx: PipelineContext) -> None:
        """Called once after the pipeline loop ends (even on error)."""

    async def on_error(
        self,
        record: T,
        exc: Exception,
        ctx: PipelineContext,
    ) -> None:
        """Called when ``process()`` raises. Default: log and continue."""
        ctx.log.exception(
            "middleware_error",
            middleware=self.name,
            error=str(exc),
        )

    async def apply_in_batch(
        self,
        current: list[Any],
        ctx: PipelineContext,
        chain: Any,
        idx: int,
    ) -> Any:
        """Apply this stage per-record within a batch context by default."""
        next_batch: list[Any] = []
        for record in current:
            if record is None:
                next_batch.append(None)
                continue
            result = await chain.process_range(idx, idx + 1, record, ctx)
            if result.failure is not None:
                next_batch.append(None)
            else:
                next_batch.append(result.value)
        return next_batch
