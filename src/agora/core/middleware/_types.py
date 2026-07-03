"""Shared middleware result and planning types."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol, TypeGuard, TypeVar, runtime_checkable

from agora.core.data_plane import DataPlane

if TYPE_CHECKING:
    import asyncio

    from agora.core.context import PipelineContext

T_contra = TypeVar("T_contra", contravariant=True)
U_co = TypeVar("U_co", covariant=True)


@dataclass(frozen=True, slots=True)
class MiddlewareFailure:
    """Structured middleware failure surfaced to the runtime."""

    stage: str
    record: Any
    middleware: str
    exception: Exception


@dataclass(frozen=True, slots=True)
class MiddlewareProcessResult:
    """Outcome of processing a record through some or all middlewares."""

    value: Any | None
    failure: MiddlewareFailure | None = None


@dataclass(frozen=True, slots=True)
class PipelinedBatchStageSpec:
    """Runtime-selected batch stage that can submit whole batches concurrently."""

    index: int
    middleware: PipelinedBatchMiddleware
    name: str
    max_in_flight: int
    ordered: bool
    arrow_stage: bool


class MiddlewareDataPlane(StrEnum):
    """Logical data plane flowing between middleware stages."""

    PYTHON_ROWS = DataPlane.PYTHON_ROWS.value
    ARROW_BATCHES = DataPlane.ARROW_BATCHES.value


@dataclass(frozen=True, slots=True)
class MiddlewareModeSpec:
    """One row in the middleware compatibility matrix."""

    index: int
    name: str
    data_plane: MiddlewareDataPlane


@runtime_checkable
class PipeableMiddleware(Protocol[T_contra, U_co]):
    """Middleware shape accepted by ``Pipeline.pipe()`` and ``MiddlewareChain``."""

    name: str

    async def process(self, record: T_contra, ctx: PipelineContext) -> U_co | None:
        """Transform one record and optionally drop it."""

    async def on_start(self, ctx: PipelineContext) -> None:
        """Prepare middleware state before the run begins."""

    async def on_stop(self, ctx: PipelineContext) -> None:
        """Release middleware state when the run ends."""

    async def on_error(
        self,
        record: T_contra,
        exc: Exception,
        ctx: PipelineContext,
    ) -> None:
        """Handle a processing failure for one record."""

    async def apply_in_batch(
        self,
        current: list[Any],
        ctx: PipelineContext,
        chain: Any,
        idx: int,
    ) -> Any:
        """Apply the middleware inside batch execution."""


@runtime_checkable
class BufferedSubmitMiddleware(PipeableMiddleware[Any, Any], Protocol):
    """Middleware that can submit record work for buffered execution."""

    name: str
    min_concurrency: int

    async def submit(self, record: Any, ctx: PipelineContext) -> asyncio.Future[Any | None]:
        """Queue a record and return a future for the eventual result."""


@runtime_checkable
class DrainableBufferedMiddleware(BufferedSubmitMiddleware, Protocol):
    """Buffered middleware that can flush pending records before shutdown."""

    async def drain_pending(self, ctx: PipelineContext | None = None) -> None:
        """Flush any buffered records waiting to be processed."""


@runtime_checkable
class PipelinedBatchMiddleware(PipeableMiddleware[Any, Any], Protocol):
    """Batch middleware that can submit batches concurrently."""

    name: str
    batch_in_flight_limit: int
    ordered_batch_commits: bool

    async def submit_batch(self, batch: Any, ctx: PipelineContext) -> asyncio.Task[Any]:
        """Queue a batch and return a task for the eventual batch result."""


@runtime_checkable
class DrainablePipelinedBatchMiddleware(PipelinedBatchMiddleware, Protocol):
    """Pipelined batch middleware that can flush batch-local buffers."""

    async def drain_pending_batches(self, ctx: PipelineContext) -> None:
        """Flush any batch-local pending buffers before shutdown."""


def is_buffered_submit_middleware(
    middleware: object,
) -> TypeGuard[BufferedSubmitMiddleware]:
    """Return True when *middleware* supports buffered submit() execution."""
    min_concurrency = getattr(middleware, "min_concurrency", 1)
    return callable(getattr(middleware, "submit", None)) and isinstance(min_concurrency, int)


def is_drainable_buffered_middleware(
    middleware: object,
) -> TypeGuard[DrainableBufferedMiddleware]:
    """Return True when *middleware* exposes drain_pending()."""
    return is_buffered_submit_middleware(middleware) and callable(
        getattr(middleware, "drain_pending", None)
    )


def is_pipelined_batch_middleware(
    middleware: object,
) -> TypeGuard[PipelinedBatchMiddleware]:
    """Return True when *middleware* supports submit_batch() execution."""
    batch_in_flight_limit = getattr(middleware, "batch_in_flight_limit", 1)
    return callable(getattr(middleware, "submit_batch", None)) and isinstance(
        batch_in_flight_limit, int
    )


def is_drainable_pipelined_batch_middleware(
    middleware: object,
) -> TypeGuard[DrainablePipelinedBatchMiddleware]:
    """Return True when *middleware* exposes drain_pending_batches()."""
    return is_pipelined_batch_middleware(middleware) and callable(
        getattr(middleware, "drain_pending_batches", None)
    )
