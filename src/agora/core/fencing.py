"""Run fencing primitives for distributed pipeline execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from agora.core.errors import PipelineError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.core.context import PipelineContext

RUN_FENCE_CONTEXT_KEY = "agora.run_fence"


class FenceLostError(PipelineError):
    """Raised when a worker tries to write after losing its run fence."""

    def with_context(
        self,
        *,
        pipeline_id: str | None = None,
        run_id: str | None = None,
        stage: str | None = None,
        source_name: str | None = None,
        sink_name: str | None = None,
        checkpoint: Any | None = None,
    ) -> FenceLostError:
        message = str(self.args[0]) if self.args else str(self)
        return FenceLostError(
            message,
            pipeline_id=pipeline_id if pipeline_id is not None else self.pipeline_id,
            run_id=run_id if run_id is not None else self.run_id,
            stage=stage if stage is not None else self.stage,
            source_name=source_name if source_name is not None else self.source_name,
            sink_name=sink_name if sink_name is not None else self.sink_name,
            checkpoint=checkpoint if checkpoint is not None else self.checkpoint,
        )


@dataclass(frozen=True, slots=True)
class RunFence:
    """Run-scoped fencing token and backend validator."""

    pipeline_id: str
    worker_id: str
    fencing_token: int
    validate: Callable[[], Awaitable[bool]]

    async def assert_active(self) -> None:
        if await self.validate():
            return
        raise FenceLostError(
            "Pipeline run lost its distributed lease fence before a side effect.",
            pipeline_id=self.pipeline_id,
            stage="fencing",
        )


def bind_run_fence(ctx: PipelineContext, fence: RunFence | None) -> None:
    """Attach a run fence to *ctx* when distributed execution provides one."""
    if fence is not None:
        ctx.set(RUN_FENCE_CONTEXT_KEY, fence)


def get_run_fence(ctx: PipelineContext) -> RunFence | None:
    """Return the run fence attached to *ctx*, if any."""
    get_value = getattr(ctx, "get", None)
    if callable(get_value):
        value = get_value(RUN_FENCE_CONTEXT_KEY)
    else:
        extras = getattr(ctx, "extras", None)
        value = extras.get(RUN_FENCE_CONTEXT_KEY) if isinstance(extras, dict) else None
    return value if isinstance(value, RunFence) else None


async def assert_run_fence_active(ctx: PipelineContext) -> None:
    """Fail if the current run has lost its distributed write fence."""
    fence = get_run_fence(ctx)
    if fence is not None:
        await fence.assert_active()


__all__ = [
    "RUN_FENCE_CONTEXT_KEY",
    "FenceLostError",
    "RunFence",
    "assert_run_fence_active",
    "bind_run_fence",
    "get_run_fence",
]
