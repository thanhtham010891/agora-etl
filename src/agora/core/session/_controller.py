"""Pipeline lifecycle controller facade."""

from __future__ import annotations

from typing import TYPE_CHECKING

from agora.core.runtime._writer_transport import WriterTransport
from agora.core.session._support import (
    create_run_state,
    make_delivery_engine,
    open_runtime_sinks,
    restore_checkpoint,
    shutdown_runtime_components,
)

if TYPE_CHECKING:
    from agora.core._executor_types import PipelineRuntimeSpec
    from agora.core.context import PipelineContext
    from agora.core.runtime import DeliveryEngine
    from agora.core.session._state import PipelineRunState


class PipelineLifecycleController:
    """Own runtime lifecycle sequencing for a prepared pipeline."""

    def __init__(self, spec: PipelineRuntimeSpec) -> None:
        self._spec = spec
        self._transport: WriterTransport | None = None

    def create_run_state(
        self,
        *,
        run_id: str | None,
        source_limit: int | None,
    ) -> PipelineRunState:
        return create_run_state(
            self._spec,
            run_id=run_id,
            source_limit=source_limit,
        )

    def make_delivery_coordinator(self) -> DeliveryEngine:
        self._transport = WriterTransport(writer=self._spec.writer)
        return make_delivery_engine(self._spec, transport=self._transport)

    async def restore_checkpoint(self, ctx: PipelineContext) -> None:
        await restore_checkpoint(self._spec, ctx)

    async def start_runtime(self, state: PipelineRunState) -> None:
        await self._spec.chain.start_all(state.ctx)
        state.middlewares_started = True
        state.writer_opened, state.dlq_opened = await open_runtime_sinks(self._spec, state.ctx)

    async def shutdown_runtime(self, state: PipelineRunState) -> Exception | None:
        return await shutdown_runtime_components(
            self._spec,
            state,
            transport=self._transport,
        )
