"""Shared execution-spec types for pipeline orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.core.context import PipelineContext
    from agora.core.fencing import RunFence
    from agora.core.middleware import MiddlewareChain
    from agora.core.source import BaseSource
    from agora.core.types import DeliveryConfig
    from agora.core.writer import Writer


@dataclass(slots=True)
class PipelineRuntimeSpec:
    """Immutable runtime inputs needed to execute a prepared pipeline."""

    source: BaseSource[Any]
    chain: MiddlewareChain[Any, Any]
    writer: Writer[Any]
    pipeline_id: str
    config: DeliveryConfig
    live_metrics_callback: Callable[[PipelineContext], Awaitable[None]] | None = None
    run_fence: RunFence | None = None
