"""Runtime planning primitives for Agora pipeline execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from agora.core.batch import is_arrow_native_sink, is_batch_capable_source
from agora.core.source import DeliveryHookSource

if TYPE_CHECKING:
    from agora.core.middleware import MiddlewareChain
    from agora.core.source import BaseSource
    from agora.core.writer import Writer


class RuntimeLane(StrEnum):
    """Execution lane selected for a prepared pipeline run."""

    LINEAR = "linear"
    BUFFERED = "buffered"
    BATCH = "batch"


@dataclass(frozen=True, slots=True)
class BufferedStageSpec:
    """Runtime-selected buffered middleware stage."""

    index: int
    middleware: Any
    name: str
    concurrency: int


@dataclass(frozen=True, slots=True)
class WriterExecutionPlan:
    """Derived write-path decisions for a single runtime plan."""

    batch_size: int
    direct_flush_eligible: bool = False
    arrow_fast_path: bool = False
    arrow_chain: bool = False


@dataclass(frozen=True, slots=True)
class RuntimePlan:
    """Immutable execution plan built once before pipeline run starts."""

    lane: RuntimeLane
    source_name: str
    batch_source: bool
    has_delivery_hooks: bool
    buffered_stages: tuple[BufferedStageSpec, ...] = field(default_factory=tuple)
    writer: WriterExecutionPlan = field(default_factory=lambda: WriterExecutionPlan(batch_size=1))

    @property
    def uses_buffered_lane(self) -> bool:
        return self.lane == RuntimeLane.BUFFERED

    @property
    def uses_batch_lane(self) -> bool:
        return self.lane == RuntimeLane.BATCH


def _buffered_stage_specs(chain: MiddlewareChain[Any, Any]) -> tuple[BufferedStageSpec, ...]:
    # Only middleware that asks for concurrency > 1 justifies the buffered lane.
    # A submit()-capable stage with min_concurrency == 1 gains nothing from
    # per-record task orchestration, so it runs on the linear lane via process().
    return tuple(
        BufferedStageSpec(
            index=index,
            middleware=middleware,
            name=getattr(middleware, "name", "buffered"),
            concurrency=concurrency,
        )
        for index, middleware in chain.buffered_stages()
        if (concurrency := max(1, getattr(middleware, "min_concurrency", 1))) > 1
    )


def _direct_flush_eligible(
    source: BaseSource[Any],
    writer: Writer[Any],
    writer_batch_size: int,
) -> bool:
    if writer_batch_size <= 1:
        return False
    writer_caps = getattr(writer, "_sink_batch_writable", None)
    return bool(writer_caps is not None and len(writer_caps) == 1 and writer_caps[0])


def _arrow_fast_path_selected(
    source: BaseSource[Any],
    chain: MiddlewareChain[Any, Any],
    writer: Writer[Any],
) -> bool:
    # The arrow fast path requires a source that emits pa.RecordBatch (not list[dict]).
    if not getattr(source, "emits_arrow_batches", False):
        return False
    # Allow the path when the chain is empty OR every stage is Arrow-native.
    # A mixed chain (any regular Middleware/BatchMiddleware) falls back to to_pylist().
    if not (chain.middleware_count() == 0 or chain.has_only_arrow_batch_stages()):
        return False
    if is_arrow_native_sink(writer):
        return True
    inner_sinks = getattr(writer, "_sinks", None)
    return bool(inner_sinks and len(inner_sinks) == 1 and is_arrow_native_sink(inner_sinks[0]))


def build_runtime_plan(
    source: BaseSource[Any],
    chain: MiddlewareChain[Any, Any],
    writer: Writer[Any],
    *,
    writer_batch_size: int,
) -> RuntimePlan:
    """Build the immutable runtime plan for a prepared pipeline."""

    buffered_stages = _buffered_stage_specs(chain)
    batch_source = is_batch_capable_source(source)
    has_delivery_hooks = isinstance(source, DeliveryHookSource)

    if batch_source:
        lane = RuntimeLane.BATCH
    elif buffered_stages:
        lane = RuntimeLane.BUFFERED
    else:
        lane = RuntimeLane.LINEAR

    arrow_fast_path = batch_source and _arrow_fast_path_selected(source, chain, writer)
    writer_plan = WriterExecutionPlan(
        batch_size=max(writer_batch_size, 1),
        direct_flush_eligible=_direct_flush_eligible(source, writer, writer_batch_size),
        arrow_fast_path=arrow_fast_path,
        arrow_chain=arrow_fast_path and chain.has_only_arrow_batch_stages(),
    )
    return RuntimePlan(
        lane=lane,
        source_name=source.source_name,
        batch_source=batch_source,
        has_delivery_hooks=has_delivery_hooks,
        buffered_stages=buffered_stages,
        writer=writer_plan,
    )


__all__ = [
    "BufferedStageSpec",
    "RuntimeLane",
    "RuntimePlan",
    "WriterExecutionPlan",
    "build_runtime_plan",
]
