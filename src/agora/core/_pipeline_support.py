"""Helpers for pipeline builder and bound-pipeline orchestration."""

from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from typing import TYPE_CHECKING, Any, TypeVar, cast

from agora.core.explain import PipelineExplain
from agora.core.runtime import build_runtime_plan
from agora.core.sink import BaseSink, SinkFanOut
from agora.core.tracing import NoopTracer
from agora.core.types import DeliveryConfig

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from agora.core.middleware import MiddlewareChain
    from agora.core.source import BaseSource
    from agora.core.writer import Writer

TResult = TypeVar("TResult")


def normalize_delivery_config(
    config: DeliveryConfig | None,
    *,
    pipeline_id: str,
) -> DeliveryConfig:
    """Return a runtime-safe delivery config with pipeline defaults applied."""
    config = config or DeliveryConfig()
    return replace(
        config,
        checkpoint_key=config.checkpoint_key or pipeline_id,
        checkpoint_every=max(config.checkpoint_every, 1),
        batch_size=max(config.batch_size, 1),
        tracer=config.tracer or NoopTracer(),
    )


def build_sink_fanout_writer(
    sinks: list[BaseSink[Any]],
    *,
    sink_concurrency: int | None,
) -> SinkFanOut[Any]:
    """Construct the canonical sink fan-out writer for a pipeline."""
    writer: SinkFanOut[Any] = SinkFanOut(sinks)
    if sink_concurrency is not None:
        writer = writer.with_concurrency(sink_concurrency)
    return writer


def explain_pipeline(
    *,
    source: BaseSource[Any],
    chain: MiddlewareChain[Any, Any],
    writer: Writer[Any],
    pipeline_id: str,
    config: DeliveryConfig,
    max_records: int | None,
) -> PipelineExplain:
    """Resolve the runtime plan for explain-mode without starting execution."""
    limited_source = source.limit(max_records) if max_records is not None else source
    plan = build_runtime_plan(
        limited_source,
        chain,
        writer,
        writer_batch_size=config.batch_size,
    )
    return PipelineExplain.from_runtime_plan(
        pipeline_id=pipeline_id,
        plan=plan,
        source_limit=max_records,
    )


def run_async_sync_bridge(coro: Coroutine[Any, Any, TResult]) -> TResult:
    """Run an async pipeline from sync code, even when a loop already exists."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is None:
        return asyncio.run(coro)

    result: object | None = None
    exc: BaseException | None = None

    def _run_in_thread() -> None:
        nonlocal result, exc
        try:
            result = asyncio.run(coro)
        except BaseException as err:
            exc = err

    thread = threading.Thread(target=_run_in_thread, daemon=True)
    thread.start()
    thread.join()

    if exc is not None:
        raise exc
    return cast("TResult", result)
