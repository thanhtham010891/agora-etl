"""Helpers for pipeline builder and bound-pipeline orchestration."""

from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from typing import TYPE_CHECKING, Any, TypeVar, cast

from agora.core.acceleration import normalize_acceleration_mode
from agora.core.explain import PipelineExplain
from agora.core.runtime import build_runtime_plan
from agora.core.sink import BaseSink, SinkFanOut
from agora.core.tracing import NoopTracer
from agora.core.types import Backpressure, DeliveryConfig
from agora.core.types._config import PerformanceProfileSettings

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from agora.core.middleware import MiddlewareChain
    from agora.core.source import BaseSource
    from agora.core.writer import Writer

TResult = TypeVar("TResult")


def normalize_performance_profile(profile: str) -> str:
    """Normalize user-facing performance profile values."""
    value = profile.strip().lower()
    if value not in {"balanced", "throughput", "low_latency"}:
        raise ValueError("performance profile must be one of: balanced, throughput, low_latency")
    return value


def normalize_delivery_config(
    config: DeliveryConfig | None,
    *,
    pipeline_id: str,
) -> DeliveryConfig:
    """Return a runtime-safe delivery config with pipeline defaults applied."""
    config = config or DeliveryConfig()
    normalized = replace(
        config,
        acceleration_mode=normalize_acceleration_mode(config.acceleration_mode),
        performance_profile=normalize_performance_profile(config.performance_profile),
        checkpoint_key=config.checkpoint_key or pipeline_id,
        checkpoint_every=max(config.checkpoint_every, 1),
        batch_size=max(config.batch_size, 1),
        tracer=config.tracer or NoopTracer(),
    )
    return apply_performance_profile_defaults(normalized)


def apply_performance_profile_defaults(config: DeliveryConfig) -> DeliveryConfig:
    """Apply deterministic profile defaults while preserving explicit overrides."""
    profile = normalize_performance_profile(config.performance_profile)
    if profile == "throughput":
        return replace(
            config,
            batch_size=config.batch_size if config.batch_size > 1 else 1_000,
            batch_flush_interval_ms=(
                config.batch_flush_interval_ms
                if config.batch_flush_interval_ms is not None
                else 100
            ),
            max_buffer_size=config.max_buffer_size if config.max_buffer_size is not None else 1_024,
            backpressure=config.backpressure
            or Backpressure.adaptive(
                min_buffer_size=1,
                max_buffer_size=4_096,
                scale_up_step=64,
                scale_down_step=64,
                writer_slow_ms=50.0,
                checkpoint_slow_ms=25.0,
            ),
        )
    if profile == "low_latency":
        return replace(
            config,
            batch_size=max(1, config.batch_size),
            batch_flush_interval_ms=(
                config.batch_flush_interval_ms if config.batch_flush_interval_ms is not None else 10
            ),
            max_buffer_size=config.max_buffer_size if config.max_buffer_size is not None else 32,
            backpressure=config.backpressure
            or Backpressure.adaptive(
                min_buffer_size=1,
                max_buffer_size=64,
                scale_up_step=1,
                scale_down_step=1,
                writer_slow_ms=10.0,
                checkpoint_slow_ms=5.0,
            ),
        )
    return config


def resolved_performance_profile_settings(
    config: DeliveryConfig,
    *,
    source_prefetch_limit: int | None = None,
) -> PerformanceProfileSettings:
    """Return the concrete knobs represented by the normalized profile."""
    bp = config.backpressure
    return PerformanceProfileSettings(
        profile=normalize_performance_profile(config.performance_profile),
        writer_batch_size=max(config.batch_size, 1),
        flush_cadence_ms=config.batch_flush_interval_ms,
        prefetch_limit=source_prefetch_limit,
        max_in_flight_batches=config.max_buffer_size,
        backpressure_min_buffer_size=bp.min_buffer_size if bp is not None else None,
        backpressure_max_buffer_size=bp.max_buffer_size if bp is not None else None,
        backpressure_writer_slow_ms=bp.writer_slow_ms if bp is not None else None,
        backpressure_checkpoint_slow_ms=bp.checkpoint_slow_ms if bp is not None else None,
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
        config=config,
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
