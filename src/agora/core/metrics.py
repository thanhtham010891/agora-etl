"""
agora/core/metrics.py
=====================
Pipeline observability — metrics collected automatically at every stage.

``PipelineMetrics`` is mutated in-place as the pipeline runs.
``PipelineRunSummary`` is an immutable snapshot returned at the end.

AI metrics (``AIMiddlewareMetrics``, ``AIMetrics``) are populated only
when AI middlewares are present — zero-cost for non-AI pipelines.
"""

from __future__ import annotations

import contextlib
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agora.core.checkpoint import Checkpoint
    from agora.schema.metrics import SchemaMetrics

# ======================================================================
# AI-specific per-middleware metrics
# ======================================================================


@dataclass
class AIMiddlewareMetrics:
    """AI-specific metrics for a single AI middleware stage.

    Populated only when an ``AIMiddleware`` subclass runs.
    All fields default to 0 so non-AI middlewares are unaffected.
    """

    llm_calls: int = 0  # total provider.complete() calls (cache misses)
    cache_hits: int = 0  # responses served from LLMCache
    cache_misses: int = 0  # calls that hit the provider
    input_tokens: int = 0  # cumulative prompt tokens
    output_tokens: int = 0  # cumulative completion tokens
    errors: int = 0  # LLM calls that raised exceptions

    # Quality signals (populated by specific middlewares)
    validation_pass: int = 0  # AIValidateMiddleware: records that passed
    validation_fail: int = 0  # AIValidateMiddleware: records that failed
    category_counts: dict[str, int] = field(default_factory=dict)  # AIClassifyMiddleware

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    @property
    def cache_hit_rate(self) -> float:
        total = self.cache_hits + self.cache_misses
        return self.cache_hits / total if total > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "llm_calls": self.llm_calls,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_hit_rate": round(self.cache_hit_rate, 4),
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
            "errors": self.errors,
            "validation_pass": self.validation_pass,
            "validation_fail": self.validation_fail,
            "category_counts": self.category_counts,
        }


# ======================================================================
# Pipeline-level AI aggregates
# ======================================================================


@dataclass
class AIMetrics:
    """Aggregated AI metrics across all AI middlewares in a pipeline run.

    Returned as ``PipelineRunSummary.ai``.
    ``None`` when no AI middleware ran (non-AI pipelines are unaffected).
    """

    total_llm_calls: int = 0
    total_cache_hits: int = 0
    total_cache_misses: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_errors: int = 0
    total_validation_pass: int = 0
    total_validation_fail: int = 0

    @property
    def total_tokens(self) -> int:
        return self.total_input_tokens + self.total_output_tokens

    @property
    def cache_hit_rate(self) -> float:
        total = self.total_cache_hits + self.total_cache_misses
        return self.total_cache_hits / total if total > 0 else 0.0

    @property
    def validation_pass_rate(self) -> float:
        total = self.total_validation_pass + self.total_validation_fail
        return self.total_validation_pass / total if total > 0 else 0.0

    def absorb(self, mw: AIMiddlewareMetrics) -> None:
        """Merge per-middleware AI metrics into pipeline totals."""
        self.total_llm_calls += mw.llm_calls
        self.total_cache_hits += mw.cache_hits
        self.total_cache_misses += mw.cache_misses
        self.total_input_tokens += mw.input_tokens
        self.total_output_tokens += mw.output_tokens
        self.total_errors += mw.errors
        self.total_validation_pass += mw.validation_pass
        self.total_validation_fail += mw.validation_fail

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_llm_calls": self.total_llm_calls,
            "total_cache_hits": self.total_cache_hits,
            "total_cache_misses": self.total_cache_misses,
            "cache_hit_rate": round(self.cache_hit_rate, 4),
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "total_tokens": self.total_tokens,
            "total_errors": self.total_errors,
            "validation_pass_rate": round(self.validation_pass_rate, 4),
        }


# ======================================================================
# Per-middleware metrics
# ======================================================================


@dataclass
class MiddlewareMetrics:
    """Metrics for a single middleware stage."""

    name: str
    records_in: int = 0
    records_out: int = 0  # records passed downstream
    records_dropped: int = 0  # returned None
    records_errored: int = 0  # raised exception
    total_time_ms: float = 0.0
    ai: AIMiddlewareMetrics = field(default_factory=AIMiddlewareMetrics)
    schema: SchemaMetrics | None = None  # populated by SchemaMiddleware

    @property
    def avg_time_ms(self) -> float:
        if self.records_in == 0:
            return 0.0
        return self.total_time_ms / self.records_in


# ======================================================================
# Pipeline-level metrics (live, mutable)
# ======================================================================


@dataclass
class PipelineMetrics:
    """Mutable metrics container — updated in-place during pipeline.run()."""

    started_at: float = field(default_factory=time.monotonic)

    # Totals across all stages
    records_consumed: int = 0  # emitted by source
    records_written: int = 0  # accepted by at least one sink
    records_dropped: int = 0  # dropped by any middleware
    records_errored: int = 0  # unrecoverable errors

    # Breakdown by source key (e.g. "api_source", "file_source")
    by_source: dict[str, int] = field(default_factory=dict)

    # Per-middleware breakdown
    by_middleware: dict[str, MiddlewareMetrics] = field(default_factory=dict)

    # Runtime pressure / bounded-memory signals
    runtime: RuntimeMetrics = field(default_factory=lambda: RuntimeMetrics())
    last_checkpoint: Checkpoint | None = None
    _live_metric_overlays: list[Any] = field(default_factory=list, repr=False)

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    def middleware(self, name: str) -> MiddlewareMetrics:
        """Return (creating if needed) the MiddlewareMetrics for *name*."""
        if name not in self.by_middleware:
            self.by_middleware[name] = MiddlewareMetrics(name=name)
        return self.by_middleware[name]

    def inc_source(self, source_key: str, count: int = 1) -> None:
        self.by_source[source_key] = self.by_source.get(source_key, 0) + count

    def aggregate_ai(self) -> AIMetrics | None:
        """Aggregate AI metrics across all middlewares that had any LLM activity.

        Returns ``None`` if no AI middleware ran (avoids polluting non-AI
        health output with empty AI blocks).
        """
        agg = AIMetrics()
        has_ai = False
        for mw in self.by_middleware.values():
            m = mw.ai
            if m.llm_calls > 0 or m.cache_hits > 0:
                agg.absorb(m)
                has_ai = True
        return agg if has_ai else None

    def register_live_metric_overlay(self, overlay: Any) -> None:
        self._live_metric_overlays.append(overlay)

    def unregister_live_metric_overlay(self, overlay: Any) -> None:
        with contextlib.suppress(ValueError):
            self._live_metric_overlays.remove(overlay)

    def snapshot(self, pipeline_id: str, run_id: str) -> PipelineRunSummary:
        elapsed = time.monotonic() - self.started_at
        records_consumed = self.records_consumed
        records_written = self.records_written
        by_source = dict(self.by_source)

        for overlay in self._live_metric_overlays:
            pending = overlay.snapshot_pending()
            pending_consumed = pending.get("records_consumed", 0)
            pending_written = pending.get("records_written", 0)
            if pending_consumed:
                records_consumed += pending_consumed
                source_name = pending.get("source_name")
                if source_name:
                    by_source[source_name] = by_source.get(source_name, 0) + pending_consumed
            if pending_written:
                records_written += pending_written

        return PipelineRunSummary(
            pipeline_id=pipeline_id,
            run_id=run_id,
            elapsed_seconds=elapsed,
            records_consumed=records_consumed,
            records_written=records_written,
            records_dropped=self.records_dropped,
            records_errored=self.records_errored,
            by_source=by_source,
            by_middleware=dict(self.by_middleware.items()),
            ai=self.aggregate_ai(),
            runtime=self.runtime.copy(),
            last_checkpoint=self.last_checkpoint,
        )


# ======================================================================
# Runtime pressure metrics
# ======================================================================


@dataclass
class RuntimeMetrics:
    """Runtime pressure signals captured during a pipeline run."""

    execution_lane: str = ""
    direct_flush_active: bool = False
    arrow_fast_path_active: bool = False
    arrow_chain_active: bool = False
    source_prefetch_enabled: bool = False
    source_prefetch_limit: int = 0
    source_prefetch_block_count: int = 0
    source_prefetch_max_depth: int = 0
    rust_prefetch_active: bool = False
    rust_prefetch_wait_count: int = 0
    rust_prefetch_batch_drain_count: int = 0
    rust_prefetch_push_batch_count: int = 0
    source_record_error_count: int = 0
    source_record_drop_count: int = 0
    buffered_stage_limit: int = 0
    buffered_stage_max_in_flight: int = 0
    buffered_stage_drain_count: int = 0
    adaptive_backpressure_enabled: bool = False
    adaptive_backpressure_min_limit: int = 0
    adaptive_backpressure_max_limit: int = 0
    adaptive_backpressure_scale_up_count: int = 0
    adaptive_backpressure_scale_down_count: int = 0
    checkpoint_enabled: bool = False
    checkpoint_save_count: int = 0
    checkpoint_save_max_batch_size: int = 0
    checkpoint_save_time_ms: float = 0.0
    checkpoint_failure_count: int = 0
    dlq_failure_count: int = 0
    writer_flush_count: int = 0
    writer_flush_max_batch_size: int = 0
    writer_flush_time_ms: float = 0.0

    def copy(self) -> RuntimeMetrics:
        from dataclasses import replace

        return replace(self)

    def to_dict(self) -> dict[str, int | bool | float | str]:
        return {
            "execution_lane": self.execution_lane,
            "direct_flush_active": self.direct_flush_active,
            "arrow_fast_path_active": self.arrow_fast_path_active,
            "arrow_chain_active": self.arrow_chain_active,
            "source_prefetch_enabled": self.source_prefetch_enabled,
            "source_prefetch_limit": self.source_prefetch_limit,
            "source_prefetch_block_count": self.source_prefetch_block_count,
            "source_prefetch_max_depth": self.source_prefetch_max_depth,
            "rust_prefetch_active": self.rust_prefetch_active,
            "rust_prefetch_wait_count": self.rust_prefetch_wait_count,
            "rust_prefetch_batch_drain_count": self.rust_prefetch_batch_drain_count,
            "rust_prefetch_push_batch_count": self.rust_prefetch_push_batch_count,
            "source_record_error_count": self.source_record_error_count,
            "source_record_drop_count": self.source_record_drop_count,
            "buffered_stage_limit": self.buffered_stage_limit,
            "buffered_stage_max_in_flight": self.buffered_stage_max_in_flight,
            "buffered_stage_drain_count": self.buffered_stage_drain_count,
            "adaptive_backpressure_enabled": self.adaptive_backpressure_enabled,
            "adaptive_backpressure_min_limit": self.adaptive_backpressure_min_limit,
            "adaptive_backpressure_max_limit": self.adaptive_backpressure_max_limit,
            "adaptive_backpressure_scale_up_count": self.adaptive_backpressure_scale_up_count,
            "adaptive_backpressure_scale_down_count": self.adaptive_backpressure_scale_down_count,
            "checkpoint_enabled": self.checkpoint_enabled,
            "checkpoint_save_count": self.checkpoint_save_count,
            "checkpoint_save_max_batch_size": self.checkpoint_save_max_batch_size,
            "checkpoint_save_time_ms": self.checkpoint_save_time_ms,
            "checkpoint_failure_count": self.checkpoint_failure_count,
            "dlq_failure_count": self.dlq_failure_count,
            "writer_flush_count": self.writer_flush_count,
            "writer_flush_max_batch_size": self.writer_flush_max_batch_size,
            "writer_flush_time_ms": self.writer_flush_time_ms,
        }


# ======================================================================
# Run summary (immutable, returned to caller)
# ======================================================================


@dataclass(frozen=True)
class PipelineRunSummary:
    """Immutable snapshot of metrics at the end of a pipeline run."""

    pipeline_id: str
    run_id: str
    elapsed_seconds: float
    records_consumed: int
    records_written: int
    records_dropped: int
    records_errored: int
    by_source: dict[str, int]
    by_middleware: dict[str, MiddlewareMetrics]
    ai: AIMetrics | None = None  # None when no AI middleware ran
    runtime: RuntimeMetrics = field(default_factory=RuntimeMetrics)
    last_checkpoint: Checkpoint | None = None

    def __str__(self) -> str:
        parts = [
            f"consumed={self.records_consumed}",
            f"written={self.records_written}",
            f"dropped={self.records_dropped}",
            f"errors={self.records_errored}",
            f"elapsed={self.elapsed_seconds:.1f}s",
        ]
        if self.ai is not None:
            parts.append(f"llm_calls={self.ai.total_llm_calls}")
            parts.append(f"tokens={self.ai.total_tokens}")
        return f"PipelineRunSummary({', '.join(parts)})"
