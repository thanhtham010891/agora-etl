"""
tests/ai/test_ai_metrics.py
============================
Tests for AI observability layer:
  - AIMiddlewareMetrics fields and properties
  - AIMetrics aggregation (absorb)
  - PipelineMetrics.aggregate_ai() — None for non-AI, populated for AI runs
  - PipelineRunSummary.ai field present/absent
  - AIMiddleware._cached_complete populates ctx metrics (cache hit + miss)
  - AIValidateMiddleware tracks validation_pass / validation_fail
  - AIClassifyMiddleware tracks category_counts
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from agora.core.context import PipelineContext
from agora.core.metrics import (
    AIMetrics,
    AIMiddlewareMetrics,
    PipelineMetrics,
    PipelineRunSummary,
    RuntimeMetrics,
)

# ======================================================================
# AIMiddlewareMetrics unit tests
# ======================================================================


def test_ai_middleware_metrics_defaults():
    m = AIMiddlewareMetrics()
    assert m.llm_calls == 0
    assert m.cache_hits == 0
    assert m.cache_misses == 0
    assert m.total_tokens == 0
    assert m.cache_hit_rate == 0.0


def test_ai_middleware_metrics_cache_hit_rate():
    m = AIMiddlewareMetrics(cache_hits=3, cache_misses=7)
    assert m.cache_hit_rate == pytest.approx(0.3)


def test_ai_middleware_metrics_to_dict_keys():
    m = AIMiddlewareMetrics(llm_calls=5, input_tokens=100, output_tokens=50)
    d = m.to_dict()
    assert d["llm_calls"] == 5
    assert d["total_tokens"] == 150
    assert "cache_hit_rate" in d


# ======================================================================
# AIMetrics aggregation tests
# ======================================================================


def test_ai_metrics_absorb():
    agg = AIMetrics()
    mw = AIMiddlewareMetrics(
        llm_calls=10,
        cache_hits=5,
        cache_misses=5,
        input_tokens=200,
        output_tokens=100,
        validation_pass=8,
        validation_fail=2,
    )
    agg.absorb(mw)
    assert agg.total_llm_calls == 10
    assert agg.total_cache_hits == 5
    assert agg.total_tokens == 300
    assert agg.total_validation_pass == 8
    assert agg.validation_pass_rate == pytest.approx(0.8)


def test_ai_metrics_absorb_multiple():
    agg = AIMetrics()
    agg.absorb(AIMiddlewareMetrics(llm_calls=3, input_tokens=60, output_tokens=30))
    agg.absorb(AIMiddlewareMetrics(llm_calls=7, input_tokens=140, output_tokens=70))
    assert agg.total_llm_calls == 10
    assert agg.total_tokens == 300


# ======================================================================
# PipelineMetrics.aggregate_ai() tests
# ======================================================================


def test_aggregate_ai_returns_none_for_non_ai_pipeline():
    m = PipelineMetrics()
    m.middleware("map").records_in = 100
    # No LLM calls → should be None
    assert m.aggregate_ai() is None


def test_aggregate_ai_returns_metrics_when_ai_ran():
    m = PipelineMetrics()
    m.middleware("ai_enrich").ai.llm_calls = 5
    m.middleware("ai_enrich").ai.input_tokens = 100
    m.middleware("ai_enrich").ai.cache_hits = 2
    m.middleware("ai_enrich").ai.cache_misses = 3

    result = m.aggregate_ai()
    assert result is not None
    assert result.total_llm_calls == 5
    assert result.total_cache_hits == 2


def test_aggregate_ai_aggregates_multiple_middlewares():
    m = PipelineMetrics()
    m.middleware("ai_enrich").ai.llm_calls = 5
    m.middleware("ai_enrich").ai.input_tokens = 100
    m.middleware("ai_validate").ai.cache_hits = 10
    m.middleware("ai_validate").ai.cache_misses = 5

    result = m.aggregate_ai()
    assert result is not None
    assert result.total_llm_calls == 5
    assert result.total_cache_hits == 10


# ======================================================================
# PipelineRunSummary.ai field
# ======================================================================


def test_run_summary_ai_field_none_by_default():
    summary = PipelineRunSummary(
        pipeline_id="test",
        run_id="run-1",
        elapsed_seconds=1.0,
        records_consumed=10,
        records_written=10,
        records_dropped=0,
        records_errored=0,
        by_source={},
        by_middleware={},
        ai=None,
    )
    assert summary.ai is None
    assert "llm_calls" not in str(summary)


def test_runtime_metrics_defaults():
    runtime = RuntimeMetrics()
    assert runtime.source_prefetch_enabled is False
    assert runtime.source_prefetch_limit == 0
    assert runtime.source_prefetch_block_count == 0
    assert runtime.source_record_error_count == 0
    assert runtime.source_record_drop_count == 0
    assert runtime.buffered_stage_limit == 0


def test_run_summary_str_includes_ai_when_present():
    ai = AIMetrics(total_llm_calls=5, total_input_tokens=100, total_output_tokens=50)
    summary = PipelineRunSummary(
        pipeline_id="test",
        run_id="run-1",
        elapsed_seconds=1.0,
        records_consumed=10,
        records_written=10,
        records_dropped=0,
        records_errored=0,
        by_source={},
        by_middleware={},
        ai=ai,
    )
    s = str(summary)
    assert "llm_calls=5" in s
    assert "tokens=150" in s


# ======================================================================
# AIMiddleware._cached_complete metrics wiring
# ======================================================================


def _make_ctx() -> PipelineContext:
    return PipelineContext(pipeline_id="test", metrics=PipelineMetrics())


def _make_provider(content: str = '{"result": "ok"}') -> MagicMock:
    from agora.ai.providers.base import CompletionResponse

    provider = MagicMock()
    provider.model = "test-model"
    provider.complete = AsyncMock(
        return_value=CompletionResponse(
            content=content,
            model="test-model",
            input_tokens=10,
            output_tokens=5,
        )
    )
    return provider


@pytest.mark.asyncio
async def test_cached_complete_tracks_cache_miss():
    from agora.middlewares.ai.enrich import AIEnrichMiddleware

    provider = _make_provider()
    mw = AIEnrichMiddleware(provider=provider, prompt_template="test {name}")
    ctx = _make_ctx()

    await mw._cached_complete("hello", ctx=ctx)

    ai_m = ctx.metrics.middleware(mw.name).ai
    assert ai_m.cache_misses == 1
    assert ai_m.llm_calls == 1
    assert ai_m.input_tokens == 10
    assert ai_m.output_tokens == 5


@pytest.mark.asyncio
async def test_cached_complete_tracks_cache_hit():
    from agora.ai.cache import InMemoryLLMCache
    from agora.middlewares.ai.enrich import AIEnrichMiddleware

    provider = _make_provider()
    cache = InMemoryLLMCache()
    mw = AIEnrichMiddleware(provider=provider, prompt_template="test", cache=cache)
    ctx = _make_ctx()

    # First call — miss
    await mw._cached_complete("hello", ctx=ctx)
    # Second call — hit
    await mw._cached_complete("hello", ctx=ctx)

    ai_m = ctx.metrics.middleware(mw.name).ai
    assert ai_m.cache_misses == 1
    assert ai_m.cache_hits == 1
    assert ai_m.llm_calls == 1  # only 1 real LLM call


# ======================================================================
# AIValidateMiddleware quality metrics
# ======================================================================


@pytest.mark.asyncio
async def test_validate_middleware_tracks_pass():
    from agora.middlewares.ai.validate import AIValidateMiddleware

    provider = _make_provider(content='{"valid": true, "issues": [], "confidence": 0.95}')
    mw = AIValidateMiddleware(provider=provider, criteria="must be valid")
    ctx = _make_ctx()

    record = {"name": "Test Place"}
    await mw.process(record, ctx)

    ai_m = ctx.metrics.middleware(mw.name).ai
    assert ai_m.validation_pass == 1
    assert ai_m.validation_fail == 0


@pytest.mark.asyncio
async def test_validate_middleware_tracks_fail():
    from agora.middlewares.ai.validate import AIValidateMiddleware

    provider = _make_provider(
        content='{"valid": false, "issues": ["missing address"], "confidence": 0.9}'
    )
    mw = AIValidateMiddleware(provider=provider, criteria="needs address", on_invalid="flag")
    ctx = _make_ctx()

    record = {"name": "Test Place"}
    await mw.process(record, ctx)

    ai_m = ctx.metrics.middleware(mw.name).ai
    assert ai_m.validation_pass == 0
    assert ai_m.validation_fail == 1


# ======================================================================
# AIClassifyMiddleware category_counts
# ======================================================================


@pytest.mark.asyncio
async def test_classify_middleware_tracks_category_counts():
    from agora.middlewares.ai.classify import AIClassifyMiddleware

    categories = ["restaurant", "hotel", "cafe"]
    provider = _make_provider(content='{"category": "restaurant", "confidence": 0.92}')
    mw = AIClassifyMiddleware(
        provider=provider,
        source_fields=["name"],
        categories=categories,
    )
    ctx = _make_ctx()

    await mw.process({"name": "Pho Hanoi"}, ctx)
    await mw.process({"name": "Banh Mi Shop"}, ctx)

    ai_m = ctx.metrics.middleware(mw.name).ai
    assert ai_m.category_counts.get("restaurant", 0) == 2
