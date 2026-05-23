from __future__ import annotations

from agora.core.source import (
    BaseSource,
    SourceRuntimeMetrics,
    is_prefetch_capable,
    prefetch_limit_for,
    source_runtime_metrics,
)


class _PrefetchSource(BaseSource[int]):
    source_name = "prefetch"
    supports_prefetch = True
    prefetch_limit = 3

    async def stream(self):
        yield 1


class _NoPrefetchSource(BaseSource[int]):
    source_name = "no_prefetch"
    supports_prefetch = False
    prefetch_limit = 9

    async def stream(self):
        yield 1


class _MetricsSource(BaseSource[int]):
    source_name = "metrics"

    async def stream(self):
        yield 1

    def runtime_metrics(self) -> SourceRuntimeMetrics:
        return SourceRuntimeMetrics(record_error_count=2, record_drop_count=1)


def test_prefetch_helper_reports_enabled_source() -> None:
    source = _PrefetchSource()
    assert is_prefetch_capable(source) is True
    assert prefetch_limit_for(source) == 3


def test_prefetch_helper_disables_source_without_opt_in() -> None:
    source = _NoPrefetchSource()
    assert is_prefetch_capable(source) is False
    assert prefetch_limit_for(source) == 0


def test_source_runtime_metrics_helper_returns_custom_metrics() -> None:
    metrics = source_runtime_metrics(_MetricsSource())
    assert metrics.record_error_count == 2
    assert metrics.record_drop_count == 1


def test_source_runtime_metrics_helper_falls_back_to_defaults() -> None:
    metrics = source_runtime_metrics(object())
    assert metrics == SourceRuntimeMetrics()
