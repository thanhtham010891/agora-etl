from __future__ import annotations

import warnings

import pytest

from agora.core.data_plane import DataPlane, SourceDataPlaneSpec
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


def test_source_data_plane_spec_warns_once_for_legacy_bool_flags() -> None:
    class _LegacyBatchSource(BaseSource[int]):
        source_name = "legacy_batch"
        supports_batch_emit = True

        async def stream(self):
            yield 1

    source = _LegacyBatchSource()
    with pytest.deprecated_call(match="legacy source data-plane bool flags"):
        first = source.data_plane_spec()
    second = source.data_plane_spec()

    assert first.emitted_plane is DataPlane.PYTHON_BATCHES
    assert second.emitted_plane is DataPlane.PYTHON_BATCHES


def test_source_data_plane_spec_does_not_warn_for_explicit_contract() -> None:
    class _ExplicitBatchSource(BaseSource[int]):
        source_name = "explicit_batch"

        async def stream(self):
            yield 1

        def data_plane_spec(self) -> SourceDataPlaneSpec:
            return SourceDataPlaneSpec(
                source_name=self.source_name,
                emitted_plane=DataPlane.PYTHON_BATCHES,
                supports_batch_emit=True,
                emits_arrow_batches=False,
            )

    source = _ExplicitBatchSource()
    with warnings.catch_warnings(record=True) as record:
        spec = source.data_plane_spec()

    assert spec.emitted_plane is DataPlane.PYTHON_BATCHES
    assert len(record) == 0
