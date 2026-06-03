from __future__ import annotations

from datetime import UTC, datetime

import pytest

from agora.core.metrics import MiddlewareMetrics, PipelineRunSummary, RuntimeMetrics
from agora.core.runtime import HotPathMetrics
from agora.metrics.collector import MetricsCollector
from agora.metrics.exporters import PrometheusTextExporter


def _make_summary(
    *,
    elapsed_seconds: float = 2.0,
    records_consumed: int = 20,
    records_written: int = 18,
    records_dropped: int = 1,
    records_errored: int = 1,
    runtime: RuntimeMetrics | None = None,
    by_middleware: dict[str, MiddlewareMetrics] | None = None,
) -> PipelineRunSummary:
    return PipelineRunSummary(
        pipeline_id="test",
        run_id="run-1",
        elapsed_seconds=elapsed_seconds,
        records_consumed=records_consumed,
        records_written=records_written,
        records_dropped=records_dropped,
        records_errored=records_errored,
        by_source={},
        by_middleware=by_middleware or {},
        runtime=runtime or RuntimeMetrics(),
    )


@pytest.mark.asyncio
async def test_health_dict_includes_last_run_throughput_and_runtime_snapshot() -> None:
    collector = MetricsCollector()
    runtime = RuntimeMetrics(
        execution_lane="buffered",
        direct_flush_active=False,
        arrow_fast_path_active=False,
        arrow_chain_active=False,
        source_prefetch_limit=64,
        source_prefetch_max_depth=16,
        source_prefetch_block_count=3,
        rust_prefetch_active=True,
        rust_prefetch_wait_count=4,
        rust_prefetch_batch_drain_count=5,
        rust_prefetch_push_batch_count=6,
        buffered_stage_limit=8,
        buffered_stage_max_in_flight=7,
        checkpoint_save_count=4,
        checkpoint_save_time_ms=12.5,
        writer_flush_count=2,
        writer_flush_time_ms=25.0,
        dlq_failure_count=1,
        adaptive_backpressure_scale_up_count=2,
        adaptive_backpressure_scale_down_count=1,
    )

    await collector.record_run("orders", summary=_make_summary(runtime=runtime))

    health = collector.to_health_dict()
    pipeline = health["pipelines"]["orders"]

    assert pipeline["last_run_duration_s"] == 2.0
    assert pipeline["last_run_throughput_rps"] == 10.0
    assert pipeline["runtime"]["total_source_prefetch_block_count"] == 3
    assert pipeline["runtime"]["total_rust_prefetch_runs"] == 1
    assert pipeline["runtime"]["total_rust_prefetch_wait_count"] == 4
    assert pipeline["runtime"]["total_rust_prefetch_batch_drain_count"] == 5
    assert pipeline["runtime"]["total_rust_prefetch_push_batch_count"] == 6
    assert pipeline["runtime"]["total_checkpoint_save_count"] == 4
    assert pipeline["runtime"]["total_writer_flush_count"] == 2
    assert pipeline["runtime"]["total_dlq_failure_count"] == 1
    assert pipeline["runtime"]["total_adaptive_scale_up_count"] == 2
    assert pipeline["runtime"]["total_adaptive_scale_down_count"] == 1
    assert pipeline["runtime"]["last_run"]["execution_lane"] == "buffered"
    assert pipeline["runtime"]["last_run"]["direct_flush_active"] is False
    assert pipeline["runtime"]["last_run"]["rust_prefetch_active"] is True
    assert pipeline["runtime"]["last_run"]["rust_prefetch_wait_count"] == 4
    assert pipeline["runtime"]["last_run"]["buffered_stage_max_in_flight"] == 7
    assert pipeline["runtime"]["last_run"]["checkpoint_save_time_ms"] == 12.5
    assert pipeline["runtime"]["last_run"]["writer_flush_time_ms"] == 25.0


@pytest.mark.asyncio
async def test_health_dict_lists_registered_pipeline_and_live_run_snapshot() -> None:
    collector = MetricsCollector()
    runtime = RuntimeMetrics(
        execution_lane="linear",
        direct_flush_active=True,
        writer_flush_count=3,
        writer_flush_time_ms=7.5,
    )

    await collector.register_pipeline("orders", schedule="continuous")
    await collector.record_live_run(
        "orders",
        summary=_make_summary(
            elapsed_seconds=4.0,
            records_consumed=40,
            records_written=39,
            records_dropped=1,
            records_errored=0,
            runtime=runtime,
        ),
        run_id="run-live",
        started_at=datetime(2026, 6, 3, 10, 0, tzinfo=UTC),
    )

    health = collector.to_health_dict()
    pipeline = health["pipelines"]["orders"]

    assert pipeline["schedule"] == "continuous"
    assert health["status"] == "running"
    assert pipeline["status"] == "running"
    assert pipeline["lifecycle"]["running"] is True
    assert pipeline["lifecycle"]["state"] == "running"
    assert pipeline["lifecycle"]["active_run_id"] == "run-live"
    assert pipeline["live"]["records_consumed"] == 40
    assert pipeline["live"]["records_written"] == 39
    assert pipeline["live"]["records_dropped"] == 1
    assert pipeline["live"]["records_pending"] == 0
    assert pipeline["live"]["throughput_rps"] == 10.0
    assert pipeline["live"]["runtime"]["execution_lane"] == "linear"
    assert pipeline["live"]["runtime"]["direct_flush_active"] is True


@pytest.mark.asyncio
async def test_health_dict_live_pending_records_explains_inflight_gap() -> None:
    collector = MetricsCollector()

    await collector.register_pipeline("orders", schedule="continuous")
    await collector.record_live_run(
        "orders",
        summary=_make_summary(
            elapsed_seconds=5.0,
            records_consumed=5000,
            records_written=4900,
            records_dropped=15,
            records_errored=4,
        ),
        run_id="run-live",
        started_at=datetime(2026, 6, 3, 10, 0, tzinfo=UTC),
    )

    health = collector.to_health_dict()
    pipeline = health["pipelines"]["orders"]

    assert pipeline["live"]["records_pending"] == 81


def test_pipeline_metrics_snapshot_includes_unflushed_hot_path_counters() -> None:
    from agora.core.metrics import PipelineMetrics

    metrics = PipelineMetrics(records_consumed=9900, records_written=9961)
    hot = HotPathMetrics.for_source("kafka", metrics=metrics)
    hot.inc_consumed(61)

    summary = metrics.snapshot(pipeline_id="orders_projection", run_id="run-live")

    assert summary.records_consumed == 9961
    assert summary.records_written == 9961
    assert summary.by_source["kafka"] == 61


@pytest.mark.asyncio
async def test_health_dict_includes_middleware_hotspots() -> None:
    collector = MetricsCollector()
    by_middleware = {
        "normalize": MiddlewareMetrics(
            name="normalize",
            records_in=20,
            records_out=20,
            total_time_ms=10.0,
        ),
        "dedup": MiddlewareMetrics(
            name="dedup",
            records_in=20,
            records_out=18,
            records_dropped=2,
            total_time_ms=30.0,
        ),
    }

    await collector.record_run("orders", summary=_make_summary(by_middleware=by_middleware))

    health = collector.to_health_dict()
    pipeline = health["pipelines"]["orders"]

    assert pipeline["slowest_middleware"] == {
        "name": "dedup",
        "avg_time_ms": 1.5,
        "total_time_ms": 30.0,
    }
    assert pipeline["middlewares"]["normalize"]["total_records_in"] == 20
    assert pipeline["middlewares"]["normalize"]["last_avg_time_ms"] == 0.5
    assert pipeline["middlewares"]["dedup"]["total_records_dropped"] == 2
    assert pipeline["middlewares"]["dedup"]["last_total_time_ms"] == 30.0


@pytest.mark.asyncio
async def test_prometheus_exporter_renders_runtime_observability_metrics() -> None:
    collector = MetricsCollector()
    runtime = RuntimeMetrics(
        execution_lane="batch",
        direct_flush_active=False,
        arrow_fast_path_active=True,
        arrow_chain_active=True,
        source_prefetch_limit=32,
        source_prefetch_max_depth=12,
        source_prefetch_block_count=5,
        rust_prefetch_active=True,
        rust_prefetch_wait_count=6,
        rust_prefetch_batch_drain_count=4,
        rust_prefetch_push_batch_count=3,
        buffered_stage_limit=10,
        buffered_stage_max_in_flight=9,
        checkpoint_save_count=6,
        checkpoint_failure_count=1,
        checkpoint_save_max_batch_size=4,
        checkpoint_save_time_ms=18.5,
        writer_flush_count=7,
        writer_flush_max_batch_size=5,
        writer_flush_time_ms=44.0,
        dlq_failure_count=2,
        adaptive_backpressure_scale_up_count=3,
        adaptive_backpressure_scale_down_count=1,
        adaptive_backpressure_min_limit=2,
        adaptive_backpressure_max_limit=16,
    )
    await collector.record_run("orders", summary=_make_summary(runtime=runtime))

    rendered = PrometheusTextExporter(collector=collector, namespace="agora").render()

    assert 'agora_pipeline_last_run_duration_seconds{pipeline_id="orders"} 2.000000' in rendered
    assert 'agora_pipeline_last_run_throughput_rps{pipeline_id="orders"} 10.000000' in rendered
    assert (
        'agora_pipeline_runtime_events_total{pipeline_id="orders",event="writer_flush"} 7'
        in rendered
    )
    assert (
        'agora_pipeline_runtime_events_total{pipeline_id="orders",event="checkpoint_failure"} 1'
        in rendered
    )
    assert (
        'agora_pipeline_runtime_events_total{pipeline_id="orders",event="rust_prefetch_wait"} 6'
        in rendered
    )
    assert (
        'agora_pipeline_runtime_last{pipeline_id="orders",signal="writer_flush_time_ms"} 44.0'
        in rendered
    )
    assert 'agora_pipeline_runtime_lane_last{pipeline_id="orders",lane="batch"} 1' in rendered
    assert (
        'agora_pipeline_runtime_last{pipeline_id="orders",signal="arrow_fast_path_active"} 1'
        in rendered
    )
    assert (
        'agora_pipeline_runtime_last{pipeline_id="orders",signal="arrow_chain_active"} 1'
        in rendered
    )
    assert (
        'agora_pipeline_runtime_last{pipeline_id="orders",signal="rust_prefetch_active"} 1'
        in rendered
    )
    assert (
        'agora_pipeline_runtime_last{pipeline_id="orders",signal="rust_prefetch_push_batch_count"} 3'
        in rendered
    )
    assert (
        'agora_pipeline_runtime_last{pipeline_id="orders",signal="buffered_stage_max_in_flight"} 9'
        in rendered
    )
    assert (
        'agora_pipeline_runtime_last{pipeline_id="orders",signal="adaptive_backpressure_max_limit"} 16'
        in rendered
    )


@pytest.mark.asyncio
async def test_prometheus_exporter_renders_live_pipeline_metrics() -> None:
    collector = MetricsCollector()
    runtime = RuntimeMetrics(
        execution_lane="linear",
        direct_flush_active=True,
        writer_flush_count=2,
        writer_flush_time_ms=11.0,
    )
    await collector.register_pipeline("orders", schedule="continuous")
    await collector.record_live_run(
        "orders",
        summary=_make_summary(
            elapsed_seconds=5.0,
            records_consumed=50,
            records_written=49,
            records_dropped=1,
            records_errored=0,
            runtime=runtime,
        ),
        run_id="run-live",
        started_at=datetime(2026, 6, 3, 10, 0, tzinfo=UTC),
    )

    rendered = PrometheusTextExporter(collector=collector, namespace="agora").render()

    assert 'agora_pipeline_registered{pipeline_id="orders",schedule="continuous"} 1' in rendered
    assert 'agora_pipeline_running{pipeline_id="orders"} 1' in rendered
    assert 'agora_pipeline_live_run_duration_seconds{pipeline_id="orders"} 5.000000' in rendered
    assert 'agora_pipeline_live_throughput_rps{pipeline_id="orders"} 10.000000' in rendered
    assert 'agora_pipeline_live_records{pipeline_id="orders",outcome="consumed"} 50' in rendered
    assert 'agora_pipeline_live_records{pipeline_id="orders",outcome="pending"} 0' in rendered
    assert (
        'agora_pipeline_runtime_current{pipeline_id="orders",signal="direct_flush_active"} 1'
        in rendered
    )
    assert 'agora_pipeline_runtime_lane_current{pipeline_id="orders",lane="linear"} 1' in rendered


@pytest.mark.asyncio
async def test_prometheus_exporter_renders_middleware_metrics() -> None:
    collector = MetricsCollector()
    by_middleware = {
        "normalize": MiddlewareMetrics(
            name="normalize",
            records_in=20,
            records_out=20,
            total_time_ms=10.0,
        ),
        "dedup": MiddlewareMetrics(
            name="dedup",
            records_in=20,
            records_out=18,
            records_dropped=2,
            total_time_ms=30.0,
        ),
    }
    await collector.record_run("orders", summary=_make_summary(by_middleware=by_middleware))

    rendered = PrometheusTextExporter(collector=collector, namespace="agora").render()

    assert (
        'agora_pipeline_middleware_records_total{pipeline_id="orders",middleware="normalize",outcome="in"} 20'
        in rendered
    )
    assert (
        'agora_pipeline_middleware_records_total{pipeline_id="orders",middleware="dedup",outcome="dropped"} 2'
        in rendered
    )
    assert (
        'agora_pipeline_middleware_time_ms_total{pipeline_id="orders",middleware="dedup"} 30.000000'
        in rendered
    )
    assert (
        'agora_pipeline_middleware_last_avg_time_ms{pipeline_id="orders",middleware="normalize"} 0.500000'
        in rendered
    )
    assert (
        'agora_pipeline_slowest_middleware_last_avg_time_ms{pipeline_id="orders",middleware="dedup"} 1.500000'
        in rendered
    )
