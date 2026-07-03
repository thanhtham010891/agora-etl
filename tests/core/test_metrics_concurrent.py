"""
tests/core/test_metrics_concurrent.py
=======================================
Property-Based Concurrent Tests — MetricsCollector Race Condition Fix

**Property 5: Bug Condition — MetricsCollector Concurrent Safety**

For any N concurrent calls to record_run(pipeline_id, summary) with the same
pipeline_id, fixed MetricsCollector SHALL ensure stats.total_runs increments
exactly N times with no lost updates.

**Validates: Requirements 2.7, 2.8, 3.7**
"""

from __future__ import annotations

import asyncio

import pytest

from agora.metrics.collector import MetricsCollector

# ======================================================================
# Property 5: Bug Condition — MetricsCollector Concurrent Safety
# ======================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize("n", [1, 5, 10, 25, 50])
async def test_concurrent_record_run_no_lost_updates(n: int) -> None:
    """Property 5: N concurrent record_run() calls → total_runs == N (no lost updates).

    For any N in [1, 50], firing N concurrent coroutines that each call
    record_run() for the same pipeline_id must result in total_runs == N.
    The asyncio.Lock in record_run() ensures atomic read-modify-write.

    **Validates: Requirements 2.7, 2.8**
    """
    collector = MetricsCollector()
    tasks = [asyncio.create_task(collector.record_run("pipe_a")) for _ in range(n)]
    await asyncio.gather(*tasks)
    stats = collector.pipeline_stats("pipe_a")
    assert stats is not None, f"Stats should exist after {n} record_run calls"
    assert stats.total_runs == n, (
        f"[PERF-2] CONCURRENT SAFETY FAILED: Expected total_runs={n} after {n} "
        f"concurrent calls, got total_runs={stats.total_runs}. "
        f"Lost {n - stats.total_runs} updates — asyncio.Lock not working correctly."
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("n", [1, 5, 10, 25, 50])
async def test_concurrent_record_run_success_count_correct(n: int) -> None:
    """Property 5: N concurrent successful record_run() calls → successful_runs == N.

    All calls without error → successful_runs must equal N exactly.

    **Validates: Requirements 2.7, 2.8**
    """
    collector = MetricsCollector()
    tasks = [asyncio.create_task(collector.record_run("pipe_b")) for _ in range(n)]
    await asyncio.gather(*tasks)
    stats = collector.pipeline_stats("pipe_b")
    assert stats is not None
    assert stats.successful_runs == n, f"Expected successful_runs={n}, got {stats.successful_runs}"
    assert stats.failed_runs == 0, f"Expected failed_runs=0, got {stats.failed_runs}"


@pytest.mark.asyncio
@pytest.mark.parametrize("n", [1, 5, 10, 25, 50])
async def test_concurrent_record_run_mixed_success_failure(n: int) -> None:
    """Property 5: N/2 success + N/2 failure concurrent calls → counts sum to N.

    successful_runs + failed_runs must always equal total_runs regardless of
    concurrent interleaving.

    **Validates: Requirements 2.7, 2.8**
    """
    collector = MetricsCollector()
    half = n // 2
    remainder = n - half

    success_tasks = [asyncio.create_task(collector.record_run("pipe_c")) for _ in range(half)]
    failure_tasks = [
        asyncio.create_task(collector.record_run("pipe_c", error=RuntimeError("err")))
        for _ in range(remainder)
    ]
    await asyncio.gather(*success_tasks, *failure_tasks)

    stats = collector.pipeline_stats("pipe_c")
    assert stats is not None
    assert stats.total_runs == n, f"Expected total_runs={n}, got {stats.total_runs}"
    assert stats.successful_runs + stats.failed_runs == stats.total_runs, (
        f"successful_runs ({stats.successful_runs}) + failed_runs ({stats.failed_runs}) "
        f"!= total_runs ({stats.total_runs})"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("n", [1, 5, 10, 25, 50])
async def test_concurrent_record_run_multiple_pipelines_isolated(n: int) -> None:
    """Property 5: Concurrent calls to different pipeline_ids are isolated.

    N concurrent calls to pipeline_a and N concurrent calls to pipeline_b
    must each result in total_runs == N independently.

    **Validates: Requirements 2.7, 2.8, 3.7**
    """
    collector = MetricsCollector()
    tasks_a = [asyncio.create_task(collector.record_run("pipe_a")) for _ in range(n)]
    tasks_b = [asyncio.create_task(collector.record_run("pipe_b")) for _ in range(n)]
    await asyncio.gather(*tasks_a, *tasks_b)

    stats_a = collector.pipeline_stats("pipe_a")
    stats_b = collector.pipeline_stats("pipe_b")

    assert stats_a is not None and stats_b is not None
    assert stats_a.total_runs == n, f"pipe_a: Expected total_runs={n}, got {stats_a.total_runs}"
    assert stats_b.total_runs == n, f"pipe_b: Expected total_runs={n}, got {stats_b.total_runs}"
