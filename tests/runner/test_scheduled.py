"""
tests/runner/test_scheduled.py
================================
Tests for Schedule and ScheduledPipeline.
All tests are purely in-process — no external services needed.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from agora.core.metrics import PipelineRunSummary
from agora.runner.policies import ExponentialBackoffPolicy
from agora.runner.scheduled import RunRecord, Schedule, ScheduledPipeline
from agora.runner.worker import WorkerPool

# ======================================================================
# Schedule
# ======================================================================


class TestSchedule:
    def test_every_seconds(self) -> None:
        s = Schedule.every(seconds=30)
        assert str(s) == "every 30s" or str(s) == "every 30.0s"  # int or float both OK
        assert s.should_repeat

    def test_every_minutes(self) -> None:
        s = Schedule.every(minutes=5)
        assert s._interval_s == 300.0

    def test_every_hours(self) -> None:
        s = Schedule.every(hours=1)
        assert s._interval_s == 3600.0

    def test_every_days(self) -> None:
        s = Schedule.every(days=1)
        assert s._interval_s == 86400.0

    def test_every_combined(self) -> None:
        s = Schedule.every(hours=1, minutes=30)
        assert s._interval_s == 5400.0

    def test_every_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="positive duration"):
            Schedule.every()

    def test_continuous(self) -> None:
        s = Schedule.continuous()
        assert s._mode == "continuous"
        assert s.should_repeat

    def test_once(self) -> None:
        s = Schedule.once()
        assert s._mode == "once"
        assert not s.should_repeat

    async def test_continuous_wait_is_immediate(self) -> None:
        """continuous() wait should yield control but not actually sleep."""
        s = Schedule.continuous()
        start = time.monotonic()
        await s.wait_until_next()
        elapsed = time.monotonic() - start
        assert elapsed < 0.1

    async def test_interval_wait_interruptible(self) -> None:
        """A long interval is interrupted by the stop_event."""
        s = Schedule.every(hours=1)
        stop = asyncio.Event()
        asyncio.get_event_loop().call_later(0.05, stop.set)
        start = time.monotonic()
        await s.wait_until_next(stop_event=stop)
        assert time.monotonic() - start < 1.0  # woke up early


# ======================================================================
# ScheduledPipeline — with a fake BoundPipeline
# ======================================================================


def _make_fake_summary(**kwargs) -> PipelineRunSummary:
    defaults = {
        "pipeline_id": "test",
        "run_id": "run-1",
        "records_consumed": 5,
        "records_written": 5,
        "records_dropped": 0,
        "records_errored": 0,
        "elapsed_seconds": 0.01,
        "by_source": {},
        "by_middleware": {},
    }
    defaults.update(kwargs)
    return PipelineRunSummary(**defaults)


def _make_factory(raises: Exception | None = None, n_runs: int = 1):
    """Return a factory that yields a fake BoundPipeline."""
    call_count = 0

    async def factory():
        nonlocal call_count
        call_count += 1

        class FakePipeline:
            async def run(self, max_records=None):
                if raises is not None and call_count <= n_runs:
                    raise raises
                return _make_fake_summary()

        return FakePipeline()

    return factory, lambda: call_count


class TestScheduledPipeline:
    def test_exponential_backoff_policy_caps_delay(self) -> None:
        policy = ExponentialBackoffPolicy(base_delay_seconds=10.0, max_delay_seconds=25.0)
        assert policy.next_delay(1) == 10.0
        assert policy.next_delay(2) == 20.0
        assert policy.next_delay(3) == 25.0

    async def test_once_runs_and_stops(self) -> None:
        factory, get_count = _make_factory()
        sp = ScheduledPipeline(factory=factory, schedule=Schedule.once())
        await sp.start()
        assert sp.run_count == 1
        assert get_count() == 1

    async def test_last_run_is_populated(self) -> None:
        factory, _ = _make_factory()
        sp = ScheduledPipeline(factory=factory, schedule=Schedule.once())
        await sp.start()
        assert sp.last_run is not None
        assert sp.last_run.ok
        assert sp.last_run.summary is not None

    async def test_stop_exits_continuous(self) -> None:
        """stop() should terminate a continuous pipeline cleanly."""
        runs = 0

        async def factory():
            nonlocal runs

            class FakePipeline:
                async def run(self, max_records=None):
                    nonlocal runs
                    runs += 1
                    await asyncio.sleep(0)
                    return _make_fake_summary()

            return FakePipeline()

        sp = ScheduledPipeline(factory=factory, schedule=Schedule.continuous())
        task = asyncio.create_task(sp.start())
        # Let it run a few times
        await asyncio.sleep(0.05)
        sp.stop()
        await task
        assert runs >= 1

    async def test_error_increments_consecutive_errors(self) -> None:
        factory, _ = _make_factory(raises=RuntimeError("fail"), n_runs=99)
        sp = ScheduledPipeline(
            factory=factory,
            schedule=Schedule.once(),
            error_backoff_seconds=0.0,
        )
        await sp.start()
        assert sp.run_count == 1
        assert sp.last_run is not None
        assert not sp.last_run.ok
        assert sp.last_run.error is not None

    async def test_max_consecutive_errors_stops_scheduler(self) -> None:
        factory, _ = _make_factory(raises=RuntimeError("fail"), n_runs=99)
        sp = ScheduledPipeline(
            factory=factory,
            schedule=Schedule.every(seconds=0.001),
            error_backoff_seconds=0.0,
            max_consecutive_errors=2,
        )
        await sp.start()
        assert sp.run_count >= 2
        assert sp._state.consecutive_errors >= 2

    async def test_observer_called_on_each_run(self) -> None:
        factory, _ = _make_factory()
        observed: list[RunRecord] = []

        async def observer(record: RunRecord) -> None:
            observed.append(record)

        sp = ScheduledPipeline(factory=factory, schedule=Schedule.once())
        sp.add_observer(observer)
        await sp.start()
        assert len(observed) == 1
        assert observed[0].ok

    async def test_on_run_complete_called(self) -> None:
        factory, _ = _make_factory()
        completed: list[RunRecord] = []

        async def on_complete(record: RunRecord) -> None:
            completed.append(record)

        sp = ScheduledPipeline(
            factory=factory,
            schedule=Schedule.once(),
            on_run_complete=on_complete,
        )
        await sp.start()
        assert len(completed) == 1

    async def test_error_backoff_is_interruptible(self) -> None:
        """stop() must wake error backoff immediately (B1 fix).

        Flow: pipeline fails → _run_once starts 60s backoff → we call stop()
        → backoff wakes early → scheduler exits. Total time should be << 60s.
        """

        # Factory that always fails
        async def factory():
            class FailPipeline:
                async def run(self, max_records=None):
                    raise RuntimeError("always fails")

            return FailPipeline()

        sp = ScheduledPipeline(
            factory=factory,
            schedule=Schedule.every(seconds=60),  # long interval (never reached)
            error_backoff_seconds=60.0,  # long backoff — will be interrupted
            max_consecutive_errors=99,  # don't auto-stop on errors
        )

        start = time.monotonic()
        task = asyncio.create_task(sp.start())

        # Wait for the first run to fail and start the backoff sleep
        await asyncio.sleep(0.1)

        # Stop should interrupt the 60s backoff immediately
        sp.stop()
        await task

        elapsed = time.monotonic() - start
        # Should exit well under 2 seconds, not wait the full 60s backoff
        assert elapsed < 2.0, f"stop() took {elapsed:.1f}s — backoff not interruptible!"

    async def test_custom_backoff_policy_is_used(self) -> None:
        waits: list[int] = []

        class FixedBackoffPolicy:
            def next_delay(self, consecutive_errors: int) -> float:
                waits.append(consecutive_errors)
                return 0.0

        factory, _ = _make_factory(raises=RuntimeError("fail"), n_runs=99)
        sp = ScheduledPipeline(
            factory=factory,
            schedule=Schedule.every(seconds=0.001),
            max_consecutive_errors=2,
            backoff_policy=FixedBackoffPolicy(),
        )
        await sp.start()
        assert waits == [1, 2]


class TestWorkerPool:
    async def test_worker_pool_returns_when_all_once_pipelines_complete(self) -> None:
        async def factory():
            class SuccessPipeline:
                async def run(self, max_records=None):
                    return _make_fake_summary(records_consumed=1, records_written=1)

            return SuccessPipeline()

        pool = WorkerPool()
        pool.register(
            ScheduledPipeline(factory=factory, schedule=Schedule.once(), pipeline_id="once_a")
        )
        pool.register(
            ScheduledPipeline(factory=factory, schedule=Schedule.once(), pipeline_id="once_b")
        )

        await asyncio.wait_for(pool.run(), timeout=1.0)

    async def test_worker_pool_raises_when_pipeline_stops_after_failure(self) -> None:
        async def failing_factory():
            class FailingPipeline:
                async def run(self, max_records=None):
                    raise RuntimeError("boom")

            return FailingPipeline()

        async def slow_factory():
            class SlowPipeline:
                async def run(self, max_records=None):
                    await asyncio.sleep(10.0)
                    return _make_fake_summary(records_consumed=1, records_written=1)

            return SlowPipeline()

        pool = WorkerPool(graceful_shutdown_timeout=0.01)
        pool.register(
            ScheduledPipeline(
                factory=failing_factory,
                schedule=Schedule.once(),
                pipeline_id="fail_once",
                error_backoff_seconds=0.0,
                max_consecutive_errors=1,
            )
        )
        pool.register(
            ScheduledPipeline(factory=slow_factory, schedule=Schedule.once(), pipeline_id="slow")
        )

        with pytest.raises(RuntimeError, match="boom"):
            await asyncio.wait_for(pool.run(), timeout=1.0)

    async def test_worker_pool_fails_fast_when_health_server_crashes(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def factory():
            class SlowPipeline:
                async def run(self, max_records=None):
                    await asyncio.sleep(1.0)
                    return _make_fake_summary(records_consumed=1, records_written=1)

            return SlowPipeline()

        class FailingHealthServer:
            def __init__(self, *args, **kwargs) -> None:
                del args, kwargs

            async def serve(self) -> None:
                raise RuntimeError("health exploded")

            def stop(self) -> None:
                return None

        import agora.health

        monkeypatch.setattr(agora.health, "HealthServer", FailingHealthServer)

        pool = WorkerPool(health_port=8080, graceful_shutdown_timeout=0.01)
        pool.register(
            ScheduledPipeline(factory=factory, schedule=Schedule.once(), pipeline_id="once_a")
        )

        with pytest.raises(RuntimeError, match="health exploded"):
            await pool.run()

    async def test_worker_pool_repeated_run_does_not_duplicate_metrics_observers(self) -> None:
        async def factory():
            class SuccessPipeline:
                async def run(self, max_records=None):
                    return _make_fake_summary(records_consumed=1, records_written=1)

            return SuccessPipeline()

        scheduled = ScheduledPipeline(
            factory=factory,
            schedule=Schedule.once(),
            pipeline_id="once_repeat",
        )
        pool = WorkerPool()
        pool.register(scheduled)

        await pool.run()
        await pool.run()

        stats = pool.metrics.get("once_repeat")
        assert stats is not None
        assert stats.total_runs == 2
        assert stats.successful_runs == 2
        assert len(scheduled._observers) == 1

    async def test_worker_pool_repeated_run_does_not_duplicate_release_observers(self) -> None:
        class FakeCoordinator:
            def __init__(self) -> None:
                self.acquire_calls: list[tuple[str, int]] = []
                self.release_calls: list[str] = []
                self.start_calls = 0
                self.stop_calls = 0

            async def start(self, worker_id: str, pipeline_ids: list[str]) -> None:
                del worker_id, pipeline_ids
                self.start_calls += 1

            async def stop(self) -> None:
                self.stop_calls += 1

            async def try_acquire_lease(self, pipeline_id: str, run_number: int) -> bool:
                self.acquire_calls.append((pipeline_id, run_number))
                return True

            async def release_lease(self, pipeline_id: str) -> None:
                self.release_calls.append(pipeline_id)

            async def list_workers(self):
                return []

        async def factory():
            class SuccessPipeline:
                async def run(self, max_records=None):
                    return _make_fake_summary(records_consumed=1, records_written=1)

            return SuccessPipeline()

        coordinator = FakeCoordinator()
        scheduled = ScheduledPipeline(
            factory=factory,
            schedule=Schedule.once(),
            pipeline_id="once_coord",
        )
        pool = WorkerPool(coordinator=coordinator)
        pool.register(scheduled)

        await pool.run()
        await pool.run()

        assert coordinator.start_calls == 2
        assert coordinator.stop_calls == 2
        assert coordinator.acquire_calls == [("once_coord", 1), ("once_coord", 2)]
        assert coordinator.release_calls == ["once_coord", "once_coord"]
        assert len(scheduled._observers) == 2
