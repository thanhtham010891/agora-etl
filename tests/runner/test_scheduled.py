"""
tests/runner/test_scheduled.py
================================
Tests for Schedule and ScheduledPipeline.
All tests are purely in-process — no external services needed.
"""

from __future__ import annotations

import asyncio
import signal
import time
from datetime import UTC, datetime

import pytest

from agora.core.fencing import FenceLostError
from agora.core.metrics import PipelineRunSummary
from agora.core.pipeline import Pipeline
from agora.core.sink import BaseSink
from agora.core.source import IterableSource
from agora.runner import LeaseState
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

    async def test_observers_property_returns_snapshot(self) -> None:
        factory, _ = _make_factory()
        called: list[str] = []

        async def observer(record: RunRecord) -> None:
            del record
            called.append("base")

        async def injected(record: RunRecord) -> None:
            del record
            called.append("injected")

        sp = ScheduledPipeline(factory=factory, schedule=Schedule.once())
        sp.add_observer(observer)
        observers = sp.observers
        observers.append(injected)

        await sp.start()

        assert called == ["base"]

    async def test_observer_added_during_notification_runs_on_next_run_only(self) -> None:
        factory, _ = _make_factory()
        called: list[str] = []
        added = False

        async def late_observer(record: RunRecord) -> None:
            del record
            called.append("late")

        async def observer(record: RunRecord) -> None:
            nonlocal added
            del record
            called.append("base")
            if not added:
                sp.add_observer(late_observer)
                added = True

        sp = ScheduledPipeline(factory=factory, schedule=Schedule.once())
        sp.add_observer(observer)

        await sp.start()
        assert called == ["base"]

        await sp.start()
        assert called == ["base", "base", "late"]

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

    async def test_skipped_interval_run_retries_on_schedule_cadence(self) -> None:
        build_calls = 0
        hook_calls = 0

        async def factory():
            nonlocal build_calls
            build_calls += 1
            raise AssertionError("factory should not run when pre_run_hook skips")

        sp = ScheduledPipeline(
            factory=factory,
            schedule=Schedule.every(seconds=0.01),
        )

        async def hook() -> bool:
            nonlocal hook_calls
            hook_calls += 1
            if hook_calls >= 2:
                sp.stop()
            return False

        sp.set_pre_run_hook(hook)

        start = time.monotonic()
        await asyncio.wait_for(sp.start(), timeout=0.5)
        elapsed = time.monotonic() - start

        assert hook_calls == 2
        assert build_calls == 0
        assert elapsed < 0.3


class TestWorkerPool:
    def test_worker_pool_rejects_duplicate_pipeline_instance(self) -> None:
        factory, _ = _make_factory()
        scheduled = ScheduledPipeline(
            factory=factory,
            schedule=Schedule.once(),
            pipeline_id="dup_instance",
        )
        pool = WorkerPool()

        pool.register(scheduled)

        with pytest.raises(ValueError, match="already registered"):
            pool.register(scheduled)

    def test_worker_pool_rejects_duplicate_pipeline_id(self) -> None:
        factory, _ = _make_factory()
        pool = WorkerPool()
        pool.register(
            ScheduledPipeline(
                factory=factory,
                schedule=Schedule.once(),
                pipeline_id="dup_id",
            )
        )

        with pytest.raises(ValueError, match="pipeline_id 'dup_id' is already registered"):
            pool.register(
                ScheduledPipeline(
                    factory=factory,
                    schedule=Schedule.once(),
                    pipeline_id="dup_id",
                )
            )

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

    async def test_worker_pool_stops_started_coordinator_when_metrics_registration_fails(
        self,
    ) -> None:
        class FailingMetrics:
            async def register_pipeline(self, pipeline_id: str, schedule: str) -> None:
                del pipeline_id, schedule
                raise RuntimeError("metrics exploded")

        class FakeCoordinator:
            def __init__(self) -> None:
                self.start_calls = 0
                self.stop_calls = 0
                self.callback = None

            async def start(self, worker_id: str, pipeline_ids: list[str]) -> None:
                del worker_id, pipeline_ids
                self.start_calls += 1

            async def stop(self) -> None:
                self.stop_calls += 1

            async def try_acquire_lease(self, pipeline_id: str, run_number: int) -> bool:
                del pipeline_id, run_number
                return True

            async def release_lease(self, pipeline_id: str) -> None:
                del pipeline_id

            def set_lease_lost_callback(self, callback) -> None:
                self.callback = callback

            async def list_workers(self):
                return []

        async def factory():
            class SuccessPipeline:
                async def run(self, max_records=None):
                    return _make_fake_summary(records_consumed=1, records_written=1)

            return SuccessPipeline()

        coordinator = FakeCoordinator()
        pool = WorkerPool(metrics=FailingMetrics(), coordinator=coordinator)
        pool.register(
            ScheduledPipeline(factory=factory, schedule=Schedule.once(), pipeline_id="once_a")
        )

        with pytest.raises(RuntimeError, match="metrics exploded"):
            await pool.run()

        assert coordinator.start_calls == 1
        assert coordinator.stop_calls == 1
        assert pool._tasks == []
        assert pool._shutdown_event is None

    async def test_worker_pool_cleans_up_when_health_server_init_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class LoopProxy:
            def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
                self._loop = loop
                self.added: list[signal.Signals] = []
                self.removed: list[signal.Signals] = []

            def add_signal_handler(self, sig: signal.Signals, callback, *args) -> None:
                del callback, args
                self.added.append(sig)

            def remove_signal_handler(self, sig: signal.Signals) -> bool:
                self.removed.append(sig)
                return True

            def __getattr__(self, name: str):
                return getattr(self._loop, name)

        class FakeCoordinator:
            def __init__(self) -> None:
                self.start_calls = 0
                self.stop_calls = 0

            async def start(self, worker_id: str, pipeline_ids: list[str]) -> None:
                del worker_id, pipeline_ids
                self.start_calls += 1

            async def stop(self) -> None:
                self.stop_calls += 1

            async def try_acquire_lease(self, pipeline_id: str, run_number: int) -> bool:
                del pipeline_id, run_number
                return True

            async def release_lease(self, pipeline_id: str) -> None:
                del pipeline_id

            def set_lease_lost_callback(self, callback) -> None:
                self.callback = callback

            async def list_workers(self):
                return []

        class FailingHealthServer:
            def __init__(self, *args, **kwargs) -> None:
                del args, kwargs
                raise RuntimeError("health init exploded")

        async def factory():
            class SuccessPipeline:
                async def run(self, max_records=None):
                    await asyncio.sleep(10.0)
                    return _make_fake_summary(records_consumed=1, records_written=1)

            return SuccessPipeline()

        import agora.health
        import agora.runner.worker as worker_module

        monkeypatch.setattr(agora.health, "HealthServer", FailingHealthServer)
        loop_proxy = LoopProxy(asyncio.get_running_loop())
        monkeypatch.setattr(worker_module.asyncio, "get_running_loop", lambda: loop_proxy)

        coordinator = FakeCoordinator()
        pool = WorkerPool(
            health_port=8080,
            graceful_shutdown_timeout=0.01,
            coordinator=coordinator,
        )
        pool.register(
            ScheduledPipeline(factory=factory, schedule=Schedule.once(), pipeline_id="once_a")
        )

        with pytest.raises(RuntimeError, match="health init exploded"):
            await pool.run()

        assert coordinator.start_calls == 1
        assert coordinator.stop_calls == 1
        assert pool._tasks == []
        assert pool._health_server is None
        assert pool._shutdown_event is None
        assert loop_proxy.added == [signal.SIGINT, signal.SIGTERM]
        assert loop_proxy.removed == [signal.SIGINT, signal.SIGTERM]

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

    async def test_worker_pool_registers_pipeline_in_metrics_before_run_completes(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        async def factory():
            class SlowPipeline:
                async def run(self, max_records=None):
                    del max_records
                    started.set()
                    await release.wait()
                    return _make_fake_summary(records_consumed=1, records_written=1)

            return SlowPipeline()

        pool = WorkerPool(graceful_shutdown_timeout=0.01)
        pool.register(
            ScheduledPipeline(factory=factory, schedule=Schedule.once(), pipeline_id="slow_once")
        )

        task = asyncio.create_task(pool.run())
        await asyncio.wait_for(started.wait(), timeout=1.0)

        stats = pool.metrics.get("slow_once")
        assert stats is not None
        assert stats.schedule == "once"

        release.set()
        await asyncio.wait_for(task, timeout=1.0)

    async def test_worker_pool_removes_signal_handlers_after_run(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class LoopProxy:
            def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
                self._loop = loop
                self.added: list[signal.Signals] = []
                self.removed: list[signal.Signals] = []

            def add_signal_handler(self, sig: signal.Signals, callback, *args) -> None:
                del callback, args
                self.added.append(sig)

            def remove_signal_handler(self, sig: signal.Signals) -> bool:
                self.removed.append(sig)
                return True

            def __getattr__(self, name: str):
                return getattr(self._loop, name)

        async def factory():
            class SuccessPipeline:
                async def run(self, max_records=None):
                    del max_records
                    return _make_fake_summary(records_consumed=1, records_written=1)

            return SuccessPipeline()

        import agora.runner.worker as worker_module

        loop_proxy = LoopProxy(asyncio.get_running_loop())
        monkeypatch.setattr(worker_module.asyncio, "get_running_loop", lambda: loop_proxy)

        pool = WorkerPool()
        pool.register(
            ScheduledPipeline(factory=factory, schedule=Schedule.once(), pipeline_id="signal_once")
        )

        await pool.run()

        assert loop_proxy.added == [signal.SIGINT, signal.SIGTERM]
        assert loop_proxy.removed == [signal.SIGINT, signal.SIGTERM]

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

            def set_lease_lost_callback(self, callback) -> None:
                self._lease_lost_callback = callback

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

    async def test_worker_pool_preserves_existing_pre_run_hook_before_lease_gating(self) -> None:
        class FakeCoordinator:
            def __init__(self) -> None:
                self.acquire_calls: list[tuple[str, int]] = []

            async def start(self, worker_id: str, pipeline_ids: list[str]) -> None:
                del worker_id, pipeline_ids

            async def stop(self) -> None:
                return None

            async def try_acquire_lease(self, pipeline_id: str, run_number: int) -> bool:
                self.acquire_calls.append((pipeline_id, run_number))
                return True

            async def release_lease(self, pipeline_id: str) -> None:
                del pipeline_id

            def set_lease_lost_callback(self, callback) -> None:
                self._lease_lost_callback = callback

            async def list_workers(self):
                return []

        hook_calls = 0
        build_calls = 0

        async def factory():
            nonlocal build_calls
            build_calls += 1
            raise AssertionError("factory should not run when user hook skips")

        async def user_hook() -> bool:
            nonlocal hook_calls
            hook_calls += 1
            return False

        scheduled = ScheduledPipeline(
            factory=factory,
            schedule=Schedule.once(),
            pipeline_id="coord_hook",
            pre_run_hook=user_hook,
        )
        pool = WorkerPool(coordinator=FakeCoordinator())
        pool.register(scheduled)

        await pool.run()

        assert hook_calls == 1
        assert build_calls == 0
        assert pool._coordinator is not None
        assert pool._coordinator.acquire_calls == []

    async def test_worker_pool_composes_live_metrics_callback_with_existing_callback(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        custom_called = asyncio.Event()

        class FakeMetrics:
            def snapshot(self, *, pipeline_id: str, run_id: str):
                return _make_fake_summary(
                    pipeline_id=pipeline_id,
                    run_id=run_id,
                    records_consumed=3,
                    records_written=2,
                    records_dropped=0,
                    records_errored=0,
                    elapsed_seconds=0.5,
                )

        class FakePipeline:
            def __init__(self) -> None:
                self._callback = None

            def set_live_metrics_callback(self, callback) -> None:
                self._callback = callback

            async def run(self, max_records=None):
                del max_records
                started.set()
                assert self._callback is not None

                class FakeCtx:
                    def __init__(self) -> None:
                        self.metrics = FakeMetrics()
                        self.run_id = "run-live"
                        self.started_at = datetime.now(UTC)

                await self._callback(FakeCtx())
                await release.wait()
                return _make_fake_summary(
                    pipeline_id="live_composed",
                    run_id="run-live",
                    records_consumed=3,
                    records_written=3,
                    records_dropped=0,
                    records_errored=0,
                    elapsed_seconds=0.5,
                )

        async def factory():
            return FakePipeline()

        async def custom_live_metrics_callback(ctx) -> None:
            del ctx
            custom_called.set()

        scheduled = ScheduledPipeline(
            factory=factory,
            schedule=Schedule.once(),
            pipeline_id="live_composed",
            live_metrics_callback=custom_live_metrics_callback,
        )
        pool = WorkerPool(graceful_shutdown_timeout=0.01)
        pool.register(scheduled)

        task = asyncio.create_task(pool.run())
        await asyncio.wait_for(started.wait(), timeout=1.0)
        await asyncio.wait_for(custom_called.wait(), timeout=1.0)

        stats = pool.metrics.get("live_composed")
        assert stats is not None
        assert stats.is_running is True
        assert stats.active_run_id == "run-live"
        assert stats.live_records_consumed == 3
        assert stats.live_records_written == 2

        release.set()
        await asyncio.wait_for(task, timeout=1.0)

    async def test_worker_pool_rewires_updated_pre_run_hook_without_losing_lease_gating(
        self,
    ) -> None:
        class FakeCoordinator:
            def __init__(self) -> None:
                self.acquire_calls: list[tuple[str, int]] = []
                self.release_calls: list[str] = []

            async def start(self, worker_id: str, pipeline_ids: list[str]) -> None:
                del worker_id, pipeline_ids

            async def stop(self) -> None:
                return None

            async def try_acquire_lease(self, pipeline_id: str, run_number: int) -> bool:
                self.acquire_calls.append((pipeline_id, run_number))
                return True

            async def release_lease(self, pipeline_id: str) -> None:
                self.release_calls.append(pipeline_id)

            def set_lease_lost_callback(self, callback) -> None:
                self._lease_lost_callback = callback

            async def list_workers(self):
                return []

        hook_calls = 0

        async def factory():
            class SuccessPipeline:
                async def run(self, max_records=None):
                    return _make_fake_summary(records_consumed=1, records_written=1)

            return SuccessPipeline()

        scheduled = ScheduledPipeline(
            factory=factory,
            schedule=Schedule.once(),
            pipeline_id="rewire_hook",
        )
        coordinator = FakeCoordinator()
        pool = WorkerPool(coordinator=coordinator)
        pool.register(scheduled)

        await pool.run()

        async def user_hook() -> bool:
            nonlocal hook_calls
            hook_calls += 1
            return True

        scheduled.set_pre_run_hook(user_hook)
        await pool.run()

        assert hook_calls == 1
        assert coordinator.acquire_calls == [("rewire_hook", 1), ("rewire_hook", 2)]
        assert coordinator.release_calls == ["rewire_hook", "rewire_hook"]

    async def test_worker_pool_rewires_updated_live_metrics_callback_without_losing_worker_metrics(
        self,
    ) -> None:
        first_release = asyncio.Event()
        second_started = asyncio.Event()
        second_release = asyncio.Event()
        updated_custom_called = asyncio.Event()

        class FakeMetrics:
            def __init__(self, *, consumed: int, written: int) -> None:
                self._consumed = consumed
                self._written = written

            def snapshot(self, *, pipeline_id: str, run_id: str):
                return _make_fake_summary(
                    pipeline_id=pipeline_id,
                    run_id=run_id,
                    records_consumed=self._consumed,
                    records_written=self._written,
                    records_dropped=0,
                    records_errored=0,
                    elapsed_seconds=0.5,
                )

        class FakePipeline:
            def __init__(
                self,
                *,
                started_event: asyncio.Event | None,
                release_event: asyncio.Event,
                consumed: int,
                written: int,
            ) -> None:
                self._callback = None
                self._started_event = started_event
                self._release_event = release_event
                self._metrics = FakeMetrics(consumed=consumed, written=written)

            def set_live_metrics_callback(self, callback) -> None:
                self._callback = callback

            async def run(self, max_records=None):
                del max_records
                if self._started_event is not None:
                    self._started_event.set()
                assert self._callback is not None

                class FakeCtx:
                    def __init__(self, metrics, run_id: str) -> None:
                        self.metrics = metrics
                        self.run_id = run_id
                        self.started_at = datetime.now(UTC)

                await self._callback(FakeCtx(self._metrics, "run-live"))
                await self._release_event.wait()
                return _make_fake_summary(
                    pipeline_id="rewire_live",
                    run_id="run-live",
                    records_consumed=self._metrics._consumed,
                    records_written=self._metrics._consumed,
                    records_dropped=0,
                    records_errored=0,
                    elapsed_seconds=0.5,
                )

        build_count = 0

        async def factory():
            nonlocal build_count
            build_count += 1
            if build_count == 1:
                return FakePipeline(
                    started_event=None,
                    release_event=first_release,
                    consumed=1,
                    written=1,
                )
            return FakePipeline(
                started_event=second_started,
                release_event=second_release,
                consumed=4,
                written=3,
            )

        scheduled = ScheduledPipeline(
            factory=factory,
            schedule=Schedule.once(),
            pipeline_id="rewire_live",
        )
        pool = WorkerPool(graceful_shutdown_timeout=0.01)
        pool.register(scheduled)

        first_task = asyncio.create_task(pool.run())
        await asyncio.sleep(0)
        first_release.set()
        await asyncio.wait_for(first_task, timeout=1.0)

        async def updated_custom_callback(ctx) -> None:
            del ctx
            updated_custom_called.set()

        scheduled.set_live_metrics_callback(updated_custom_callback)

        second_task = asyncio.create_task(pool.run())
        await asyncio.wait_for(second_started.wait(), timeout=1.0)
        await asyncio.wait_for(updated_custom_called.wait(), timeout=1.0)

        stats = pool.metrics.get("rewire_live")
        assert stats is not None
        assert stats.is_running is True
        assert stats.active_run_id == "run-live"
        assert stats.live_records_consumed == 4
        assert stats.live_records_written == 3

        second_release.set()
        await asyncio.wait_for(second_task, timeout=1.0)

    async def test_worker_pool_fence_blocks_write_when_coordinator_token_is_stale(self) -> None:
        class RecordingSink(BaseSink[int]):
            def __init__(self) -> None:
                self.records: list[int] = []

            async def write(self, record: int) -> None:
                self.records.append(record)

        class FakeCoordinator:
            def __init__(self) -> None:
                self.valid = False
                self.validate_calls: list[tuple[str, int]] = []
                self.release_calls: list[str] = []
                self.lease = LeaseState(
                    pipeline_id="fenced_once",
                    run_number=1,
                    worker_id="worker-a",
                    fencing_token=11,
                    acquired_at="now",
                )

            async def start(self, worker_id: str, pipeline_ids: list[str]) -> None:
                del worker_id, pipeline_ids

            async def stop(self) -> None:
                return None

            async def try_acquire_lease(self, pipeline_id: str, run_number: int) -> bool:
                self.lease = LeaseState(
                    pipeline_id=pipeline_id,
                    run_number=run_number,
                    worker_id="worker-a",
                    fencing_token=11,
                    acquired_at="now",
                )
                return True

            async def release_lease(self, pipeline_id: str) -> None:
                self.release_calls.append(pipeline_id)

            def current_lease(self, pipeline_id: str) -> LeaseState | None:
                return self.lease if pipeline_id == self.lease.pipeline_id else None

            async def validate_lease(self, pipeline_id: str, fencing_token: int) -> bool:
                self.validate_calls.append((pipeline_id, fencing_token))
                return self.valid

            def set_lease_lost_callback(self, callback) -> None:
                self._lease_lost_callback = callback

            async def list_workers(self):
                return []

        sink = RecordingSink()

        async def factory():
            return Pipeline(IterableSource([1]), id="fenced_once").build(sink)

        coordinator = FakeCoordinator()
        pool = WorkerPool(coordinator=coordinator)
        pool.register(
            ScheduledPipeline(
                factory=factory,
                schedule=Schedule.once(),
                pipeline_id="fenced_once",
                error_backoff_seconds=0.0,
                max_consecutive_errors=1,
            )
        )

        with pytest.raises(FenceLostError):
            await pool.run()

        assert sink.records == []
        assert coordinator.validate_calls == [("fenced_once", 11), ("fenced_once", 11)]
        assert coordinator.release_calls == ["fenced_once"]
