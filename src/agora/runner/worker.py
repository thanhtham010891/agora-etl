"""
agora/runner/worker.py
=======================
``WorkerPool`` — run multiple ``ScheduledPipeline``s concurrently.

The worker pool is the top-level runtime for ``agora worker``.
It starts all registered pipelines as concurrent asyncio tasks, exposes
a metrics collector, and optionally serves a health HTTP server.

Usage::

    pool = WorkerPool(health_port=8080)
    pool.register(ScheduledPipeline(factory=build_ingest, schedule=Schedule.every(hours=6)))
    pool.register(ScheduledPipeline(factory=build_consumer, schedule=Schedule.continuous()))
    await pool.run()  # blocks until all done or Ctrl+C

``agora worker`` CLI uses this internally.

Shutdown sequence
-----------------
1. SIGINT/SIGTERM received → ``_shutdown_event`` is set
2. All ``ScheduledPipeline.stop()`` are called → sleeping pipelines wake up
3. ``HealthServer.stop()`` is signalled
4. ``WorkerPool`` waits up to ``graceful_shutdown_timeout`` for tasks to finish
5. Remaining tasks are force-cancelled
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
from contextlib import suppress
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import logstruct

from agora.metrics.collector import MetricsCollector

if TYPE_CHECKING:
    from agora.core.context import PipelineContext
    from agora.health import HealthServer
    from agora.runner.coordinator import WorkerCoordinator
    from agora.runner.runtime import RunRecord
    from agora.runner.scheduled import ScheduledPipeline

logger = logstruct.getLogger(__name__)


class WorkerPool:
    """Concurrent runner for multiple ``ScheduledPipeline``s.

    Parameters
    ----------
    graceful_shutdown_timeout:
        Seconds to wait for running pipelines to finish after stop()
        before force-cancelling (default: 30s).
    health_port:
        If set, starts a ``HealthServer`` on this port exposing
        ``/health``, ``/metrics``, and ``/ready`` endpoints.
        Default: ``None`` (no health server).
    metrics:
        Shared ``MetricsCollector`` instance.  Created automatically
        if not provided.  Inject a custom one for testing.
    """

    def __init__(
        self,
        graceful_shutdown_timeout: float = 30.0,
        health_port: int | None = None,
        health_host: str = "127.0.0.1",
        health_auth_token: str | None = None,
        metrics: MetricsCollector | None = None,
        coordinator: WorkerCoordinator | None = None,
    ) -> None:
        self._pipelines: list[ScheduledPipeline] = []
        self._shutdown_timeout = graceful_shutdown_timeout
        self._health_port = health_port
        self._health_host = health_host
        self._health_auth_token = health_auth_token
        self._tasks: list[asyncio.Task[None]] = []
        self._shutdown_event: asyncio.Event | None = None
        self._health_server: HealthServer | None = None
        self.metrics = metrics or MetricsCollector()
        self._coordinator = coordinator
        self._metrics_observers: dict[ScheduledPipeline, object] = {}
        self._lease_release_observers: dict[ScheduledPipeline, object] = {}

    def register(self, pipeline: ScheduledPipeline) -> WorkerPool:
        """Register a scheduled pipeline.  Returns self for chaining."""
        self._pipelines.append(pipeline)
        return self

    def registered_pipelines(self) -> list[ScheduledPipeline]:
        """Return a snapshot of registered pipelines."""
        return list(self._pipelines)

    def set_health_auth_token(self, token: str | None) -> None:
        """Override the health server auth token after construction."""
        self._health_auth_token = token

    # ------------------------------------------------------------------ #
    # Run                                                                  #
    # ------------------------------------------------------------------ #

    async def run(self) -> None:
        """Start all pipelines (+ optional health server).  Blocks until shutdown."""
        if not self._pipelines:
            logger.warning("worker_pool_empty")
            return

        self._shutdown_event = asyncio.Event()

        logger.info(
            "worker_pool_start",
            pipelines=[p.pipeline_id for p in self._pipelines],
            count=len(self._pipelines),
            health_port=self._health_port,
        )

        # Start distributed coordinator (if configured)
        if self._coordinator is not None:
            worker_id = f"{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"
            await self._coordinator.start(
                worker_id=worker_id,
                pipeline_ids=[p.pipeline_id for p in self._pipelines],
            )
            for p in self._pipelines:
                self._wire_lease_gating(p)

        # Wire metrics into each pipeline via public Observer API
        for p in self._pipelines:
            await self.metrics.register_pipeline(
                p.pipeline_id,
                schedule=str(p.schedule),
            )
            if p.live_metrics_callback is None:
                p.set_live_metrics_callback(self._make_live_metrics_callback(p.pipeline_id))
            if p not in self._metrics_observers:
                callback = self._make_metrics_callback(p.pipeline_id)
                p.add_observer(callback)
                self._metrics_observers[p] = callback

        # Install signal handlers (SIGINT + SIGTERM → graceful shutdown)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with suppress(NotImplementedError, RuntimeError):
                loop.add_signal_handler(sig, self._shutdown_event.set)

        # Launch pipeline tasks
        self._tasks = [asyncio.create_task(p.start(), name=p.pipeline_id) for p in self._pipelines]

        # Optional health server task
        if self._health_port is not None:
            from agora.health import HealthServer

            self._health_server = HealthServer(
                port=self._health_port,
                host=self._health_host,
                collector=self.metrics,
                auth_token=self._health_auth_token,
            )
            self._tasks.append(
                asyncio.create_task(self._health_server.serve(), name="health-server")
            )

        pipeline_tasks = list(self._tasks[: len(self._pipelines)])
        pipeline_task_map = dict(zip(pipeline_tasks, self._pipelines, strict=True))
        health_tasks = self._tasks[len(self._pipelines) :]
        shutdown_wait = asyncio.create_task(
            self._shutdown_event.wait(), name="worker-shutdown-wait"
        )

        try:
            pending: set[asyncio.Task[Any]] = {shutdown_wait, *pipeline_tasks, *health_tasks}
            while pending:
                done, pending = await asyncio.wait(
                    pending,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if shutdown_wait in done:
                    logger.info("worker_pool_shutdown_requested")
                    await self._graceful_stop()
                    break

                if any(task in done for task in health_tasks):
                    health_error = next(
                        (
                            task.exception()
                            for task in health_tasks
                            if task in done
                            and not task.cancelled()
                            and task.exception() is not None
                        ),
                        None,
                    )
                    await self._graceful_stop()
                    if health_error is not None:
                        logger.error("worker_pool_health_server_error", error=str(health_error))
                        raise health_error
                    raise RuntimeError("Health server stopped unexpectedly")

                pipeline_error = self._terminal_pipeline_error(done, pipeline_task_map)
                if pipeline_error is not None:
                    logger.error("worker_pool_pipeline_error", error=str(pipeline_error))
                    await self._graceful_stop()
                    raise pipeline_error

                if all(task.done() for task in pipeline_tasks):
                    logger.info("worker_pool_pipelines_complete")
                    if self._health_server is not None:
                        self._health_server.stop()
                    if health_tasks:
                        await asyncio.gather(*health_tasks, return_exceptions=True)
                    if self._coordinator is not None:
                        await self._coordinator.stop()
                    break
        except asyncio.CancelledError:
            # asyncio.run() was cancelled externally (e.g. double Ctrl+C)
            logger.info("worker_pool_cancelled")
            await self._force_cancel()
        finally:
            shutdown_wait.cancel()
            await asyncio.gather(shutdown_wait, return_exceptions=True)
            self._tasks = []
            self._health_server = None
            self._shutdown_event = None
            self._log_final_stats()

    # ------------------------------------------------------------------ #
    # Shutdown                                                             #
    # ------------------------------------------------------------------ #

    async def _graceful_stop(self) -> None:
        """Signal all pipelines to stop, wait for graceful_shutdown_timeout."""
        # 1. Signal each pipeline — sleeping ones wake up immediately
        for p in self._pipelines:
            p.stop()

        # 2. Signal health server
        if self._health_server is not None:
            self._health_server.stop()

        # 3. Wait up to timeout for all tasks to complete
        if not self._tasks:
            if self._coordinator is not None:
                await self._coordinator.stop()
            return

        _done, pending = await asyncio.wait(
            self._tasks,
            timeout=self._shutdown_timeout,
        )

        if pending:
            logger.warning(
                "worker_pool_force_cancel",
                remaining=len(pending),
                timeout=self._shutdown_timeout,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

        # 4. Release all leases and deregister from coordinator
        if self._coordinator is not None:
            await self._coordinator.stop()

    async def _force_cancel(self) -> None:
        """Immediately cancel all tasks (double Ctrl+C path)."""
        for task in self._tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._coordinator is not None:
            with suppress(Exception):
                await self._coordinator.stop()

    # ------------------------------------------------------------------ #
    # Metrics                                                              #
    # ------------------------------------------------------------------ #

    def _make_metrics_callback(self, pipeline_id: str) -> Any:
        async def _callback(record: RunRecord) -> None:
            await self.metrics.record_run(
                pipeline_id=pipeline_id,
                summary=record.summary,
                error=record.error,
            )

        return _callback

    def _make_live_metrics_callback(self, pipeline_id: str) -> Any:
        async def _callback(ctx: PipelineContext) -> None:
            summary = ctx.metrics.snapshot(
                pipeline_id=pipeline_id,
                run_id=ctx.run_id,
            )
            await self.metrics.record_live_run(
                pipeline_id=pipeline_id,
                summary=summary,
                run_id=ctx.run_id,
                started_at=ctx.started_at,
            )

        return _callback

    def _wire_lease_gating(self, pipeline: ScheduledPipeline) -> None:
        """Attach lease acquire/release hooks to a pipeline for distributed mode."""
        coordinator = self._coordinator
        assert coordinator is not None

        async def _pre_run_hook() -> bool:
            return await coordinator.try_acquire_lease(pipeline.pipeline_id, pipeline.run_count + 1)

        async def _release_observer(record: RunRecord) -> None:
            await coordinator.release_lease(pipeline.pipeline_id)

        pipeline.set_pre_run_hook(_pre_run_hook)
        if pipeline not in self._lease_release_observers:
            pipeline.add_observer(_release_observer)
            self._lease_release_observers[pipeline] = _release_observer

    def _terminal_pipeline_error(
        self,
        completed: set[asyncio.Task[None]],
        pipeline_task_map: dict[asyncio.Task[None], ScheduledPipeline],
    ) -> BaseException | None:
        for task, pipeline in pipeline_task_map.items():
            if task not in completed:
                continue
            if task.cancelled():
                continue
            task_error = task.exception()
            if task_error is not None:
                return task_error
            last_run = pipeline.last_run
            if last_run is not None and last_run.error is not None:
                return last_run.error
        return None

    def _log_final_stats(self) -> None:
        total_runs = sum(p.run_count for p in self._pipelines)
        logger.info(
            "worker_pool_stopped",
            pipelines=len(self._pipelines),
            total_runs=total_runs,
        )
        for p in self._pipelines:
            last = p.last_run
            if last and last.ok and last.summary:
                logger.info(
                    "worker_final_stats",
                    pipeline=p.pipeline_id,
                    total_runs=p.run_count,
                    last_consumed=last.summary.records_consumed,
                    last_written=last.summary.records_written,
                )
