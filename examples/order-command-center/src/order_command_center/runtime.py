"""Projection worker runtime and a deliberate crash-window fault injector.

This module is the operational boundary of the example.  A projection owns one
``WorkerPool`` and one native Agora health/metrics surface.  PostgreSQL and
Redis therefore remain independently deployable and recoverable while sharing
the same lifecycle, retry and reporting contract.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from time import monotonic
from typing import TYPE_CHECKING, Any

from agora.metrics import MetricsCollector
from agora.runner import Schedule, ScheduledPipeline, WorkerPool

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from pathlib import Path


class FailOnceAfterFlush:
    """Inject one failure after sink durability and before Kafka acknowledgement.

    ``hard_exit`` deliberately terminates the worker without running cleanup.
    It models the only crash boundary that matters for replay: a process loss
    after the sink is durable, but before the source can commit its offset.
    """

    def __init__(self, sink: Any, marker: Path, *, hard_exit: bool = False) -> None:
        self._sink = sink
        self._marker = marker
        self._hard_exit = hard_exit

    async def open(self) -> None:
        await self._sink.open()

    async def close(self) -> None:
        await self._sink.close()

    async def write(self, record: object) -> None:
        await self._sink.write(record)

    async def flush(self) -> None:
        await self._sink.flush()
        if not self._marker.exists():
            self._marker.parent.mkdir(parents=True, exist_ok=True)
            self._marker.write_text("sink flushed before Kafka acknowledgement\n", encoding="utf-8")
            if self._hard_exit:
                os._exit(75)
            raise RuntimeError("DEMO crash window: sink flushed, Kafka acknowledgement not reached")

    def __getattr__(self, name: str) -> object:
        return getattr(self._sink, name)


class FlushBeforeAcknowledgeSink:
    """Make a sink write durable before Agora runs the source success hook.

    KafkaSource exposes an acknowledgement hook to Agora's delivery engine.
    A backend sink may buffer ``write`` calls, so returning from ``write`` is
    not enough to safely advance Kafka. This decorator flushes each delivery
    batch before the engine receives a successful result and can acknowledge
    the corresponding source records.
    """

    def __init__(self, sink: Any) -> None:
        self._sink = sink

    async def open(self) -> None:
        await self._sink.open()

    async def close(self) -> None:
        await self._sink.close()

    async def write(self, record: object) -> None:
        await self._sink.write(record)
        await self._sink.flush()

    async def write_batch(self, records: list[object]) -> object:
        write_batch = getattr(self._sink, "write_batch", None)
        if callable(write_batch):
            result = await write_batch(records)
        else:
            for record in records:
                await self._sink.write(record)
            result = None
        await self._sink.flush()
        return result

    async def flush(self) -> None:
        await self._sink.flush()

    def __getattr__(self, name: str) -> object:
        return getattr(self._sink, name)


@dataclass(frozen=True, slots=True)
class ProjectionSpec:
    """Stable operational contract for one independently deployed projection."""

    pipeline_id: str
    process_name: str
    consumer_group: str
    metrics_host: str
    metrics_port: int | None
    metrics_auth_token: str | None
    idle_log_interval_seconds: int
    error_backoff_seconds: float
    max_consecutive_errors: int


class ProjectionRuntime:
    """Run one projection using Agora's native worker lifecycle.

    One runtime intentionally owns one projection only.  Sharing a process
    would couple PostgreSQL ledger durability to Redis cache availability and
    make lag, replay and rollout actions ambiguous.
    """

    def __init__(self, spec: ProjectionSpec) -> None:
        self._spec = spec

    async def run(
        self,
        *,
        build_pipeline: Callable[[], Awaitable[object]],
        max_records: int | None,
        forever: bool,
        emit_report: bool,
    ) -> int:
        delivered = 0
        completed_runs = 0
        failed_runs = 0
        idle_runs = 0
        idle_started_at: float | None = None

        def emit(event: str, **fields: object) -> None:
            print(
                json.dumps(
                    {
                        "event": event,
                        "projection": self._spec.pipeline_id,
                        "consumer_group": self._spec.consumer_group,
                        **fields,
                    },
                    default=str,
                ),
                flush=True,
            )

        async def report_run(record: object) -> None:
            nonlocal delivered, completed_runs, failed_runs, idle_runs, idle_started_at
            summary = getattr(record, "summary", None)
            error = getattr(record, "error", None)
            run_number = getattr(record, "run_number", None)
            if error is not None:
                failed_runs += 1
                emit(
                    "projection_run_failed",
                    run=run_number,
                    error_type=type(error).__name__,
                    error=str(error),
                )
                return
            if summary is None:
                return
            completed_runs += 1
            consumed = int(getattr(summary, "records_consumed", 0))
            written = int(getattr(summary, "records_written", 0))
            errored = int(getattr(summary, "records_errored", 0))
            delivered += written
            if forever and consumed == written == errored == 0:
                idle_runs += 1
                now = monotonic()
                if idle_started_at is None:
                    idle_started_at = now
                if now - idle_started_at < self._spec.idle_log_interval_seconds:
                    return
                emit(
                    "projection_idle",
                    idle_runs=idle_runs,
                    idle_seconds=round(now - idle_started_at, 3),
                )
                idle_runs = 0
                idle_started_at = now
                return

            idle_runs = 0
            idle_started_at = None
            emit(
                "projection_run_completed",
                run=run_number,
                records_consumed=consumed,
                records_written=written,
                records_errored=errored,
                elapsed_seconds=round(float(getattr(summary, "elapsed_seconds", 0)), 3),
            )

        schedule = Schedule.continuous() if forever else Schedule.once()
        scheduled = ScheduledPipeline(
            factory=build_pipeline,
            schedule=schedule,
            pipeline_id=self._spec.pipeline_id,
            max_records=max_records,
            error_backoff_seconds=self._spec.error_backoff_seconds,
            max_consecutive_errors=self._spec.max_consecutive_errors,
        )
        scheduled.add_observer(report_run)

        collector = MetricsCollector(process_name=self._spec.process_name)
        worker = WorkerPool(
            health_port=self._spec.metrics_port,
            health_host=self._spec.metrics_host,
            health_auth_token=self._spec.metrics_auth_token,
            metrics=collector,
        )
        worker.register(scheduled)
        print(
            json.dumps(
                {
                    "event": "projection_worker_starting",
                    "projection": self._spec.pipeline_id,
                    "consumer_group": self._spec.consumer_group,
                    "mode": "continuous" if forever else "once",
                    "metrics_port": self._spec.metrics_port,
                    "max_consecutive_errors": self._spec.max_consecutive_errors,
                }
            ),
            flush=True,
        )
        await worker.run()
        if emit_report:
            print(
                json.dumps(
                    {
                        "projection": self._spec.pipeline_id,
                        "delivered": delivered,
                        "completed_runs": completed_runs,
                        "failed_runs": failed_runs,
                    },
                    indent=2,
                )
            )
        return delivered
