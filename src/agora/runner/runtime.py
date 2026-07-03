"""Runtime helpers for scheduled pipeline execution."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import logstruct

if TYPE_CHECKING:
    from agora.core.fencing import RunFence
    from agora.core.metrics import PipelineRunSummary

logger = logstruct.getLogger(__name__)


async def interruptible_sleep(
    seconds: float,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Sleep for *seconds*, waking early if *stop_event* is set."""
    if stop_event is None or seconds <= 0:
        await asyncio.sleep(max(seconds, 0))
        return
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)


@dataclass
class RunRecord:
    """Stats from a single scheduled pipeline run."""

    run_number: int
    started_at: float
    summary: PipelineRunSummary | None = None
    error: BaseException | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


@dataclass
class ScheduledPipelineState:
    """Mutable scheduler state separated from pipeline configuration."""

    run_number: int = 0
    consecutive_errors: int = 0
    history: deque[RunRecord] = field(default_factory=lambda: deque(maxlen=100))
    running: bool = False
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    abort_event: asyncio.Event = field(default_factory=asyncio.Event)
    run_fence: RunFence | None = None

    @property
    def last_run(self) -> RunRecord | None:
        return self.history[-1] if self.history else None


class LeaseLost(Exception):  # noqa: N818
    """Raised internally when a run is aborted because its lease was lost."""


class ScheduledPipelineRunner:
    """Execute the loop for a configured ``ScheduledPipeline``."""

    def __init__(self, pipeline: Any) -> None:
        self._pipeline = pipeline

    async def run(self) -> None:
        state = self._pipeline.state
        state.running = True
        state.stop_event.clear()

        logger.info(
            "scheduler_start",
            pipeline=self._pipeline.pipeline_id,
            schedule=str(self._pipeline.schedule),
        )

        try:
            while not state.stop_event.is_set():
                ran = await self._run_once()

                if not self._should_continue():
                    break

                if ran:
                    await self._pipeline.schedule.wait_until_next(state.stop_event)
                else:
                    await self._pipeline.schedule.wait_after_skip(state.stop_event)

        except asyncio.CancelledError:
            logger.info("scheduler_cancelled", pipeline=self._pipeline.pipeline_id)
        finally:
            state.running = False
            logger.info(
                "scheduler_stopped",
                pipeline=self._pipeline.pipeline_id,
                total_runs=state.run_number,
            )

    def _should_continue(self) -> bool:
        state = self._pipeline.state
        if not self._pipeline.schedule.should_repeat:
            return False

        if state.consecutive_errors < self._pipeline.max_consecutive_errors:
            return True

        logger.error(
            "scheduler_max_errors_reached",
            pipeline=self._pipeline.pipeline_id,
            consecutive=state.consecutive_errors,
        )
        return False

    async def _notify_run_complete(self, record: RunRecord) -> None:
        on_run_complete = self._pipeline.on_run_complete
        if on_run_complete is not None:
            try:
                await on_run_complete(record)
            except Exception as cb_exc:
                logger.warning("scheduler_callback_error", error=str(cb_exc))

        for observer in self._pipeline.observers:
            try:
                await observer(record)
            except Exception as obs_exc:
                logger.warning("scheduler_observer_error", error=str(obs_exc))

    async def _handle_run_failure(self, record: RunRecord, exc: Exception) -> None:
        state = self._pipeline.state
        record.error = exc
        state.consecutive_errors += 1

        logger.exception(
            "scheduler_run_error",
            pipeline=self._pipeline.pipeline_id,
            run=state.run_number,
            consecutive_errors=state.consecutive_errors,
            error=str(exc),
        )

        wait = self._pipeline.backoff_policy.next_delay(state.consecutive_errors)
        logger.info("scheduler_error_backoff", wait_s=round(wait, 1))
        await interruptible_sleep(wait, state.stop_event)

    def _handle_run_success(self, record: RunRecord, summary: PipelineRunSummary) -> None:
        state = self._pipeline.state
        record.summary = summary
        state.consecutive_errors = 0

        logger.info(
            "scheduler_run_done",
            pipeline=self._pipeline.pipeline_id,
            run=state.run_number,
            consumed=summary.records_consumed,
            written=summary.records_written,
            elapsed=round(summary.elapsed_seconds, 1),
        )

    async def _run_once(self) -> bool:
        state = self._pipeline.state

        hook = self._pipeline.pre_run_hook
        if hook is not None and not await hook():
            return False

        state.run_number += 1
        record = RunRecord(run_number=state.run_number, started_at=time.monotonic())
        state.abort_event.clear()

        logger.info(
            "scheduler_run_start",
            pipeline=self._pipeline.pipeline_id,
            run=state.run_number,
        )

        try:
            pipeline = await self._pipeline.build()
            live_metrics_callback = self._pipeline.live_metrics_callback
            if live_metrics_callback is not None and hasattr(pipeline, "set_live_metrics_callback"):
                pipeline.set_live_metrics_callback(live_metrics_callback)
            if hasattr(pipeline, "set_run_fence"):
                pipeline.set_run_fence(state.run_fence)
            summary = await self._run_with_abort(pipeline, state)
            self._handle_run_success(record, summary)
        except asyncio.CancelledError as exc:
            record.error = exc
            raise
        except LeaseLost as exc:
            record.error = exc
            logger.warning(
                "scheduler_run_aborted_lease_lost",
                pipeline=self._pipeline.pipeline_id,
                run=state.run_number,
            )
        except Exception as exc:
            await self._handle_run_failure(record, exc)
        finally:
            state.history.append(record)
            await self._notify_run_complete(record)
            state.run_fence = None

        return True

    async def _run_with_abort(self, pipeline: Any, state: ScheduledPipelineState) -> Any:
        """Run *pipeline*, cancelling it if the lease-lost abort fires first."""
        run_task = asyncio.ensure_future(pipeline.run(max_records=self._pipeline.max_records))
        abort_task = asyncio.ensure_future(state.abort_event.wait())
        try:
            done, _pending = await asyncio.wait(
                {run_task, abort_task}, return_when=asyncio.FIRST_COMPLETED
            )
            if run_task in done:
                return run_task.result()
            run_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await run_task
            raise LeaseLost(self._pipeline.pipeline_id)
        finally:
            abort_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await abort_task


__all__ = [
    "LeaseLost",
    "RunRecord",
    "ScheduledPipelineRunner",
    "ScheduledPipelineState",
    "interruptible_sleep",
]
