"""
agora/runner/scheduled.py
==========================
``ScheduledPipeline`` — run a pipeline on a schedule (interval or cron).

Design
------
A ``ScheduledPipeline`` wraps a **factory coroutine** (not a pipeline
instance) because pipelines are stateful and should be rebuilt each run.

    async def build() -> BoundPipeline:
        return await build_ingest_pipeline(extractor)

    scheduled = ScheduledPipeline(
        factory=build,
        schedule=Schedule.every(hours=6),
        pipeline_id="places_ingest",
    )
    await scheduled.start()   # runs forever, Ctrl+C to stop

Schedule types
--------------
    Schedule.every(seconds=N)       — fixed interval after completion
    Schedule.every(minutes=N)
    Schedule.every(hours=N)
    Schedule.cron("0 */6 * * *")   — cron expression (requires agora-etl-plugins[cron])
    Schedule.continuous()           — no delay; restart immediately
    Schedule.once()                 — run exactly once then stop

Error handling
--------------
On pipeline error: log, wait ``error_backoff_seconds``, retry.
On consecutive errors > ``max_consecutive_errors``: stop.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import logstruct

from agora.runner.policies import BackoffPolicy, ExponentialBackoffPolicy
from agora.runner.runtime import (
    RunRecord,
    ScheduledPipelineRunner,
    ScheduledPipelineState,
    interruptible_sleep,
)

if TYPE_CHECKING:
    from agora.core.pipeline import BoundPipeline

logger = logstruct.getLogger(__name__)

# Factory type: async callable that returns a BoundPipeline
PipelineFactory = Callable[[], Awaitable["BoundPipeline[Any]"]]


# ======================================================================
# Schedule — defines when to run
# ======================================================================


@dataclass(frozen=True)
class Schedule:
    """Defines the timing for a ``ScheduledPipeline``.

    Create via class methods — do not construct directly.
    """

    _mode: str  # "interval" | "cron" | "continuous" | "once"
    _interval_s: float  # seconds between runs (interval mode)
    _cron_expr: str  # cron expression (cron mode)

    # ------------------------------------------------------------------ #
    # Constructors                                                         #
    # ------------------------------------------------------------------ #

    @classmethod
    def every(
        cls,
        seconds: float = 0,
        minutes: float = 0,
        hours: float = 0,
        days: float = 0,
    ) -> Schedule:
        """Run every N seconds/minutes/hours/days after completion."""
        total = seconds + minutes * 60 + hours * 3600 + days * 86400
        if total <= 0:
            raise ValueError("Schedule.every() requires a positive duration")
        return cls(_mode="interval", _interval_s=total, _cron_expr="")

    @classmethod
    def cron(cls, expression: str) -> Schedule:
        """Run on a cron schedule, e.g. ``'0 */6 * * *'``."""
        try:
            from agora_plugins.cron import validate_cron_expression
        except ImportError:
            raise ImportError(
                "Cron schedules require 'agora-etl-plugins[cron]'. Install it: pip install 'agora-etl-plugins[cron]'"
            ) from None
        validate_cron_expression(expression)
        return cls(_mode="cron", _interval_s=0.0, _cron_expr=expression)

    @classmethod
    def continuous(cls) -> Schedule:
        """Restart immediately after each run completes (no delay)."""
        return cls(_mode="continuous", _interval_s=0.0, _cron_expr="")

    @classmethod
    def once(cls) -> Schedule:
        """Run exactly once then stop."""
        return cls(_mode="once", _interval_s=0.0, _cron_expr="")

    # ------------------------------------------------------------------ #
    # Runtime                                                              #
    # ------------------------------------------------------------------ #

    async def wait_until_next(self, stop_event: asyncio.Event | None = None) -> None:
        """Wait until the next scheduled run.

        Parameters
        ----------
        stop_event:
            When set, the wait is interrupted immediately regardless of the
            remaining time.  Pass ``ScheduledPipeline._stop_event`` so that
            ``stop()`` wakes up sleeping pipelines instantly.
        """
        if self._mode == "interval":
            await interruptible_sleep(self._interval_s, stop_event)
        elif self._mode == "cron":
            now = time.time()
            try:
                from agora_plugins.cron import seconds_until_next_run
            except ImportError:
                raise ImportError(
                    "Cron schedules require 'agora-etl-plugins'. Install it: pip install 'agora-etl-plugins[cron]'"
                ) from None
            next_run = now + seconds_until_next_run(self._cron_expr, now)
            wait = max(next_run - now, 0)
            logger.debug("schedule_cron_wait", seconds=round(wait, 1))
            await interruptible_sleep(wait, stop_event)
        elif self._mode == "continuous":
            await asyncio.sleep(0)  # yield control then restart
        # "once" — no wait needed (caller checks should_repeat)

    @property
    def should_repeat(self) -> bool:
        return self._mode != "once"

    def __str__(self) -> str:
        if self._mode == "interval":
            return f"every {self._interval_s}s"
        if self._mode == "cron":
            return f"cron({self._cron_expr!r})"
        return self._mode


# ======================================================================
# ScheduledPipeline
# ======================================================================


class ScheduledPipeline:
    """Run a pipeline on a repeating schedule.

    Parameters
    ----------
    factory:
        ``async () -> BoundPipeline`` — called before EACH run.
        This ensures the pipeline is freshly built every time.
    schedule:
        When to run (see ``Schedule``).
    pipeline_id:
        Human-readable name (shown in logs and ``agora worker`` status).
    max_records:
        Stop each run after N records (useful for batch producer runs).
    error_backoff_seconds:
        Wait time after a failed run before retrying (default: 60s).
    max_consecutive_errors:
        Stop the scheduler after N consecutive errors (default: 5).
    backoff_policy:
        Strategy object that computes retry delay after consecutive errors.
        Defaults to an exponential backoff capped at 10 minutes.
    on_run_complete:
        Optional async callback: ``(RunRecord) -> None``.
    """

    def __init__(
        self,
        factory: PipelineFactory,
        schedule: Schedule,
        pipeline_id: str = "scheduled",
        max_records: int | None = None,
        error_backoff_seconds: float = 60.0,
        max_consecutive_errors: int = 5,
        backoff_policy: BackoffPolicy | None = None,
        on_run_complete: Callable[[RunRecord], Awaitable[None]] | None = None,
        pre_run_hook: Callable[[], Awaitable[bool]] | None = None,
    ) -> None:
        self._factory = factory
        self._schedule = schedule
        self._pipeline_id = pipeline_id
        self._max_records = max_records
        self._error_backoff = error_backoff_seconds
        self._max_errors = max_consecutive_errors
        self._backoff_policy = backoff_policy or ExponentialBackoffPolicy(
            base_delay_seconds=error_backoff_seconds,
        )
        self._on_run_complete = on_run_complete
        # Called before each run; return False to skip this run without
        # counting as an error (used by WorkerPool for lease gating).
        self._pre_run_hook = pre_run_hook

        self._state = ScheduledPipelineState()

        # Observer list — WorkerPool and other integrations register
        # callbacks via add_observer() (replaces private _worker_pool_callback).
        self._observers: list[Callable[[RunRecord], Awaitable[None]]] = []

    # ------------------------------------------------------------------ #
    # Observer API                                                         #
    # ------------------------------------------------------------------ #

    def set_pre_run_hook(self, hook: Callable[[], Awaitable[bool]] | None) -> None:
        """Set the pre-run hook (used by WorkerPool for lease gating)."""
        self._pre_run_hook = hook

    def add_observer(self, callback: Callable[[RunRecord], Awaitable[None]]) -> None:
        """Register a run-complete observer.

        Observers are called in registration order after each run
        completes (success or failure).  Errors in observers are
        logged but do not stop notification of subsequent observers.

        Parameters
        ----------
        callback:
            ``async def callback(record: RunRecord) -> None``
        """
        self._observers.append(callback)

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """Start the scheduler loop.  Blocks until stopped or max errors reached."""
        await ScheduledPipelineRunner(self).run()

    def stop(self) -> None:
        """Signal the scheduler to stop after the current run completes."""
        self._state.stop_event.set()

    @property
    def is_running(self) -> bool:
        return self._state.running

    @property
    def pipeline_id(self) -> str:
        """Stable identifier for this pipeline (shown in logs and CLI)."""
        return self._pipeline_id

    @property
    def schedule(self) -> Schedule:
        """The schedule controlling when this pipeline runs."""
        return self._schedule

    @property
    def run_count(self) -> int:
        return self._state.run_number

    @property
    def last_run(self) -> RunRecord | None:
        return self._state.last_run
