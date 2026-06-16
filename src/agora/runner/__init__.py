"""
agora/runner/__init__.py
========================
agora runtime — scheduled and concurrent pipeline execution.

Registry
--------
``runner_registry`` provides plugin-style access::

    from agora.runner import runner_registry

    cls = runner_registry.get_or_raise("scheduled")

Public API::

    from agora.runner import ScheduledPipeline, WorkerPool, Schedule

    # Interval-based:
    schedule = Schedule.every(hours=6)

    # Cron-based:
    schedule = Schedule.cron("0 */6 * * *")

    # Continuous (runs on loop, no wait):
    schedule = Schedule.continuous()
"""

from agora.core.registry import Registry
from agora.runner.coordinator import LeaseState, WorkerCoordinator, WorkerInfo
from agora.runner.policies import BackoffPolicy, ExponentialBackoffPolicy
from agora.runner.scheduled import Schedule, ScheduledPipeline
from agora.runner.worker import WorkerPool

# ======================================================================
# Runner Registry
# ======================================================================

runner_registry: Registry[type] = Registry(name="runner")

# Register built-in runners
runner_registry.register("scheduled", ScheduledPipeline)
runner_registry.register("worker_pool", WorkerPool)

__all__ = [
    "BackoffPolicy",
    "ExponentialBackoffPolicy",
    "LeaseState",
    "Schedule",
    "ScheduledPipeline",
    "WorkerCoordinator",
    "WorkerInfo",
    "WorkerPool",
    "runner_registry",
]
