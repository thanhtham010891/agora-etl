"""
agora/runner/coordinator.py
============================
``WorkerCoordinator`` — abstract interface for distributed worker coordination.

Implementations (e.g. ``RedisWorkerCoordinator`` in ``agora-etl-plugins``)
handle lease acquisition, worker heartbeat, and fleet discovery so that
multiple ``WorkerPool`` processes can share pipeline assignment without
duplicate runs.

Usage::

    from agora_plugins.distributed import RedisWorkerCoordinator

    pool = WorkerPool(
        coordinator=RedisWorkerCoordinator(redis_url="redis://localhost:6379"),
    )
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


@dataclass(frozen=True, slots=True)
class LeaseState:
    """Local snapshot of a distributed pipeline lease."""

    pipeline_id: str
    run_number: int
    worker_id: str
    fencing_token: int
    acquired_at: str
    renewed_at: str | None = None


@dataclass
class WorkerInfo:
    """Snapshot of a live worker's state as reported by the coordinator."""

    worker_id: str
    hostname: str
    pid: int
    status: str  # "running" | "draining" | "stopped"
    assigned_pipelines: list[str] = field(default_factory=list)
    last_heartbeat_at: str = ""


class WorkerCoordinator(ABC):
    """Abstract coordination backend for distributed ``WorkerPool`` deployments.

    A coordinator is responsible for:
    - Registering this worker in a shared discovery store (``start``)
    - Acquiring exclusive per-pipeline leases before each run (``try_acquire_lease``)
    - Releasing leases after each run or on shutdown (``release_lease`` / ``stop``)
    - Listing all live workers in the fleet (``list_workers``)

    When ``WorkerPool`` is constructed without a coordinator (``coordinator=None``),
    it runs in single-process mode with no coordination overhead.
    """

    @abstractmethod
    async def start(self, worker_id: str, pipeline_ids: list[str]) -> None:
        """Register this worker and start the heartbeat loop.

        Called once by ``WorkerPool.run()`` before any pipeline tasks are launched.

        Parameters
        ----------
        worker_id:
            Unique identifier for this worker process.
        pipeline_ids:
            IDs of all pipelines registered with this pool.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Release all leases, stop heartbeat, and deregister this worker.

        Called by ``WorkerPool._graceful_stop()`` after all pipelines have stopped.
        """

    @abstractmethod
    async def try_acquire_lease(self, pipeline_id: str, run_number: int) -> bool:
        """Attempt to acquire exclusive ownership of a pipeline run.

        Returns ``True`` if this worker now holds the lease, ``False`` if another
        worker already holds it (caller should skip this run).

        Parameters
        ----------
        pipeline_id:
            The pipeline to acquire.
        run_number:
            The run counter from ``ScheduledPipeline.run_count + 1``.
        """

    @abstractmethod
    async def release_lease(self, pipeline_id: str) -> None:
        """Release the lease for a pipeline after a run completes.

        No-op if this worker does not currently hold the lease.
        """

    def current_lease(self, pipeline_id: str) -> LeaseState | None:
        """Return this worker's locally held lease for *pipeline_id*, if any."""
        del pipeline_id
        return None

    async def validate_lease(self, pipeline_id: str, fencing_token: int) -> bool:
        """Return whether *fencing_token* is still the active lease token.

        Backends should override this with an authoritative shared-store check.
        The default is a local fallback for simple test coordinators.
        """
        lease = self.current_lease(pipeline_id)
        return lease is not None and lease.fencing_token == fencing_token

    @abstractmethod
    async def list_workers(self) -> list[WorkerInfo]:
        """Return all currently live workers visible to this coordinator."""

    async def connect(self) -> None:  # noqa: B027
        """Open a read-only connection for fleet inspection (e.g. ``agora worker --list``).

        Default is a no-op. Implementations that require an explicit connection
        step before ``list_workers()`` can be called outside of ``start()``
        should override this.
        """

    async def close(self) -> None:  # noqa: B027
        """Close the read-only connection opened by ``connect()``.

        Default is a no-op.
        """

    # Invoked with a ``pipeline_id`` when a lease held by this worker is lost
    # while the run is still in flight (e.g. renewal finds the lease taken over
    # by another worker). ``WorkerPool`` registers a callback that aborts the
    # in-flight run so a fenced-out worker stops writing.
    _lease_lost_callback: Callable[[str], Awaitable[None]] | None = None

    def set_lease_lost_callback(self, callback: Callable[[str], Awaitable[None]] | None) -> None:
        """Register a callback invoked when a held lease is lost mid-run.

        Implementations that detect lease loss during renewal should invoke
        the stored callback so the runtime can abort the in-flight run.
        """
        self._lease_lost_callback = callback


__all__ = ["LeaseState", "WorkerCoordinator", "WorkerInfo"]
