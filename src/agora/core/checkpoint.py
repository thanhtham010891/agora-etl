"""Checkpoint abstractions for resumable sources."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol, TypeGuard, TypeVar, runtime_checkable

from agora.state.backend import MemoryBackend, SQLiteBackend, StateValue

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from agora.state.backend import StateBackend

T = TypeVar("T")

CheckpointValue = StateValue
"""Serializable checkpoint value."""


@dataclass(frozen=True)
class Checkpoint:
    """Persisted progress snapshot for a pipeline source."""

    pipeline_id: str
    run_id: str
    source: str
    value: CheckpointValue
    recorded_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class CheckpointStore(ABC):
    """Persistence contract for source checkpoints."""

    @abstractmethod
    async def load(self, key: str) -> Checkpoint | None:
        """Return the last stored checkpoint for *key*, if any."""

    @abstractmethod
    async def save(self, key: str, checkpoint: Checkpoint) -> None:
        """Persist *checkpoint* under *key*."""

    async def close(self) -> None:
        """Release store resources."""
        return


@runtime_checkable
class CheckpointableSource(Protocol[T]):
    """Protocol for sources that support checkpoint/resume."""

    source_name: str
    supports_checkpoint: bool

    def current_checkpoint(self) -> CheckpointValue:
        """Return current position for checkpoint."""
        ...

    async def prepare_resume(self, checkpoint: Checkpoint | None) -> None:
        """Initialize source to resume from checkpoint."""
        ...

    def stream(self) -> AsyncGenerator[T, None]:
        """Yield records asynchronously."""
        ...


def is_checkpoint_capable(source: object) -> TypeGuard[CheckpointableSource[Any]]:
    """Return True when *source* explicitly supports checkpoint resume."""
    return isinstance(source, CheckpointableSource) and bool(
        getattr(source, "supports_checkpoint", False)
    )


class BackendCheckpointStore(CheckpointStore):
    """Checkpoint store backed by a generic state backend."""

    def __init__(self, backend: StateBackend, namespace: str = "checkpoint") -> None:
        self._backend = backend
        self._namespace = namespace

    async def load(self, key: str) -> Checkpoint | None:
        entry = await asyncio.to_thread(self._backend.get, self._full_key(key))
        if entry is None:
            return None

        value = entry.value
        if not isinstance(value, dict):
            raise TypeError(
                f"Checkpoint data for key {key!r} is corrupted: expected dict, got {type(value)!r}"
            )
        recorded_at = value.get("recorded_at")
        return Checkpoint(
            pipeline_id=str(value["pipeline_id"]),
            run_id=str(value["run_id"]),
            source=str(value["source"]),
            value=value.get("value"),
            recorded_at=(
                datetime.fromisoformat(str(recorded_at))
                if isinstance(recorded_at, str)
                else datetime.now(UTC)
            ),
        )

    async def save(self, key: str, checkpoint: Checkpoint) -> None:
        payload = {
            "pipeline_id": checkpoint.pipeline_id,
            "run_id": checkpoint.run_id,
            "source": checkpoint.source,
            "value": checkpoint.value,
            "recorded_at": checkpoint.recorded_at.isoformat(),
        }
        await asyncio.to_thread(self._backend.set, self._full_key(key), payload)

    async def close(self) -> None:
        await asyncio.to_thread(self._backend.close)

    def _full_key(self, key: str) -> str:
        return f"{self._namespace}:{key}"


class InMemoryCheckpointStore(BackendCheckpointStore):
    """Simple in-memory checkpoint store for tests and local runs."""

    def __init__(self, namespace: str = "checkpoint") -> None:
        super().__init__(backend=MemoryBackend(), namespace=namespace)


class SQLiteCheckpointStore(BackendCheckpointStore):
    """SQLite-backed checkpoint store that persists across restarts."""

    def __init__(
        self,
        path: str | Path = ".agora_checkpoint.db",
        namespace: str = "checkpoint",
    ) -> None:
        super().__init__(backend=SQLiteBackend(path=path), namespace=namespace)
