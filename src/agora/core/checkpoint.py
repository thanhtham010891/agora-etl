"""Checkpoint abstractions for resumable sources."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, TypeGuard, TypeVar, runtime_checkable

from agora.core.errors import AgoraError
from agora.state.backend import MemoryBackend, SQLiteBackend, StateValue

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from agora.state.backend import StateBackend

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)

CheckpointValue = StateValue
"""Serializable checkpoint value."""


@dataclass(frozen=True, slots=True)
class SourceIdentity:
    """Cheap filesystem fingerprint persisted beside a file resume cursor."""

    uri: str
    size_bytes: int
    modified_time_ns: int
    device: int | None = None
    inode: int | None = None

    @classmethod
    def for_file(cls, path: str | Path) -> SourceIdentity:
        """Build an O(1) filesystem identity without reading file contents."""
        resolved = Path(path).expanduser().resolve()
        stat = resolved.stat()
        return cls(
            uri=resolved.as_uri(),
            size_bytes=stat.st_size,
            modified_time_ns=stat.st_mtime_ns,
            device=getattr(stat, "st_dev", None),
            inode=getattr(stat, "st_ino", None),
        )

    def to_dict(self) -> dict[str, int | str | None]:
        return {
            "uri": self.uri,
            "size_bytes": self.size_bytes,
            "modified_time_ns": self.modified_time_ns,
            "device": self.device,
            "inode": self.inode,
        }

    @classmethod
    def from_dict(cls, value: object) -> SourceIdentity:
        if not isinstance(value, dict):
            raise TypeError(
                f"Checkpoint source_identity is corrupted: expected dict, got {type(value)!r}"
            )
        required = ("uri", "size_bytes", "modified_time_ns")
        missing = [key for key in required if key not in value]
        if missing:
            raise TypeError(
                f"Checkpoint source_identity is corrupted: missing required fields {missing!r}"
            )
        try:
            return cls(
                uri=str(value["uri"]),
                size_bytes=int(value["size_bytes"]),
                modified_time_ns=int(value["modified_time_ns"]),
                device=int(value["device"]) if value.get("device") is not None else None,
                inode=int(value["inode"]) if value.get("inode") is not None else None,
            )
        except (TypeError, ValueError) as exc:
            raise TypeError("Checkpoint source_identity is corrupted: invalid field type") from exc


class SourceIdentityMismatchPolicy(StrEnum):
    """How a file source reacts when the saved resume target changed."""

    FAIL_CLOSED = "fail_closed"
    RESET = "reset"
    ALLOW = "allow"


class SourceIdentityMismatchError(AgoraError):
    """Raised when a file checkpoint cannot safely resume its current source."""


@dataclass(frozen=True)
class Checkpoint:
    """Persisted progress snapshot for a pipeline source."""

    pipeline_id: str
    run_id: str
    source: str
    value: CheckpointValue
    recorded_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    source_identity: SourceIdentity | None = None


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
class CheckpointableSource(Protocol[T_co]):
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


@runtime_checkable
class CheckpointIdentityProvider(Protocol):
    """Optional source contract for binding a checkpoint to its input."""

    def checkpoint_source_identity(self) -> SourceIdentity | None:
        """Return the identity that must match before this checkpoint resumes."""
        ...


def is_checkpoint_capable(source: object) -> TypeGuard[CheckpointableSource[Any]]:
    """Return True when *source* explicitly supports checkpoint resume."""
    return isinstance(source, CheckpointableSource) and bool(
        getattr(source, "supports_checkpoint", False)
    )


def checkpoint_source_identity(source: object) -> SourceIdentity | None:
    """Get an explicitly provided source identity, when available."""
    if not isinstance(source, CheckpointIdentityProvider):
        return None
    identity = source.checkpoint_source_identity()
    if identity is not None and not isinstance(identity, SourceIdentity):
        raise TypeError(
            "checkpoint_source_identity() must return SourceIdentity or None, "
            f"got {type(identity)!r}"
        )
    return identity


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
        missing = [k for k in ("pipeline_id", "run_id", "source") if k not in value]
        if missing:
            raise TypeError(
                f"Checkpoint data for key {key!r} is corrupted: missing required fields {missing!r}"
            )
        recorded_at = value.get("recorded_at")
        source_identity = value.get("source_identity")
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
            source_identity=(
                SourceIdentity.from_dict(source_identity) if source_identity is not None else None
            ),
        )

    async def save(self, key: str, checkpoint: Checkpoint) -> None:
        payload = {
            "pipeline_id": checkpoint.pipeline_id,
            "run_id": checkpoint.run_id,
            "source": checkpoint.source,
            "value": checkpoint.value,
            "recorded_at": checkpoint.recorded_at.isoformat(),
            "source_identity": (
                checkpoint.source_identity.to_dict()
                if checkpoint.source_identity is not None
                else None
            ),
        }
        await asyncio.to_thread(self._backend.set, self._full_key(key), payload)

    async def delete(self, key: str) -> bool:
        """Delete the stored checkpoint for *key* when present."""
        full_key = self._full_key(key)
        existing = await asyncio.to_thread(self._backend.get, full_key)
        if existing is None:
            return False
        await asyncio.to_thread(self._backend.delete, full_key)
        return True

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
