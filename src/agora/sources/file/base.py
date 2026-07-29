"""agora/sources/file/base.py — abstract FileSource."""

from __future__ import annotations

import queue
from abc import abstractmethod
from typing import TYPE_CHECKING, Generic, TypeVar

import logstruct

from agora.core.checkpoint import (
    Checkpoint,
    SourceIdentity,
    SourceIdentityMismatchError,
    SourceIdentityMismatchPolicy,
)
from agora.core.source import BaseSource

if TYPE_CHECKING:
    import threading
    from collections.abc import AsyncIterator

T = TypeVar("T")
_QUEUE_PUT_POLL_TIMEOUT_S = 0.05
logger = logstruct.getLogger(__name__)


class FileSource(BaseSource[T], Generic[T]):
    """Abstract base for file-based sources.

    Implementers override ``read_records()`` — an async generator that
    yields records from a file.  agora handles the stream() contract.

    File I/O should use ``asyncio.to_thread()`` for blocking calls.
    """

    source_name: str = "file"
    supports_prefetch: bool = True
    prefetch_limit: int = 2
    supports_checkpoint: bool = True
    # Optional advisory: name of an Arrow-native counterpart source. When set,
    # the planner may emit a one-time hint suggesting the Arrow fast path for
    # transform-free pipelines whose sink already accepts Arrow batches.
    arrow_alternative_hint: str | None = None

    def _configure_source_identity_policy(
        self,
        policy: SourceIdentityMismatchPolicy | str,
    ) -> None:
        try:
            self._source_identity_mismatch_policy = SourceIdentityMismatchPolicy(policy)
        except ValueError as exc:
            choices = ", ".join(item.value for item in SourceIdentityMismatchPolicy)
            raise ValueError(
                f"source_identity_mismatch_policy must be one of {choices}; got {policy!r}"
            ) from exc

    def checkpoint_source_identity(self) -> SourceIdentity | None:
        """Return the filesystem identity persisted beside the resume cursor."""
        return SourceIdentity.for_file(self._path)  # type: ignore[attr-defined]

    def _accept_checkpoint_identity(self, checkpoint: Checkpoint | None) -> bool:
        """Return whether *checkpoint* may supply a resume cursor safely."""
        if checkpoint is None:
            return False

        saved_identity = checkpoint.source_identity
        current_identity = self.checkpoint_source_identity()
        if saved_identity is not None and saved_identity == current_identity:
            return True

        policy = getattr(
            self,
            "_source_identity_mismatch_policy",
            SourceIdentityMismatchPolicy.FAIL_CLOSED,
        )
        reason = (
            "checkpoint has no source identity (legacy checkpoint)"
            if saved_identity is None
            else "saved source identity differs from the current file"
        )
        message = (
            f"Cannot safely resume source {self.source_name!r}: {reason}. "
            f"Use source_identity_mismatch_policy={SourceIdentityMismatchPolicy.RESET.value!r} "
            "to start from the beginning, or 'allow' only when preserving the "
            "saved cursor is known to be safe."
        )
        if policy == SourceIdentityMismatchPolicy.FAIL_CLOSED:
            raise SourceIdentityMismatchError(message)
        if policy == SourceIdentityMismatchPolicy.RESET:
            logger.warning(
                "file_source_checkpoint_identity_reset",
                source=self.source_name,
                path=str(self._path),  # type: ignore[attr-defined]
                reason=reason,
            )
            return False

        logger.warning(
            "file_source_checkpoint_identity_mismatch_allowed",
            source=self.source_name,
            path=str(self._path),  # type: ignore[attr-defined]
            reason=reason,
        )
        return True

    @abstractmethod
    def read_records(self) -> AsyncIterator[T]:
        """Yield records from the file."""
        raise NotImplementedError

    async def stream(self) -> AsyncIterator[T]:  # type: ignore[override]
        async for record in self.read_records():
            yield record

    @staticmethod
    def _queue_put_until_stopped(
        batch_queue: queue.Queue[object],
        item: object,
        stop_event: threading.Event,
    ) -> bool:
        """Put *item* onto a bounded queue unless shutdown was requested.

        File-backed sources use a producer thread plus a bounded queue so they
        can stream records without loading entire files into memory. When the
        async consumer stops early, the producer must be able to observe that
        shutdown request instead of blocking forever on ``queue.put()``.
        """
        while not stop_event.is_set():
            try:
                batch_queue.put(item, timeout=_QUEUE_PUT_POLL_TIMEOUT_S)
                return True
            except queue.Full:
                continue
        return False
