"""Source-side exception models."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agora.core.checkpoint import CheckpointValue


class SourceRecordError(RuntimeError):
    """Record-scoped source failure that the runtime can DLQ precisely."""

    def __init__(
        self,
        exc: Exception,
        *,
        record: Any,
        checkpoint: CheckpointValue = None,
        source: str | None = None,
        stage: str = "source_record",
    ) -> None:
        super().__init__(str(exc))
        self.original = exc
        self.record = record
        self.checkpoint = checkpoint
        self.source = source
        self.stage = stage
