"""Common recovery capability contracts for Agora sources and plugins."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol, TypeVar

SourceRecoveryContractT = TypeVar(
    "SourceRecoveryContractT",
    bound="SourceRecoveryContractSnapshot",
    covariant=True,
)


class SourceRecoveryMode(StrEnum):
    """Shared source recovery vocabulary for rerun/resume semantics."""

    FULL_RERUN = "full_rerun"
    CHECKPOINT_RERUN = "checkpoint_rerun"


@dataclass(frozen=True, slots=True)
class SourceRecoveryContractSnapshot:
    """Machine-readable source recovery capability contract."""

    mode: SourceRecoveryMode
    supports_checkpoint: bool
    requires_pipeline_rerun: bool = True
    transparent_failover: bool = False
    checkpoint_fields: tuple[str, ...] = ()
    checkpoint_params: dict[str, str] = field(default_factory=dict)
    on_record_error: str = "fail_closed"

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode.value,
            "supports_checkpoint": self.supports_checkpoint,
            "requires_pipeline_rerun": self.requires_pipeline_rerun,
            "transparent_failover": self.transparent_failover,
            "checkpoint_fields": list(self.checkpoint_fields),
            "checkpoint_params": dict(self.checkpoint_params),
            "on_record_error": self.on_record_error,
        }


class SourceRecoveryContractProvider(Protocol[SourceRecoveryContractT]):
    """Protocol for sources that declare their recovery capability contract."""

    def recovery_contract(self) -> SourceRecoveryContractT:
        """Return a machine-readable recovery contract."""


__all__ = [
    "SourceRecoveryContractProvider",
    "SourceRecoveryContractSnapshot",
    "SourceRecoveryMode",
]
