"""Common failure and poison-record taxonomy for Agora components."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class FailureClassification(StrEnum):
    """Shared machine-readable failure categories for DLQ and diagnostics."""

    DESERIALIZATION = "deserialization"
    SERIALIZATION = "serialization"
    SCHEMA_EVOLUTION = "schema_evolution"
    SCHEMA_VALIDATION = "schema_validation"
    SCHEMA_REGISTRY_BINDING_MISMATCH = "schema_registry_binding_mismatch"
    SCHEMA_DRIFT = "schema_drift"
    CONSTRAINT_VIOLATION = "constraint_violation"
    TYPE_MISMATCH = "type_mismatch"
    CONNECTIVITY = "connectivity"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


PoisonRecordClassification = FailureClassification


class PoisonRecordPolicy(StrEnum):
    """Shared source-side poison-record handling policy."""

    FAIL_CLOSED = "fail_closed"
    LOG_AND_CONTINUE = "log_and_continue"
    DLQ_AND_CONTINUE = "dlq_and_continue"
    DLQ_AND_FAIL_CLOSED = "dlq_and_fail_closed"


@dataclass(frozen=True, slots=True, kw_only=True)
class PoisonRecordInfo:
    """Structured poison-record metadata for DLQ payloads and incident tooling."""

    classification: FailureClassification
    reason: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "classification": self.classification.value,
            "reason": self.reason,
            "details": dict(self.details),
        }


__all__ = [
    "FailureClassification",
    "PoisonRecordClassification",
    "PoisonRecordInfo",
    "PoisonRecordPolicy",
]
