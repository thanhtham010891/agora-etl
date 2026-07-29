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
    AUTHORIZATION = "authorization"
    CONFIGURATION = "configuration"
    UNKNOWN = "unknown"


PoisonRecordClassification = FailureClassification


class AlertSeverity(StrEnum):
    """Severity emitted with a failure decision for operators and alerting."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass(frozen=True, slots=True, kw_only=True)
class FailureDecision:
    """One failure outcome shared by retry, DLQ routing, and diagnostics.

    ``retryable`` only describes whether repeating the failed operation is
    sensible. ``dlq_eligible`` describes whether a record-level DLQ can retain
    the failure safely; infrastructure and authorization failures remain
    fail-closed rather than being silently acknowledged as poison records.
    """

    classification: FailureClassification
    retryable: bool
    dlq_eligible: bool
    alert_severity: AlertSeverity
    reason: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "classification": self.classification.value,
            "retryable": self.retryable,
            "dlq_eligible": self.dlq_eligible,
            "alert_severity": self.alert_severity.value,
            "reason": self.reason,
            "details": dict(self.details),
        }


def classify_failure(exc: Exception) -> FailureDecision:
    """Classify a core failure without assuming a backend-specific dependency.

    Plugin adapters may attach a precomputed ``failure_decision`` to preserve
    provider-specific classification after retry exhaustion. Structured poison
    metadata is also promoted so DLQ records and retry telemetry agree.
    """
    attached = getattr(exc, "failure_decision", None)
    if isinstance(attached, FailureDecision):
        return attached

    poison_info = getattr(exc, "poison_info", None)
    if isinstance(poison_info, PoisonRecordInfo):
        return FailureDecision(
            classification=poison_info.classification,
            retryable=False,
            dlq_eligible=True,
            alert_severity=AlertSeverity.ERROR,
            reason=poison_info.reason,
            details=dict(poison_info.details),
        )

    if isinstance(exc, TimeoutError):
        return FailureDecision(
            classification=FailureClassification.TIMEOUT,
            retryable=True,
            dlq_eligible=False,
            alert_severity=AlertSeverity.WARNING,
            reason=type(exc).__name__,
        )
    if isinstance(exc, (ConnectionError, OSError)):
        return FailureDecision(
            classification=FailureClassification.CONNECTIVITY,
            retryable=True,
            dlq_eligible=False,
            alert_severity=AlertSeverity.WARNING,
            reason=type(exc).__name__,
        )
    if isinstance(exc, (UnicodeDecodeError,)):
        return FailureDecision(
            classification=FailureClassification.DESERIALIZATION,
            retryable=False,
            dlq_eligible=True,
            alert_severity=AlertSeverity.ERROR,
            reason=type(exc).__name__,
        )
    if isinstance(exc, (UnicodeEncodeError,)):
        return FailureDecision(
            classification=FailureClassification.SERIALIZATION,
            retryable=False,
            dlq_eligible=True,
            alert_severity=AlertSeverity.ERROR,
            reason=type(exc).__name__,
        )
    return FailureDecision(
        classification=FailureClassification.UNKNOWN,
        retryable=False,
        dlq_eligible=True,
        alert_severity=AlertSeverity.ERROR,
        reason=type(exc).__name__,
    )


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
    "AlertSeverity",
    "FailureClassification",
    "FailureDecision",
    "PoisonRecordClassification",
    "PoisonRecordInfo",
    "PoisonRecordPolicy",
    "classify_failure",
]
