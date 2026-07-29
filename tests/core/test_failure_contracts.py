from __future__ import annotations

from agora.core.failures import (
    AlertSeverity,
    FailureClassification,
    FailureDecision,
    PoisonRecordClassification,
    PoisonRecordInfo,
    PoisonRecordPolicy,
    classify_failure,
)
from agora.core.metrics import RuntimeMetrics


def test_failure_classification_exposes_common_poison_categories() -> None:
    assert FailureClassification.DESERIALIZATION.value == "deserialization"
    assert FailureClassification.SCHEMA_DRIFT.value == "schema_drift"
    assert FailureClassification.CONSTRAINT_VIOLATION.value == "constraint_violation"
    assert FailureClassification.TYPE_MISMATCH.value == "type_mismatch"
    assert PoisonRecordClassification is FailureClassification


def test_poison_record_info_to_dict_uses_common_shape() -> None:
    info = PoisonRecordInfo(
        classification=FailureClassification.SCHEMA_VALIDATION,
        reason="json_schema",
        details={"field": "amount"},
    )

    assert info.to_dict() == {
        "classification": "schema_validation",
        "reason": "json_schema",
        "details": {"field": "amount"},
    }


def test_poison_record_policy_exposes_common_handling_modes() -> None:
    assert PoisonRecordPolicy.FAIL_CLOSED.value == "fail_closed"
    assert PoisonRecordPolicy.LOG_AND_CONTINUE.value == "log_and_continue"
    assert PoisonRecordPolicy.DLQ_AND_CONTINUE.value == "dlq_and_continue"
    assert PoisonRecordPolicy.DLQ_AND_FAIL_CLOSED.value == "dlq_and_fail_closed"


def test_failure_decision_classifies_transient_and_poison_failures() -> None:
    transient = classify_failure(TimeoutError("backend unavailable"))
    poison = classify_failure(UnicodeDecodeError("utf-8", b"\\xff", 0, 1, "invalid"))

    assert transient == FailureDecision(
        classification=FailureClassification.TIMEOUT,
        retryable=True,
        dlq_eligible=False,
        alert_severity=AlertSeverity.WARNING,
        reason="TimeoutError",
    )
    assert poison.classification == FailureClassification.DESERIALIZATION
    assert poison.retryable is False
    assert poison.dlq_eligible is True
    assert poison.alert_severity == AlertSeverity.ERROR


def test_failure_decision_promotes_structured_poison_metadata() -> None:
    class _StructuredError(RuntimeError):
        def __init__(self) -> None:
            self.poison_info = PoisonRecordInfo(
                classification=FailureClassification.CONSTRAINT_VIOLATION,
                reason="unique_key",
                details={"constraint": "events_pkey"},
            )

    decision = classify_failure(_StructuredError())

    assert decision.to_dict() == {
        "classification": "constraint_violation",
        "retryable": False,
        "dlq_eligible": True,
        "alert_severity": "error",
        "reason": "unique_key",
        "details": {"constraint": "events_pkey"},
    }


def test_runtime_metrics_preserve_failure_decision_counts_in_snapshot() -> None:
    metrics = RuntimeMetrics()
    metrics.record_failure_decision(
        FailureDecision(
            classification=FailureClassification.TIMEOUT,
            retryable=True,
            dlq_eligible=False,
            alert_severity=AlertSeverity.WARNING,
        ).to_dict()
    )

    snapshot = metrics.copy()

    assert snapshot.failure_classification_counts == {"timeout": 1}
    assert snapshot.failure_alert_severity_counts == {"warning": 1}
    assert snapshot.last_failure_decision == {
        "classification": "timeout",
        "retryable": True,
        "dlq_eligible": False,
        "alert_severity": "warning",
        "reason": None,
        "details": {},
    }
    assert snapshot.to_dict()["failure_classification_counts"] == {"timeout": 1}
