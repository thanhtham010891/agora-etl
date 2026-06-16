from __future__ import annotations

from agora.core.failures import (
    FailureClassification,
    PoisonRecordClassification,
    PoisonRecordInfo,
)


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
