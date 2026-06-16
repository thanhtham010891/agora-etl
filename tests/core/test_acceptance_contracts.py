from __future__ import annotations

from agora.core.acceptance import AcceptanceFinding, AcceptanceReport


class _Thresholds:
    def to_dict(self) -> dict[str, int]:
        return {"max_lag": 0}


def test_acceptance_finding_to_dict_omits_component_when_absent() -> None:
    finding = AcceptanceFinding(
        metric="source.lag",
        message="lag exceeded",
        value=3,
        threshold=0,
    )

    assert finding.to_dict() == {
        "metric": "source.lag",
        "message": "lag exceeded",
        "value": 3,
        "threshold": 0,
    }


def test_acceptance_report_to_dict_supports_threshold_objects_and_component() -> None:
    finding = AcceptanceFinding(
        component="kafka_source",
        metric="lag",
        message="lag exceeded",
        value=3,
        threshold=0,
    )
    report = AcceptanceReport(
        component="kafka_source",
        passed=False,
        thresholds=_Thresholds(),
        findings=(finding,),
    )

    payload = report.to_dict()

    assert payload["component"] == "kafka_source"
    assert payload["passed"] is False
    assert payload["thresholds"] == {"max_lag": 0}
    assert payload["findings"] == [
        {
            "component": "kafka_source",
            "metric": "lag",
            "message": "lag exceeded",
            "value": 3,
            "threshold": 0,
        }
    ]
    assert isinstance(payload["evaluated_at"], str)
