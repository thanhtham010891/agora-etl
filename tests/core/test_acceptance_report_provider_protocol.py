"""Test AcceptanceReportProvider protocol compliance."""

from __future__ import annotations

from agora.core.acceptance import (
    AcceptanceFinding,
    AcceptanceReport,
    AcceptanceReportProvider,
    has_acceptance_report,
)


class _MockAcceptanceSurface:
    def acceptance_report(self, thresholds: object = None) -> AcceptanceReport:
        return AcceptanceReport(
            passed=True,
            thresholds={} if thresholds is None else thresholds,
            findings=(),
            component="mock",
        )


class _MockWithoutAcceptanceReport:
    def metrics_snapshot(self) -> dict[str, bool]:
        return {"ready": True}


def test_acceptance_report_provider_protocol_isinstance() -> None:
    provider = _MockAcceptanceSurface()
    non_provider = _MockWithoutAcceptanceReport()

    assert isinstance(provider, AcceptanceReportProvider)
    assert not isinstance(non_provider, AcceptanceReportProvider)


def test_has_acceptance_report_helper_matches_protocol() -> None:
    provider = _MockAcceptanceSurface()
    non_provider = _MockWithoutAcceptanceReport()

    assert has_acceptance_report(provider) is True
    assert has_acceptance_report(non_provider) is False


def test_acceptance_report_provider_returns_machine_readable_report() -> None:
    provider = _MockAcceptanceSurface()
    report = provider.acceptance_report(
        thresholds={"require_ready": True},
    )

    assert report.passed is True
    assert report.component == "mock"
    assert report.thresholds == {"require_ready": True}
    assert report.findings == ()


def test_acceptance_finding_remains_machine_readable() -> None:
    finding = AcceptanceFinding(
        metric="ready",
        message="Component is not ready.",
        value=False,
        threshold=True,
        component="mock",
    )

    assert finding.to_dict() == {
        "metric": "ready",
        "message": "Component is not ready.",
        "value": False,
        "threshold": True,
        "component": "mock",
    }
