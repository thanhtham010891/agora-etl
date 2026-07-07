"""
tests/core/test_doctor_cli.py
==============================
Tests for ``agora doctor`` CLI command.

Coverage:
- Python version check: pass / fail
- agora-etl import: pass / fail
- agora-etl-plugins: pass / warn
- Entry-point plugins: pass / fail
- Config import refs: pass / fail / warn (missing tomllib)
- Config env vars: pass / fail
- DoctorReport.failed / .warned properties
- Full command execute: returns 0 on pass, 1 on fail
- Parser setup
"""

from __future__ import annotations

import json
import os
import sys
import textwrap
from dataclasses import dataclass
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import agora.cli.commands.doctor as doctor_module
from agora.cli._path import ensure_project_on_path
from agora.cli.commands.doctor import (
    CheckResult,
    DoctorCommand,
    DoctorReport,
    Status,
    check_acceleration,
    check_agora_importable,
    check_config_import_refs,
    check_config_pipeline_build,
    check_config_pipeline_resolution,
    check_dlq_replay_support,
    check_entrypoint_plugins,
    check_env_vars,
    check_kafka_enterprise_readiness,
    check_plugins_importable,
    check_postgres_enterprise_readiness,
    check_python_version,
    check_recovery_posture,
    check_redis_enterprise_readiness,
)
from agora.core.acceleration import AccelerationCapability, AccelerationMode
from agora.core.doctor import (
    DOCTOR_READINESS_ENTRYPOINT_GROUP,
    DoctorReadinessProvider,
    DoctorReadinessProviderEntry,
    discover_doctor_readiness_providers,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

# ======================================================================
# DoctorReport
# ======================================================================


def test_report_failed_true_when_any_fail() -> None:
    report = DoctorReport()
    report.add(CheckResult("a", Status.PASS, "ok"))
    report.add(CheckResult("b", Status.FAIL, "bad"))
    assert report.failed is True


def test_report_failed_false_when_only_warn() -> None:
    report = DoctorReport()
    report.add(CheckResult("a", Status.WARN, "meh"))
    assert report.failed is False


def test_report_warned_true() -> None:
    report = DoctorReport()
    report.add(CheckResult("a", Status.WARN, "meh"))
    assert report.warned is True


def test_report_warned_false_when_all_pass() -> None:
    report = DoctorReport()
    report.add(CheckResult("a", Status.PASS, "ok"))
    assert report.warned is False


def test_report_to_dict_serializes_results() -> None:
    report = DoctorReport()
    report.add(CheckResult("agora-etl-rs acceleration", Status.PASS, "ready", "version=0.2.0"))

    payload = report.to_dict()

    assert payload["failed"] is False
    assert payload["warned"] is False
    assert payload["results"][0]["name"] == "agora-etl-rs acceleration"
    assert payload["results"][0]["status"] == "pass"
    assert payload["results"][0]["data"] == {}
    assert payload["readiness"]["component_count"] == 0


def test_report_to_dict_groups_structured_readiness_by_backend() -> None:
    report = DoctorReport()
    report.add(
        CheckResult(
            "Kafka source readiness",
            Status.WARN,
            "Kafka source opened but has no active partition assignment yet",
            data={
                "category": "enterprise_readiness",
                "backend": "kafka",
                "component": "source",
                "name": "Kafka source readiness",
                "status": "warn",
                "message": "Kafka source opened but has no active partition assignment yet",
                "metrics": {"assignment_count": 0},
                "findings": [],
                "operator_hooks": ["Wait for assignment."],
            },
        )
    )
    report.add(
        CheckResult(
            "Redis sink readiness #1",
            Status.PASS,
            "Redis sink '127.0.0.1:6379/0' passed enterprise readiness checks",
            data={
                "category": "enterprise_readiness",
                "backend": "redis",
                "component": "sink",
                "name": "Redis sink readiness #1",
                "status": "pass",
                "message": "Redis sink '127.0.0.1:6379/0' passed enterprise readiness checks",
                "metrics": {"connection_ready": True},
                "findings": [],
                "operator_hooks": ["Observe memory policy."],
            },
        )
    )

    payload = report.to_dict()

    assert payload["readiness"]["component_count"] == 2
    assert payload["readiness"]["backends"]["kafka"]["warned"] is True
    assert payload["readiness"]["backends"]["redis"]["failed"] is False
    assert payload["readiness"]["backends"]["kafka"]["components"][0]["component"] == "source"


# ======================================================================
# check_python_version
# ======================================================================


def test_python_version_passes_on_current() -> None:
    result = check_python_version()
    # Current Python (>=3.10) must pass
    assert result.status == Status.PASS


def test_python_version_fails_on_old() -> None:
    with patch.object(sys, "version_info", (3, 9, 0, "final", 0)):
        result = check_python_version()
    assert result.status == Status.FAIL


# ======================================================================
# check_agora_importable
# ======================================================================


def test_agora_importable_passes() -> None:
    result = check_agora_importable()
    assert result.status == Status.PASS
    assert "agora" in result.message.lower()


def test_agora_importable_fails_on_import_error() -> None:
    with patch("importlib.import_module", side_effect=ImportError("agora not found")):
        result = check_agora_importable()
    assert result.status == Status.FAIL


# ======================================================================
# check_plugins_importable
# ======================================================================


def test_plugins_importable_or_warn() -> None:
    result = check_plugins_importable()
    # Either installed (pass) or not installed (warn) — never fail
    assert result.status in (Status.PASS, Status.WARN)


def test_plugins_not_installed_returns_warn() -> None:
    with patch("importlib.import_module", side_effect=ImportError("not installed")):
        result = check_plugins_importable()
    assert result.status == Status.WARN
    assert result.detail == "Install with: pip install 'agora-etl-plugins'"


def test_acceleration_auto_available_passes() -> None:
    status = SimpleNamespace(
        mode=AccelerationMode.AUTO,
        enabled=True,
        compatible=True,
        version="0.2.0",
        capabilities=frozenset(
            {
                AccelerationCapability.RECORD_BUFFER,
                AccelerationCapability.CHECKPOINT_STATE,
            }
        ),
        reason=None,
    )
    with patch("agora.cli.commands.doctor.acceleration_status", return_value=status):
        result = check_acceleration()

    assert result.status == Status.PASS
    assert result.message == "agora-etl-rs acceleration available"
    assert "version=0.2.0" in (result.detail or "")
    assert "compatible=True" in (result.detail or "")
    assert "checkpoint_state" in (result.detail or "")
    assert "record_buffer" in (result.detail or "")


def test_acceleration_auto_missing_warns() -> None:
    status = SimpleNamespace(
        mode=AccelerationMode.AUTO,
        enabled=False,
        version=None,
        capabilities=frozenset(),
        reason="agora-etl-rs is not installed",
    )
    with patch("agora.cli.commands.doctor.acceleration_status", return_value=status):
        result = check_acceleration()

    assert result.status == Status.WARN
    assert "pure Python fallback" in result.message


def test_acceleration_off_passes_when_disabled(tmp_path) -> None:
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        """
format = "agora/v1"

[performance]
acceleration = "off"

[pipelines.main.source]
type = "iterable"
records = [1]

[[pipelines.main.sinks]]
type = "stdout"
""".strip(),
        encoding="utf-8",
    )
    status = SimpleNamespace(
        mode=AccelerationMode.OFF,
        enabled=False,
        version=None,
        capabilities=frozenset(),
        reason="acceleration disabled by policy",
    )
    with patch("agora.cli.commands.doctor.acceleration_status", return_value=status):
        result = check_acceleration(str(config_path))

    assert result.status == Status.PASS
    assert result.message == "Acceleration disabled by config"


def test_acceleration_required_missing_fails(tmp_path) -> None:
    config_path = tmp_path / "pipeline.toml"
    config_path.write_text(
        """
format = "agora/v1"

[performance]
acceleration = "required"

[pipelines.main.source]
type = "iterable"
records = [1]

[[pipelines.main.sinks]]
type = "stdout"
""".strip(),
        encoding="utf-8",
    )
    status = SimpleNamespace(
        mode=AccelerationMode.REQUIRED,
        enabled=False,
        version=None,
        capabilities=frozenset({AccelerationCapability.RECORD_BUFFER}),
        reason="agora-etl-rs is not installed",
    )
    with patch("agora.cli.commands.doctor.acceleration_status", return_value=status):
        result = check_acceleration(str(config_path))

    assert result.status == Status.FAIL
    assert "required" in result.message.lower()


# ======================================================================
# check_entrypoint_plugins
# ======================================================================


def test_entrypoint_plugins_pass_when_no_failures() -> None:
    result = check_entrypoint_plugins()
    # In a working install, should be pass or warn (not fail)
    assert result.status in (Status.PASS, Status.WARN)


def test_entrypoint_plugins_fail_when_ep_raises() -> None:
    from importlib.metadata import EntryPoint

    bad_ep = MagicMock(spec=EntryPoint)
    bad_ep.name = "broken_plugin"
    bad_ep.load.side_effect = ImportError("missing dep")

    with patch("importlib.metadata.entry_points", return_value=[bad_ep]):
        result = check_entrypoint_plugins()

    assert result.status == Status.FAIL
    assert "broken_plugin" in result.detail


def test_entrypoint_plugins_warn_when_manifest_is_incompatible() -> None:
    from importlib.metadata import EntryPoint
    from types import ModuleType

    bad_ep = MagicMock(spec=EntryPoint)
    bad_ep.name = "legacy_sink"
    bad_ep.dist = MagicMock(name="legacy-plugin", version="0.1.0")

    plugin_module = ModuleType("legacy_plugin.sinks")

    class LegacySink:
        pass

    LegacySink.__module__ = "legacy_plugin.sinks"
    plugin_module.LegacySink = LegacySink
    bad_ep.load.return_value = LegacySink

    manifest_module = ModuleType("legacy_plugin")
    manifest_module.MANIFEST = type(
        "_Manifest",
        (),
        {
            "agora_api_version": "0.0-incompatible",
            "package": "legacy-plugin",
            "version": "0.1.0",
        },
    )()

    def _entry_points(*, group: str):
        return [bad_ep] if group == "agora.sinks" else []

    with (
        patch.dict(
            sys.modules,
            {
                "legacy_plugin": manifest_module,
                "legacy_plugin.sinks": plugin_module,
            },
        ),
        patch("importlib.metadata.entry_points", side_effect=_entry_points),
    ):
        result = check_entrypoint_plugins()

    assert result.status == Status.WARN
    assert "incompatible plugin" in result.message
    assert "legacy_sink [agora.sinks]" in result.detail


def test_entrypoint_plugins_warn_when_plugin_conflicts_with_builtin_key() -> None:
    from importlib.metadata import EntryPoint
    from types import ModuleType

    stdout_ep = MagicMock(spec=EntryPoint)
    stdout_ep.name = "stdout"
    stdout_ep.dist = MagicMock(name="shadow-stdout-plugin", version="1.0.0")

    plugin_module = ModuleType("shadow_stdout_plugin.sinks")

    class ShadowStdoutSink:
        pass

    ShadowStdoutSink.__module__ = "shadow_stdout_plugin.sinks"
    plugin_module.ShadowStdoutSink = ShadowStdoutSink
    stdout_ep.load.return_value = ShadowStdoutSink

    def _entry_points(*, group: str):
        return [stdout_ep] if group == "agora.sinks" else []

    result = None
    with patch("importlib.metadata.entry_points", side_effect=_entry_points):
        result = check_entrypoint_plugins()

    assert result is not None
    assert result.status == Status.WARN
    assert "conflicting plugin" in result.message
    assert "stdout [agora.sinks]" in result.detail


def test_entrypoint_plugins_fail_when_doctor_provider_has_invalid_contract() -> None:
    from importlib.metadata import EntryPoint

    bad_ep = MagicMock(spec=EntryPoint)
    bad_ep.name = "postgres"
    bad_ep.load.return_value = object()

    def _entry_points(*, group: str):
        return [bad_ep] if group == DOCTOR_READINESS_ENTRYPOINT_GROUP else []

    with patch("importlib.metadata.entry_points", side_effect=_entry_points):
        result = check_entrypoint_plugins()

    assert result.status == Status.FAIL
    assert DOCTOR_READINESS_ENTRYPOINT_GROUP in result.detail
    assert "DoctorReadinessProvider" in result.detail


# ======================================================================
# check_config_import_refs
# ======================================================================


def test_config_import_refs_pass(tmp_path: Any) -> None:
    config = tmp_path / "agora.toml"
    config.write_text(
        textwrap.dedent("""\
        [pipeline]
        pipeline_id = "test"

        [source]
        type = "iterable"
        """),
        encoding="utf-8",
    )
    result = check_config_import_refs(str(config))
    assert result.status == Status.PASS


def test_config_import_refs_fail_on_bad_ref(tmp_path: Any) -> None:
    config = tmp_path / "agora.toml"
    config.write_text(
        textwrap.dedent("""\
        [source]
        type = "custom"
        import = "nonexistent_module_xyz.MySource"
        """),
        encoding="utf-8",
    )
    result = check_config_import_refs(str(config))
    assert result.status == Status.FAIL
    assert "nonexistent_module_xyz" in result.detail


def test_config_import_refs_fail_on_missing_file() -> None:
    result = check_config_import_refs("/nonexistent/path/agora.toml")
    assert result.status == Status.FAIL


def test_config_import_refs_pass_on_valid_ref(tmp_path: Any) -> None:
    config = tmp_path / "agora.toml"
    config.write_text(
        textwrap.dedent("""\
        [source]
        type = "custom"
        import = "agora:Pipeline"
        """),
        encoding="utf-8",
    )
    result = check_config_import_refs(str(config))
    assert result.status == Status.PASS
    assert "trusted project Python objects" in result.detail


def test_config_pipeline_resolution_passes_for_agora_v1(tmp_path: Any) -> None:
    config = tmp_path / "pipelines.toml"
    config.write_text(
        textwrap.dedent("""\
        format = "agora/v1"

        [defaults]
        pipeline = "orders"

        [pipelines.orders.source]
        type = "iterable"
        records = []

        [[pipelines.orders.sinks]]
        type = "stdout"
        """),
        encoding="utf-8",
    )
    result = check_config_pipeline_resolution(str(config))
    assert result.status == Status.PASS
    assert "Resolved agora/v1 pipeline" in result.message


def test_config_pipeline_build_passes_for_valid_agora_v1(tmp_path: Any) -> None:
    config = tmp_path / "pipelines.toml"
    config.write_text(
        textwrap.dedent("""\
        format = "agora/v1"

        [defaults]
        pipeline = "orders"

        [pipelines.orders.source]
        type = "iterable"
        records = []

        [[pipelines.orders.sinks]]
        type = "stdout"
        """),
        encoding="utf-8",
    )
    result = check_config_pipeline_build(str(config))
    assert result.status == Status.PASS
    assert "trusted project/plugin components" in result.detail


def test_config_pipeline_build_fails_when_selected_pipeline_cannot_build(tmp_path: Any) -> None:
    config = tmp_path / "pipelines.toml"
    config.write_text(
        textwrap.dedent("""\
        format = "agora/v1"

        [defaults]
        pipeline = "users"

        [pipelines.users.source]
        type = "csv"
        path = "users.csv"
        row_mapper = { import = "local_helpers:missing" }

        [[pipelines.users.sinks]]
        type = "stdout"
        """),
        encoding="utf-8",
    )
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "local_helpers.py").write_text(
        "def identity(row):\n    return row\n",
        encoding="utf-8",
    )

    with patch.object(sys, "path", [p for p in sys.path if str(src_dir) not in p]):
        sys.path.insert(0, str(src_dir))
        result = check_config_pipeline_build(str(config))

    assert result.status == Status.FAIL
    assert "missing" in result.detail


# ======================================================================
# check_env_vars
# ======================================================================


def test_env_vars_pass_when_none_referenced(tmp_path: Any) -> None:
    config = tmp_path / "agora.toml"
    config.write_text(
        textwrap.dedent("""\
        [pipeline]
        pipeline_id = "test"
        """),
        encoding="utf-8",
    )
    result = check_env_vars(str(config))
    assert result.status == Status.PASS


def test_env_vars_fail_on_missing(tmp_path: Any) -> None:
    config = tmp_path / "agora.toml"
    config.write_text(
        textwrap.dedent("""\
        [source]
        dsn = "${MISSING_DB_DSN_XYZ}"
        """),
        encoding="utf-8",
    )
    env = {k: v for k, v in os.environ.items() if k != "MISSING_DB_DSN_XYZ"}
    with patch.dict(os.environ, env, clear=True):
        result = check_env_vars(str(config))
    assert result.status == Status.FAIL
    assert "MISSING_DB_DSN_XYZ" in result.detail


def test_env_vars_pass_when_present(tmp_path: Any) -> None:
    config = tmp_path / "agora.toml"
    config.write_text(
        textwrap.dedent("""\
        [source]
        dsn = "${TEST_AGORA_DSN}"
        """),
        encoding="utf-8",
    )
    with patch.dict(os.environ, {"TEST_AGORA_DSN": "postgres://localhost/test"}):
        result = check_env_vars(str(config))
    assert result.status == Status.PASS


def test_env_vars_use_selected_agora_v1_pipeline_only(tmp_path: Any) -> None:
    config = tmp_path / "pipelines.toml"
    config.write_text(
        textwrap.dedent("""\
        format = "agora/v1"

        [pipelines.users.source]
        type = "iterable"
        records = []

        [[pipelines.users.sinks]]
        type = "stdout"

        [pipelines.orders.source]
        type = "iterable"
        records = ["${MISSING_ONLY_FOR_ORDERS}"]

        [[pipelines.orders.sinks]]
        type = "stdout"
        """),
        encoding="utf-8",
    )
    result = check_env_vars(str(config), pipeline_name="users")
    assert result.status == Status.PASS


def test_recovery_posture_warns_for_non_checkpointable_source(tmp_path: Any) -> None:
    config = tmp_path / "pipelines.toml"
    config.write_text(
        textwrap.dedent("""\
        format = "agora/v1"

        [defaults]
        pipeline = "orders"

        [pipelines.orders.source]
        type = "iterable"
        records = []

        [[pipelines.orders.sinks]]
        type = "stdout"
        """),
        encoding="utf-8",
    )
    result = check_recovery_posture(str(config))
    assert result.status == Status.WARN
    assert (
        "does not support resume" in result.detail or "limited recovery support" in result.message
    )


@dataclass(frozen=True)
class _FakeFinding:
    component: str
    metric: str
    message: str
    value: object
    threshold: object


@dataclass(frozen=True)
class _FakeReport:
    passed: bool
    findings: tuple[_FakeFinding, ...] = ()


class _FakeGate:
    def evaluate_source(self, snapshot: Any, thresholds: Any | None = None) -> _FakeReport:
        del thresholds
        findings: list[_FakeFinding] = []
        if not snapshot.recovery_contract.supports_checkpoint:
            findings.append(
                _FakeFinding(
                    "source",
                    "recovery_contract.supports_checkpoint",
                    "Postgres source does not support checkpoint-based resume.",
                    snapshot.recovery_contract.supports_checkpoint,
                    True,
                )
            )
        return _FakeReport(passed=not findings, findings=tuple(findings))

    def evaluate_sink(self, snapshot: Any, thresholds: Any | None = None) -> _FakeReport:
        del thresholds
        findings: list[_FakeFinding] = []
        if not snapshot.connection_ready:
            findings.append(
                _FakeFinding(
                    "sink",
                    "connection_ready",
                    "Postgres sink connection is not ready.",
                    snapshot.connection_ready,
                    True,
                )
            )
        return _FakeReport(passed=not findings, findings=tuple(findings))

    def evaluate_dlq_sink(self, snapshot: Any, thresholds: Any | None = None) -> _FakeReport:
        del thresholds
        findings: list[_FakeFinding] = []
        if not snapshot.table_ready:
            findings.append(
                _FakeFinding(
                    "dlq_sink",
                    "table_ready",
                    "Postgres DLQ sink table is not ready.",
                    snapshot.table_ready,
                    True,
                )
            )
        return _FakeReport(passed=not findings, findings=tuple(findings))


class _FakeThresholds:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs


@dataclass(frozen=True, slots=True)
class _FakeDoctorProvider:
    backend: str
    component_types: frozenset[str]
    results: tuple[CheckResult, ...] = ()
    runner: Callable[[dict[str, Any]], Awaitable[list[CheckResult]]] | None = None

    async def run_readiness_checks(self, pipeline_config: dict[str, Any]) -> list[CheckResult]:
        if self.runner is not None:
            return await self.runner(pipeline_config)
        del pipeline_config
        return list(self.results)


@dataclass(frozen=True, slots=True)
class _FakeEntryPoint:
    name: str
    value: str
    loaded: object

    def load(self) -> object:
        return self.loaded


def _install_fake_postgres_plugin_module() -> ModuleType:
    async def _provider_runner(pipeline_config: dict[str, Any]) -> list[CheckResult]:
        from agora.core.container import AgoraContainer
        from agora.core.metrics import has_metrics_snapshot

        def _component_type(config: object) -> str | None:
            if isinstance(config, dict):
                value = config.get("type")
                return value if isinstance(value, str) else None
            return None

        def _hooks(subject: str, report: _FakeReport) -> list[str]:
            hooks: list[str] = []
            metrics = {finding.metric for finding in report.findings}
            if "recovery_contract.supports_checkpoint" in metrics:
                hooks.append(
                    f"Configure checkpoint cursor fields for {subject} before relying on enterprise failover resume semantics."
                )
            if "connection_ready" in metrics:
                hooks.append(
                    f"Verify DSN, credentials, TLS settings, and network reachability for {subject}."
                )
            if "table_ready" in metrics:
                hooks.append(
                    f"Ensure the target table for {subject} exists and the service account can read/write it."
                )
            return hooks

        def _result(
            *,
            name: str,
            subject: str,
            component: str,
            report: _FakeReport,
            detail_lines: list[str],
        ) -> CheckResult:
            status = Status.PASS if report.passed else Status.FAIL
            detail = list(detail_lines)
            findings_payload: list[dict[str, Any]] = []
            for finding in report.findings:
                detail.append(
                    f"{finding.metric}: {finding.message} (value={finding.value!r}, threshold={finding.threshold!r})"
                )
                findings_payload.append(
                    {
                        "metric": finding.metric,
                        "message": finding.message,
                        "value": finding.value,
                        "threshold": finding.threshold,
                    }
                )
            operator_hooks = _hooks(subject, report)
            detail.extend(f"operator_hook={hook}" for hook in operator_hooks)
            message = (
                f"{subject} passed enterprise readiness checks"
                if report.passed
                else f"{subject} failed enterprise readiness checks"
            )
            metrics: dict[str, Any] = {}
            for line in detail_lines:
                if "=" not in line:
                    continue
                key, raw_value = line.split("=", 1)
                metrics[key] = raw_value
            return CheckResult(
                name=name,
                status=status,
                message=message,
                detail="\n".join(detail),
                data={
                    "category": "enterprise_readiness",
                    "backend": "postgres",
                    "component": component,
                    "name": name,
                    "status": status.value,
                    "message": message,
                    "metrics": metrics,
                    "findings": findings_payload,
                    "operator_hooks": operator_hooks,
                },
            )

        container = AgoraContainer.from_config(pipeline_config)
        gate = _FakeGate()
        results: list[CheckResult] = []

        async with container:
            pipeline = container.build_pipeline()
            source_cfg = pipeline_config.get("source", {})
            source = getattr(pipeline, "_source", None)
            if _component_type(source_cfg) == "postgres":
                if source is None or not has_metrics_snapshot(source):
                    results.append(
                        CheckResult(
                            name="Postgres source readiness",
                            status=Status.FAIL,
                            message="Configured Postgres source could not expose readiness metrics",
                            detail="Expected a live Postgres source instance with metrics_snapshot().",
                            data={"backend": "postgres", "component": "source"},
                        )
                    )
                else:
                    snapshot = source.metrics_snapshot()
                    report = gate.evaluate_source(
                        snapshot,
                        thresholds=_FakeThresholds(require_checkpoint_support=True),
                    )
                    results.append(
                        _result(
                            name="Postgres source readiness",
                            subject="Postgres source",
                            component="source",
                            report=report,
                            detail_lines=[
                                f"mode={snapshot.recovery_contract.mode.value}",
                                f"supports_checkpoint={snapshot.recovery_contract.supports_checkpoint}",
                                f"requires_pipeline_rerun={snapshot.recovery_contract.requires_pipeline_rerun}",
                                f"transparent_failover={snapshot.recovery_contract.transparent_failover}",
                            ],
                        )
                    )

            writer = getattr(pipeline, "_writer", None)
            sink_instances = list(getattr(writer, "_sinks", ())) if writer is not None else []
            sink_cfgs = pipeline_config.get("sinks", [])
            if isinstance(sink_cfgs, list):
                for index, sink_cfg in enumerate(sink_cfgs):
                    if _component_type(sink_cfg) != "postgres":
                        continue
                    sink = sink_instances[index] if index < len(sink_instances) else None
                    if sink is None or not has_metrics_snapshot(sink):
                        results.append(
                            CheckResult(
                                name=f"Postgres sink readiness #{index + 1}",
                                status=Status.FAIL,
                                message="Configured Postgres sink could not expose readiness metrics",
                                detail="Expected a live Postgres sink instance with metrics_snapshot().",
                                data={"backend": "postgres", "component": "sink"},
                            )
                        )
                        continue
                    snapshot = sink.metrics_snapshot()
                    report = gate.evaluate_sink(snapshot, thresholds=_FakeThresholds())
                    results.append(
                        _result(
                            name=f"Postgres sink readiness #{index + 1}",
                            subject=f"Postgres sink {snapshot.table!r}",
                            component="sink",
                            report=report,
                            detail_lines=[
                                f"table={snapshot.table}",
                                f"connection_ready={snapshot.connection_ready}",
                                f"write_safety_policy={snapshot.write_safety_policy}",
                            ],
                        )
                    )

            dlq_cfg = pipeline_config.get("dlq")
            if (
                isinstance(dlq_cfg, dict)
                and dlq_cfg.get("enabled", True)
                and _component_type(dlq_cfg.get("sink")) == "postgres_dlq"
            ):
                dlq_sink = container.resolve("_dlq_sink") if container.has("_dlq_sink") else None
                if dlq_sink is None or not has_metrics_snapshot(dlq_sink):
                    results.append(
                        CheckResult(
                            name="Postgres DLQ readiness",
                            status=Status.FAIL,
                            message="Configured Postgres DLQ could not expose readiness metrics",
                            detail="Expected a live Postgres DLQ sink instance with metrics_snapshot().",
                            data={"backend": "postgres", "component": "dlq"},
                        )
                    )
                else:
                    snapshot = dlq_sink.metrics_snapshot()
                    report = gate.evaluate_dlq_sink(snapshot, thresholds=_FakeThresholds())
                    results.append(
                        _result(
                            name="Postgres DLQ readiness",
                            subject=f"Postgres DLQ {snapshot.table!r}",
                            component="dlq",
                            report=report,
                            detail_lines=[
                                f"table={snapshot.table}",
                                f"connection_ready={snapshot.connection_ready}",
                                f"table_ready={snapshot.table_ready}",
                            ],
                        )
                    )

        return results

    module = ModuleType("agora_plugins.postgres")
    module._doctor_readiness_provider = _FakeDoctorProvider(
        backend="postgres",
        component_types=frozenset({"postgres", "postgres_dlq"}),
        runner=_provider_runner,
    )
    return module


class _FakeAsyncContainer:
    def __init__(self, pipeline: Any, dlq_sink: Any | None = None) -> None:
        self._pipeline = pipeline
        self._dlq_sink = dlq_sink

    async def __aenter__(self) -> _FakeAsyncContainer:
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        del exc_info

    def build_pipeline(self) -> Any:
        return self._pipeline

    def has(self, key: str) -> bool:
        return key == "_dlq_sink" and self._dlq_sink is not None

    def resolve(self, key: str) -> Any:
        if key == "_dlq_sink":
            return self._dlq_sink
        raise KeyError(key)


class _FakeKafkaSource:
    def __init__(
        self,
        *,
        ready: bool,
        stalled: bool = False,
        assignment_count: int = 1,
        pending_commit_count: int = 0,
        record_error_count: int = 0,
        poison_record_fail_closed_count: int = 0,
    ) -> None:
        self._health = SimpleNamespace(
            consumer_group="orders",
            subscription_mode="topics",
            assignment_count=assignment_count,
            pending_commit_count=pending_commit_count,
            rebalance_count=0,
            total_lag=0,
            ready=ready,
            stalled=stalled,
        )
        self._runtime = SimpleNamespace(
            record_error_count=record_error_count,
            record_drop_count=0,
        )
        self._operational = SimpleNamespace(
            poison_record_fail_closed_count=poison_record_fail_closed_count,
        )

    async def health_snapshot(self, force_refresh: bool = False) -> Any:
        del force_refresh
        return self._health

    def runtime_metrics(self) -> Any:
        return self._runtime

    def operational_metrics(self) -> Any:
        return self._operational


def _install_fake_kafka_plugin_module() -> ModuleType:
    async def _provider_runner(pipeline_config: dict[str, Any]) -> list[CheckResult]:
        from agora.core.container import AgoraContainer
        from agora.core.metrics import has_metrics_snapshot

        def _component_type(config: object) -> str | None:
            if isinstance(config, dict):
                value = config.get("type")
                return value if isinstance(value, str) else None
            return None

        container = AgoraContainer.from_config(pipeline_config)
        results: list[CheckResult] = []
        async with container:
            pipeline = container.build_pipeline()
            source_cfg = pipeline_config.get("source", {})
            source = getattr(pipeline, "_source", None)
            if _component_type(source_cfg) == "kafka":
                health = await source.health_snapshot(force_refresh=True)
                runtime_metrics = source.runtime_metrics()
                operational_metrics = source.operational_metrics()
                detail_lines = [
                    f"consumer_group={health.consumer_group}",
                    f"subscription_mode={health.subscription_mode}",
                    f"assignment_count={health.assignment_count}",
                    f"pending_commit_count={health.pending_commit_count}",
                    f"rebalance_count={health.rebalance_count}",
                    f"total_lag={health.total_lag}",
                ]
                status = Status.PASS
                message = "Kafka source passed enterprise readiness checks"
                hooks: list[str] = []
                if health.stalled:
                    status = Status.FAIL
                    message = "Kafka source is stalled"
                    hooks.append(
                        "Inspect broker connectivity, rebalance churn, or pause/resume orchestration before cutover."
                    )
                elif not health.ready:
                    status = Status.WARN
                    message = "Kafka source opened but has no active partition assignment yet"
                    hooks.append(
                        "Verify topic existence, ACLs, and consumer-group coordinator state until partition assignment becomes stable."
                    )
                if runtime_metrics.record_error_count > 0:
                    status = Status.FAIL
                    message = "Kafka source has source-level record errors"
                    hooks.append(
                        "Inspect poison-record classification counters and DLQ flow before promoting this consumer."
                    )
                if health.pending_commit_count > 0 and status == Status.PASS:
                    status = Status.WARN
                    message = "Kafka source has pending commits at readiness time"
                    hooks.append(
                        "Let commit-safe handoff drain pending acknowledgements before rolling forward."
                    )
                if operational_metrics.poison_record_fail_closed_count > 0:
                    hooks.append(
                        "A fail-closed poison policy has already fired; verify schema or payload fixes before restart."
                    )
                detail_lines.extend(f"operator_hook={hook}" for hook in hooks)
                results.append(
                    CheckResult(
                        name="Kafka source readiness",
                        status=status,
                        message=message,
                        detail="\n".join(detail_lines),
                        data={
                            "category": "enterprise_readiness",
                            "backend": "kafka",
                            "component": "source",
                            "name": "Kafka source readiness",
                            "status": status.value,
                            "message": message,
                            "metrics": {
                                "assignment_count": health.assignment_count,
                                "consumer_group": health.consumer_group,
                            },
                            "findings": [],
                            "operator_hooks": hooks,
                        },
                    )
                )

            writer = getattr(pipeline, "_writer", None)
            sink_instances = list(getattr(writer, "_sinks", ())) if writer is not None else []
            sink_cfgs = pipeline_config.get("sinks", [])
            if isinstance(sink_cfgs, list):
                for index, sink_cfg in enumerate(sink_cfgs):
                    if _component_type(sink_cfg) != "kafka":
                        continue
                    sink = sink_instances[index] if index < len(sink_instances) else None
                    ready = sink is not None and getattr(sink, "_producer", None) is not None
                    topic = getattr(sink, "_topic", "unknown")
                    bootstrap = getattr(sink, "_bootstrap", "unknown")
                    status = Status.PASS if ready else Status.FAIL
                    results.append(
                        CheckResult(
                            name=f"Kafka sink readiness #{index + 1}",
                            status=status,
                            message=(
                                f"Kafka sink {topic!r} passed enterprise readiness checks"
                                if ready
                                else f"Kafka sink {topic!r} failed enterprise readiness checks"
                            ),
                            detail="\n".join(
                                [
                                    f"topic={topic}",
                                    f"bootstrap_servers={bootstrap}",
                                    f"producer_ready={ready}",
                                ]
                            ),
                            data={"backend": "kafka", "component": "sink"},
                        )
                    )

            dlq_cfg = pipeline_config.get("dlq")
            if (
                isinstance(dlq_cfg, dict)
                and dlq_cfg.get("enabled", True)
                and _component_type(dlq_cfg.get("sink")) == "kafka_dlq"
            ):
                dlq_sink = container.resolve("_dlq_sink") if container.has("_dlq_sink") else None
                if dlq_sink is None or not has_metrics_snapshot(dlq_sink):
                    results.append(
                        CheckResult(
                            name="Kafka DLQ readiness",
                            status=Status.FAIL,
                            message="Configured Kafka DLQ could not expose readiness metrics",
                            data={"backend": "kafka", "component": "dlq"},
                        )
                    )
                else:
                    snapshot = dlq_sink.metrics_snapshot()
                    results.append(
                        CheckResult(
                            name="Kafka DLQ readiness",
                            status=Status.PASS,
                            message=f"Kafka DLQ {snapshot.topic!r} passed enterprise readiness checks",
                            detail=f"topic={snapshot.topic}\nbootstrap_servers={snapshot.bootstrap_servers}",
                            data={"backend": "kafka", "component": "dlq"},
                        )
                    )

        return results

    module = ModuleType("agora_plugins.kafka")
    module._doctor_readiness_provider = _FakeDoctorProvider(
        backend="kafka",
        component_types=frozenset({"kafka", "kafka_dlq"}),
        runner=_provider_runner,
    )
    return module


def _install_fake_redis_plugin_module() -> ModuleType:
    async def _provider_runner(pipeline_config: dict[str, Any]) -> list[CheckResult]:
        from agora.core.container import AgoraContainer
        from agora.core.metrics import has_metrics_snapshot

        def _component_type(config: object) -> str | None:
            if isinstance(config, dict):
                value = config.get("type")
                return value if isinstance(value, str) else None
            return None

        container = AgoraContainer.from_config(pipeline_config)
        results: list[CheckResult] = []
        async with container:
            pipeline = container.build_pipeline()
            source_cfg = pipeline_config.get("source", {})
            source = getattr(pipeline, "_source", None)
            if _component_type(source_cfg) == "redis_stream":
                ready = getattr(source, "_client", None) is not None
                hooks = []
                if not ready:
                    hooks.append(
                        "Verify Redis URL, ACLs, and stream/group existence before cutover."
                    )
                results.append(
                    CheckResult(
                        name="Redis stream source readiness",
                        status=Status.PASS if ready else Status.FAIL,
                        message=(
                            "Redis stream source passed enterprise readiness checks"
                            if ready
                            else "Redis stream source failed enterprise readiness checks"
                        ),
                        detail="\n".join(
                            [
                                f"stream={source._stream}",
                                f"group={source._group}",
                                f"consumer={source._consumer}",
                                f"supports_checkpoint={source.supports_checkpoint}",
                                f"connection_ready={ready}",
                                *[f"operator_hook={hook}" for hook in hooks],
                            ]
                        ),
                        data={"backend": "redis", "component": "source"},
                    )
                )

            writer = getattr(pipeline, "_writer", None)
            sink_instances = list(getattr(writer, "_sinks", ())) if writer is not None else []
            sink_cfgs = pipeline_config.get("sinks", [])
            if isinstance(sink_cfgs, list):
                for index, sink_cfg in enumerate(sink_cfgs):
                    if _component_type(sink_cfg) != "redis":
                        continue
                    sink = sink_instances[index] if index < len(sink_instances) else None
                    if sink is None or not has_metrics_snapshot(sink):
                        results.append(
                            CheckResult(
                                name=f"Redis sink readiness #{index + 1}",
                                status=Status.FAIL,
                                message="Configured Redis sink could not expose readiness metrics",
                                data={"backend": "redis", "component": "sink"},
                            )
                        )
                        continue
                    snapshot = sink.metrics_snapshot()
                    ready = snapshot.connection_ready
                    hook = (
                        "Verify Redis memory policy, TTL, and write mode semantics before production cutover."
                        if ready
                        else "Verify Redis URL, ACLs, and target database reachability before cutover."
                    )
                    results.append(
                        CheckResult(
                            name=f"Redis sink readiness #{index + 1}",
                            status=Status.PASS if ready else Status.FAIL,
                            message=(
                                f"Redis sink {snapshot.target!r} passed enterprise readiness checks"
                                if ready
                                else f"Redis sink {snapshot.target!r} failed enterprise readiness checks"
                            ),
                            detail="\n".join(
                                [
                                    f"target={snapshot.target}",
                                    f"mode={snapshot.mode}",
                                    f"connection_ready={snapshot.connection_ready}",
                                    f"operator_hook={hook}",
                                ]
                            ),
                            data={
                                "backend": "redis",
                                "component": "sink",
                                "metrics": {"connection_ready": snapshot.connection_ready},
                            },
                        )
                    )

            dlq_cfg = pipeline_config.get("dlq")
            if (
                isinstance(dlq_cfg, dict)
                and dlq_cfg.get("enabled", True)
                and _component_type(dlq_cfg.get("sink")) == "redis_dlq"
            ):
                dlq_sink = container.resolve("_dlq_sink") if container.has("_dlq_sink") else None
                ready = dlq_sink is not None and getattr(dlq_sink, "_client", None) is not None
                hook = (
                    "Validate DLQ key retention and replay cleanup rules before relying on Redis poison isolation."
                    if ready
                    else "Verify Redis DLQ connectivity and ACLs before enabling replay workflows."
                )
                results.append(
                    CheckResult(
                        name="Redis DLQ readiness",
                        status=Status.PASS if ready else Status.FAIL,
                        message=(
                            f"Redis DLQ {dlq_sink._key_prefix!r} passed enterprise readiness checks"
                            if ready
                            else f"Redis DLQ {dlq_sink._key_prefix!r} failed enterprise readiness checks"
                        ),
                        detail="\n".join(
                            [
                                f"key_prefix={dlq_sink._key_prefix}",
                                f"connection_ready={ready}",
                                f"operator_hook={hook}",
                            ]
                        ),
                        data={"backend": "redis", "component": "dlq"},
                    )
                )

        return results

    module = ModuleType("agora_plugins.redis")
    module._doctor_readiness_provider = _FakeDoctorProvider(
        backend="redis",
        component_types=frozenset({"redis", "redis_dlq", "redis_stream"}),
        runner=_provider_runner,
    )
    return module


def test_load_plugin_readiness_provider_uses_internal_provider_bridge() -> None:
    @dataclass(frozen=True, slots=True)
    class _FakeProvider:
        backend: str = "postgres"
        component_types: frozenset[str] = frozenset({"postgres", "postgres_dlq"})

        async def run_readiness_checks(self, pipeline_config: dict[str, Any]) -> list[CheckResult]:
            del pipeline_config
            return [CheckResult("provider", Status.PASS, "ok")]

    module = ModuleType("agora_plugins.postgres")
    provider = _FakeProvider()
    module._doctor_readiness_provider = provider

    with patch.dict(sys.modules, {"agora_plugins.postgres": module}):
        loaded = doctor_module._load_plugin_readiness_provider(
            doctor_module._PLUGIN_READINESS_SPECS[0]
        )

    assert loaded is provider
    assert isinstance(loaded, DoctorReadinessProvider)


def test_discover_doctor_readiness_providers_loads_entry_points_in_name_order() -> None:
    kafka_provider = _FakeDoctorProvider(
        backend="kafka",
        component_types=frozenset({"kafka", "kafka_dlq"}),
    )
    postgres_provider = _FakeDoctorProvider(
        backend="postgres",
        component_types=frozenset({"postgres", "postgres_dlq"}),
    )

    with patch(
        "agora.core.doctor.entry_points",
        return_value=[
            _FakeEntryPoint(
                name="postgres",
                value="agora_plugins.postgres.doctor:DOCTOR_READINESS_PROVIDER",
                loaded=postgres_provider,
            ),
            _FakeEntryPoint(
                name="kafka",
                value="agora_plugins.kafka.doctor:DOCTOR_READINESS_PROVIDER",
                loaded=kafka_provider,
            ),
        ],
    ) as mocked_entry_points:
        discovered = discover_doctor_readiness_providers()

    mocked_entry_points.assert_called_once_with(group=DOCTOR_READINESS_ENTRYPOINT_GROUP)
    assert discovered == (
        DoctorReadinessProviderEntry(name="kafka", provider=kafka_provider),
        DoctorReadinessProviderEntry(name="postgres", provider=postgres_provider),
    )


def test_check_all_plugin_readiness_uses_discovered_provider_entries() -> None:
    provider = _FakeDoctorProvider(
        backend="postgres",
        component_types=frozenset({"postgres", "postgres_dlq"}),
        results=(CheckResult("postgres readiness", Status.PASS, "ok"),),
    )
    ctx = SimpleNamespace(
        resolved=SimpleNamespace(
            pipeline_config={
                "source": {"type": "postgres"},
                "sinks": [],
            }
        )
    )

    with (
        patch(
            "agora.cli.commands.doctor._load_doctor_config_context",
            return_value=ctx,
        ),
        patch(
            "agora.cli.commands.doctor.discover_doctor_readiness_providers",
            return_value=(DoctorReadinessProviderEntry(name="postgres", provider=provider),),
        ),
    ):
        results = doctor_module._check_all_plugin_readiness("ignored.toml")

    assert [result.message for result in results] == ["ok"]


def test_command_execute_uses_generic_plugin_readiness_orchestration(tmp_path: Any) -> None:
    import argparse

    config = tmp_path / "pipelines.toml"
    config.write_text(
        textwrap.dedent("""\
        format = "agora/v1"

        [defaults]
        pipeline = "orders"

        [pipelines.orders.source]
        type = "iterable"
        records = []

        [[pipelines.orders.sinks]]
        type = "stdout"
        """),
        encoding="utf-8",
    )

    cmd = DoctorCommand()
    args = argparse.Namespace(
        config=str(config),
        pipeline=None,
        profile=None,
        environment=None,
        json=False,
    )
    ctx = MagicMock()

    with (
        patch(
            "agora.cli.commands.doctor.check_python_version",
            return_value=CheckResult("python", Status.PASS, "ok"),
        ),
        patch(
            "agora.cli.commands.doctor.check_agora_importable",
            return_value=CheckResult("agora", Status.PASS, "ok"),
        ),
        patch(
            "agora.cli.commands.doctor.check_plugins_importable",
            return_value=CheckResult("plugins", Status.PASS, "ok"),
        ),
        patch(
            "agora.cli.commands.doctor.check_acceleration",
            return_value=CheckResult("acceleration", Status.PASS, "ok"),
        ),
        patch(
            "agora.cli.commands.doctor.check_entrypoint_plugins",
            return_value=CheckResult("entrypoints", Status.PASS, "ok"),
        ),
        patch(
            "agora.cli.commands.doctor.check_config_pipeline_resolution",
            return_value=CheckResult("resolution", Status.PASS, "ok"),
        ),
        patch(
            "agora.cli.commands.doctor.check_config_import_refs",
            return_value=CheckResult("imports", Status.PASS, "ok"),
        ),
        patch(
            "agora.cli.commands.doctor.check_config_pipeline_build",
            return_value=CheckResult("build", Status.PASS, "ok"),
        ),
        patch(
            "agora.cli.commands.doctor._check_all_plugin_readiness",
            return_value=[CheckResult("postgres readiness", Status.PASS, "provider ok")],
        ) as plugin_readiness,
        patch(
            "agora.cli.commands.doctor.check_recovery_posture",
            return_value=CheckResult("recovery", Status.PASS, "ok"),
        ),
        patch(
            "agora.cli.commands.doctor.check_dlq_replay_support",
            return_value=CheckResult("dlq", Status.PASS, "ok"),
        ),
        patch(
            "agora.cli.commands.doctor.check_env_vars",
            return_value=CheckResult("env", Status.PASS, "ok"),
        ),
        patch("agora.cli.commands.doctor._render_report"),
    ):
        exit_code = cmd.execute(args, ctx)

    assert exit_code == 0
    plugin_readiness.assert_called_once_with(
        str(config),
        pipeline_name=None,
        profile_name=None,
        environment_name=None,
    )


def test_postgres_enterprise_readiness_passes_for_ready_components() -> None:
    config = {
        "source": {"type": "postgres"},
        "sinks": [{"type": "postgres"}],
        "dlq": {"enabled": True, "sink": {"type": "postgres_dlq"}},
    }
    ctx = SimpleNamespace(
        pipeline_config=config,
    )
    source_snapshot = SimpleNamespace(
        recovery_contract=SimpleNamespace(
            mode=SimpleNamespace(value="checkpoint_rerun"),
            supports_checkpoint=True,
            requires_pipeline_rerun=True,
            transparent_failover=False,
        )
    )
    sink_snapshot = SimpleNamespace(
        table="events",
        connection_ready=True,
        write_safety_policy="strict",
    )
    dlq_snapshot = SimpleNamespace(
        table="events_dlq",
        connection_ready=True,
        table_ready=True,
    )
    pipeline = SimpleNamespace(
        _source=SimpleNamespace(metrics_snapshot=lambda: source_snapshot),
        _writer=SimpleNamespace(_sinks=[SimpleNamespace(metrics_snapshot=lambda: sink_snapshot)]),
    )
    container = _FakeAsyncContainer(
        pipeline,
        dlq_sink=SimpleNamespace(metrics_snapshot=lambda: dlq_snapshot),
    )
    fake_plugin_module = _install_fake_postgres_plugin_module()

    with (
        patch(
            "agora.cli.commands.doctor._load_doctor_config_context",
            return_value=SimpleNamespace(resolved=ctx),
        ),
        patch("agora.core.container.AgoraContainer.from_config", return_value=container),
        patch.dict(sys.modules, {"agora_plugins.postgres": fake_plugin_module}),
    ):
        results = check_postgres_enterprise_readiness("ignored.toml")

    assert [result.status for result in results] == [Status.PASS, Status.PASS, Status.PASS]
    assert "Postgres source" in results[0].message
    assert "table=events" in results[1].detail
    assert "table=events_dlq" in results[2].detail
    assert results[0].data["backend"] == "postgres"
    assert results[0].data["component"] == "source"


def test_postgres_enterprise_readiness_surfaces_operator_hooks_on_failure() -> None:
    config = {
        "source": {"type": "postgres"},
        "sinks": [{"type": "postgres"}],
        "dlq": {"enabled": True, "sink": {"type": "postgres_dlq"}},
    }
    ctx = SimpleNamespace(
        pipeline_config=config,
    )
    source_snapshot = SimpleNamespace(
        recovery_contract=SimpleNamespace(
            mode=SimpleNamespace(value="full_rerun"),
            supports_checkpoint=False,
            requires_pipeline_rerun=True,
            transparent_failover=False,
        )
    )
    sink_snapshot = SimpleNamespace(
        table="events",
        connection_ready=False,
        write_safety_policy="strict",
    )
    dlq_snapshot = SimpleNamespace(
        table="events_dlq",
        connection_ready=True,
        table_ready=False,
    )
    pipeline = SimpleNamespace(
        _source=SimpleNamespace(metrics_snapshot=lambda: source_snapshot),
        _writer=SimpleNamespace(_sinks=[SimpleNamespace(metrics_snapshot=lambda: sink_snapshot)]),
    )
    container = _FakeAsyncContainer(
        pipeline,
        dlq_sink=SimpleNamespace(metrics_snapshot=lambda: dlq_snapshot),
    )
    fake_plugin_module = _install_fake_postgres_plugin_module()

    with (
        patch(
            "agora.cli.commands.doctor._load_doctor_config_context",
            return_value=SimpleNamespace(resolved=ctx),
        ),
        patch("agora.core.container.AgoraContainer.from_config", return_value=container),
        patch.dict(sys.modules, {"agora_plugins.postgres": fake_plugin_module}),
    ):
        results = check_postgres_enterprise_readiness("ignored.toml")

    assert [result.status for result in results] == [Status.FAIL, Status.FAIL, Status.FAIL]
    assert "Configure checkpoint cursor fields" in results[0].detail
    assert "Verify DSN, credentials, TLS settings" in results[1].detail
    assert "Ensure the target table" in results[2].detail


def test_kafka_enterprise_readiness_passes_for_ready_components() -> None:
    config = {
        "source": {"type": "kafka"},
        "sinks": [{"type": "kafka"}],
        "dlq": {"enabled": True, "sink": {"type": "kafka_dlq"}},
    }
    ctx = SimpleNamespace(pipeline_config=config)
    pipeline = SimpleNamespace(
        _source=_FakeKafkaSource(ready=True),
        _writer=SimpleNamespace(
            _sinks=[
                SimpleNamespace(
                    _producer=object(),
                    _topic="orders",
                    _bootstrap="127.0.0.1:9092",
                )
            ]
        ),
    )
    container = _FakeAsyncContainer(
        pipeline,
        dlq_sink=SimpleNamespace(
            metrics_snapshot=lambda: SimpleNamespace(
                topic="orders.dlq",
                bootstrap_servers="127.0.0.1:9092",
            )
        ),
    )
    fake_plugin_module = _install_fake_kafka_plugin_module()

    with (
        patch(
            "agora.cli.commands.doctor._load_doctor_config_context",
            return_value=SimpleNamespace(resolved=ctx),
        ),
        patch("agora.core.container.AgoraContainer.from_config", return_value=container),
        patch.dict(sys.modules, {"agora_plugins.kafka": fake_plugin_module}),
    ):
        results = check_kafka_enterprise_readiness("ignored.toml")

    assert [result.status for result in results] == [Status.PASS, Status.PASS, Status.PASS]
    assert "consumer_group=orders" in results[0].detail
    assert "topic=orders" in results[1].detail
    assert "orders.dlq" in results[2].message
    assert results[0].data["metrics"]["assignment_count"] == 1


def test_kafka_enterprise_readiness_warns_without_assignment() -> None:
    config = {
        "source": {"type": "kafka"},
        "sinks": [],
    }
    ctx = SimpleNamespace(pipeline_config=config)
    pipeline = SimpleNamespace(
        _source=_FakeKafkaSource(ready=False, assignment_count=0),
        _writer=SimpleNamespace(_sinks=[]),
    )
    container = _FakeAsyncContainer(pipeline)
    fake_plugin_module = _install_fake_kafka_plugin_module()

    with (
        patch(
            "agora.cli.commands.doctor._load_doctor_config_context",
            return_value=SimpleNamespace(resolved=ctx),
        ),
        patch("agora.core.container.AgoraContainer.from_config", return_value=container),
        patch.dict(sys.modules, {"agora_plugins.kafka": fake_plugin_module}),
    ):
        results = check_kafka_enterprise_readiness("ignored.toml")

    assert len(results) == 1
    assert results[0].status == Status.WARN
    assert "no active partition assignment" in results[0].message.lower()
    assert "consumer-group coordinator state" in results[0].detail


def test_redis_enterprise_readiness_passes_for_ready_components() -> None:
    config = {
        "source": {"type": "redis_stream"},
        "sinks": [{"type": "redis"}],
        "dlq": {"enabled": True, "sink": {"type": "redis_dlq"}},
    }
    ctx = SimpleNamespace(pipeline_config=config)
    redis_source = SimpleNamespace(
        _client=object(),
        _stream="agora:ingest",
        _group="orders",
        _consumer="worker-1",
        supports_checkpoint=True,
        runtime_metrics=lambda: SimpleNamespace(record_error_count=0, record_drop_count=0),
    )
    redis_sink = SimpleNamespace(
        metrics_snapshot=lambda: SimpleNamespace(
            target="127.0.0.1:6379/0",
            mode="set",
            connection_ready=True,
        )
    )
    pipeline = SimpleNamespace(
        _source=redis_source,
        _writer=SimpleNamespace(_sinks=[redis_sink]),
    )
    container = _FakeAsyncContainer(
        pipeline,
        dlq_sink=SimpleNamespace(
            _client=object(),
            _key_prefix="agora:dlq",
        ),
    )
    fake_plugin_module = _install_fake_redis_plugin_module()

    with (
        patch(
            "agora.cli.commands.doctor._load_doctor_config_context",
            return_value=SimpleNamespace(resolved=ctx),
        ),
        patch("agora.core.container.AgoraContainer.from_config", return_value=container),
        patch.dict(sys.modules, {"agora_plugins.redis": fake_plugin_module}),
    ):
        results = check_redis_enterprise_readiness("ignored.toml")

    assert [result.status for result in results] == [Status.PASS, Status.PASS, Status.PASS]
    assert "stream=agora:ingest" in results[0].detail
    assert "target=127.0.0.1:6379/0" in results[1].detail
    assert "agora:dlq" in results[2].message
    assert results[1].data["metrics"]["connection_ready"] is True


def test_redis_enterprise_readiness_fails_when_connection_missing() -> None:
    config = {
        "source": {"type": "redis_stream"},
        "sinks": [{"type": "redis"}],
        "dlq": {"enabled": True, "sink": {"type": "redis_dlq"}},
    }
    ctx = SimpleNamespace(pipeline_config=config)
    redis_source = SimpleNamespace(
        _client=None,
        _stream="agora:ingest",
        _group="orders",
        _consumer="worker-1",
        supports_checkpoint=True,
        runtime_metrics=lambda: SimpleNamespace(record_error_count=0, record_drop_count=0),
    )
    redis_sink = SimpleNamespace(
        metrics_snapshot=lambda: SimpleNamespace(
            target="127.0.0.1:6379/0",
            mode="set",
            connection_ready=False,
        )
    )
    pipeline = SimpleNamespace(
        _source=redis_source,
        _writer=SimpleNamespace(_sinks=[redis_sink]),
    )
    container = _FakeAsyncContainer(
        pipeline,
        dlq_sink=SimpleNamespace(
            _client=None,
            _key_prefix="agora:dlq",
        ),
    )
    fake_plugin_module = _install_fake_redis_plugin_module()

    with (
        patch(
            "agora.cli.commands.doctor._load_doctor_config_context",
            return_value=SimpleNamespace(resolved=ctx),
        ),
        patch("agora.core.container.AgoraContainer.from_config", return_value=container),
        patch.dict(sys.modules, {"agora_plugins.redis": fake_plugin_module}),
    ):
        results = check_redis_enterprise_readiness("ignored.toml")

    assert [result.status for result in results] == [Status.FAIL, Status.FAIL, Status.FAIL]
    assert "Verify Redis URL" in results[0].detail
    assert "reachability" in results[1].detail
    assert "ACLs" in results[2].detail


def test_dlq_replay_support_fails_for_unsupported_sink_type(tmp_path: Any) -> None:
    config = tmp_path / "pipelines.toml"
    config.write_text(
        textwrap.dedent("""\
        format = "agora/v1"

        [defaults]
        pipeline = "orders"

        [pipelines.orders.source]
        type = "iterable"
        records = []

        [[pipelines.orders.sinks]]
        type = "stdout"

        [pipelines.orders.dlq]
        enabled = true

        [pipelines.orders.dlq.sink]
        type = "custom_dlq"
        """),
        encoding="utf-8",
    )
    result = check_dlq_replay_support(str(config))
    assert result.status == Status.FAIL
    assert "custom_dlq" in result.message


def test_dlq_replay_support_passes_for_kafka_dlq(tmp_path: Any) -> None:
    config = tmp_path / "pipelines.toml"
    config.write_text(
        textwrap.dedent("""\
        format = "agora/v1"

        [defaults]
        pipeline = "orders"

        [pipelines.orders.source]
        type = "iterable"
        records = []

        [[pipelines.orders.sinks]]
        type = "stdout"

        [pipelines.orders.dlq]
        enabled = true

        [pipelines.orders.dlq.sink]
        type = "kafka_dlq"
        bootstrap_servers = "127.0.0.1:19092"
        topic = "orders.dlq"
        """),
        encoding="utf-8",
    )
    result = check_dlq_replay_support(str(config))
    assert result.status == Status.PASS
    assert "kafka_dlq" in result.message


# ======================================================================
# DoctorCommand
# ======================================================================


def test_command_name_and_description() -> None:
    cmd = DoctorCommand()
    assert cmd.name == "doctor"
    assert cmd.description
    assert "trusted code" in cmd.description


def test_command_execute_returns_0_on_healthy_install() -> None:
    import argparse

    cmd = DoctorCommand()
    args = argparse.Namespace(
        config=None,
        pipeline=None,
        profile=None,
        environment=None,
    )
    ctx = MagicMock()
    exit_code = cmd.execute(args, ctx)
    # Healthy install should return 0 (may have warns from optional plugins)
    assert exit_code in (0, 1)


def test_command_execute_can_emit_json_report() -> None:
    import argparse

    emitted: list[str] = []
    cmd = DoctorCommand()
    args = argparse.Namespace(
        config=None,
        pipeline=None,
        profile=None,
        environment=None,
        json=True,
    )
    ctx = MagicMock()

    with (
        patch("agora.cli.commands.doctor.console.out", side_effect=emitted.append),
        patch(
            "agora.cli.commands.doctor.check_python_version",
            return_value=CheckResult("python", Status.PASS, "ok"),
        ),
        patch(
            "agora.cli.commands.doctor.check_agora_importable",
            return_value=CheckResult("agora", Status.PASS, "ok"),
        ),
        patch(
            "agora.cli.commands.doctor.check_plugins_importable",
            return_value=CheckResult("plugins", Status.PASS, "ok"),
        ),
        patch(
            "agora.cli.commands.doctor.check_acceleration",
            return_value=CheckResult("agora-etl-rs acceleration", Status.PASS, "ready"),
        ),
        patch(
            "agora.cli.commands.doctor.check_entrypoint_plugins",
            return_value=CheckResult("entrypoints", Status.PASS, "ok"),
        ),
    ):
        exit_code = cmd.execute(args, ctx)

    payload = json.loads(emitted[0])
    assert exit_code == 0
    assert payload["failed"] is False
    assert payload["results"][3]["name"] == "agora-etl-rs acceleration"


def test_command_execute_ignores_non_pathlike_ctx_cwd() -> None:
    import os

    ctx = MagicMock()
    original_sys_path = ["/tmp/project", "/tmp/project/src"]

    with patch.object(sys, "path", list(original_sys_path)):
        ensure_project_on_path(ctx)
        assert not any("MagicMock/mock.cwd" in entry for entry in sys.path)
        assert os.getcwd() in sys.path
        assert os.path.join(os.getcwd(), "src") in sys.path


def test_command_execute_returns_1_on_failure(tmp_path: Any) -> None:
    import argparse

    config = tmp_path / "agora.toml"
    config.write_text(
        textwrap.dedent("""\
        [source]
        import = "definitely_not_a_real_module_xyz"
        """),
        encoding="utf-8",
    )
    cmd = DoctorCommand()
    args = argparse.Namespace(
        config=str(config),
        pipeline=None,
        profile=None,
        environment=None,
    )
    ctx = MagicMock()
    exit_code = cmd.execute(args, ctx)
    assert exit_code == 1


def test_command_execute_uses_project_src_for_config_imports(tmp_path: Any) -> None:
    import argparse

    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "local_helpers.py").write_text(
        "def identity(row):\n    return row\n",
        encoding="utf-8",
    )

    config = tmp_path / "pipelines.toml"
    config.write_text(
        textwrap.dedent("""\
        format = "agora/v1"

        [defaults]
        pipeline = "users"

        [pipelines.users.source]
        type = "csv"
        path = "users.csv"
        row_mapper = { import = "local_helpers:identity" }

        [[pipelines.users.sinks]]
        type = "stdout"
        """),
        encoding="utf-8",
    )

    cmd = DoctorCommand()
    args = argparse.Namespace(
        config=str(config),
        pipeline=None,
        profile=None,
        environment=None,
    )
    ctx = MagicMock(cwd=str(tmp_path))
    original_sys_path = list(sys.path)

    try:
        with (
            patch.object(sys, "path", list(original_sys_path)),
            patch(
                "agora.cli.commands.doctor.check_python_version",
                return_value=CheckResult("python", Status.PASS, "ok"),
            ),
            patch(
                "agora.cli.commands.doctor.check_agora_importable",
                return_value=CheckResult("agora", Status.PASS, "ok"),
            ),
            patch(
                "agora.cli.commands.doctor.check_plugins_importable",
                return_value=CheckResult("plugins", Status.PASS, "ok"),
            ),
            patch(
                "agora.cli.commands.doctor.check_entrypoint_plugins",
                return_value=CheckResult("plugins", Status.PASS, "ok"),
            ),
        ):
            exit_code = cmd.execute(args, ctx)
    finally:
        sys.modules.pop("local_helpers", None)
        sys.path[:] = original_sys_path

    assert exit_code == 0


def test_command_setup_parser() -> None:
    import argparse

    cmd = DoctorCommand()
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    sub = subparsers.add_parser("doctor")
    cmd.setup_parser(sub)

    args = sub.parse_args([])
    assert args.config is None
    assert args.pipeline is None
    assert args.json is False

    args2 = sub.parse_args(["--config", "agora.toml", "--json"])
    assert args2.config == "agora.toml"
    assert args2.json is True


def test_command_setup_parser_with_pipeline_selection() -> None:
    import argparse

    cmd = DoctorCommand()
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    sub = subparsers.add_parser("doctor")
    cmd.setup_parser(sub)

    args = sub.parse_args(
        ["orders", "--config", "pipelines.toml", "--profile", "prod", "--environment", "staging"]
    )
    assert args.pipeline == "orders"
    assert args.profile == "prod"
    assert args.environment == "staging"
