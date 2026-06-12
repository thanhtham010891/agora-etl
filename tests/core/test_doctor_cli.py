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
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

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
    check_plugins_importable,
    check_python_version,
    check_recovery_posture,
)
from agora.core.acceleration import AccelerationCapability, AccelerationMode

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
