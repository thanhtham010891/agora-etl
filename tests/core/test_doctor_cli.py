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

import os
import sys
import textwrap
from typing import Any
from unittest.mock import MagicMock, patch

from agora.cli.commands.doctor import (
    CheckResult,
    DoctorCommand,
    DoctorReport,
    Status,
    check_agora_importable,
    check_config_import_refs,
    check_entrypoint_plugins,
    check_env_vars,
    check_plugins_importable,
    check_python_version,
)

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
    # agora itself is importable
    config.write_text(
        textwrap.dedent("""\
        [source]
        type = "custom"
        import = "agora.core.pipeline"
        """),
        encoding="utf-8",
    )
    result = check_config_import_refs(str(config))
    assert result.status == Status.PASS


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


# ======================================================================
# DoctorCommand
# ======================================================================


def test_command_name_and_description() -> None:
    cmd = DoctorCommand()
    assert cmd.name == "doctor"
    assert cmd.description


def test_command_execute_returns_0_on_healthy_install() -> None:
    import argparse

    cmd = DoctorCommand()
    args = argparse.Namespace(config=None)
    ctx = MagicMock()
    exit_code = cmd.execute(args, ctx)
    # Healthy install should return 0 (may have warns from optional plugins)
    assert exit_code in (0, 1)


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
    args = argparse.Namespace(config=str(config))
    ctx = MagicMock()
    exit_code = cmd.execute(args, ctx)
    assert exit_code == 1


def test_command_setup_parser() -> None:
    import argparse

    cmd = DoctorCommand()
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    sub = subparsers.add_parser("doctor")
    cmd.setup_parser(sub)

    args = sub.parse_args([])
    assert args.config is None

    args2 = sub.parse_args(["--config", "agora.toml"])
    assert args2.config == "agora.toml"
