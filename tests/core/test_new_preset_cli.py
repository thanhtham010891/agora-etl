"""
tests/core/test_new_preset_cli.py
===================================
Tests for ``agora new <name> --preset`` scaffold command.

Coverage:
- file-etl preset generates expected files
- kafka-consumer preset generates expected files
- postgres-incremental preset generates expected files
- Unknown preset raises CommandError
- Existing directory raises CommandError
- Default preset is file-etl
- Generated pyproject.toml includes correct extras per preset
- Generated pipeline files are syntactically valid Python
- Generated test files are syntactically valid Python
"""

from __future__ import annotations

import argparse
import ast
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from agora.cli.commands.base import CommandError
from agora.cli.commands.new import NewCommand, _scaffold

if TYPE_CHECKING:
    from pathlib import Path

# ======================================================================
# Helpers
# ======================================================================


def _make_args(name: str, preset: str = "file-etl") -> argparse.Namespace:
    return argparse.Namespace(name=name, preset=preset)


def _make_ctx(cwd: str) -> MagicMock:
    ctx = MagicMock()
    ctx.cwd = cwd
    return ctx


def _assert_valid_python(path: Path) -> None:
    """Assert that *path* contains syntactically valid Python."""
    source = path.read_text(encoding="utf-8")
    try:
        ast.parse(source)
    except SyntaxError as exc:
        pytest.fail(f"{path} has invalid Python syntax: {exc}")


# ======================================================================
# file-etl preset
# ======================================================================


def test_file_etl_creates_expected_files(tmp_path: Any) -> None:
    _scaffold(tmp_path / "myproject", "myproject", "file-etl")
    root = tmp_path / "myproject"

    assert (root / "agora.toml").exists()
    assert (root / "pyproject.toml").exists()
    assert (root / "README.md").exists()
    assert (root / "src" / "pipelines" / "__init__.py").exists()
    assert (root / "src" / "pipelines" / "ingest.py").exists()
    assert (root / "tests" / "test_ingest.py").exists()


def test_file_etl_pipeline_is_valid_python(tmp_path: Any) -> None:
    _scaffold(tmp_path / "p", "p", "file-etl")
    _assert_valid_python(tmp_path / "p" / "src" / "pipelines" / "ingest.py")


def test_file_etl_test_is_valid_python(tmp_path: Any) -> None:
    _scaffold(tmp_path / "p", "p", "file-etl")
    _assert_valid_python(tmp_path / "p" / "tests" / "test_ingest.py")


def test_file_etl_pyproject_has_no_extra_deps(tmp_path: Any) -> None:
    _scaffold(tmp_path / "p", "p", "file-etl")
    content = (tmp_path / "p" / "pyproject.toml").read_text()
    assert "agora-etl" in content
    assert "agora-etl-plugins" not in content


def test_file_etl_agora_toml_has_project_name(tmp_path: Any) -> None:
    _scaffold(tmp_path / "myapp", "myapp", "file-etl")
    content = (tmp_path / "myapp" / "agora.toml").read_text()
    assert 'name = "myapp"' in content


# ======================================================================
# kafka-consumer preset
# ======================================================================


def test_kafka_consumer_creates_expected_files(tmp_path: Any) -> None:
    _scaffold(tmp_path / "kp", "kp", "kafka-consumer")
    root = tmp_path / "kp"

    assert (root / "agora.toml").exists()
    assert (root / "pyproject.toml").exists()
    assert (root / "README.md").exists()
    assert (root / "agora.env.example").exists()
    assert (root / "src" / "pipelines" / "consumer.py").exists()
    assert (root / "tests" / "test_consumer.py").exists()


def test_kafka_consumer_pipeline_is_valid_python(tmp_path: Any) -> None:
    _scaffold(tmp_path / "kp", "kp", "kafka-consumer")
    _assert_valid_python(tmp_path / "kp" / "src" / "pipelines" / "consumer.py")


def test_kafka_consumer_test_is_valid_python(tmp_path: Any) -> None:
    _scaffold(tmp_path / "kp", "kp", "kafka-consumer")
    _assert_valid_python(tmp_path / "kp" / "tests" / "test_consumer.py")


def test_kafka_consumer_pyproject_includes_kafka_extra(tmp_path: Any) -> None:
    _scaffold(tmp_path / "kp", "kp", "kafka-consumer")
    content = (tmp_path / "kp" / "pyproject.toml").read_text()
    assert "kafka" in content


def test_kafka_consumer_env_example_has_vars(tmp_path: Any) -> None:
    _scaffold(tmp_path / "kp", "kp", "kafka-consumer")
    content = (tmp_path / "kp" / "agora.env.example").read_text()
    assert "KAFKA_BOOTSTRAP_SERVERS" in content
    assert "KAFKA_TOPIC" in content


# ======================================================================
# postgres-incremental preset
# ======================================================================


def test_postgres_incremental_creates_expected_files(tmp_path: Any) -> None:
    _scaffold(tmp_path / "pp", "pp", "postgres-incremental")
    root = tmp_path / "pp"

    assert (root / "agora.toml").exists()
    assert (root / "pyproject.toml").exists()
    assert (root / "README.md").exists()
    assert (root / "agora.env.example").exists()
    assert (root / "src" / "pipelines" / "extract.py").exists()
    assert (root / "tests" / "test_extract.py").exists()


def test_postgres_incremental_pipeline_is_valid_python(tmp_path: Any) -> None:
    _scaffold(tmp_path / "pp", "pp", "postgres-incremental")
    _assert_valid_python(tmp_path / "pp" / "src" / "pipelines" / "extract.py")


def test_postgres_incremental_test_is_valid_python(tmp_path: Any) -> None:
    _scaffold(tmp_path / "pp", "pp", "postgres-incremental")
    _assert_valid_python(tmp_path / "pp" / "tests" / "test_extract.py")


def test_postgres_incremental_pyproject_includes_postgres_extra(tmp_path: Any) -> None:
    _scaffold(tmp_path / "pp", "pp", "postgres-incremental")
    content = (tmp_path / "pp" / "pyproject.toml").read_text()
    assert "postgres" in content


def test_postgres_incremental_env_example_has_database_url(tmp_path: Any) -> None:
    _scaffold(tmp_path / "pp", "pp", "postgres-incremental")
    content = (tmp_path / "pp" / "agora.env.example").read_text()
    assert "DATABASE_URL" in content


# ======================================================================
# Command class — error handling
# ======================================================================


def test_unknown_preset_raises_command_error(tmp_path: Any) -> None:
    cmd = NewCommand()
    args = _make_args("myproject", preset="nonexistent-preset")
    ctx = _make_ctx(str(tmp_path))
    with pytest.raises(CommandError, match="Unknown preset"):
        cmd.execute(args, ctx)


def test_existing_directory_raises_command_error(tmp_path: Any) -> None:
    (tmp_path / "existing").mkdir()
    cmd = NewCommand()
    args = _make_args("existing", preset="file-etl")
    ctx = _make_ctx(str(tmp_path))
    with pytest.raises(CommandError, match="already exists"):
        cmd.execute(args, ctx)


def test_default_preset_is_file_etl(tmp_path: Any) -> None:
    cmd = NewCommand()
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    sub = subparsers.add_parser("new")
    cmd.setup_parser(sub)
    args = sub.parse_args(["myproject"])
    assert args.preset == "file-etl"


def test_command_execute_file_etl_succeeds(tmp_path: Any) -> None:
    cmd = NewCommand()
    args = _make_args("newproject", preset="file-etl")
    ctx = _make_ctx(str(tmp_path))
    exit_code = cmd.execute(args, ctx)
    assert exit_code == 0
    assert (tmp_path / "newproject" / "agora.toml").exists()


def test_command_setup_parser_accepts_all_presets() -> None:
    cmd = NewCommand()
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    sub = subparsers.add_parser("new")
    cmd.setup_parser(sub)

    for preset in ["file-etl", "kafka-consumer", "postgres-incremental"]:
        args = sub.parse_args(["proj", "--preset", preset])
        assert args.preset == preset
