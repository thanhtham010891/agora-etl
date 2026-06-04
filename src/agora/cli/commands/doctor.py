"""
agora/cli/commands/doctor.py
==============================
``agora doctor`` — pre-flight health check for an Agora installation.

Checks (all read-only, safe to run in CI):
1. Python version compatibility
2. agora-etl importability
3. agora-etl-plugins importability (optional)
4. Entry-point plugin loading
5. Config import refs resolve (when --config provided)
6. Required env vars present (when --config provided)

Each check returns pass / warn / fail.

Usage::

    agora doctor
    agora doctor --config agora.toml
"""

from __future__ import annotations

import importlib
import os
import sys
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from agora.cli.commands.base import BaseCommand
from agora.cli.console import console

if TYPE_CHECKING:
    import argparse

    from agora.cli.context import AgoraContext

_MIN_PYTHON = (3, 10)
_AGORA_PACKAGE = "agora"
_PLUGINS_PACKAGE = "agora_plugins"
_ENTRYPOINT_GROUP = "agora.sources"  # representative group


class Status(Enum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"


@dataclass
class CheckResult:
    name: str
    status: Status
    message: str
    detail: str = ""


@dataclass
class DoctorReport:
    results: list[CheckResult] = field(default_factory=list)

    def add(self, result: CheckResult) -> None:
        self.results.append(result)

    @property
    def failed(self) -> bool:
        return any(r.status == Status.FAIL for r in self.results)

    @property
    def warned(self) -> bool:
        return any(r.status == Status.WARN for r in self.results)


# ======================================================================
# Individual checks
# ======================================================================


def check_python_version() -> CheckResult:
    current = sys.version_info[:2]
    if current >= _MIN_PYTHON:
        return CheckResult(
            name="Python version",
            status=Status.PASS,
            message=f"Python {sys.version.split()[0]}",
        )
    return CheckResult(
        name="Python version",
        status=Status.FAIL,
        message=f"Python {sys.version.split()[0]} is below minimum {'.'.join(map(str, _MIN_PYTHON))}",
        detail="Upgrade to Python 3.10 or later.",
    )


def check_agora_importable() -> CheckResult:
    try:
        mod = importlib.import_module(_AGORA_PACKAGE)
        version = getattr(mod, "__version__", "unknown")
        return CheckResult(
            name="agora-etl",
            status=Status.PASS,
            message=f"agora-etl {version} importable",
        )
    except ImportError as exc:
        return CheckResult(
            name="agora-etl",
            status=Status.FAIL,
            message="agora-etl is not importable",
            detail=str(exc),
        )


def check_plugins_importable() -> CheckResult:
    try:
        mod = importlib.import_module(_PLUGINS_PACKAGE)
        version = getattr(mod, "__version__", "unknown")
        return CheckResult(
            name="agora-etl-plugins",
            status=Status.PASS,
            message=f"agora-etl-plugins {version} importable",
        )
    except ImportError:
        return CheckResult(
            name="agora-etl-plugins",
            status=Status.WARN,
            message="agora-etl-plugins not installed",
            detail="Install with: pip install agora-etl-plugins",
        )


def check_entrypoint_plugins() -> CheckResult:
    """Try to load all entry-point plugins and report failures."""
    try:
        from importlib.metadata import entry_points

        groups = [
            "agora.sources",
            "agora.sinks",
            "agora.middlewares",
        ]
        failed: list[str] = []
        loaded = 0
        for group in groups:
            eps = entry_points(group=group)
            for ep in eps:
                try:
                    ep.load()
                    loaded += 1
                except Exception as exc:
                    failed.append(f"{ep.name}: {type(exc).__name__}: {exc}")

        if failed:
            return CheckResult(
                name="Entry-point plugins",
                status=Status.FAIL,
                message=f"{len(failed)} plugin(s) failed to load",
                detail="\n".join(failed),
            )
        return CheckResult(
            name="Entry-point plugins",
            status=Status.PASS,
            message=f"{loaded} plugin(s) loaded cleanly"
            if loaded
            else "No entry-point plugins registered",
        )
    except Exception as exc:
        return CheckResult(
            name="Entry-point plugins",
            status=Status.WARN,
            message="Could not enumerate entry-point plugins",
            detail=str(exc),
        )


def check_config_import_refs(config_path: str) -> CheckResult:
    """Check that all import refs in the config file can be imported."""
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ImportError:
            return CheckResult(
                name="Config import refs",
                status=Status.WARN,
                message="Cannot parse config: tomllib/tomli not available",
                detail="Install tomli: pip install tomli",
            )

    try:
        with open(config_path, "rb") as f:
            config = tomllib.load(f)
    except Exception as exc:
        return CheckResult(
            name="Config import refs",
            status=Status.FAIL,
            message=f"Cannot read config file: {config_path}",
            detail=str(exc),
        )

    import_paths = _collect_import_refs(config)
    if not import_paths:
        return CheckResult(
            name="Config import refs",
            status=Status.PASS,
            message="No import refs found in config",
        )

    failed: list[str] = []
    for path in import_paths:
        module_path = path.split(":")[0]
        try:
            importlib.import_module(module_path)
        except ImportError as exc:
            failed.append(f"{path}: {exc}")

    if failed:
        return CheckResult(
            name="Config import refs",
            status=Status.FAIL,
            message=f"{len(failed)} import ref(s) cannot be resolved",
            detail="\n".join(failed),
        )
    return CheckResult(
        name="Config import refs",
        status=Status.PASS,
        message=f"{len(import_paths)} import ref(s) resolved successfully",
    )


def check_env_vars(config_path: str) -> CheckResult:
    """Check that env vars referenced in the config are present."""
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ImportError:
            return CheckResult(
                name="Environment variables",
                status=Status.WARN,
                message="Cannot parse config: tomllib/tomli not available",
            )

    try:
        with open(config_path, "rb") as f:
            config = tomllib.load(f)
    except Exception as exc:
        return CheckResult(
            name="Environment variables",
            status=Status.FAIL,
            message=f"Cannot read config file: {config_path}",
            detail=str(exc),
        )

    env_refs = _collect_env_refs(config)
    if not env_refs:
        return CheckResult(
            name="Environment variables",
            status=Status.PASS,
            message="No environment variable references found in config",
        )

    missing = [v for v in env_refs if not os.environ.get(v)]
    if missing:
        return CheckResult(
            name="Environment variables",
            status=Status.FAIL,
            message=f"{len(missing)} required env var(s) missing",
            detail=", ".join(missing),
        )
    return CheckResult(
        name="Environment variables",
        status=Status.PASS,
        message=f"{len(env_refs)} env var(s) present",
    )


# ======================================================================
# Config traversal helpers
# ======================================================================


def _collect_import_refs(obj: object, _seen: set[int] | None = None) -> list[str]:
    """Recursively collect all 'import' string values from a config dict."""
    if _seen is None:
        _seen = set()
    obj_id = id(obj)
    if obj_id in _seen:
        return []
    _seen.add(obj_id)

    results: list[str] = []
    if isinstance(obj, dict):
        if "import" in obj and isinstance(obj["import"], str):
            results.append(obj["import"])
        for v in obj.values():
            results.extend(_collect_import_refs(v, _seen))
    elif isinstance(obj, list):
        for item in obj:
            results.extend(_collect_import_refs(item, _seen))
    return results


def _collect_env_refs(obj: object, _seen: set[int] | None = None) -> list[str]:
    """Collect env var names from values like '${ENV_VAR}' or 'env:VAR_NAME'."""
    import re

    if _seen is None:
        _seen = set()
    obj_id = id(obj)
    if obj_id in _seen:
        return []
    _seen.add(obj_id)

    env_pattern = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)\}|env:([A-Z_][A-Z0-9_]*)")
    results: list[str] = []

    if isinstance(obj, str):
        for m in env_pattern.finditer(obj):
            results.append(m.group(1) or m.group(2))
    elif isinstance(obj, dict):
        for v in obj.values():
            results.extend(_collect_env_refs(v, _seen))
    elif isinstance(obj, list):
        for item in obj:
            results.extend(_collect_env_refs(item, _seen))
    return results


# ======================================================================
# Rendering
# ======================================================================


def _render_report(report: DoctorReport) -> None:
    console.section("agora doctor")

    status_icon = {
        Status.PASS: "[bold green]pass[/bold green]",
        Status.WARN: "[bold yellow]warn[/bold yellow]",
        Status.FAIL: "[bold red]fail[/bold red]",
    }

    for result in report.results:
        icon = status_icon[result.status]
        console.item(icon, result.name, result.message)
        if result.detail:
            for line in result.detail.splitlines():
                console.item("    ", line)

    console.blank()
    if report.failed:
        console.error("One or more checks failed. Fix the issues above before running pipelines.")
    elif report.warned:
        console.warn("Some optional checks have warnings.")
    else:
        console.info("All checks passed.")


# ======================================================================
# Command
# ======================================================================


class DoctorCommand(BaseCommand):
    """Run pre-flight health checks for the Agora installation."""

    name = "doctor"
    description = "Check Python version, imports, plugins, config refs, and env vars."

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--config",
            default=None,
            metavar="FILE",
            help="Optional TOML config file to check import refs and env vars against.",
        )

    def execute(self, args: argparse.Namespace, ctx: AgoraContext) -> int:
        report = DoctorReport()

        report.add(check_python_version())
        report.add(check_agora_importable())
        report.add(check_plugins_importable())
        report.add(check_entrypoint_plugins())

        if args.config:
            report.add(check_config_import_refs(args.config))
            report.add(check_env_vars(args.config))

        _render_report(report)
        return 1 if report.failed else 0


__all__ = [
    "CheckResult",
    "DoctorCommand",
    "DoctorReport",
    "Status",
    "check_agora_importable",
    "check_config_import_refs",
    "check_entrypoint_plugins",
    "check_env_vars",
    "check_plugins_importable",
    "check_python_version",
]
