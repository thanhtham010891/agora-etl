"""
agora/cli/commands/doctor.py
==============================
``agora doctor`` — pre-flight health check for an Agora installation.

Checks (preflight only; they do not execute pipelines, but they may still
import and construct trusted project/plugin code):
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

import asyncio
import importlib
import json
import os
import sys
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from agora.cli._path import ensure_project_on_path
from agora.cli.commands.base import BaseCommand
from agora.cli.console import console
from agora.cli.recovery import recovery_insight_for_source
from agora.config import resolve_config_document, validate_config_document
from agora.core.acceleration import acceleration_status, normalize_acceleration_mode
from agora.core.component_factory import config_component_factory
from agora.core.discovery import public_entrypoint_group_contracts
from agora.core.registry import AGORA_PLUGIN_MANIFEST_VERSION, _coerce_manifest

if TYPE_CHECKING:
    import argparse

    from agora.cli.context import AgoraContext
    from agora.config import ResolvedPipelineConfig

_MIN_PYTHON = (3, 10)
_AGORA_PACKAGE = "agora"
_PLUGINS_PACKAGE = "agora_plugins"
_SUPPORTED_DLQ_REPLAY_SINK_TYPES = frozenset(
    {"sqlite_dlq", "postgres_dlq", "redis_dlq", "kafka_dlq"}
)


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
    data: dict[str, Any] = field(default_factory=dict)


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

    def to_dict(self) -> dict[str, Any]:
        """Return a stable machine-readable report."""
        return {
            "failed": self.failed,
            "warned": self.warned,
            "results": [
                {
                    "name": result.name,
                    "status": result.status.value,
                    "message": result.message,
                    "detail": result.detail,
                    "data": dict(result.data),
                }
                for result in self.results
            ],
            "readiness": self._readiness_payload(),
        }

    def _readiness_payload(self) -> dict[str, Any]:
        components = [
            dict(result.data)
            for result in self.results
            if result.data.get("category") == "enterprise_readiness"
        ]
        by_backend: dict[str, list[dict[str, Any]]] = {}
        for component in components:
            backend = str(component.get("backend", "unknown"))
            by_backend.setdefault(backend, []).append(component)
        return {
            "component_count": len(components),
            "backends": {
                backend: {
                    "component_count": len(entries),
                    "failed": any(entry.get("status") == Status.FAIL.value for entry in entries),
                    "warned": any(entry.get("status") == Status.WARN.value for entry in entries),
                    "components": entries,
                }
                for backend, entries in by_backend.items()
            },
        }


@dataclass(frozen=True)
class DoctorConfigContext:
    """Loaded config file plus optional resolved agora/v1 pipeline selection."""

    config_path: str
    raw: dict[str, Any]
    resolved: ResolvedPipelineConfig | None = None


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


def check_acceleration(
    config_path: str | None = None,
    *,
    pipeline_name: str | None = None,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> CheckResult:
    """Report optional agora-etl-rs acceleration posture."""
    mode = "auto"
    profile = "balanced"
    if config_path is not None:
        try:
            ctx = _load_doctor_config_context(
                config_path,
                pipeline_name=pipeline_name,
                profile_name=profile_name,
                environment_name=environment_name,
            )
            if ctx.resolved is not None:
                performance = ctx.resolved.pipeline_config.get("performance", {})
                if isinstance(performance, dict):
                    mode = str(performance.get("acceleration", mode))
                    profile = str(performance.get("profile", profile))
        except Exception as exc:
            return CheckResult(
                name="agora-etl-rs acceleration",
                status=Status.WARN,
                message="Cannot resolve acceleration config",
                detail=str(exc),
            )

    normalized_mode = normalize_acceleration_mode(mode)
    status = acceleration_status(normalized_mode, refresh=normalized_mode.value == "required")
    capabilities = ", ".join(sorted(capability.value for capability in status.capabilities))
    detail = "\n".join(
        [
            f"mode={status.mode.value}",
            f"profile={profile}",
            f"version={status.version or 'unknown'}",
            f"compatible={getattr(status, 'compatible', status.enabled)}",
            f"capabilities={capabilities or 'none'}",
            f"reason={status.reason or 'ready'}",
        ]
    )
    if status.enabled:
        return CheckResult(
            name="agora-etl-rs acceleration",
            status=Status.PASS,
            message="agora-etl-rs acceleration available",
            detail=detail,
        )
    if normalized_mode.value == "required":
        return CheckResult(
            name="agora-etl-rs acceleration",
            status=Status.FAIL,
            message="Acceleration is required but unavailable",
            detail=detail,
        )
    if normalized_mode.value == "off":
        return CheckResult(
            name="agora-etl-rs acceleration",
            status=Status.PASS,
            message="Acceleration disabled by config",
            detail=detail,
        )
    return CheckResult(
        name="agora-etl-rs acceleration",
        status=Status.WARN,
        message="agora-etl-rs acceleration not available; pure Python fallback will be used",
        detail=detail,
    )


def check_entrypoint_plugins() -> CheckResult:
    """Try to load public entry-point plugins and report compatibility issues."""
    try:
        from importlib.metadata import entry_points

        failed: list[str] = []
        incompatible: list[str] = []
        conflicts: list[str] = []
        loaded = 0
        manifestless = 0
        for contract in public_entrypoint_group_contracts():
            module = importlib.import_module(contract.module_path)
            registry = getattr(module, contract.registry_attr)
            reserved_keys = {
                key
                for key, origin in getattr(registry, "_origins", {}).items()
                if origin == "manual"
            }
            seen_entrypoints: dict[str, tuple[str | None, str | None]] = {}
            eps = entry_points(group=contract.group)
            for ep in eps:
                distribution = getattr(ep, "dist", None)
                distribution_name = getattr(distribution, "name", None)
                distribution_version = getattr(distribution, "version", None)

                if ep.name in reserved_keys:
                    conflicts.append(
                        f"{ep.name} [{contract.group}] conflicts with an existing built-in/public key"
                    )
                    continue

                existing_dist = seen_entrypoints.get(ep.name)
                if existing_dist is not None:
                    if existing_dist != (distribution_name, distribution_version):
                        conflicts.append(
                            f"{ep.name} [{contract.group}] is declared by multiple installed plugins"
                        )
                    continue

                seen_entrypoints[ep.name] = (distribution_name, distribution_version)
                try:
                    plugin = ep.load()
                    metadata = _coerce_manifest(
                        plugin,
                        distribution_name=distribution_name,
                        distribution_version=distribution_version,
                    )
                    if metadata is not None and metadata.get("compatible") is False:
                        incompatible.append(
                            f"{ep.name} [{contract.group}] expects "
                            f"{metadata.get('agora_api_version')!r}, "
                            f"runtime expects {AGORA_PLUGIN_MANIFEST_VERSION!r}"
                        )
                        continue
                    if metadata is not None and metadata.get("compatible") is None:
                        manifestless += 1
                    loaded += 1
                except Exception as exc:
                    failed.append(f"{ep.name} [{contract.group}]: {type(exc).__name__}: {exc}")

        if failed:
            return CheckResult(
                name="Entry-point plugins",
                status=Status.FAIL,
                message=f"{len(failed)} plugin(s) failed to load",
                detail="\n".join([*failed, *conflicts]),
            )
        if incompatible or conflicts:
            detail_lines = [*incompatible, *conflicts]
            if manifestless:
                detail_lines.append(
                    f"{manifestless} plugin(s) loaded without MANIFEST compatibility metadata"
                )
            if conflicts and incompatible:
                message = f"{len(incompatible)} incompatible and {len(conflicts)} conflicting plugin(s) discovered"
            elif conflicts:
                message = f"{len(conflicts)} conflicting plugin(s) discovered"
            else:
                message = f"{len(incompatible)} incompatible plugin(s) discovered"
            return CheckResult(
                name="Entry-point plugins",
                status=Status.WARN,
                message=message,
                detail="\n".join(detail_lines),
            )
        return CheckResult(
            name="Entry-point plugins",
            status=Status.PASS,
            message=(
                f"{loaded} plugin(s) loaded cleanly"
                + (f" ({manifestless} without MANIFEST metadata)" if manifestless else "")
            )
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


def check_config_pipeline_resolution(
    config_path: str,
    *,
    pipeline_name: str | None = None,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> CheckResult:
    """Validate the config file and resolve one pipeline selection when applicable."""
    try:
        ctx = _load_doctor_config_context(
            config_path,
            pipeline_name=pipeline_name,
            profile_name=profile_name,
            environment_name=environment_name,
        )
    except _ConfigParseDependencyMissingError as exc:
        return CheckResult(
            name="Config selection",
            status=Status.WARN,
            message="Cannot parse config: tomllib/tomli not available",
            detail=str(exc),
        )
    except Exception as exc:
        return CheckResult(
            name="Config selection",
            status=Status.FAIL,
            message=f"Cannot resolve config file: {config_path}",
            detail=str(exc),
        )

    if ctx.resolved is None:
        return CheckResult(
            name="Config selection",
            status=Status.PASS,
            message="Generic TOML config loaded",
            detail="Not an agora/v1 document; pipeline selection checks skipped.",
        )

    details = [
        f"pipeline={ctx.resolved.pipeline_name}",
        f"profile={ctx.resolved.profile_name or 'none'}",
        f"environment={ctx.resolved.environment_name or 'none'}",
    ]
    return CheckResult(
        name="Config selection",
        status=Status.PASS,
        message=f"Resolved agora/v1 pipeline '{ctx.resolved.pipeline_name}'",
        detail="\n".join(details),
    )


def check_config_import_refs(
    config_path: str,
    *,
    pipeline_name: str | None = None,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> CheckResult:
    """Check that import refs in the selected config resolve to real Python objects."""
    try:
        ctx = _load_doctor_config_context(
            config_path,
            pipeline_name=pipeline_name,
            profile_name=profile_name,
            environment_name=environment_name,
        )
    except _ConfigParseDependencyMissingError as exc:
        return CheckResult(
            name="Config import refs",
            status=Status.WARN,
            message="Cannot parse config: tomllib/tomli not available",
            detail=str(exc),
        )
    except Exception as exc:
        return CheckResult(
            name="Config import refs",
            status=Status.FAIL,
            message=f"Cannot read config file: {config_path}",
            detail=str(exc),
        )

    config_obj: object = ctx.resolved.pipeline_config if ctx.resolved is not None else ctx.raw

    import_paths = _collect_import_refs(config_obj)
    if not import_paths:
        return CheckResult(
            name="Config import refs",
            status=Status.PASS,
            message="No import refs found in config",
        )

    failed: list[str] = []
    for path in import_paths:
        try:
            config_component_factory.resolve_value({"import": path})
        except Exception as exc:
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
        detail=(
            "Import refs execute trusted project Python objects during resolution. "
            "Review config files like code."
        ),
    )


def check_config_pipeline_build(
    config_path: str,
    *,
    pipeline_name: str | None = None,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> CheckResult:
    """Build the selected agora/v1 pipeline without executing it."""
    try:
        ctx = _load_doctor_config_context(
            config_path,
            pipeline_name=pipeline_name,
            profile_name=profile_name,
            environment_name=environment_name,
        )
    except _ConfigParseDependencyMissingError as exc:
        return CheckResult(
            name="Pipeline build",
            status=Status.WARN,
            message="Cannot parse config: tomllib/tomli not available",
            detail=str(exc),
        )
    except Exception as exc:
        return CheckResult(
            name="Pipeline build",
            status=Status.FAIL,
            message=f"Cannot read config file: {config_path}",
            detail=str(exc),
        )

    if ctx.resolved is None:
        return CheckResult(
            name="Pipeline build",
            status=Status.PASS,
            message="Skipping pipeline build for generic TOML config",
        )

    try:
        from agora.core.container import AgoraContainer

        container = AgoraContainer.from_config(ctx.resolved.pipeline_config)
        container.build_pipeline()
    except Exception as exc:
        return CheckResult(
            name="Pipeline build",
            status=Status.FAIL,
            message="Selected pipeline could not be constructed",
            detail=str(exc),
        )

    pipeline_cfg = ctx.resolved.pipeline_config
    sinks = pipeline_cfg.get("sinks", [])
    middlewares = pipeline_cfg.get("middlewares", [])
    return CheckResult(
        name="Pipeline build",
        status=Status.PASS,
        message="Selected pipeline built successfully",
        detail=(
            f"source={pipeline_cfg.get('source', {}).get('type', 'unknown')}\n"
            f"middlewares={len(middlewares)}\n"
            f"sinks={len(sinks)}\n"
            "Build-only preflight imports and constructs trusted project/plugin components "
            "without executing the pipeline."
        ),
    )


def check_recovery_posture(
    config_path: str,
    *,
    pipeline_name: str | None = None,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> CheckResult:
    """Summarize restart and replay posture for the selected agora/v1 pipeline."""
    try:
        ctx = _load_doctor_config_context(
            config_path,
            pipeline_name=pipeline_name,
            profile_name=profile_name,
            environment_name=environment_name,
        )
    except _ConfigParseDependencyMissingError as exc:
        return CheckResult(
            name="Recovery posture",
            status=Status.WARN,
            message="Cannot parse config: tomllib/tomli not available",
            detail=str(exc),
        )
    except Exception as exc:
        return CheckResult(
            name="Recovery posture",
            status=Status.FAIL,
            message=f"Cannot read config file: {config_path}",
            detail=str(exc),
        )

    if ctx.resolved is None:
        return CheckResult(
            name="Recovery posture",
            status=Status.PASS,
            message="Skipping recovery posture for generic TOML config",
        )

    pipeline_cfg = ctx.resolved.pipeline_config
    source_cfg = pipeline_cfg.get("source", {})
    source_type = source_cfg.get("type") if isinstance(source_cfg, dict) else None
    insight = recovery_insight_for_source(str(source_type) if source_type is not None else None)
    dlq_sink_type = _configured_dlq_sink_type(pipeline_cfg)

    if insight is None:
        return CheckResult(
            name="Recovery posture",
            status=Status.WARN,
            message="Source recovery semantics are unknown",
            detail="Inspect the source implementation or plugin docs.",
        )

    detail_lines = [
        f"resume_key={insight.resume_key}",
        f"granularity={insight.granularity}",
        f"resume_cost={insight.resume_cost_model}",
        f"resume_behavior={insight.resume_behavior}",
    ]
    if dlq_sink_type is None:
        detail_lines.append("dlq=disabled")
        detail_lines.append("failed records will not be replayable with `agora dlq replay`")
    else:
        detail_lines.append(f"dlq={dlq_sink_type}")

    if insight.warning is not None:
        detail_lines.append(insight.warning.message)

    if insight.support == "yes":
        return CheckResult(
            name="Recovery posture",
            status=Status.PASS,
            message=f"Source '{source_type}' supports resume",
            detail="\n".join(detail_lines),
        )
    return CheckResult(
        name="Recovery posture",
        status=Status.WARN,
        message=f"Source '{source_type}' has limited recovery support ({insight.support})",
        detail="\n".join(detail_lines),
    )


def check_postgres_enterprise_readiness(
    config_path: str,
    *,
    pipeline_name: str | None = None,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> list[CheckResult]:
    """Run readiness checks for Postgres plugin components against live endpoints."""
    try:
        ctx = _load_doctor_config_context(
            config_path,
            pipeline_name=pipeline_name,
            profile_name=profile_name,
            environment_name=environment_name,
        )
    except _ConfigParseDependencyMissingError as exc:
        return [
            CheckResult(
                name="Postgres enterprise readiness",
                status=Status.WARN,
                message="Cannot parse config: tomllib/tomli not available",
                detail=str(exc),
            )
        ]
    except Exception as exc:
        return [
            CheckResult(
                name="Postgres enterprise readiness",
                status=Status.FAIL,
                message=f"Cannot read config file: {config_path}",
                detail=str(exc),
            )
        ]

    if ctx.resolved is None:
        return []

    pipeline_cfg = ctx.resolved.pipeline_config
    if not _pipeline_uses_postgres(pipeline_cfg):
        return []

    try:
        return asyncio.run(_check_postgres_enterprise_readiness_async(pipeline_cfg))
    except Exception as exc:
        return [
            CheckResult(
                name="Postgres enterprise readiness",
                status=Status.FAIL,
                message="Postgres readiness checks could not complete",
                detail=str(exc),
            )
        ]


async def _check_postgres_enterprise_readiness_async(
    pipeline_config: dict[str, Any],
) -> list[CheckResult]:
    from agora_plugins.postgres import (
        PostgresDLQSinkEnterpriseAcceptanceThresholds,
        PostgresEnterpriseAcceptanceGate,
        PostgresSinkEnterpriseAcceptanceThresholds,
        PostgresSourceEnterpriseAcceptanceThresholds,
    )

    from agora.core.container import AgoraContainer

    container = AgoraContainer.from_config(pipeline_config)
    gate = PostgresEnterpriseAcceptanceGate()
    results: list[CheckResult] = []

    async with container:
        pipeline = container.build_pipeline()
        source_cfg = pipeline_config.get("source", {})
        source = getattr(pipeline, "_source", None)
        if _component_type(source_cfg) == "postgres":
            if source is None or not hasattr(source, "metrics_snapshot"):
                results.append(
                    CheckResult(
                        name="Postgres source readiness",
                        status=Status.FAIL,
                        message="Configured Postgres source could not expose readiness metrics",
                        detail="Expected a live Postgres source instance with metrics_snapshot().",
                        data=_structured_readiness_data(
                            backend="postgres",
                            component="source",
                            name="Postgres source readiness",
                            status=Status.FAIL,
                            message="Configured Postgres source could not expose readiness metrics",
                            metrics={},
                            findings=[
                                {
                                    "metric": "metrics_snapshot",
                                    "message": "Expected a live Postgres source instance with metrics_snapshot().",
                                    "value": None,
                                    "threshold": "present",
                                }
                            ],
                            operator_hooks=[
                                "Verify the configured Postgres source plugin loads and starts cleanly before cutover."
                            ],
                        ),
                    )
                )
            else:
                snapshot = source.metrics_snapshot()
                report = gate.evaluate_source(
                    snapshot,
                    thresholds=PostgresSourceEnterpriseAcceptanceThresholds(
                        require_checkpoint_support=True
                    ),
                )
                detail_lines = [
                    f"mode={snapshot.recovery_contract.mode.value}",
                    f"supports_checkpoint={snapshot.recovery_contract.supports_checkpoint}",
                    f"requires_pipeline_rerun={snapshot.recovery_contract.requires_pipeline_rerun}",
                    f"transparent_failover={snapshot.recovery_contract.transparent_failover}",
                ]
                results.append(
                    _postgres_readiness_result(
                        name="Postgres source readiness",
                        subject="Postgres source",
                        component="source",
                        report=report,
                        detail_lines=detail_lines,
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
                if sink is None or not hasattr(sink, "metrics_snapshot"):
                    results.append(
                        CheckResult(
                            name=f"Postgres sink readiness #{index + 1}",
                            status=Status.FAIL,
                            message="Configured Postgres sink could not expose readiness metrics",
                            detail="Expected a live Postgres sink instance with metrics_snapshot().",
                            data=_structured_readiness_data(
                                backend="postgres",
                                component="sink",
                                name=f"Postgres sink readiness #{index + 1}",
                                status=Status.FAIL,
                                message="Configured Postgres sink could not expose readiness metrics",
                                metrics={},
                                findings=[
                                    {
                                        "metric": "metrics_snapshot",
                                        "message": "Expected a live Postgres sink instance with metrics_snapshot().",
                                        "value": None,
                                        "threshold": "present",
                                    }
                                ],
                                operator_hooks=[
                                    "Verify the configured Postgres sink plugin loads and starts cleanly before cutover."
                                ],
                            ),
                        )
                    )
                    continue
                snapshot = sink.metrics_snapshot()
                report = gate.evaluate_sink(
                    snapshot,
                    thresholds=PostgresSinkEnterpriseAcceptanceThresholds(),
                )
                detail_lines = [
                    f"table={snapshot.table}",
                    f"connection_ready={snapshot.connection_ready}",
                    f"write_safety_policy={snapshot.write_safety_policy}",
                ]
                results.append(
                    _postgres_readiness_result(
                        name=f"Postgres sink readiness #{index + 1}",
                        subject=f"Postgres sink {snapshot.table!r}",
                        component="sink",
                        report=report,
                        detail_lines=detail_lines,
                    )
                )

        dlq_cfg = pipeline_config.get("dlq")
        if (
            isinstance(dlq_cfg, dict)
            and dlq_cfg.get("enabled", True)
            and _component_type(dlq_cfg.get("sink")) == "postgres_dlq"
        ):
            dlq_sink = container.resolve("_dlq_sink") if container.has("_dlq_sink") else None
            if dlq_sink is None or not hasattr(dlq_sink, "metrics_snapshot"):
                results.append(
                    CheckResult(
                        name="Postgres DLQ readiness",
                        status=Status.FAIL,
                        message="Configured Postgres DLQ could not expose readiness metrics",
                        detail="Expected a live Postgres DLQ sink instance with metrics_snapshot().",
                        data=_structured_readiness_data(
                            backend="postgres",
                            component="dlq",
                            name="Postgres DLQ readiness",
                            status=Status.FAIL,
                            message="Configured Postgres DLQ could not expose readiness metrics",
                            metrics={},
                            findings=[
                                {
                                    "metric": "metrics_snapshot",
                                    "message": "Expected a live Postgres DLQ sink instance with metrics_snapshot().",
                                    "value": None,
                                    "threshold": "present",
                                }
                            ],
                            operator_hooks=[
                                "Verify the configured Postgres DLQ plugin loads and starts cleanly before cutover."
                            ],
                        ),
                    )
                )
            else:
                snapshot = dlq_sink.metrics_snapshot()
                report = gate.evaluate_dlq_sink(
                    snapshot,
                    thresholds=PostgresDLQSinkEnterpriseAcceptanceThresholds(),
                )
                detail_lines = [
                    f"table={snapshot.table}",
                    f"connection_ready={snapshot.connection_ready}",
                    f"table_ready={snapshot.table_ready}",
                ]
                results.append(
                    _postgres_readiness_result(
                        name="Postgres DLQ readiness",
                        subject=f"Postgres DLQ {snapshot.table!r}",
                        component="dlq",
                        report=report,
                        detail_lines=detail_lines,
                    )
                )

    return results


def check_kafka_enterprise_readiness(
    config_path: str,
    *,
    pipeline_name: str | None = None,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> list[CheckResult]:
    """Run readiness checks for Kafka plugin components against live endpoints."""
    try:
        ctx = _load_doctor_config_context(
            config_path,
            pipeline_name=pipeline_name,
            profile_name=profile_name,
            environment_name=environment_name,
        )
    except _ConfigParseDependencyMissingError as exc:
        return [
            CheckResult(
                name="Kafka enterprise readiness",
                status=Status.WARN,
                message="Cannot parse config: tomllib/tomli not available",
                detail=str(exc),
            )
        ]
    except Exception as exc:
        return [
            CheckResult(
                name="Kafka enterprise readiness",
                status=Status.FAIL,
                message=f"Cannot read config file: {config_path}",
                detail=str(exc),
            )
        ]

    if ctx.resolved is None:
        return []

    pipeline_cfg = ctx.resolved.pipeline_config
    if not _pipeline_uses_component_types(pipeline_cfg, {"kafka", "kafka_dlq"}):
        return []

    try:
        return asyncio.run(_check_kafka_enterprise_readiness_async(pipeline_cfg))
    except Exception as exc:
        return [
            CheckResult(
                name="Kafka enterprise readiness",
                status=Status.FAIL,
                message="Kafka readiness checks could not complete",
                detail=str(exc),
            )
        ]


async def _check_kafka_enterprise_readiness_async(
    pipeline_config: dict[str, Any],
) -> list[CheckResult]:
    from agora.core.container import AgoraContainer

    container = AgoraContainer.from_config(pipeline_config)
    results: list[CheckResult] = []
    async with container:
        pipeline = container.build_pipeline()
        source_cfg = pipeline_config.get("source", {})
        source = getattr(pipeline, "_source", None)
        if _component_type(source_cfg) == "kafka":
            results.append(await _check_kafka_source_readiness(source))

        writer = getattr(pipeline, "_writer", None)
        sink_instances = list(getattr(writer, "_sinks", ())) if writer is not None else []
        sink_cfgs = pipeline_config.get("sinks", [])
        if isinstance(sink_cfgs, list):
            for index, sink_cfg in enumerate(sink_cfgs):
                if _component_type(sink_cfg) != "kafka":
                    continue
                sink = sink_instances[index] if index < len(sink_instances) else None
                results.append(_check_kafka_sink_readiness(sink, index=index + 1))

        dlq_cfg = pipeline_config.get("dlq")
        if (
            isinstance(dlq_cfg, dict)
            and dlq_cfg.get("enabled", True)
            and _component_type(dlq_cfg.get("sink")) == "kafka_dlq"
        ):
            dlq_sink = container.resolve("_dlq_sink") if container.has("_dlq_sink") else None
            results.append(_check_kafka_dlq_readiness(dlq_sink))

    return results


def check_redis_enterprise_readiness(
    config_path: str,
    *,
    pipeline_name: str | None = None,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> list[CheckResult]:
    """Run readiness checks for Redis plugin components against live endpoints."""
    try:
        ctx = _load_doctor_config_context(
            config_path,
            pipeline_name=pipeline_name,
            profile_name=profile_name,
            environment_name=environment_name,
        )
    except _ConfigParseDependencyMissingError as exc:
        return [
            CheckResult(
                name="Redis enterprise readiness",
                status=Status.WARN,
                message="Cannot parse config: tomllib/tomli not available",
                detail=str(exc),
            )
        ]
    except Exception as exc:
        return [
            CheckResult(
                name="Redis enterprise readiness",
                status=Status.FAIL,
                message=f"Cannot read config file: {config_path}",
                detail=str(exc),
            )
        ]

    if ctx.resolved is None:
        return []

    pipeline_cfg = ctx.resolved.pipeline_config
    if not _pipeline_uses_component_types(pipeline_cfg, {"redis", "redis_dlq", "redis_stream"}):
        return []

    try:
        return asyncio.run(_check_redis_enterprise_readiness_async(pipeline_cfg))
    except Exception as exc:
        return [
            CheckResult(
                name="Redis enterprise readiness",
                status=Status.FAIL,
                message="Redis readiness checks could not complete",
                detail=str(exc),
            )
        ]


async def _check_redis_enterprise_readiness_async(
    pipeline_config: dict[str, Any],
) -> list[CheckResult]:
    from agora.core.container import AgoraContainer

    container = AgoraContainer.from_config(pipeline_config)
    results: list[CheckResult] = []
    async with container:
        pipeline = container.build_pipeline()
        source_cfg = pipeline_config.get("source", {})
        source = getattr(pipeline, "_source", None)
        if _component_type(source_cfg) == "redis_stream":
            results.append(_check_redis_stream_source_readiness(source))

        writer = getattr(pipeline, "_writer", None)
        sink_instances = list(getattr(writer, "_sinks", ())) if writer is not None else []
        sink_cfgs = pipeline_config.get("sinks", [])
        if isinstance(sink_cfgs, list):
            for index, sink_cfg in enumerate(sink_cfgs):
                if _component_type(sink_cfg) != "redis":
                    continue
                sink = sink_instances[index] if index < len(sink_instances) else None
                results.append(_check_redis_sink_readiness(sink, index=index + 1))

        dlq_cfg = pipeline_config.get("dlq")
        if (
            isinstance(dlq_cfg, dict)
            and dlq_cfg.get("enabled", True)
            and _component_type(dlq_cfg.get("sink")) == "redis_dlq"
        ):
            dlq_sink = container.resolve("_dlq_sink") if container.has("_dlq_sink") else None
            results.append(_check_redis_dlq_readiness(dlq_sink))

    return results


def check_dlq_replay_support(
    config_path: str,
    *,
    pipeline_name: str | None = None,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> CheckResult:
    """Check whether the selected pipeline's DLQ can be replayed from the CLI."""
    try:
        ctx = _load_doctor_config_context(
            config_path,
            pipeline_name=pipeline_name,
            profile_name=profile_name,
            environment_name=environment_name,
        )
    except _ConfigParseDependencyMissingError as exc:
        return CheckResult(
            name="DLQ replay",
            status=Status.WARN,
            message="Cannot parse config: tomllib/tomli not available",
            detail=str(exc),
        )
    except Exception as exc:
        return CheckResult(
            name="DLQ replay",
            status=Status.FAIL,
            message=f"Cannot read config file: {config_path}",
            detail=str(exc),
        )

    if ctx.resolved is None:
        return CheckResult(
            name="DLQ replay",
            status=Status.PASS,
            message="Skipping DLQ replay check for generic TOML config",
        )

    dlq_sink_type = _configured_dlq_sink_type(ctx.resolved.pipeline_config)
    if dlq_sink_type is None:
        return CheckResult(
            name="DLQ replay",
            status=Status.WARN,
            message="DLQ is disabled for the selected pipeline",
            detail="`agora dlq replay` will have no configured backend for this pipeline.",
        )

    if dlq_sink_type not in _SUPPORTED_DLQ_REPLAY_SINK_TYPES:
        supported = ", ".join(sorted(_SUPPORTED_DLQ_REPLAY_SINK_TYPES))
        return CheckResult(
            name="DLQ replay",
            status=Status.FAIL,
            message=f"DLQ sink '{dlq_sink_type}' does not support `agora dlq replay`",
            detail=f"Supported replayable DLQ sinks: {supported}",
        )

    return CheckResult(
        name="DLQ replay",
        status=Status.PASS,
        message=f"DLQ sink '{dlq_sink_type}' supports `agora dlq replay`",
    )


def check_env_vars(
    config_path: str,
    *,
    pipeline_name: str | None = None,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> CheckResult:
    """Check that env vars referenced in the selected config are present."""
    try:
        ctx = _load_doctor_config_context(
            config_path,
            pipeline_name=pipeline_name,
            profile_name=profile_name,
            environment_name=environment_name,
        )
    except _ConfigParseDependencyMissingError:
        return CheckResult(
            name="Environment variables",
            status=Status.WARN,
            message="Cannot parse config: tomllib/tomli not available",
        )
    except Exception as exc:
        return CheckResult(
            name="Environment variables",
            status=Status.FAIL,
            message=f"Cannot read config file: {config_path}",
            detail=str(exc),
        )

    config_obj: object = ctx.resolved.pipeline_config if ctx.resolved is not None else ctx.raw

    env_refs = _collect_env_refs(config_obj)
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


class _ConfigParseDependencyMissingError(RuntimeError):
    """Raised when tomllib/tomli is unavailable for config parsing."""


def _load_toml_file(config_path: str) -> dict[str, Any]:
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ImportError as exc:
            raise _ConfigParseDependencyMissingError("Install tomli: pip install tomli") from exc

    with open(config_path, "rb") as f:
        raw = tomllib.load(f)
    if not isinstance(raw, dict):
        raise TypeError(f"Expected top-level TOML table in {config_path!r}")
    return raw


def _load_doctor_config_context(
    config_path: str,
    *,
    pipeline_name: str | None = None,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> DoctorConfigContext:
    raw = _load_toml_file(config_path)
    if raw.get("format") != "agora/v1":
        return DoctorConfigContext(config_path=config_path, raw=raw, resolved=None)

    validate_config_document(raw)
    resolved = resolve_config_document(
        raw,
        pipeline_name=pipeline_name,
        profile_name=profile_name,
        environment_name=environment_name or os.getenv("AGORA_ENV"),
    )
    return DoctorConfigContext(config_path=config_path, raw=raw, resolved=resolved)


def _configured_dlq_sink_type(pipeline_config: dict[str, Any]) -> str | None:
    dlq_cfg = pipeline_config.get("dlq")
    if not isinstance(dlq_cfg, dict) or not dlq_cfg.get("enabled", True):
        return None

    sink_cfg = dlq_cfg.get("sink")
    if isinstance(sink_cfg, dict):
        sink_type = sink_cfg.get("type")
        if isinstance(sink_type, str) and sink_type.strip():
            return sink_type.strip()
    return "sqlite_dlq"


def _component_type(config: object) -> str | None:
    if not isinstance(config, dict):
        return None
    component_type = config.get("type")
    if isinstance(component_type, str) and component_type.strip():
        return component_type.strip()
    return None


def _pipeline_uses_postgres(pipeline_config: dict[str, Any]) -> bool:
    if _component_type(pipeline_config.get("source")) == "postgres":
        return True
    sinks = pipeline_config.get("sinks", [])
    if isinstance(sinks, list) and any(_component_type(sink) == "postgres" for sink in sinks):
        return True
    dlq_cfg = pipeline_config.get("dlq")
    return (
        isinstance(dlq_cfg, dict)
        and dlq_cfg.get("enabled", True)
        and _component_type(dlq_cfg.get("sink")) == "postgres_dlq"
    )


def _pipeline_uses_component_types(
    pipeline_config: dict[str, Any],
    component_types: set[str],
) -> bool:
    source_type = _component_type(pipeline_config.get("source"))
    if source_type in component_types:
        return True
    sinks = pipeline_config.get("sinks", [])
    if isinstance(sinks, list) and any(_component_type(sink) in component_types for sink in sinks):
        return True
    dlq_cfg = pipeline_config.get("dlq")
    return (
        isinstance(dlq_cfg, dict)
        and dlq_cfg.get("enabled", True)
        and _component_type(dlq_cfg.get("sink")) in component_types
    )


def _postgres_readiness_result(
    *,
    name: str,
    subject: str,
    component: str,
    report: Any,
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
    hooks = _postgres_operator_hooks(subject=subject, report=report)
    detail.extend(f"operator_hook={hook}" for hook in hooks)
    return CheckResult(
        name=name,
        status=status,
        message=(
            f"{subject} passed enterprise readiness checks"
            if report.passed
            else f"{subject} failed enterprise readiness checks"
        ),
        detail="\n".join(detail),
        data=_structured_readiness_data(
            backend="postgres",
            component=component,
            name=name,
            status=status,
            message=(
                f"{subject} passed enterprise readiness checks"
                if report.passed
                else f"{subject} failed enterprise readiness checks"
            ),
            metrics=_parse_key_value_lines(detail_lines),
            findings=findings_payload,
            operator_hooks=hooks,
        ),
    )


def _postgres_operator_hooks(*, subject: str, report: Any) -> list[str]:
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
    if "poison_record_count" in metrics or "poison_record_unknown_count" in metrics:
        hooks.append(
            f"Inspect poison-record classification for {subject} before enabling automatic replay or cutover."
        )
    if "retry_count" in metrics:
        hooks.append(
            f"Investigate repeated retries on {subject}; enterprise readiness expects a clean steady-state startup."
        )
    return hooks


async def _check_kafka_source_readiness(source: object) -> CheckResult:
    if source is None or not hasattr(source, "health_snapshot"):
        return CheckResult(
            name="Kafka source readiness",
            status=Status.FAIL,
            message="Configured Kafka source could not expose readiness state",
            detail="Expected a live Kafka source instance with health_snapshot().",
            data=_structured_readiness_data(
                backend="kafka",
                component="source",
                name="Kafka source readiness",
                status=Status.FAIL,
                message="Configured Kafka source could not expose readiness state",
                metrics={},
                findings=[
                    {
                        "metric": "health_snapshot",
                        "message": "Expected a live Kafka source instance with health_snapshot().",
                        "value": None,
                        "threshold": "present",
                    }
                ],
                operator_hooks=[
                    "Verify the configured Kafka source plugin loads and starts cleanly before cutover."
                ],
            ),
        )

    health = await _call_async_with_optional_kwargs(source, "health_snapshot", force_refresh=True)
    runtime_metrics = getattr(source, "runtime_metrics", lambda: None)()
    operational_metrics = getattr(source, "operational_metrics", lambda: None)()
    detail_lines = [
        f"consumer_group={getattr(health, 'consumer_group', 'unknown')}",
        f"subscription_mode={getattr(health, 'subscription_mode', 'unknown')}",
        f"assignment_count={getattr(health, 'assignment_count', 'unknown')}",
        f"pending_commit_count={getattr(health, 'pending_commit_count', 'unknown')}",
        f"rebalance_count={getattr(health, 'rebalance_count', 'unknown')}",
    ]
    total_lag = getattr(health, "total_lag", None)
    if total_lag is not None:
        detail_lines.append(f"total_lag={total_lag}")
    status = Status.PASS
    message = "Kafka source passed enterprise readiness checks"
    hooks: list[str] = []
    if getattr(health, "stalled", False):
        status = Status.FAIL
        message = "Kafka source is stalled"
        hooks.append(
            "Inspect broker connectivity, rebalance churn, or pause/resume orchestration before cutover."
        )
    elif not getattr(health, "ready", False):
        status = Status.WARN
        message = "Kafka source opened but has no active partition assignment yet"
        hooks.append(
            "Verify topic existence, ACLs, and consumer-group coordinator state until partition assignment becomes stable."
        )
    if getattr(runtime_metrics, "record_error_count", 0) > 0:
        status = Status.FAIL
        message = "Kafka source has source-level record errors"
        hooks.append(
            "Inspect poison-record classification counters and DLQ flow before promoting this consumer."
        )
    if getattr(health, "pending_commit_count", 0) > 0 and status == Status.PASS:
        status = Status.WARN
        message = "Kafka source has pending commits at readiness time"
        hooks.append(
            "Let commit-safe handoff drain pending acknowledgements before rolling forward."
        )
    if getattr(operational_metrics, "poison_record_fail_closed_count", 0) > 0:
        hooks.append(
            "A fail-closed poison policy has already fired; verify schema or payload fixes before restart."
        )
    detail_lines.extend(f"operator_hook={hook}" for hook in dict.fromkeys(hooks))
    rendered_hooks = list(dict.fromkeys(hooks))
    return CheckResult(
        name="Kafka source readiness",
        status=status,
        message=message,
        detail="\n".join(detail_lines),
        data=_structured_readiness_data(
            backend="kafka",
            component="source",
            name="Kafka source readiness",
            status=status,
            message=message,
            metrics=_parse_key_value_lines(
                [line for line in detail_lines if not line.startswith("operator_hook=")]
            ),
            findings=[],
            operator_hooks=rendered_hooks,
        ),
    )


def _check_kafka_sink_readiness(sink: object, *, index: int) -> CheckResult:
    if sink is None:
        return CheckResult(
            name=f"Kafka sink readiness #{index}",
            status=Status.FAIL,
            message="Configured Kafka sink instance is missing",
            data=_structured_readiness_data(
                backend="kafka",
                component="sink",
                name=f"Kafka sink readiness #{index}",
                status=Status.FAIL,
                message="Configured Kafka sink instance is missing",
                metrics={},
                findings=[
                    {
                        "metric": "sink_instance",
                        "message": "Configured Kafka sink instance is missing",
                        "value": None,
                        "threshold": "present",
                    }
                ],
                operator_hooks=[
                    "Verify the configured Kafka sink plugin loads and starts cleanly before cutover."
                ],
            ),
        )
    producer = getattr(sink, "_producer", None)
    topic = getattr(sink, "_topic", "unknown")
    bootstrap = getattr(sink, "_bootstrap", "unknown")
    ready = producer is not None
    detail_lines = [
        f"topic={topic}",
        f"bootstrap_servers={bootstrap}",
        f"producer_ready={ready}",
        "operator_hook=Verify idempotent producer auth, TLS, and topic ACLs before cutover."
        if not ready
        else "operator_hook=Producer startup succeeded; keep an eye on delivery acks during first live traffic.",
    ]
    message = (
        f"Kafka sink {topic!r} passed enterprise readiness checks"
        if ready
        else f"Kafka sink {topic!r} failed enterprise readiness checks"
    )
    hooks = [
        "Verify idempotent producer auth, TLS, and topic ACLs before cutover."
        if not ready
        else "Producer startup succeeded; keep an eye on delivery acks during first live traffic."
    ]
    return CheckResult(
        name=f"Kafka sink readiness #{index}",
        status=Status.PASS if ready else Status.FAIL,
        message=message,
        detail="\n".join(detail_lines),
        data=_structured_readiness_data(
            backend="kafka",
            component="sink",
            name=f"Kafka sink readiness #{index}",
            status=Status.PASS if ready else Status.FAIL,
            message=message,
            metrics=_parse_key_value_lines(
                [line for line in detail_lines if not line.startswith("operator_hook=")]
            ),
            findings=[],
            operator_hooks=hooks,
        ),
    )


def _check_kafka_dlq_readiness(dlq_sink: object) -> CheckResult:
    if dlq_sink is None or not hasattr(dlq_sink, "metrics_snapshot"):
        return CheckResult(
            name="Kafka DLQ readiness",
            status=Status.FAIL,
            message="Configured Kafka DLQ could not expose readiness metrics",
            detail="Expected a live Kafka DLQ sink instance with metrics_snapshot().",
            data=_structured_readiness_data(
                backend="kafka",
                component="dlq",
                name="Kafka DLQ readiness",
                status=Status.FAIL,
                message="Configured Kafka DLQ could not expose readiness metrics",
                metrics={},
                findings=[
                    {
                        "metric": "metrics_snapshot",
                        "message": "Expected a live Kafka DLQ sink instance with metrics_snapshot().",
                        "value": None,
                        "threshold": "present",
                    }
                ],
                operator_hooks=[
                    "Verify the configured Kafka DLQ plugin loads and starts cleanly before cutover."
                ],
            ),
        )
    snapshot = dlq_sink.metrics_snapshot()
    topic = getattr(snapshot, "topic", "unknown")
    bootstrap = getattr(snapshot, "bootstrap_servers", "unknown")
    message = f"Kafka DLQ {topic!r} passed enterprise readiness checks"
    hooks = [
        "Validate DLQ topic retention and replay consumers before relying on poison-record isolation."
    ]
    return CheckResult(
        name="Kafka DLQ readiness",
        status=Status.PASS,
        message=message,
        detail="\n".join(
            [
                f"topic={topic}",
                f"bootstrap_servers={bootstrap}",
                f"operator_hook={hooks[0]}",
            ]
        ),
        data=_structured_readiness_data(
            backend="kafka",
            component="dlq",
            name="Kafka DLQ readiness",
            status=Status.PASS,
            message=message,
            metrics={"topic": topic, "bootstrap_servers": bootstrap},
            findings=[],
            operator_hooks=hooks,
        ),
    )


def _check_redis_stream_source_readiness(source: object) -> CheckResult:
    if source is None:
        return CheckResult(
            name="Redis stream source readiness",
            status=Status.FAIL,
            message="Configured Redis stream source instance is missing",
            data=_structured_readiness_data(
                backend="redis",
                component="source",
                name="Redis stream source readiness",
                status=Status.FAIL,
                message="Configured Redis stream source instance is missing",
                metrics={},
                findings=[
                    {
                        "metric": "source_instance",
                        "message": "Configured Redis stream source instance is missing",
                        "value": None,
                        "threshold": "present",
                    }
                ],
                operator_hooks=[
                    "Verify the configured Redis stream source plugin loads and starts cleanly before cutover."
                ],
            ),
        )
    ready = getattr(source, "_client", None) is not None
    runtime_metrics = getattr(source, "runtime_metrics", lambda: None)()
    supports_checkpoint = bool(getattr(source, "supports_checkpoint", False))
    detail_lines = [
        f"stream={getattr(source, '_stream', 'unknown')}",
        f"group={getattr(source, '_group', 'unknown')}",
        f"consumer={getattr(source, '_consumer', 'unknown')}",
        f"supports_checkpoint={supports_checkpoint}",
        f"connection_ready={ready}",
    ]
    status = Status.PASS if ready and supports_checkpoint else Status.FAIL
    message = (
        "Redis stream source passed enterprise readiness checks"
        if status == Status.PASS
        else "Redis stream source failed enterprise readiness checks"
    )
    hooks: list[str] = []
    if not ready:
        hooks.append("Verify Redis URL, ACLs, and stream/group existence before cutover.")
    if not supports_checkpoint:
        hooks.append(
            "Redis stream recovery expects checkpointable message IDs before enterprise cutover."
        )
    if getattr(runtime_metrics, "record_error_count", 0) > 0:
        status = Status.FAIL
        message = "Redis stream source has source-level record errors"
        hooks.append("Inspect deserializer failures or reclaim loops before restarting consumers.")
    rendered_hooks = list(dict.fromkeys(hooks))
    detail_lines.extend(f"operator_hook={hook}" for hook in rendered_hooks)
    return CheckResult(
        name="Redis stream source readiness",
        status=status,
        message=message,
        detail="\n".join(detail_lines),
        data=_structured_readiness_data(
            backend="redis",
            component="source",
            name="Redis stream source readiness",
            status=status,
            message=message,
            metrics=_parse_key_value_lines(
                [line for line in detail_lines if not line.startswith("operator_hook=")]
            ),
            findings=[],
            operator_hooks=rendered_hooks,
        ),
    )


def _check_redis_sink_readiness(sink: object, *, index: int) -> CheckResult:
    if sink is None or not hasattr(sink, "metrics_snapshot"):
        return CheckResult(
            name=f"Redis sink readiness #{index}",
            status=Status.FAIL,
            message="Configured Redis sink could not expose readiness metrics",
            detail="Expected a live Redis sink instance with metrics_snapshot().",
            data=_structured_readiness_data(
                backend="redis",
                component="sink",
                name=f"Redis sink readiness #{index}",
                status=Status.FAIL,
                message="Configured Redis sink could not expose readiness metrics",
                metrics={},
                findings=[
                    {
                        "metric": "metrics_snapshot",
                        "message": "Expected a live Redis sink instance with metrics_snapshot().",
                        "value": None,
                        "threshold": "present",
                    }
                ],
                operator_hooks=[
                    "Verify the configured Redis sink plugin loads and starts cleanly before cutover."
                ],
            ),
        )
    snapshot = sink.metrics_snapshot()
    ready = bool(getattr(snapshot, "connection_ready", False))
    message = (
        f"Redis sink {snapshot.target!r} passed enterprise readiness checks"
        if ready
        else f"Redis sink {snapshot.target!r} failed enterprise readiness checks"
    )
    hooks = [
        "Verify Redis memory policy, TTL, and write mode semantics before production cutover."
        if ready
        else "Verify Redis URL, ACLs, and target database reachability before cutover."
    ]
    return CheckResult(
        name=f"Redis sink readiness #{index}",
        status=Status.PASS if ready else Status.FAIL,
        message=message,
        detail="\n".join(
            [
                f"target={snapshot.target}",
                f"mode={snapshot.mode}",
                f"connection_ready={snapshot.connection_ready}",
                f"operator_hook={hooks[0]}",
            ]
        ),
        data=_structured_readiness_data(
            backend="redis",
            component="sink",
            name=f"Redis sink readiness #{index}",
            status=Status.PASS if ready else Status.FAIL,
            message=message,
            metrics={
                "target": snapshot.target,
                "mode": snapshot.mode,
                "connection_ready": snapshot.connection_ready,
            },
            findings=[],
            operator_hooks=hooks,
        ),
    )


def _check_redis_dlq_readiness(dlq_sink: object) -> CheckResult:
    if dlq_sink is None:
        return CheckResult(
            name="Redis DLQ readiness",
            status=Status.FAIL,
            message="Configured Redis DLQ instance is missing",
            data=_structured_readiness_data(
                backend="redis",
                component="dlq",
                name="Redis DLQ readiness",
                status=Status.FAIL,
                message="Configured Redis DLQ instance is missing",
                metrics={},
                findings=[
                    {
                        "metric": "dlq_instance",
                        "message": "Configured Redis DLQ instance is missing",
                        "value": None,
                        "threshold": "present",
                    }
                ],
                operator_hooks=[
                    "Verify the configured Redis DLQ plugin loads and starts cleanly before cutover."
                ],
            ),
        )
    ready = getattr(dlq_sink, "_client", None) is not None
    key_prefix = getattr(dlq_sink, "_key_prefix", "agora:dlq")
    message = (
        f"Redis DLQ {key_prefix!r} passed enterprise readiness checks"
        if ready
        else f"Redis DLQ {key_prefix!r} failed enterprise readiness checks"
    )
    hooks = [
        "Validate DLQ key retention and replay cleanup rules before relying on Redis poison isolation."
        if ready
        else "Verify Redis DLQ connectivity and ACLs before enabling replay workflows."
    ]
    return CheckResult(
        name="Redis DLQ readiness",
        status=Status.PASS if ready else Status.FAIL,
        message=message,
        detail="\n".join(
            [
                f"key_prefix={key_prefix}",
                f"connection_ready={ready}",
                f"operator_hook={hooks[0]}",
            ]
        ),
        data=_structured_readiness_data(
            backend="redis",
            component="dlq",
            name="Redis DLQ readiness",
            status=Status.PASS if ready else Status.FAIL,
            message=message,
            metrics={
                "key_prefix": key_prefix,
                "connection_ready": ready,
            },
            findings=[],
            operator_hooks=hooks,
        ),
    )


async def _call_async_with_optional_kwargs(
    instance: object,
    method_name: str,
    **kwargs: Any,
) -> Any:
    method = getattr(instance, method_name)
    try:
        return await method(**kwargs)
    except TypeError:
        return await method()


def _structured_readiness_data(
    *,
    backend: str,
    component: str,
    name: str,
    status: Status,
    message: str,
    metrics: dict[str, Any],
    findings: list[dict[str, Any]],
    operator_hooks: list[str],
) -> dict[str, Any]:
    return {
        "category": "enterprise_readiness",
        "backend": backend,
        "component": component,
        "name": name,
        "status": status.value,
        "message": message,
        "metrics": metrics,
        "findings": findings,
        "operator_hooks": operator_hooks,
    }


def _parse_key_value_lines(lines: list[str]) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for line in lines:
        if "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        metrics[key] = _coerce_scalar(raw_value)
    return metrics


def _coerce_scalar(value: str) -> Any:
    normalized = value.strip()
    if normalized.lower() in {"true", "false"}:
        return normalized.lower() == "true"
    try:
        return int(normalized)
    except ValueError:
        pass
    return normalized


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
    description = (
        "Preflight Python/version/plugin/config health. "
        "Config and plugin imports are treated as trusted code."
    )

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "pipeline",
            nargs="?",
            default=None,
            help="Optional named pipeline to select from an agora/v1 config file.",
        )
        parser.add_argument(
            "--config",
            default=None,
            metavar="FILE",
            help="Optional TOML config file to check import refs and env vars against.",
        )
        parser.add_argument(
            "--profile",
            default=None,
            help="Select a config profile overlay from [profiles.<name>].",
        )
        parser.add_argument(
            "--environment",
            default=None,
            help="Select a config environment overlay from [environments.<name>].",
        )
        parser.add_argument(
            "--json",
            action="store_true",
            help="Emit a machine-readable JSON doctor report.",
        )

    def execute(self, args: argparse.Namespace, ctx: AgoraContext) -> int:
        ensure_project_on_path(ctx)
        report = DoctorReport()

        report.add(check_python_version())
        report.add(check_agora_importable())
        report.add(check_plugins_importable())
        report.add(
            check_acceleration(
                args.config,
                pipeline_name=args.pipeline,
                profile_name=args.profile,
                environment_name=args.environment,
            )
        )
        report.add(check_entrypoint_plugins())

        if args.config:
            report.add(
                check_config_pipeline_resolution(
                    args.config,
                    pipeline_name=args.pipeline,
                    profile_name=args.profile,
                    environment_name=args.environment,
                )
            )
            report.add(
                check_config_import_refs(
                    args.config,
                    pipeline_name=args.pipeline,
                    profile_name=args.profile,
                    environment_name=args.environment,
                )
            )
            report.add(
                check_config_pipeline_build(
                    args.config,
                    pipeline_name=args.pipeline,
                    profile_name=args.profile,
                    environment_name=args.environment,
                )
            )
            for readiness_result in check_postgres_enterprise_readiness(
                args.config,
                pipeline_name=args.pipeline,
                profile_name=args.profile,
                environment_name=args.environment,
            ):
                report.add(readiness_result)
            for readiness_result in check_kafka_enterprise_readiness(
                args.config,
                pipeline_name=args.pipeline,
                profile_name=args.profile,
                environment_name=args.environment,
            ):
                report.add(readiness_result)
            for readiness_result in check_redis_enterprise_readiness(
                args.config,
                pipeline_name=args.pipeline,
                profile_name=args.profile,
                environment_name=args.environment,
            ):
                report.add(readiness_result)
            report.add(
                check_recovery_posture(
                    args.config,
                    pipeline_name=args.pipeline,
                    profile_name=args.profile,
                    environment_name=args.environment,
                )
            )
            report.add(
                check_dlq_replay_support(
                    args.config,
                    pipeline_name=args.pipeline,
                    profile_name=args.profile,
                    environment_name=args.environment,
                )
            )
            report.add(
                check_env_vars(
                    args.config,
                    pipeline_name=args.pipeline,
                    profile_name=args.profile,
                    environment_name=args.environment,
                )
            )

        if getattr(args, "json", False):
            console.out(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        else:
            _render_report(report)
        return 1 if report.failed else 0


__all__ = [
    "CheckResult",
    "DoctorCommand",
    "DoctorConfigContext",
    "DoctorReport",
    "Status",
    "check_acceleration",
    "check_agora_importable",
    "check_config_import_refs",
    "check_config_pipeline_build",
    "check_config_pipeline_resolution",
    "check_dlq_replay_support",
    "check_entrypoint_plugins",
    "check_env_vars",
    "check_kafka_enterprise_readiness",
    "check_plugins_importable",
    "check_postgres_enterprise_readiness",
    "check_python_version",
    "check_recovery_posture",
    "check_redis_enterprise_readiness",
]
