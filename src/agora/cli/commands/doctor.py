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
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from agora.cli._path import ensure_project_on_path
from agora.cli.commands.base import BaseCommand
from agora.cli.console import console
from agora.cli.recovery import recovery_insight_for_source
from agora.config import resolve_config_document, validate_config_document
from agora.core.acceleration import acceleration_status, normalize_acceleration_mode
from agora.core.component_factory import config_component_factory
from agora.core.discovery import public_entrypoint_group_contracts
from agora.core.doctor import (
    DOCTOR_READINESS_ENTRYPOINT_GROUP,
    CheckResult,
    DoctorReadinessProvider,
    DoctorReadinessProviderEntry,
    DoctorReport,
    Status,
    discover_doctor_readiness_providers,
)
from agora.core.packaging import FIRST_PARTY_PLUGIN_DISTRIBUTION, first_party_plugin_install_detail
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


@dataclass(frozen=True)
class DoctorConfigContext:
    """Loaded config file plus optional resolved agora/v1 pipeline selection."""

    config_path: str
    raw: dict[str, Any]
    resolved: ResolvedPipelineConfig | None = None


@dataclass(frozen=True, slots=True)
class _PluginReadinessSpec:
    package_name: str
    readiness_name: str
    backend: str
    component_types: frozenset[str]


_PLUGIN_READINESS_SPECS = (
    _PluginReadinessSpec(
        package_name="agora_plugins.postgres",
        readiness_name="Postgres enterprise readiness",
        backend="postgres",
        component_types=frozenset({"postgres", "postgres_dlq"}),
    ),
    _PluginReadinessSpec(
        package_name="agora_plugins.kafka",
        readiness_name="Kafka enterprise readiness",
        backend="kafka",
        component_types=frozenset({"kafka", "kafka_dlq"}),
    ),
    _PluginReadinessSpec(
        package_name="agora_plugins.redis",
        readiness_name="Redis enterprise readiness",
        backend="redis",
        component_types=frozenset({"redis", "redis_dlq", "redis_stream"}),
    ),
)


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
            name=FIRST_PARTY_PLUGIN_DISTRIBUTION,
            status=Status.PASS,
            message=f"{FIRST_PARTY_PLUGIN_DISTRIBUTION} {version} importable",
        )
    except ImportError:
        return CheckResult(
            name=FIRST_PARTY_PLUGIN_DISTRIBUTION,
            status=Status.WARN,
            message=f"{FIRST_PARTY_PLUGIN_DISTRIBUTION} not installed",
            detail=first_party_plugin_install_detail(),
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

        seen_doctor_entrypoints: dict[str, tuple[str | None, str | None]] = {}
        for ep in entry_points(group=DOCTOR_READINESS_ENTRYPOINT_GROUP):
            distribution = getattr(ep, "dist", None)
            distribution_name = getattr(distribution, "name", None)
            distribution_version = getattr(distribution, "version", None)

            existing_dist = seen_doctor_entrypoints.get(ep.name)
            if existing_dist is not None:
                if existing_dist != (distribution_name, distribution_version):
                    conflicts.append(
                        f"{ep.name} [{DOCTOR_READINESS_ENTRYPOINT_GROUP}] is declared by multiple installed plugins"
                    )
                continue

            seen_doctor_entrypoints[ep.name] = (distribution_name, distribution_version)
            try:
                provider = ep.load()
                if not isinstance(provider, DoctorReadinessProvider):
                    failed.append(
                        f"{ep.name} [{DOCTOR_READINESS_ENTRYPOINT_GROUP}]: "
                        "TypeError: loaded object does not implement DoctorReadinessProvider"
                    )
                    continue
                loaded += 1
            except Exception as exc:
                failed.append(
                    f"{ep.name} [{DOCTOR_READINESS_ENTRYPOINT_GROUP}]: {type(exc).__name__}: {exc}"
                )

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
    """Compatibility wrapper around the PostgreSQL plugin-owned provider."""
    return _check_single_plugin_readiness(
        _PLUGIN_READINESS_SPECS[0],
        config_path,
        pipeline_name=pipeline_name,
        profile_name=profile_name,
        environment_name=environment_name,
    )


def check_kafka_enterprise_readiness(
    config_path: str,
    *,
    pipeline_name: str | None = None,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> list[CheckResult]:
    """Compatibility wrapper around the Kafka plugin-owned provider."""
    return _check_single_plugin_readiness(
        _PLUGIN_READINESS_SPECS[1],
        config_path,
        pipeline_name=pipeline_name,
        profile_name=profile_name,
        environment_name=environment_name,
    )


def check_redis_enterprise_readiness(
    config_path: str,
    *,
    pipeline_name: str | None = None,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> list[CheckResult]:
    """Compatibility wrapper around the Redis plugin-owned provider."""
    return _check_single_plugin_readiness(
        _PLUGIN_READINESS_SPECS[2],
        config_path,
        pipeline_name=pipeline_name,
        profile_name=profile_name,
        environment_name=environment_name,
    )


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


def _pipeline_uses_component_types(
    pipeline_config: dict[str, Any],
    component_types: frozenset[str],
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


def _check_single_plugin_readiness(
    spec: _PluginReadinessSpec,
    config_path: str,
    *,
    pipeline_name: str | None = None,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> list[CheckResult]:
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
                name=spec.readiness_name,
                status=Status.WARN,
                message="Cannot parse config: tomllib/tomli not available",
                detail=str(exc),
            )
        ]
    except Exception as exc:
        return [
            CheckResult(
                name=spec.readiness_name,
                status=Status.FAIL,
                message=f"Cannot read config file: {config_path}",
                detail=str(exc),
            )
        ]

    if ctx.resolved is None:
        return []

    return _run_plugin_readiness_checks_for_context(
        spec, pipeline_config=ctx.resolved.pipeline_config
    )


def _check_all_plugin_readiness(
    config_path: str,
    *,
    pipeline_name: str | None = None,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> list[CheckResult]:
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
                name="Plugin enterprise readiness",
                status=Status.WARN,
                message="Cannot parse config: tomllib/tomli not available",
                detail=str(exc),
            )
        ]
    except Exception as exc:
        return [
            CheckResult(
                name="Plugin enterprise readiness",
                status=Status.FAIL,
                message=f"Cannot read config file: {config_path}",
                detail=str(exc),
            )
        ]

    if ctx.resolved is None:
        return []

    try:
        providers = discover_doctor_readiness_providers()
    except Exception as exc:
        return [
            CheckResult(
                name="Plugin readiness provider discovery",
                status=Status.FAIL,
                message="Cannot discover plugin readiness providers",
                detail=str(exc),
            )
        ]

    results: list[CheckResult] = []
    pipeline_config = ctx.resolved.pipeline_config
    for entry in providers:
        results.extend(
            _run_discovered_plugin_readiness_provider(
                entry=entry,
                pipeline_config=pipeline_config,
            )
        )
    return results


def _run_plugin_readiness_checks_for_context(
    spec: _PluginReadinessSpec,
    *,
    pipeline_config: dict[str, Any],
) -> list[CheckResult]:
    if not _pipeline_uses_component_types(pipeline_config, spec.component_types):
        return []
    try:
        return _run_plugin_readiness_provider(spec=spec, pipeline_config=pipeline_config)
    except Exception as exc:
        return [
            CheckResult(
                name=spec.readiness_name,
                status=Status.FAIL,
                message=f"{spec.backend.capitalize()} readiness checks could not complete",
                detail=str(exc),
            )
        ]


def _run_discovered_plugin_readiness_provider(
    *,
    entry: DoctorReadinessProviderEntry,
    pipeline_config: dict[str, Any],
) -> list[CheckResult]:
    provider = entry.provider
    if not _pipeline_uses_component_types(pipeline_config, provider.component_types):
        return []

    try:
        return asyncio.run(provider.run_readiness_checks(pipeline_config))
    except Exception as exc:
        return [
            CheckResult(
                name=f"{provider.backend.replace('_', ' ').title()} enterprise readiness",
                status=Status.FAIL,
                message=f"{provider.backend.replace('_', ' ').title()} readiness checks could not complete",
                detail=str(exc),
            )
        ]


def _load_plugin_readiness_provider(spec: _PluginReadinessSpec) -> DoctorReadinessProvider:
    plugin_module = importlib.import_module(spec.package_name)
    provider = getattr(plugin_module, "_doctor_readiness_provider", None)
    if provider is None:
        raise RuntimeError(
            f"{spec.package_name!r} does not expose a matching doctor readiness provider."
        )
    return _coerce_plugin_readiness_provider(spec=spec, provider=provider)


def _coerce_plugin_readiness_provider(
    *,
    spec: _PluginReadinessSpec,
    provider: object,
) -> DoctorReadinessProvider:
    backend = getattr(provider, "backend", None)
    component_types = getattr(provider, "component_types", None)
    runner = getattr(provider, "run_readiness_checks", None)
    if backend != spec.backend:
        raise RuntimeError(
            f"{spec.package_name!r} doctor readiness provider reported backend {backend!r}; "
            f"expected {spec.backend!r}."
        )
    if component_types != spec.component_types:
        raise RuntimeError(
            f"{spec.package_name!r} doctor readiness provider reported component_types "
            f"{component_types!r}; expected {spec.component_types!r}."
        )
    if not callable(runner):
        raise TypeError(
            f"{spec.package_name!r} doctor readiness provider must define run_readiness_checks()."
        )
    return cast("DoctorReadinessProvider", provider)


def _run_plugin_readiness_provider(
    *,
    spec: _PluginReadinessSpec,
    pipeline_config: dict[str, Any],
) -> list[CheckResult]:
    provider = _load_plugin_readiness_provider(spec)
    return asyncio.run(provider.run_readiness_checks(pipeline_config))


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
            for readiness_result in _check_all_plugin_readiness(
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
