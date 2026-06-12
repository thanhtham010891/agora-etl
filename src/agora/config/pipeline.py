"""Schema validation and overlay resolution for declarative pipeline configs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from agora.core.errors import ConfigError

if TYPE_CHECKING:
    from collections.abc import Iterable

# Allowlist: only dotted Python identifiers optionally followed by :attr.
# Blocks absolute paths (/etc/...), shell metacharacters, and traversal (../).
_IMPORT_PATH_RE = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*"
    r"(:[A-Za-z_][A-Za-z0-9_]*)?$"
)


def validate_import_path(value: str, *, field_label: str = "import") -> str:
    """Validate format of a declarative import path from a TOML config.

    Only checks that the path is a valid dotted Python identifier with an
    optional ':attribute' suffix. Namespace allowlist enforcement happens at
    the actual import execution layer (component_factory), not here.

    Raises ConfigError if the path does not match the expected format.
    """
    value = value.strip()
    if not _IMPORT_PATH_RE.match(value):
        raise ConfigError(
            f"Invalid {field_label} path {value!r}: must be a dotted Python identifier "
            "with an optional ':attribute' suffix (e.g. 'agora.sinks.file:FileSink')."
        )
    return value


class ImportRefConfig(BaseModel):
    """Declarative import reference used inside TOML configs."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    import_path: str = Field(alias="import")

    @field_validator("import_path")
    @classmethod
    def _validate_import_path(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Import reference cannot be empty.")
        try:
            validate_import_path(value, field_label="import")
        except ConfigError as exc:
            raise ValueError(str(exc)) from exc
        return value


class ComponentConfig(BaseModel):
    """Base shape for source, middleware, and sink configs."""

    model_config = ConfigDict(extra="allow")

    type: str

    @field_validator("type")
    @classmethod
    def _validate_type(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Component type cannot be empty.")
        return value


class DedupConfig(BaseModel):
    """Schema for the special-case dedup middleware section."""

    model_config = ConfigDict(extra="allow")

    key: str | ImportRefConfig
    store: ComponentConfig | None = None
    strategy: ComponentConfig | None = None

    @field_validator("key")
    @classmethod
    def _validate_key(cls, value: str | ImportRefConfig) -> str | ImportRefConfig:
        if isinstance(value, str):
            value = value.strip()
            if not value:
                raise ValueError("Dedup key cannot be empty.")
        return value


class DLQConfig(BaseModel):
    """Schema for optional dead-letter queue configuration."""

    model_config = ConfigDict(extra="allow")

    enabled: bool = True
    failure_policy: Literal["log_only", "raise"] = "log_only"
    sink: ComponentConfig | None = None

    @field_validator("sink")
    @classmethod
    def _validate_sink(cls, value: ComponentConfig | None) -> ComponentConfig | None:
        return value


class TracingConfig(BaseModel):
    """Schema for optional tracing configuration."""

    model_config = ConfigDict(extra="allow")

    enabled: bool = True
    backend: Literal["noop", "in_memory", "opentelemetry"] = "opentelemetry"
    auto_configure: bool = True
    service_name: str | None = None

    @field_validator("service_name")
    @classmethod
    def _validate_service_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("Tracing service_name cannot be empty.")
        return value


class PerformanceConfig(BaseModel):
    """Schema for optional runtime performance policy."""

    model_config = ConfigDict(extra="forbid")

    acceleration: Literal["auto", "off", "required"] = "auto"
    profile: Literal["balanced", "throughput", "low_latency"] = "balanced"

    @field_validator("acceleration", "profile", mode="before")
    @classmethod
    def _normalize_policy_value(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip().lower()
        return value


class PipelineConfig(BaseModel):
    """Validated root schema for one declarative pipeline."""

    model_config = ConfigDict(extra="allow")

    pipeline_id: str = "pipeline"
    source: ComponentConfig
    middlewares: list[ComponentConfig] = Field(default_factory=list)
    dedup: DedupConfig | None = None
    dlq: DLQConfig | None = None
    schedule: ScheduleConfig | None = None
    tracing: TracingConfig | None = None
    performance: PerformanceConfig | None = None
    sinks: list[ComponentConfig] = Field(default_factory=list)

    @field_validator("pipeline_id")
    @classmethod
    def _validate_pipeline_id(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Pipeline ID cannot be empty.")
        return value

    @model_validator(mode="after")
    def _validate_required_runtime_sections(self) -> PipelineConfig:
        if not self.sinks:
            raise ValueError("At least one sink must be defined.")
        return self


class ConfigDefaults(BaseModel):
    """Default selectors used when CLI flags are omitted."""

    model_config = ConfigDict(extra="forbid")

    pipeline: str | None = None
    profile: str | None = None
    environment: str | None = None

    @field_validator("pipeline", "profile", "environment")
    @classmethod
    def _validate_optional_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("Selector values cannot be empty.")
        return value


class OverlayScope(BaseModel):
    """Pipeline-scoped overlays for one profile or environment."""

    model_config = ConfigDict(extra="forbid")

    pipelines: dict[str, dict[str, Any]] = Field(default_factory=dict)
    worker: dict[str, Any] | None = None


class ScheduleConfig(BaseModel):
    """Schema for optional scheduled worker execution."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["every", "cron", "continuous", "once"]
    seconds: float = 0.0
    minutes: float = 0.0
    hours: float = 0.0
    days: float = 0.0
    expression: str | None = None

    @model_validator(mode="after")
    def _validate_schedule_shape(self) -> ScheduleConfig:
        if self.mode == "every":
            total = self.seconds + self.minutes * 60 + self.hours * 3600 + self.days * 86400
            if total <= 0:
                raise ValueError("Schedule.every requires a positive duration.")
            if self.expression is not None:
                raise ValueError("Schedule.every does not accept expression.")
            self.expression = None
            return self
        if self.mode == "cron":
            if self.expression is None or not self.expression.strip():
                raise ValueError("Schedule.cron requires expression.")
            self.expression = self.expression.strip()
            self.seconds = 0.0
            self.minutes = 0.0
            self.hours = 0.0
            self.days = 0.0
            return self
        if self.mode in {"continuous", "once"}:
            if self.expression is not None:
                raise ValueError(f"Schedule.{self.mode} does not accept expression.")
            if any(value != 0 for value in (self.seconds, self.minutes, self.hours, self.days)):
                raise ValueError(f"Schedule.{self.mode} does not accept duration fields.")
            self.expression = None
            self.seconds = 0.0
            self.minutes = 0.0
            self.hours = 0.0
            self.days = 0.0
            return self
        raise ValueError(f"Unknown schedule mode {self.mode!r}.")


class WorkerConfig(BaseModel):
    """Schema for config-driven worker startup."""

    model_config = ConfigDict(extra="forbid")

    graceful_shutdown_timeout: float = 30.0
    health_port: int | None = None
    health_host: str = "127.0.0.1"
    health_auth_token: str | None = None


class ConfigDocument(BaseModel):
    """Top-level declarative config file schema."""

    model_config = ConfigDict(extra="forbid")

    format: Literal["agora/v1"]
    defaults: ConfigDefaults = Field(default_factory=ConfigDefaults)
    performance: PerformanceConfig | None = None
    worker: WorkerConfig | None = None
    pipelines: dict[str, dict[str, Any]]
    profiles: dict[str, OverlayScope] = Field(default_factory=dict)
    environments: dict[str, OverlayScope] = Field(default_factory=dict)

    @field_validator("pipelines")
    @classmethod
    def _validate_pipelines(cls, value: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        if not value:
            raise ValueError("At least one pipeline must be defined.")
        return value


@dataclass(frozen=True, slots=True)
class ResolvedPipelineConfig:
    """One fully resolved pipeline selection from a config document."""

    pipeline_name: str
    profile_name: str | None
    environment_name: str | None
    pipeline_config: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ResolvedWorkerConfig:
    """One fully resolved worker config document."""

    profile_name: str | None
    environment_name: str | None
    worker_config: dict[str, Any]
    pipelines: tuple[ResolvedPipelineConfig, ...]


def validate_pipeline_config(config: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize one pipeline config dictionary."""
    _ensure_pipeline_has_sinks(config)
    try:
        validated = PipelineConfig.model_validate(config)
    except ValidationError as exc:
        raise ConfigError(_format_validation_error("pipeline", exc)) from exc

    return validated.model_dump(mode="python", by_alias=True, exclude_none=True)


def validate_config_document(config: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize one top-level config document."""
    try:
        validated = ConfigDocument.model_validate(config)
    except ValidationError as exc:
        raise ConfigError(_format_validation_error("config document", exc)) from exc

    return validated.model_dump(mode="python", by_alias=True, exclude_none=True)


def resolve_config_document(
    config: dict[str, Any],
    *,
    pipeline_name: str | None = None,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> ResolvedPipelineConfig:
    """Resolve pipeline selection and overlays from a validated config document."""
    try:
        document = ConfigDocument.model_validate(config)
    except ValidationError as exc:
        raise ConfigError(_format_validation_error("config document", exc)) from exc

    selected_pipeline = _select_name(
        explicit_value=pipeline_name,
        default_value=document.defaults.pipeline,
        available_names=document.pipelines.keys(),
        label="pipeline",
    )

    base_pipeline = document.pipelines.get(selected_pipeline)
    if base_pipeline is None:
        available = ", ".join(sorted(document.pipelines))
        raise ConfigError(f"Pipeline '{selected_pipeline}' was not found. Available: {available}")

    document_defaults: dict[str, Any] = {}
    if document.performance is not None:
        document_defaults["performance"] = document.performance.model_dump(
            mode="python",
            by_alias=True,
            exclude_none=True,
        )
    merged = deep_merge(document_defaults, base_pipeline)
    selected_profile = _select_optional_overlay(profile_name, document.defaults.profile)
    selected_environment = _select_optional_overlay(
        environment_name,
        document.defaults.environment,
    )

    merged = _apply_named_overlay(
        merged,
        label="profile",
        selected_name=selected_profile,
        scopes=document.profiles,
        pipeline_name=selected_pipeline,
    )
    merged = _apply_named_overlay(
        merged,
        label="environment",
        selected_name=selected_environment,
        scopes=document.environments,
        pipeline_name=selected_pipeline,
    )
    merged.setdefault("pipeline_id", selected_pipeline)

    return ResolvedPipelineConfig(
        pipeline_name=selected_pipeline,
        profile_name=selected_profile,
        environment_name=selected_environment,
        pipeline_config=validate_pipeline_config(merged),
    )


def describe_pipeline_config(config: dict[str, Any]) -> dict[str, Any]:
    """Return a compact plan-friendly summary for a validated pipeline config."""
    _ensure_pipeline_has_sinks(config)
    validated = PipelineConfig.model_validate(config)
    import_refs = collect_import_references(validated.model_dump(mode="python", by_alias=True))

    dedup = None
    if validated.dedup is not None:
        key_value = validated.dedup.key
        dedup = {
            "key": _describe_config_value(key_value),
            "store": validated.dedup.store.type if validated.dedup.store is not None else None,
            "strategy": (
                validated.dedup.strategy.type if validated.dedup.strategy is not None else None
            ),
        }

    dlq = None
    if validated.dlq is not None:
        dlq_sink_type = validated.dlq.sink.type if validated.dlq.sink is not None else None
        if validated.dlq.enabled and dlq_sink_type is None:
            dlq_sink_type = "sqlite_dlq (implicit)"
        dlq = {
            "enabled": validated.dlq.enabled,
            "failure_policy": validated.dlq.failure_policy,
            "sink": dlq_sink_type,
        }

    tracing = None
    if validated.tracing is not None:
        service_name = validated.tracing.service_name
        if validated.tracing.enabled and service_name is None:
            service_name = validated.pipeline_id
        tracing = {
            "enabled": validated.tracing.enabled,
            "backend": validated.tracing.backend,
            "auto_configure": validated.tracing.auto_configure,
            "service_name": service_name,
        }

    performance = {
        "acceleration": (
            validated.performance.acceleration
            if validated.performance is not None
            else PerformanceConfig().acceleration
        ),
        "profile": (
            validated.performance.profile
            if validated.performance is not None
            else PerformanceConfig().profile
        ),
    }

    schedule = None
    if validated.schedule is not None:
        schedule = {
            "mode": validated.schedule.mode,
            "expression": validated.schedule.expression,
            "seconds": validated.schedule.seconds,
            "minutes": validated.schedule.minutes,
            "hours": validated.schedule.hours,
            "days": validated.schedule.days,
        }

    return {
        "pipeline_id": validated.pipeline_id,
        "source": validated.source.type,
        "middlewares": [middleware.type for middleware in validated.middlewares],
        "dedup": dedup,
        "dlq": dlq,
        "schedule": schedule,
        "tracing": tracing,
        "performance": performance,
        "sinks": [sink.type for sink in validated.sinks],
        "import_refs": import_refs,
    }


def resolve_worker_config_document(
    config: dict[str, Any],
    *,
    profile_name: str | None = None,
    environment_name: str | None = None,
) -> ResolvedWorkerConfig:
    """Resolve worker-level config and every scheduled pipeline from one document."""
    try:
        document = ConfigDocument.model_validate(config)
    except ValidationError as exc:
        raise ConfigError(_format_validation_error("config document", exc)) from exc

    selected_profile = _select_optional_overlay(profile_name, document.defaults.profile)
    selected_environment = _select_optional_overlay(
        environment_name,
        document.defaults.environment,
    )

    worker_config: dict[str, Any] = (
        document.worker.model_dump(mode="python", by_alias=True, exclude_none=True)
        if document.worker is not None
        else {}
    )
    worker_config = _apply_worker_overlay(
        worker_config,
        selected_name=selected_profile,
        scopes=document.profiles,
    )
    worker_config = _apply_worker_overlay(
        worker_config,
        selected_name=selected_environment,
        scopes=document.environments,
    )
    if worker_config:
        try:
            worker_config = WorkerConfig.model_validate(worker_config).model_dump(
                mode="python",
                by_alias=True,
                exclude_none=True,
            )
        except ValidationError as exc:
            raise ConfigError(_format_validation_error("worker config", exc)) from exc

    resolved_pipelines: list[ResolvedPipelineConfig] = []
    for pipeline_name in document.pipelines:
        resolved = resolve_config_document(
            config,
            pipeline_name=pipeline_name,
            profile_name=selected_profile,
            environment_name=selected_environment,
        )
        if "schedule" not in resolved.pipeline_config:
            raise ConfigError(
                f"Pipeline '{pipeline_name}' is missing schedule. "
                "Config-driven workers require [pipelines.<name>.schedule]."
            )
        resolved_pipelines.append(resolved)

    return ResolvedWorkerConfig(
        profile_name=selected_profile,
        environment_name=selected_environment,
        worker_config=worker_config,
        pipelines=tuple(resolved_pipelines),
    )


def collect_import_references(config: Any, *, path: str = "") -> list[str]:
    """Return every declarative import reference found in *config*.

    Each result is formatted as ``path=module:attribute`` so callers can
    surface the trust boundary clearly in CLI output.
    """
    refs: list[str] = []

    if isinstance(config, list):
        for index, item in enumerate(config):
            child_path = f"{path}.{index}" if path else str(index)
            refs.extend(collect_import_references(item, path=child_path))
        return refs

    if isinstance(config, dict):
        if set(config.keys()) == {"import"}:
            import_path = config.get("import")
            if isinstance(import_path, str):
                refs.append(f"{path or '<root>'}={import_path}")
            return refs
        for key, item in config.items():
            child_path = f"{path}.{key}" if path else str(key)
            refs.extend(collect_import_references(item, path=child_path))
    return refs


def deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge dict overlays; lists and scalars replace wholesale."""
    merged = dict(base)
    for key, overlay_value in overlay.items():
        base_value = merged.get(key)
        if isinstance(base_value, dict) and isinstance(overlay_value, dict):
            merged[key] = deep_merge(base_value, overlay_value)
        else:
            merged[key] = overlay_value
    return merged


def _apply_named_overlay(
    config: dict[str, Any],
    *,
    label: str,
    selected_name: str | None,
    scopes: dict[str, OverlayScope],
    pipeline_name: str,
) -> dict[str, Any]:
    if selected_name is None:
        return config

    scope = scopes.get(selected_name)
    if scope is None:
        available = ", ".join(sorted(scopes)) or "none"
        raise ConfigError(f"Unknown {label} '{selected_name}'. Available: {available}")

    overlay = scope.pipelines.get(pipeline_name)
    if overlay is None:
        return config
    return deep_merge(config, overlay)


def _apply_worker_overlay(
    config: dict[str, Any],
    *,
    selected_name: str | None,
    scopes: dict[str, OverlayScope],
) -> dict[str, Any]:
    if selected_name is None:
        return config
    scope = scopes.get(selected_name)
    if scope is None:
        available = ", ".join(sorted(scopes)) or "none"
        raise ConfigError(f"Unknown overlay '{selected_name}'. Available: {available}")
    if scope.worker is None:
        return config
    return deep_merge(config, scope.worker)


def _select_name(
    *,
    explicit_value: str | None,
    default_value: str | None,
    available_names: Iterable[str],
    label: str,
) -> str:
    selected = _select_optional_overlay(explicit_value, default_value)
    available = sorted(available_names)
    if selected is not None:
        return selected
    if len(available) == 1:
        return available[0]
    raise ConfigError(f"Config defines multiple {label}s. Select one: {', '.join(available)}")


def _select_optional_overlay(explicit_value: str | None, default_value: str | None) -> str | None:
    if explicit_value is not None:
        stripped = explicit_value.strip()
        if not stripped:
            raise ConfigError("Selector values cannot be empty.")
        return stripped
    return default_value


def _describe_config_value(value: Any) -> str:
    if isinstance(value, ImportRefConfig):
        return value.import_path
    return str(value)


def _ensure_pipeline_has_sinks(config: dict[str, Any]) -> None:
    sinks = config.get("sinks")
    if isinstance(sinks, list) and sinks:
        return
    raise ConfigError("Invalid pipeline:\n  - sinks: At least one sink must be defined.")


def _format_validation_error(label: str, exc: ValidationError) -> str:
    lines = [f"Invalid {label}:"]
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"])
        lines.append(f"  - {location}: {error['msg']}")
    return "\n".join(lines)


__all__ = [
    "ComponentConfig",
    "ConfigDefaults",
    "ConfigDocument",
    "DLQConfig",
    "DedupConfig",
    "ImportRefConfig",
    "OverlayScope",
    "PerformanceConfig",
    "PipelineConfig",
    "ResolvedPipelineConfig",
    "ResolvedWorkerConfig",
    "ScheduleConfig",
    "TracingConfig",
    "WorkerConfig",
    "collect_import_references",
    "deep_merge",
    "describe_pipeline_config",
    "resolve_config_document",
    "resolve_worker_config_document",
    "validate_config_document",
    "validate_import_path",
    "validate_pipeline_config",
]
