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


class PipelineConfig(BaseModel):
    """Validated root schema for one declarative pipeline."""

    model_config = ConfigDict(extra="allow")

    pipeline_id: str = "pipeline"
    source: ComponentConfig
    middlewares: list[ComponentConfig] = Field(default_factory=list)
    dedup: DedupConfig | None = None
    dlq: DLQConfig | None = None
    tracing: TracingConfig | None = None
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


class ConfigDocument(BaseModel):
    """Top-level declarative config file schema."""

    model_config = ConfigDict(extra="forbid")

    format: Literal["agora/v1"]
    defaults: ConfigDefaults = Field(default_factory=ConfigDefaults)
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

    merged = deep_merge(base_pipeline, {})
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
            "service_name": service_name,
        }

    return {
        "pipeline_id": validated.pipeline_id,
        "source": validated.source.type,
        "middlewares": [middleware.type for middleware in validated.middlewares],
        "dedup": dedup,
        "dlq": dlq,
        "tracing": tracing,
        "sinks": [sink.type for sink in validated.sinks],
        "import_refs": import_refs,
    }


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
    "PipelineConfig",
    "ResolvedPipelineConfig",
    "TracingConfig",
    "collect_import_references",
    "deep_merge",
    "describe_pipeline_config",
    "resolve_config_document",
    "validate_config_document",
    "validate_import_path",
    "validate_pipeline_config",
]
