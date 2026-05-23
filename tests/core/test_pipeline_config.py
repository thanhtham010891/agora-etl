from __future__ import annotations

import pytest

from agora.config import (
    describe_pipeline_config,
    resolve_config_document,
    validate_config_document,
    validate_pipeline_config,
)
from agora.core.errors import ConfigError


def test_validate_config_document_requires_agora_v1_format() -> None:
    with pytest.raises(ConfigError, match="format: Field required"):
        validate_config_document(
            {
                "pipelines": {
                    "main": {
                        "source": {"type": "iterable", "records": [1]},
                    }
                }
            }
        )


def test_validate_config_document_requires_pipelines() -> None:
    with pytest.raises(ConfigError, match="pipelines: Field required"):
        validate_config_document({"format": "agora/v1"})


def test_validate_pipeline_config_reports_component_type_errors() -> None:
    with pytest.raises(ConfigError, match=r"middlewares\.0\.type: Field required"):
        validate_pipeline_config(
            {
                "source": {"type": "iterable", "records": [1]},
                "middlewares": [{}],
            }
        )


def test_resolve_config_document_applies_profile_then_environment_overlays() -> None:
    resolved = resolve_config_document(
        {
            "format": "agora/v1",
            "defaults": {
                "pipeline": "events",
                "profile": "batch",
                "environment": "prod",
            },
            "pipelines": {
                "events": {
                    "source": {"type": "iterable", "records": [1], "batch_size": 1},
                    "middlewares": [{"type": "enrich", "field": "city", "value": "HCM"}],
                    "sinks": [{"type": "stdout"}],
                }
            },
            "profiles": {
                "batch": {
                    "pipelines": {
                        "events": {
                            "source": {"batch_size": 100},
                            "middlewares": [{"type": "enrich", "field": "city", "value": "SGN"}],
                        }
                    }
                }
            },
            "environments": {
                "prod": {
                    "pipelines": {
                        "events": {
                            "source": {"records": [9, 8, 7]},
                        }
                    }
                }
            },
        }
    )

    assert resolved.pipeline_name == "events"
    assert resolved.profile_name == "batch"
    assert resolved.environment_name == "prod"
    assert resolved.pipeline_config["pipeline_id"] == "events"
    assert resolved.pipeline_config["source"] == {
        "type": "iterable",
        "records": [9, 8, 7],
        "batch_size": 100,
    }
    assert resolved.pipeline_config["middlewares"] == [
        {"type": "enrich", "field": "city", "value": "SGN"}
    ]


def test_validate_pipeline_config_allows_implicit_builtin_dlq_sink() -> None:
    validated = validate_pipeline_config(
        {
            "source": {"type": "iterable", "records": [1]},
            "dlq": {"enabled": True},
        }
    )

    assert validated["dlq"] == {"enabled": True, "failure_policy": "log_only"}


def test_validate_pipeline_config_includes_tracing_defaults() -> None:
    validated = validate_pipeline_config(
        {
            "pipeline_id": "traced",
            "source": {"type": "iterable", "records": [1]},
            "tracing": {"enabled": True},
        }
    )

    assert validated["tracing"] == {"enabled": True, "backend": "opentelemetry"}


def test_resolve_config_document_rejects_unknown_environment() -> None:
    with pytest.raises(ConfigError, match="Unknown environment 'qa'"):
        resolve_config_document(
            {
                "format": "agora/v1",
                "pipelines": {
                    "events": {
                        "source": {"type": "iterable", "records": [1]},
                    }
                },
            },
            pipeline_name="events",
            environment_name="qa",
        )


def test_describe_pipeline_config_infers_implicit_stdout_sink() -> None:
    plan = describe_pipeline_config(
        {
            "pipeline_id": "implicit-sink",
            "source": {"type": "iterable", "records": [1]},
        }
    )

    assert plan == {
        "pipeline_id": "implicit-sink",
        "source": "iterable",
        "middlewares": [],
        "dedup": None,
        "dlq": None,
        "tracing": None,
        "sinks": ["stdout (implicit)"],
    }


def test_describe_pipeline_config_includes_dlq_summary() -> None:
    plan = describe_pipeline_config(
        {
            "pipeline_id": "with-dlq",
            "source": {"type": "iterable", "records": [1]},
            "dlq": {
                "enabled": True,
                "failure_policy": "raise",
                "sink": {"type": "sqlite_dlq", "path": ".agora_dlq.db"},
            },
            "sinks": [{"type": "stdout"}],
        }
    )

    assert plan["dlq"] == {
        "enabled": True,
        "failure_policy": "raise",
        "sink": "sqlite_dlq",
    }


def test_describe_pipeline_config_includes_tracing_summary() -> None:
    plan = describe_pipeline_config(
        {
            "pipeline_id": "with-tracing",
            "source": {"type": "iterable", "records": [1]},
            "tracing": {"enabled": True, "backend": "in_memory"},
            "sinks": [{"type": "stdout"}],
        }
    )

    assert plan["tracing"] == {
        "enabled": True,
        "backend": "in_memory",
        "service_name": "with-tracing",
    }


def test_describe_pipeline_config_marks_disabled_tracing() -> None:
    plan = describe_pipeline_config(
        {
            "pipeline_id": "without-tracing",
            "source": {"type": "iterable", "records": [1]},
            "tracing": {"enabled": False},
            "sinks": [{"type": "stdout"}],
        }
    )

    assert plan["tracing"] == {
        "enabled": False,
        "backend": "opentelemetry",
        "service_name": None,
    }


def test_describe_pipeline_config_marks_implicit_builtin_dlq_sink() -> None:
    plan = describe_pipeline_config(
        {
            "pipeline_id": "implicit-dlq",
            "source": {"type": "iterable", "records": [1]},
            "dlq": {"enabled": True},
            "sinks": [{"type": "stdout"}],
        }
    )

    assert plan["dlq"] == {
        "enabled": True,
        "failure_policy": "log_only",
        "sink": "sqlite_dlq (implicit)",
    }
