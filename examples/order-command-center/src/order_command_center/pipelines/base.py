"""Common execution and CLI contract for projection modules."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from order_command_center.runtime import ProjectionRuntime, ProjectionSpec

if TYPE_CHECKING:
    import argparse

    from order_command_center.settings import ProjectionSettings, Settings

PipelineBuilder = Callable[[], Awaitable[object]]


@dataclass(frozen=True, slots=True)
class ProjectionRunOptions:
    """Invocation-specific options shared by every projection module."""

    max_records: int | None
    forever: bool
    emit_report: bool
    metrics_host: str | None
    metrics_port: int | None
    metrics_auth_token: str | None


async def execute_projection(
    *,
    settings: Settings,
    projection: ProjectionSettings,
    options: ProjectionRunOptions,
    build_pipeline: PipelineBuilder,
) -> int:
    """Run a role-specific pipeline through the common operational runtime."""

    return await ProjectionRuntime(
        ProjectionSpec(
            pipeline_id=projection.pipeline_id,
            process_name=projection.process_name,
            consumer_group=projection.consumer_group,
            metrics_host=options.metrics_host or settings.metrics_host,
            metrics_port=(
                settings.metrics_port if options.metrics_port is None else options.metrics_port
            ),
            metrics_auth_token=options.metrics_auth_token or settings.metrics_auth_token,
            idle_log_interval_seconds=settings.projection_idle_log_interval_seconds,
            error_backoff_seconds=settings.projection_error_backoff_seconds,
            max_consecutive_errors=settings.projection_max_consecutive_errors,
        )
    ).run(
        build_pipeline=build_pipeline,
        max_records=options.max_records,
        forever=options.forever,
        emit_report=options.emit_report,
    )


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the standard operational arguments to a projection CLI."""

    parser.add_argument("--max-records", type=int, default=9)
    parser.add_argument("--forever", action="store_true", help="Run until the process is stopped.")
    parser.add_argument("--quiet", action="store_true", help="Suppress final run summary.")
    parser.add_argument("--metrics-host")
    parser.add_argument("--metrics-port", type=int)
    parser.add_argument("--metrics-auth-token")


def options_from_arguments(arguments: argparse.Namespace) -> ProjectionRunOptions:
    """Convert shared CLI arguments into a typed projection invocation."""

    return ProjectionRunOptions(
        max_records=None if arguments.forever else arguments.max_records,
        forever=arguments.forever,
        emit_report=not arguments.quiet,
        metrics_host=arguments.metrics_host,
        metrics_port=arguments.metrics_port,
        metrics_auth_token=arguments.metrics_auth_token,
    )
