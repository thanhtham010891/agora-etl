"""Shared middleware result and planning types."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from agora.core.data_plane import DataPlane


@dataclass(frozen=True, slots=True)
class MiddlewareFailure:
    """Structured middleware failure surfaced to the runtime."""

    stage: str
    record: Any
    middleware: str
    exception: Exception


@dataclass(frozen=True, slots=True)
class MiddlewareProcessResult:
    """Outcome of processing a record through some or all middlewares."""

    value: Any | None
    failure: MiddlewareFailure | None = None


@dataclass(frozen=True, slots=True)
class PipelinedBatchStageSpec:
    """Runtime-selected batch stage that can submit whole batches concurrently."""

    index: int
    middleware: Any
    name: str
    max_in_flight: int
    ordered: bool
    arrow_stage: bool


class MiddlewareDataPlane(StrEnum):
    """Logical data plane flowing between middleware stages."""

    PYTHON_ROWS = DataPlane.PYTHON_ROWS.value
    ARROW_BATCHES = DataPlane.ARROW_BATCHES.value


@dataclass(frozen=True, slots=True)
class MiddlewareModeSpec:
    """One row in the middleware compatibility matrix."""

    index: int
    name: str
    data_plane: MiddlewareDataPlane
