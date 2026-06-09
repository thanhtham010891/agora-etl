"""Pipeline context model shared across middleware and runtime."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from agora.core.context._logging import _BoundLogger, build_bound_logger
from agora.core.context._tracing import _NoopSpanScope, _PipelineSpanScope, create_trace_scope
from agora.core.tracing import NoopTracer

if TYPE_CHECKING:
    from agora.core.metrics import PipelineMetrics
    from agora.core.tracing import PipelineTracer, TraceSpan


@dataclass
class PipelineContext:
    """Shared state for a single pipeline run."""

    pipeline_id: str
    metrics: PipelineMetrics
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    extras: dict[str, Any] = field(default_factory=dict)
    tracer: PipelineTracer = field(default_factory=NoopTracer)
    _trace_stack: list[Any] = field(default_factory=list, init=False, repr=False)
    _logger: Any = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._logger = build_bound_logger(
            pipeline_id=self.pipeline_id,
            run_id=self.run_id,
        )

    @property
    def log(self) -> _BoundLogger:
        return self._logger  # type: ignore[no-any-return]

    def set(self, key: str, value: Any) -> None:
        self.extras[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        return self.extras.get(key, default)

    def pop(self, key: str, default: Any = None) -> Any:
        return self.extras.pop(key, default)

    def current_span(self) -> TraceSpan | None:
        if not self._trace_stack:
            return None
        return self._trace_stack[-1]  # type: ignore[no-any-return]

    def trace_span(self, name: str, **attributes: Any) -> _NoopSpanScope | _PipelineSpanScope:
        return create_trace_scope(self, name, **attributes)
