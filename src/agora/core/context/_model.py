"""Pipeline context model shared across middleware and runtime."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from agora.core.context._logging import _BoundLogger, build_bound_logger
from agora.core.context._tracing import (
    _NOOP_SPAN_SCOPE,
    _NoopSpanScope,
    _normalize_trace_attributes,
    _PipelineSpanScope,
)
from agora.core.tracing import InMemoryTracer, NoopTracer

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
    _trace_stack: list[TraceSpan] = field(default_factory=list, init=False, repr=False)
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
        return self._trace_stack[-1]

    def _start_trace_span(
        self,
        name: str,
        *,
        attributes: dict[str, Any] | None = None,
        normalize: bool = True,
        push: bool = True,
        share_attributes: bool = False,
    ) -> TraceSpan | None:
        tracer = self.tracer
        if type(tracer) is NoopTracer:
            return None

        stack = self._trace_stack
        parent = stack[-1] if stack else None
        normalized_attributes = (
            None
            if attributes is None
            else _normalize_trace_attributes(attributes)
            if normalize
            else attributes
        )
        if (
            share_attributes
            and type(tracer) is InMemoryTracer
            and normalized_attributes is not None
        ):
            in_memory_tracer = cast("Any", tracer)
            span = in_memory_tracer.start_span_shared(
                name,
                attributes=normalized_attributes,
                parent=parent,
            )
        else:
            span = tracer.start_span(
                name,
                attributes=normalized_attributes,
                parent=parent,
            )
        if push:
            stack.append(span)
        return cast("TraceSpan", span)

    def _finish_trace_span(
        self,
        span: TraceSpan | None,
        exc: BaseException | None = None,
    ) -> None:
        if span is None:
            return

        stack = self._trace_stack
        if stack and stack[-1] is span:
            stack.pop()
        if exc is not None:
            span.record_exception(exc)
            span.set_attribute("error", True)
        span.end()

    def trace_span(self, name: str, **attributes: Any) -> _NoopSpanScope | _PipelineSpanScope:
        # Hot path: most runs use NoopTracer. Short-circuit here to skip the
        # extra create_trace_scope() call frame (invoked once per record).
        span = self._start_trace_span(
            name,
            attributes=attributes if attributes else None,
            push=False,
        )
        if span is None:
            return _NOOP_SPAN_SCOPE
        return _PipelineSpanScope(self._trace_stack, span)
