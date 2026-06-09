"""Tracing support helpers for pipeline context."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora.core.tracing import NoopTracer

if TYPE_CHECKING:
    from agora.core.context._model import PipelineContext
    from agora.core.tracing import TraceSpan


class _NoopSpanScope:
    """Zero-allocation context manager returned by trace_span() when NoopTracer is active."""

    __slots__ = ()

    def __enter__(self) -> None:
        return None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        del exc_type, exc_val, exc_tb


_NOOP_SPAN_SCOPE = _NoopSpanScope()


class _PipelineSpanScope:
    """Context manager that maintains nested span state on ``PipelineContext``."""

    __slots__ = ("_ctx", "_span")

    def __init__(self, ctx: PipelineContext, span: TraceSpan) -> None:
        self._ctx = ctx
        self._span = span

    def __enter__(self) -> TraceSpan:
        self._ctx._trace_stack.append(self._span)
        return self._span

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        del exc_type, exc_tb
        if self._ctx._trace_stack and self._ctx._trace_stack[-1] is self._span:
            self._ctx._trace_stack.pop()
        if exc_val is not None:
            self._span.record_exception(exc_val)
            self._span.set_attribute("error", True)
        self._span.end()


def _normalize_trace_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def create_trace_scope(
    ctx: PipelineContext,
    name: str,
    **attributes: Any,
) -> _NoopSpanScope | _PipelineSpanScope:
    """Create the appropriate span scope for the configured tracer."""
    if isinstance(ctx.tracer, NoopTracer):
        del name, attributes
        return _NOOP_SPAN_SCOPE

    parent = ctx.current_span()
    span = ctx.tracer.start_span(
        name,
        attributes={key: _normalize_trace_value(value) for key, value in attributes.items()},
        parent=parent,
    )
    return _PipelineSpanScope(ctx, span)
