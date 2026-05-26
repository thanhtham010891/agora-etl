"""
agora/core/context.py
=====================
``PipelineContext`` — shared state injected into every middleware call.

The context carries:
- Identity (pipeline_id, run_id)
- Shared config (AgoraSettings subclass)
- Live metrics (mutated in-place)
- A structured logger bound with pipeline metadata
- ``extras``: user-defined key/value store for cross-middleware communication

Contexts are created once per ``BoundPipeline.run()`` call and are
NOT shared across concurrent runs.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import logstruct

from agora.core.tracing import NoopTracer

if TYPE_CHECKING:
    from agora.core.metrics import PipelineMetrics
    from agora.core.tracing import PipelineTracer, TraceSpan


@dataclass
class PipelineContext:
    """Shared state for a single pipeline run.

    Parameters
    ----------
    pipeline_id:
        Stable identifier for the pipeline definition (e.g. "places").
    run_id:
        Unique identifier for this specific invocation. Auto-generated.
    metrics:
        Live metrics container — mutated by the runner.
    extras:
        Shared mutable key/value store for cross-middleware communication
        (e.g. passing enrichment results to downstream stages).
        All middlewares in a run share the same dict — there is no per-stage
        isolation. Use namespaced keys (e.g. ``"enrich.place_id"``) to avoid
        accidental overwrites between middlewares.
    """

    pipeline_id: str
    metrics: PipelineMetrics
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    extras: dict[str, Any] = field(default_factory=dict)
    tracer: PipelineTracer = field(default_factory=NoopTracer)
    _trace_stack: list[Any] = field(default_factory=list, init=False, repr=False)

    # Structured logger with bound pipeline/run context
    _logger: Any = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._logger = _BoundLogger(
            logstruct.getLogger("agora.pipeline"),
            pipeline_id=self.pipeline_id,
            run_id=self.run_id,
        )

    @property
    def log(self) -> _BoundLogger:
        """Return the bound structured logger."""
        return self._logger

    # ------------------------------------------------------------------ #
    # Extras helpers                                                       #
    # ------------------------------------------------------------------ #

    def set(self, key: str, value: Any) -> None:
        """Store a value in the extras dict."""
        self.extras[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value from the extras dict."""
        return self.extras.get(key, default)

    def pop(self, key: str, default: Any = None) -> Any:
        """Remove and return a value from the extras dict."""
        return self.extras.pop(key, default)

    def current_span(self) -> TraceSpan | None:
        """Return the currently active span for this run, if any."""
        if not self._trace_stack:
            return None
        return self._trace_stack[-1]

    def trace_span(self, name: str, **attributes: Any) -> _PipelineSpanScope:
        """Create a nested tracing span scoped to a block of pipeline work."""
        parent = self.current_span()
        if isinstance(self.tracer, NoopTracer):
            span = self.tracer.start_span(name)
        else:
            span = self.tracer.start_span(
                name,
                attributes={
                    key: _normalize_trace_value(value) for key, value in attributes.items()
                },
                parent=parent,
            )
        return _PipelineSpanScope(self, span)


class _BoundLogger:
    """Thin wrapper that prepends bound fields to every log call.

    logstruct's StructuredLogger doesn't support .bind() — this wrapper
    merges pre-bound kwargs into each call so the API stays identical.

    Uses ``__getattr__`` delegation to automatically support any new
    methods added to the underlying logger (W12 fix).
    """

    __slots__ = ("_bound", "_logger")

    def __init__(self, logger: Any, **bound: Any) -> None:
        self._logger = logger
        self._bound = bound

    def __getattr__(self, name: str) -> Any:
        original = getattr(self._logger, name)
        if callable(original):

            def _bound_call(msg: str, **kw: Any) -> Any:
                return original(msg, **{**self._bound, **kw})

            return _bound_call
        return original

    # Explicit methods retained for IDE autocompletion / type hints
    def debug(self, msg: str, **kw: Any) -> None:
        self._logger.debug(msg, **{**self._bound, **kw})

    def info(self, msg: str, **kw: Any) -> None:
        self._logger.info(msg, **{**self._bound, **kw})

    def warning(self, msg: str, **kw: Any) -> None:
        self._logger.warning(msg, **{**self._bound, **kw})

    def error(self, msg: str, **kw: Any) -> None:
        self._logger.error(msg, **{**self._bound, **kw})

    def exception(self, msg: str, **kw: Any) -> None:
        self._logger.exception(msg, **{**self._bound, **kw})


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
        if self._ctx._trace_stack:
            self._ctx._trace_stack.pop()
        if exc_val is not None:
            self._span.record_exception(exc_val)
            self._span.set_attribute("error", True)
        self._span.end()


def _normalize_trace_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)
