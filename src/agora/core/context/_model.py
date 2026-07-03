"""Pipeline context model shared across middleware and runtime."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
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


SuccessHook = Callable[[], Awaitable[None]]


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
    _success_hooks: dict[int, list[tuple[SuccessHook, SuccessHook | None]]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
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

    def register_success_hook(
        self,
        record: Any,
        hook: SuccessHook,
        *,
        on_discard: SuccessHook | None = None,
    ) -> None:
        """Run *hook* only after this record is durably handled.

        Middleware can use this for side effects that must not happen before
        sink/checkpoint success, such as persisting dedup markers.  If the
        record is transformed later in the middleware chain, runtime transfers
        the hook to the transformed object.
        """
        self._success_hooks.setdefault(id(record), []).append((hook, on_discard))

    def transfer_success_hooks(self, source_record: Any, target_record: Any) -> None:
        """Move success hooks from *source_record* to *target_record*."""
        if source_record is target_record:
            return
        hooks = self._success_hooks.pop(id(source_record), None)
        if hooks:
            self._success_hooks.setdefault(id(target_record), []).extend(hooks)

    def pop_success_hooks(self, *records: Any) -> list[SuccessHook]:
        """Return and remove success hooks attached to the supplied records."""
        hooks: list[SuccessHook] = []
        seen_ids: set[int] = set()
        for record in records:
            record_id = id(record)
            if record_id in seen_ids:
                continue
            seen_ids.add(record_id)
            entries = self._success_hooks.pop(record_id, [])
            hooks.extend(hook for hook, _discard in entries)
        return hooks

    async def discard_success_hooks(self, *records: Any) -> None:
        """Discard hooks for records that will not be committed.

        Optional discard callbacks are best-effort cleanup hooks for middleware
        reservations; failures are logged but do not mask the original delivery
        error.
        """
        seen_ids: set[int] = set()
        for record in records:
            record_id = id(record)
            if record_id in seen_ids:
                continue
            seen_ids.add(record_id)
            entries = self._success_hooks.pop(record_id, [])
            for _hook, discard in entries:
                if discard is None:
                    continue
                try:
                    await discard()
                except Exception as exc:
                    self.log.exception(
                        "pipeline_success_hook_discard_error",
                        error=str(exc),
                    )

    def current_span(self) -> TraceSpan | None:
        if not self._trace_stack:
            return None
        return self._trace_stack[-1]

    @property
    def trace_depth(self) -> int:
        """Return the current nesting depth of active trace spans."""
        return len(self._trace_stack)

    def start_trace_span(
        self,
        name: str,
        *,
        attributes: dict[str, Any] | None = None,
        normalize: bool = True,
        push: bool = True,
        share_attributes: bool = False,
    ) -> TraceSpan | None:
        """Start a span using the same fast-path rules as runtime internals."""
        return self._start_trace_span(
            name,
            attributes=attributes,
            normalize=normalize,
            push=push,
            share_attributes=share_attributes,
        )

    def finish_trace_span(
        self,
        span: TraceSpan | None,
        exc: BaseException | None = None,
    ) -> None:
        """Finish a span previously created by ``start_trace_span()``."""
        self._finish_trace_span(span, exc)

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
