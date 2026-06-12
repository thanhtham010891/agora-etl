"""In-memory tracing primitives useful for tests and debugging."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

try:
    from agora_rs import InMemoryTracer as _RustInMemoryTracer
    from agora_rs import RecordedSpan as _RustRecordedSpan
except ImportError:
    if TYPE_CHECKING:
        from agora.core.tracing._protocols import TraceSpan

    def _parent_name(parent: TraceSpan | None) -> str | None:
        if isinstance(parent, _FallbackRecordedSpan):
            return parent.name
        return getattr(parent, "name", None)

    class _FallbackRecordedSpan:
        """Simple span implementation useful for tests and lightweight debugging."""

        __slots__ = (
            "_attributes_shared",
            "_events",
            "_exceptions",
            "attributes",
            "ended",
            "name",
            "parent_name",
        )

        def __init__(
            self,
            *,
            name: str,
            parent_name: str | None = None,
            attributes: dict[str, Any] | None = None,
            shared_attributes: bool = False,
        ) -> None:
            self.name = name
            self.parent_name = parent_name
            self.attributes = {} if attributes is None else attributes
            self._attributes_shared = shared_attributes
            self._events: list[tuple[str, dict[str, Any] | None]] | None = None
            self._exceptions: list[str] | None = None
            self.ended = False

        @property
        def events(self) -> list[tuple[str, dict[str, Any] | None]]:
            events = self._events
            if events is None:
                events = []
                self._events = events
            return events

        @property
        def exceptions(self) -> list[str]:
            exceptions = self._exceptions
            if exceptions is None:
                exceptions = []
                self._exceptions = exceptions
            return exceptions

        def set_attribute(self, key: str, value: Any) -> None:
            if self._attributes_shared:
                self.attributes = dict(self.attributes)
                self._attributes_shared = False
            self.attributes[key] = value

        def add_event(self, name: str, attributes: dict[str, Any] | None = None) -> None:
            events = self._events
            if events is None:
                events = []
                self._events = events
            events.append((name, dict(attributes) if attributes is not None else None))

        def record_exception(self, exc: BaseException) -> None:
            exceptions = self._exceptions
            if exceptions is None:
                exceptions = []
                self._exceptions = exceptions
            exceptions.append(f"{type(exc).__name__}: {exc}")

        def end(self) -> None:
            self.ended = True

    class _FallbackInMemoryTracer:
        """Test/debug tracer that keeps completed spans in memory."""

        __slots__ = ("spans",)

        def __init__(self) -> None:
            self.spans: list[_FallbackRecordedSpan] = []

        def start_span(
            self,
            name: str,
            *,
            attributes: dict[str, Any] | None = None,
            parent: TraceSpan | None = None,
        ) -> TraceSpan:
            span = _FallbackRecordedSpan(
                name=name,
                parent_name=_parent_name(parent),
                attributes={} if attributes is None else attributes,
            )
            self.spans.append(span)
            return cast("TraceSpan", span)

        def start_span_shared(
            self,
            name: str,
            *,
            attributes: dict[str, Any],
            parent: TraceSpan | None = None,
        ) -> TraceSpan:
            span = _FallbackRecordedSpan(
                name=name,
                parent_name=_parent_name(parent),
                attributes=attributes,
                shared_attributes=True,
            )
            self.spans.append(span)
            return cast("TraceSpan", span)

    _ExportedInMemoryTracer: Any = _FallbackInMemoryTracer
    _ExportedRecordedSpan: Any = _FallbackRecordedSpan
else:
    _ExportedInMemoryTracer: Any = _RustInMemoryTracer  # type: ignore[no-redef]
    _ExportedRecordedSpan: Any = _RustRecordedSpan  # type: ignore[no-redef]

InMemoryTracer = _ExportedInMemoryTracer
RecordedSpan = _ExportedRecordedSpan

__all__ = ["InMemoryTracer", "RecordedSpan"]
