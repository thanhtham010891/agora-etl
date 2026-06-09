"""In-memory tracing primitives useful for tests and debugging."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agora.core.tracing._protocols import TraceSpan


@dataclass
class RecordedSpan:
    """Simple span implementation useful for tests and lightweight debugging."""

    name: str
    parent_name: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)
    events: list[tuple[str, dict[str, Any] | None]] = field(default_factory=list)
    exceptions: list[str] = field(default_factory=list)
    ended: bool = False

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def add_event(self, name: str, attributes: dict[str, Any] | None = None) -> None:
        self.events.append((name, dict(attributes) if attributes is not None else None))

    def record_exception(self, exc: BaseException) -> None:
        self.exceptions.append(f"{type(exc).__name__}: {exc}")

    def end(self) -> None:
        self.ended = True


class InMemoryTracer:
    """Test/debug tracer that keeps completed spans in memory."""

    def __init__(self) -> None:
        self.spans: list[RecordedSpan] = []

    def start_span(
        self,
        name: str,
        *,
        attributes: dict[str, Any] | None = None,
        parent: TraceSpan | None = None,
    ) -> TraceSpan:
        span = RecordedSpan(
            name=name,
            parent_name=getattr(parent, "name", None) if parent is not None else None,
            attributes=dict(attributes or {}),
        )
        self.spans.append(span)
        return span
