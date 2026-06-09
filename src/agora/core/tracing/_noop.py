"""No-op tracing primitives."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agora.core.tracing._protocols import TraceSpan


@dataclass
class NoopSpan:
    """Default no-op span used when tracing is disabled."""

    name: str

    def set_attribute(self, key: str, value: Any) -> None:
        del key, value

    def add_event(self, name: str, attributes: dict[str, Any] | None = None) -> None:
        del name, attributes

    def record_exception(self, exc: BaseException) -> None:
        del exc

    def end(self) -> None:
        return None


class NoopTracer:
    """Default tracer that allocates no-op spans."""

    def start_span(
        self,
        name: str,
        *,
        attributes: dict[str, Any] | None = None,
        parent: TraceSpan | None = None,
    ) -> TraceSpan:
        del attributes, parent
        return NoopSpan(name=name)
