"""Immutable write outcomes."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Outcome of a ``Writer.write()`` call."""

    written: bool = True
    errors: list[Exception] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.written and not self.errors

    @property
    def partial(self) -> bool:
        return self.written and bool(self.errors)
