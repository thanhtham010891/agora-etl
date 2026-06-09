"""Per-middleware observability models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from agora.core.metrics._ai import AIMiddlewareMetrics

if TYPE_CHECKING:
    from agora.schema.metrics import SchemaMetrics


@dataclass
class MiddlewareMetrics:
    """Metrics for a single middleware stage."""

    name: str
    records_in: int = 0
    records_out: int = 0
    records_dropped: int = 0
    records_errored: int = 0
    total_time_ms: float = 0.0
    ai: AIMiddlewareMetrics = field(default_factory=AIMiddlewareMetrics)
    schema: SchemaMetrics | None = None

    @property
    def avg_time_ms(self) -> float:
        if self.records_in == 0:
            return 0.0
        return self.total_time_ms / self.records_in
