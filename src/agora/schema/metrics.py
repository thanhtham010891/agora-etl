"""
agora/schema/metrics.py
========================
Schema metrics for tracking schema changes.

Integrates with Agora's metrics system to track schema evolution.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SchemaMetrics:
    """Metrics for schema inference and evolution.

    Tracks schema changes during pipeline execution.
    Integrates with PipelineMetrics.by_middleware[name].schema

    Attributes
    ----------
    columns_added:
        Number of new columns added to schema.
    columns_removed:
        Number of columns removed (reserved for future).
    types_widened:
        Number of times column types were widened (e.g., INTEGER → FLOAT).
    type_conflicts:
        Number of type conflicts resolved (widened to STRING).
    records_observed:
        Total number of records observed for schema inference.
    schema_version:
        Current schema version number.
    """

    columns_added: int = 0
    columns_removed: int = 0
    types_widened: int = 0
    type_conflicts: int = 0
    records_observed: int = 0
    schema_version: int = 1

    def __str__(self) -> str:
        """Human-readable summary."""
        parts = [
            f"v{self.schema_version}",
            f"{self.records_observed} records",
        ]
        if self.columns_added:
            parts.append(f"+{self.columns_added} cols")
        if self.types_widened:
            parts.append(f"{self.types_widened} widened")
        if self.type_conflicts:
            parts.append(f"{self.type_conflicts} conflicts")
        return ", ".join(parts)
