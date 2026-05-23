"""
agora/schema/runtime.py
=======================
Runtime helpers for incremental schema tracking during pipeline execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, TypeVar

from agora.schema.evolution import SchemaChange, SchemaEvolution, SchemaEvolutionError
from agora.schema.inference import SchemaInferrer
from agora.schema.metrics import SchemaMetrics
from agora.schema.records import discard_unknown_columns, record_violates_schema
from agora.schema.types import Schema, SchemaContract

T = TypeVar("T")


@dataclass
class SchemaProcessResult(Generic[T]):
    """Outcome of processing one record against the active schema."""

    record: T | None
    schema: Schema | None
    changes: list[SchemaChange] = field(default_factory=list)


class SchemaProcessor(Generic[T]):
    """Pure incremental schema state machine used by SchemaMiddleware."""

    def __init__(
        self,
        table: str,
        contract: SchemaContract = SchemaContract.EVOLVE,
    ) -> None:
        self._table = table
        self._contract = contract
        self._current_schema: Schema | None = None
        self._pending_error: SchemaEvolutionError | None = None
        self._metrics = SchemaMetrics()

    @property
    def current_schema(self) -> Schema | None:
        return self._current_schema

    @property
    def metrics(self) -> SchemaMetrics:
        return self._metrics

    @property
    def pending_error(self) -> SchemaEvolutionError | None:
        return self._pending_error

    def load_schema(self, schema: Schema | None) -> None:
        """Seed the processor with an existing persisted schema."""
        self._current_schema = schema
        self._pending_error = None
        self._metrics = SchemaMetrics()
        if schema is not None:
            self._metrics.schema_version = schema.version

    def process(self, record: T) -> SchemaProcessResult[T]:
        """Apply the active contract, evolve schema, and return the forwarded record."""
        candidate = record

        if self._current_schema is not None:
            if self._contract == SchemaContract.DISCARD_ROW and record_violates_schema(
                record,
                self._current_schema,
            ):
                return SchemaProcessResult(record=None, schema=self._current_schema)

            if self._contract == SchemaContract.DISCARD_COLUMN:
                candidate = discard_unknown_columns(record, self._current_schema.columns.keys())

        record_schema = self._infer_record_schema(candidate)
        self._metrics.records_observed += 1

        if not record_schema.columns:
            return SchemaProcessResult(record=candidate, schema=self._current_schema)

        if self._current_schema is None:
            self._current_schema = record_schema
            self._metrics.columns_added += len(record_schema.columns)
            self._metrics.schema_version = record_schema.version
            return SchemaProcessResult(
                record=candidate,
                schema=self._current_schema,
                changes=_initial_schema_changes(record_schema),
            )

        evolution = SchemaEvolution(contract=self._contract)
        try:
            merged = evolution.merge(self._current_schema, record_schema)
        except SchemaEvolutionError as exc:
            if self._contract == SchemaContract.FREEZE:
                self._pending_error = self._pending_error or exc
                return SchemaProcessResult(record=None, schema=self._current_schema)
            raise

        self._current_schema = merged
        self._metrics.columns_added += sum(
            1 for change in evolution.changes if change.change_type == "column_added"
        )
        self._metrics.types_widened += sum(
            1 for change in evolution.changes if change.change_type == "type_widened"
        )
        self._metrics.type_conflicts += sum(
            1 for change in evolution.changes if change.change_type == "type_conflict"
        )
        self._metrics.schema_version = merged.version

        return SchemaProcessResult(
            record=candidate,
            schema=merged,
            changes=evolution.get_changes(),
        )

    def _infer_record_schema(self, record: T) -> Schema:
        inferrer = SchemaInferrer(table=self._table)
        inferrer.observe(record)
        return inferrer.finalize()


def _initial_schema_changes(schema: Schema) -> list[SchemaChange]:
    """Represent a brand-new inferred schema as a list of column additions."""
    changes: list[SchemaChange] = []
    for name in sorted(schema.columns.keys()):
        col = schema.columns[name]
        changes.append(
            SchemaChange(
                change_type="column_added",
                column_name=name,
                new_value=col.data_type.value,
                message=f"New column '{name}' added ({col.data_type.value})",
            )
        )
    return changes
