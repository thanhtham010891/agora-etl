"""
agora/schema/evolution.py
==========================
Schema evolution and merging.

Handles schema changes: new columns, type widening, and conflict resolution.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from agora.schema.types import Column, DataType, Schema, SchemaContract, can_widen, widen_type


@dataclass
class SchemaChange:
    """Represents a single schema change.

    Used for tracking and metrics.
    """

    change_type: str  # "column_added", "type_widened", "type_conflict"
    column_name: str
    old_value: Any = None
    new_value: Any = None
    message: str = ""


class SchemaEvolutionError(Exception):
    """Raised when schema evolution violates contract."""


class SchemaEvolution:
    """Handles schema evolution and merging.

    Merges old and new schemas according to contract rules.
    Tracks changes for metrics and logging.
    """

    def __init__(self, contract: SchemaContract = SchemaContract.EVOLVE) -> None:
        """Initialize evolution handler.

        Parameters
        ----------
        contract:
            Schema contract mode (default: EVOLVE).
        """
        self.contract = contract
        self.changes: list[SchemaChange] = []

    def merge(self, old: Schema, new: Schema) -> Schema:
        """Merge old and new schemas according to contract.

        Parameters
        ----------
        old:
            Existing schema.
        new:
            Newly inferred schema.

        Returns
        -------
        Schema
            Evolved schema.

        Raises
        ------
        SchemaEvolutionError
            If evolution violates contract (FREEZE mode).
        """
        self.changes.clear()

        # Start with old schema columns
        merged_columns: dict[str, Column] = {}

        # Process existing columns
        for name, old_col in old.columns.items():
            if name in new.columns:
                # Column exists in both: check for type changes
                new_col = new.columns[name]
                merged_col = self._merge_column(old_col, new_col)
                merged_columns[name] = merged_col
            else:
                # Column only in old schema: keep it
                merged_columns[name] = old_col

        # Process new columns
        for name, new_col in new.columns.items():
            if name not in old.columns:
                # New column
                self._handle_new_column(name, new_col, merged_columns)

        # Create evolved schema
        evolved = Schema(
            table=old.table,
            columns=merged_columns,
            version=old.version + (1 if self.changes else 0),
        )

        return evolved

    def _merge_column(self, old: Column, new: Column) -> Column:
        """Merge old and new column definitions.

        Handles type widening and nullability updates.

        Parameters
        ----------
        old:
            Existing column.
        new:
            New column definition.

        Returns
        -------
        Column
            Merged column.

        Raises
        ------
        SchemaEvolutionError
            If type change violates contract.
        """
        # Check if type changed
        if old.data_type != new.data_type:
            if can_widen(old.data_type, new.data_type):
                # Type widening allowed
                widened = widen_type(old.data_type, new.data_type)
                if widened != old.data_type:
                    self._record_change(
                        SchemaChange(
                            change_type="type_widened",
                            column_name=old.name,
                            old_value=old.data_type.value,
                            new_value=widened.value,
                            message=f"Column '{old.name}' type widened: {old.data_type.value} → {widened.value}",
                        )
                    )
                    if self.contract == SchemaContract.FREEZE:
                        raise SchemaEvolutionError(
                            f"Schema frozen: cannot widen column '{old.name}' type from {old.data_type.value} to {widened.value}"
                        )
                return Column(
                    name=old.name,
                    data_type=widened,
                    nullable=old.nullable or new.nullable,
                    inferred_from=old.inferred_from + new.inferred_from,
                )
            # Type conflict
            self._record_change(
                SchemaChange(
                    change_type="type_conflict",
                    column_name=old.name,
                    old_value=old.data_type.value,
                    new_value=new.data_type.value,
                    message=f"Column '{old.name}' type conflict: {old.data_type.value} vs {new.data_type.value} → widened to STRING",
                )
            )
            if self.contract == SchemaContract.FREEZE:
                raise SchemaEvolutionError(
                    f"Schema frozen: type conflict for column '{old.name}' ({old.data_type.value} vs {new.data_type.value})"
                )
            # Widen to STRING (fallback)
            return Column(
                name=old.name,
                data_type=DataType.STRING,
                nullable=old.nullable or new.nullable,
                inferred_from=old.inferred_from + new.inferred_from,
            )

        # No type change: update nullable and sample count
        return Column(
            name=old.name,
            data_type=old.data_type,
            nullable=old.nullable or new.nullable,
            inferred_from=old.inferred_from + new.inferred_from,
        )

    def _handle_new_column(self, name: str, col: Column, merged: dict[str, Column]) -> None:
        """Handle a new column according to contract.

        Parameters
        ----------
        name:
            Column name.
        col:
            New column definition.
        merged:
            Merged columns dict (modified in place).

        Raises
        ------
        SchemaEvolutionError
            If new column violates contract.
        """
        if self.contract == SchemaContract.EVOLVE:
            # Add new column
            merged[name] = col
            self._record_change(
                SchemaChange(
                    change_type="column_added",
                    column_name=name,
                    new_value=col.data_type.value,
                    message=f"New column '{name}' added ({col.data_type.value})",
                )
            )
        elif self.contract == SchemaContract.FREEZE:
            # Reject new column
            raise SchemaEvolutionError(f"Schema frozen: cannot add new column '{name}'")
        elif self.contract == SchemaContract.DISCARD_COLUMN:
            # Silently discard new column
            self._record_change(
                SchemaChange(
                    change_type="column_discarded",
                    column_name=name,
                    new_value=col.data_type.value,
                    message=f"New column '{name}' discarded (contract: DISCARD_COLUMN)",
                )
            )
        # DISCARD_ROW is handled at middleware level (not here)

    def _record_change(self, change: SchemaChange) -> None:
        """Record a schema change for tracking."""
        self.changes.append(change)

    def get_changes(self) -> list[SchemaChange]:
        """Get list of changes from last merge.

        Returns
        -------
        list[SchemaChange]
            List of schema changes.
        """
        return self.changes.copy()

    def has_changes(self) -> bool:
        """Check if last merge produced any changes.

        Returns
        -------
        bool
            True if schema changed.
        """
        return len(self.changes) > 0


def evolve_schema(
    old: Schema, new: Schema, contract: SchemaContract = SchemaContract.EVOLVE
) -> tuple[Schema, list[SchemaChange]]:
    """Convenience function to evolve schema.

    Parameters
    ----------
    old:
        Existing schema.
    new:
        Newly inferred schema.
    contract:
        Schema contract mode (default: EVOLVE).

    Returns
    -------
    tuple[Schema, list[SchemaChange]]
        Evolved schema and list of changes.

    Raises
    ------
    SchemaEvolutionError
        If evolution violates contract.

    Example
    -------
    >>> old = Schema(table="users", columns={"id": Column("id", DataType.INTEGER)})
    >>> new = Schema(table="users", columns={"id": Column("id", DataType.INTEGER), "name": Column("name", DataType.STRING)})
    >>> evolved, changes = evolve_schema(old, new)
    >>> len(changes)
    1
    >>> changes[0].change_type
    'column_added'
    """
    evolution = SchemaEvolution(contract=contract)
    evolved = evolution.merge(old, new)
    return evolved, evolution.get_changes()
