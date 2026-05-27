"""
agora/schema/inference.py
==========================
Schema inference from records.

Automatically infers schemas by observing Python dicts, Pydantic models, and dataclasses.
"""

from __future__ import annotations

from typing import Any, cast

from agora.schema.types import Column, DataType, Schema, infer_python_type


def extract_record_fields(record: Any) -> dict[str, Any]:
    """Extract fields from a record-like object."""
    if isinstance(record, dict):
        return cast("dict[str, Any]", record)

    if hasattr(record, "model_dump"):
        return cast("dict[str, Any]", record.model_dump())

    if hasattr(record, "__dict__"):
        return cast("dict[str, Any]", record.__dict__)

    return {}


class SchemaInferrer:
    """Infers schema from a stream of records.

    Observes records one at a time and builds a schema incrementally.
    Handles dict, Pydantic models, and dataclasses uniformly.

    Usage::

        inferrer = SchemaInferrer(table="places")
        for record in records:
            inferrer.observe(record)
        schema = inferrer.finalize()

    Attributes
    ----------
    table:
        Table name for the schema.
    columns:
        Accumulated column definitions.
    records_observed:
        Total number of records observed.
    """

    def __init__(self, table: str) -> None:
        """Initialize inferrer.

        Parameters
        ----------
        table:
            Table name for the schema.
        """
        self.table = table
        self.columns: dict[str, Column] = {}
        self.records_observed = 0

    def observe(self, record: Any) -> None:
        """Observe a single record and update schema.

        Extracts fields from the record and infers types.
        Handles:
        - dict: direct field access
        - Pydantic models: via model_dump()
        - dataclasses: via __dict__
        - Other objects: via __dict__ (fallback)

        Parameters
        ----------
        record:
            Record to observe (dict, Pydantic model, dataclass, or object).
        """
        self.records_observed += 1

        # Convert record to dict
        fields = self._extract_fields(record)

        # Update schema with observed fields
        for name, value in fields.items():
            self._observe_field(name, value)

    def _extract_fields(self, record: Any) -> dict[str, Any]:
        """Extract fields from a record.

        Handles multiple record types uniformly.

        Parameters
        ----------
        record:
            Record to extract fields from.

        Returns
        -------
        dict[str, Any]
            Field name → value mapping.
        """
        return extract_record_fields(record)

    def _observe_field(self, name: str, value: Any) -> None:
        """Observe a single field and update column definition.

        If column doesn't exist, create it.
        If column exists, update type (widen if needed) and increment sample count.

        Parameters
        ----------
        name:
            Field name.
        value:
            Field value.
        """
        inferred_type = infer_python_type(value)

        if name not in self.columns:
            # New column
            self.columns[name] = Column(
                name=name,
                data_type=inferred_type,
                # Sparse semi-structured data should stay writable by default.
                nullable=True,
                inferred_from=1,
            )
        else:
            # Existing column: update
            col = self.columns[name]
            col.inferred_from += 1

            # Update nullable if we see a None
            if value is None:
                col.nullable = True

            # Update type if needed (widen)
            if inferred_type != col.data_type:
                col.data_type = self._widen_column_type(col.data_type, inferred_type)

    def _widen_column_type(self, current: DataType, new: DataType) -> DataType:
        """Widen column type to accommodate new value.

        Widening rules:
        - NULL → any type (first non-null wins)
        - INTEGER → FLOAT (lossless)
        - Any conflict → STRING (fallback)

        Parameters
        ----------
        current:
            Current column type.
        new:
            New value type.

        Returns
        -------
        DataType
            Widened type.
        """
        if current == new:
            return current

        # NULL widens to anything
        if current == DataType.NULL:
            return new
        if new == DataType.NULL:
            return current

        # INTEGER widens to FLOAT
        if current == DataType.INTEGER and new == DataType.FLOAT:
            return DataType.FLOAT
        if current == DataType.FLOAT and new == DataType.INTEGER:
            return DataType.FLOAT

        # Conflict: widen to STRING
        return DataType.STRING

    def finalize(self) -> Schema:
        """Finalize schema after observing all records.

        Returns
        -------
        Schema
            Inferred schema.
        """
        return Schema(
            table=self.table,
            columns=self.columns.copy(),
            version=1,
        )

    def reset(self) -> None:
        """Reset inferrer state (for reuse)."""
        self.columns.clear()
        self.records_observed = 0


def infer_schema(records: list[Any], table: str) -> Schema:
    """Convenience function to infer schema from a list of records.

    Parameters
    ----------
    records:
        List of records to infer from.
    table:
        Table name for the schema.

    Returns
    -------
    Schema
        Inferred schema.

    Example
    -------
    >>> records = [
    ...     {"id": 1, "name": "Alice", "score": 95.5},
    ...     {"id": 2, "name": "Bob", "score": 87},
    ... ]
    >>> schema = infer_schema(records, table="students")
    >>> schema.columns.keys()
    dict_keys(['id', 'name', 'score'])
    """
    inferrer = SchemaInferrer(table=table)
    for record in records:
        inferrer.observe(record)
    return inferrer.finalize()
