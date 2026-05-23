"""
agora/schema/types.py
=====================
Core types for schema inference and evolution.

Defines the type system, schema representation, and evolution contracts.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

import logstruct

if TYPE_CHECKING:
    from pydantic import BaseModel

logger = logstruct.getLogger(__name__)


class DataType(StrEnum):
    """Data types supported by schema inference.

    8 core types covering most ETL use cases:
    - STRING: text, varchar
    - INTEGER: int, bigint
    - FLOAT: float, double
    - BOOLEAN: bool
    - TIMESTAMP: datetime, date
    - JSON: dict, list (nested structures)
    - BYTES: binary data
    - NULL: unknown type (requires widening on first non-null value)
    """

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    JSON = "json"
    BYTES = "bytes"
    NULL = "null"


class SchemaContract(StrEnum):
    """Schema evolution contract modes.

    Controls how schema changes are handled:
    - EVOLVE: Add columns, widen types (default, most permissive)
    - FREEZE: Reject any schema changes (strict validation)
    - DISCARD_COLUMN: Drop new columns silently
    - DISCARD_ROW: Drop entire records with schema violations
    """

    EVOLVE = "evolve"
    FREEZE = "freeze"
    DISCARD_COLUMN = "discard_column"
    DISCARD_ROW = "discard_row"


@dataclass
class Column:
    """Schema column definition.

    Attributes
    ----------
    name:
        Column name.
    data_type:
        Inferred data type.
    nullable:
        Whether column accepts NULL values (default: True).
    inferred_from:
        Number of records observed to infer this column (for confidence).
    """

    name: str
    data_type: DataType
    nullable: bool = True
    inferred_from: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dict for JSON storage."""
        return {
            "name": self.name,
            "data_type": self.data_type.value,
            "nullable": self.nullable,
            "inferred_from": self.inferred_from,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Column:
        """Deserialize from dict."""
        return cls(
            name=data["name"],
            data_type=DataType(data["data_type"]),
            nullable=data["nullable"],
            inferred_from=data.get("inferred_from", 0),
        )


@dataclass
class Schema:
    """Schema definition for a table.

    Attributes
    ----------
    table:
        Table name.
    columns:
        Column definitions keyed by column name.
    version:
        Schema version number (increments on changes).
    hash:
        Content-based hash for change detection (computed automatically).
    """

    table: str
    columns: dict[str, Column]
    version: int = 1
    hash: str = field(init=False, default="")

    def __post_init__(self) -> None:
        """Compute hash after initialization."""
        self.hash = self._compute_hash()

    def _compute_hash(self) -> str:
        """Compute content-based hash of schema.

        Hash includes table name, column names, types, and nullability.
        Used for change detection across pipeline runs.
        """
        content = {
            "table": self.table,
            "columns": {
                name: {
                    "data_type": col.data_type.value,
                    "nullable": col.nullable,
                }
                for name, col in sorted(self.columns.items())
            },
        }
        json_str = json.dumps(content, sort_keys=True)
        return hashlib.sha256(json_str.encode()).hexdigest()[:16]

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dict for JSON storage."""
        return {
            "table": self.table,
            "columns": {name: col.to_dict() for name, col in self.columns.items()},
            "version": self.version,
            "hash": self.hash,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Schema:
        """Deserialize from dict."""
        columns = {name: Column.from_dict(col_data) for name, col_data in data["columns"].items()}
        schema = cls(
            table=data["table"],
            columns=columns,
            version=data.get("version", 1),
        )
        # Verify hash matches (detect corruption)
        stored_hash = data.get("hash", "")
        if stored_hash and stored_hash != schema.hash:
            logger.warning(
                "schema_hash_mismatch",
                table=data["table"],
                stored=stored_hash,
                computed=schema.hash,
            )
        return schema

    def column_names(self) -> list[str]:
        """Return sorted list of column names."""
        return sorted(self.columns.keys())

    def has_column(self, name: str) -> bool:
        """Check if column exists in schema."""
        return name in self.columns

    def get_column(self, name: str) -> Column | None:
        """Get column by name, or None if not found."""
        return self.columns.get(name)

    def to_pydantic_model(self, model_name: str | None = None) -> type[BaseModel]:
        """Generate a Pydantic model from this schema."""
        from agora.schema.pydantic import schema_to_pydantic_model

        return schema_to_pydantic_model(self, model_name=model_name)


def infer_python_type(value: Any) -> DataType:
    """Infer DataType from a Python value.

    Type detection rules:
    - None → NULL
    - bool → BOOLEAN (check before int, since bool is subclass of int)
    - int → INTEGER
    - float → FLOAT
    - str → STRING
    - bytes → BYTES
    - datetime/date → TIMESTAMP
    - dict/list → JSON
    - Everything else → STRING (fallback)

    Parameters
    ----------
    value:
        Python value to inspect.

    Returns
    -------
    DataType
        Inferred data type.
    """
    if value is None:
        return DataType.NULL

    # Check bool before int (bool is subclass of int in Python)
    if isinstance(value, bool):
        return DataType.BOOLEAN

    if isinstance(value, int):
        return DataType.INTEGER

    if isinstance(value, float):
        return DataType.FLOAT

    if isinstance(value, str):
        return DataType.STRING

    if isinstance(value, bytes):
        return DataType.BYTES

    if isinstance(value, (datetime, date)):
        return DataType.TIMESTAMP

    if isinstance(value, (dict, list)):
        return DataType.JSON

    # Fallback: convert to string
    return DataType.STRING


def can_widen(from_type: DataType, to_type: DataType) -> bool:
    """Check if type widening is allowed.

    Widening rules:
    - NULL → any type (first non-null value wins)
    - INTEGER → FLOAT (lossless widening)
    - Any type → STRING (fallback for conflicts)

    Parameters
    ----------
    from_type:
        Current column type.
    to_type:
        New value type.

    Returns
    -------
    bool
        True if widening is allowed.
    """
    if from_type == to_type:
        return True

    # NULL can widen to anything
    if from_type == DataType.NULL:
        return True

    # INTEGER can widen to FLOAT
    if from_type == DataType.INTEGER and to_type == DataType.FLOAT:
        return True

    # Anything can widen to STRING (fallback)
    return to_type == DataType.STRING


def widen_type(from_type: DataType, to_type: DataType) -> DataType:
    """Compute widened type.

    Returns the wider of two types according to widening rules.

    Parameters
    ----------
    from_type:
        Current column type.
    to_type:
        New value type.

    Returns
    -------
    DataType
        Widened type.
    """
    if from_type == to_type:
        return from_type

    # NULL widens to anything (both directions)
    if from_type == DataType.NULL:
        return to_type
    if to_type == DataType.NULL:
        return from_type

    # INTEGER widens to FLOAT (both directions)
    if from_type == DataType.INTEGER and to_type == DataType.FLOAT:
        return DataType.FLOAT
    if from_type == DataType.FLOAT and to_type == DataType.INTEGER:
        return DataType.FLOAT

    # Conflict: widen to STRING
    return DataType.STRING
