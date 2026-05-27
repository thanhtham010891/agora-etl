"""
agora/schema/pydantic.py
========================
Generate Pydantic models from inferred Agora schemas.
"""

from __future__ import annotations

import keyword
import re
from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, create_model

from agora.schema.types import DataType, Schema

_INVALID_IDENTIFIER_RE = re.compile(r"\W+")


def schema_to_pydantic_model(
    schema: Schema,
    *,
    model_name: str | None = None,
) -> type[BaseModel]:
    """Build a Pydantic model class from *schema*."""
    fields: dict[str, tuple[Any, Any]] = {}
    used_field_names: set[str] = set()

    for column_name in schema.column_names():
        column = schema.columns[column_name]
        field_name = _python_field_name(column_name, used_field_names)
        annotation = _annotation_for_column(column)
        default = None if column.nullable else ...
        field = Field(
            default=default,
            alias=column_name,
            serialization_alias=column_name,
        )
        fields[field_name] = (annotation, field)

    generated_name = model_name or _default_model_name(schema.table)
    return create_model(  # type: ignore[call-overload, no-any-return]
        generated_name,
        __base__=BaseModel,
        __config__=ConfigDict(populate_by_name=True),
        **fields,
    )


def _annotation_for_column(column: Any) -> Any:
    base_type = _python_type_for_data_type(column.data_type)
    if column.nullable and column.data_type != DataType.NULL:
        return base_type | None
    if column.data_type == DataType.NULL:
        return Any | None
    return base_type


def _python_type_for_data_type(data_type: DataType) -> Any:
    mapping: dict[DataType, Any] = {
        DataType.STRING: str,
        DataType.INTEGER: int,
        DataType.FLOAT: float,
        DataType.BOOLEAN: bool,
        DataType.TIMESTAMP: datetime | date,
        DataType.JSON: dict[str, Any] | list[Any],
        DataType.BYTES: bytes,
        DataType.NULL: Any,
    }
    return mapping[data_type]


def _default_model_name(table: str) -> str:
    parts = [
        segment for segment in _INVALID_IDENTIFIER_RE.split(table.replace(".", "_")) if segment
    ]
    base = "".join(part.capitalize() for part in parts) or "InferredSchema"
    return f"{base}Model"


def _python_field_name(column_name: str, used_names: set[str]) -> str:
    normalized = _INVALID_IDENTIFIER_RE.sub("_", column_name).strip("_")
    if not normalized:
        normalized = "field"
    if normalized[0].isdigit():
        normalized = f"field_{normalized}"
    if keyword.iskeyword(normalized):
        normalized = f"{normalized}_"

    field_name = normalized
    suffix = 2
    while field_name in used_names:
        field_name = f"{normalized}_{suffix}"
        suffix += 1

    used_names.add(field_name)
    return field_name
