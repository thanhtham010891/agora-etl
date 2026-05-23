"""
agora/schema/records.py
=======================
Helpers for applying schema contracts to records without mutation.
"""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING, TypeVar

from agora.schema.inference import extract_record_fields
from agora.schema.types import DataType, Schema, can_widen, infer_python_type

if TYPE_CHECKING:
    from collections.abc import Collection

T = TypeVar("T")


def record_violates_schema(record: T, schema: Schema) -> bool:
    """Return True when *record* introduces columns or types outside *schema*."""
    for name, value in extract_record_fields(record).items():
        current_col = schema.columns.get(name)
        if current_col is None:
            return True

        inferred_type = infer_python_type(value)
        if inferred_type == DataType.NULL:
            continue
        if inferred_type != current_col.data_type and not can_widen(
            current_col.data_type,
            inferred_type,
        ):
            return True
        if inferred_type != current_col.data_type:
            return True

    return False


def discard_unknown_columns(record: T, allowed_columns: Collection[str]) -> T:
    """Return a record copy containing only the allowed columns."""
    allowed = set(allowed_columns)

    if isinstance(record, dict):
        return {name: value for name, value in record.items() if name in allowed}  # type: ignore[return-value]

    if hasattr(record, "__dict__"):
        sanitized = copy.copy(record)
        for name in list(vars(sanitized)):
            if name not in allowed:
                delattr(sanitized, name)
        return sanitized

    return record
