"""
agora/utils/records.py
=======================
Record merging utility — the single source of truth for merging
updates into records without mutation.

Supports:
  - ``dict``      — returns ``{**record, **updates}``
  - Pydantic      — uses ``model_copy(update=…)``
  - dataclass     — shallow copy + ``object.__setattr__``

This replaces the duplicated pattern that appeared in 5+ middleware
files (W5 in the audit).  Fix once → fix everywhere.
"""

from __future__ import annotations

from copy import copy
from typing import Any, TypeVar, cast

T = TypeVar("T")


def merge_into_record(record: T, updates: dict[str, Any]) -> T:
    """Merge *updates* into *record* without mutation.

    Works with dicts, Pydantic BaseModel instances, dataclasses,
    and plain objects with ``__dict__``.

    Parameters
    ----------
    record:
        The original record.  Never mutated.
    updates:
        Key-value pairs to merge.

    Returns
    -------
    T
        A new record of the same type with *updates* applied.

    Examples
    --------
    >>> merge_into_record({"name": "A"}, {"category": "B"})
    {'name': 'A', 'category': 'B'}
    """
    if not updates:
        return record

    # Fast path: plain dict
    if isinstance(record, dict):
        return cast("T", {**record, **updates})

    # Pydantic v2 path
    if hasattr(record, "model_copy"):
        return cast("T", record.model_copy(update=updates))

    # Dataclass / plain object — shallow copy + setattr
    enriched = copy(record)
    for key, value in updates.items():
        if hasattr(enriched, key):
            object.__setattr__(enriched, key, value)
    return enriched
