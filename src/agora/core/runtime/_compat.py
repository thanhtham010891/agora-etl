"""Compatibility helpers for the ``agora.core.runtime`` facade."""

from __future__ import annotations

import warnings
from typing import Any


def resolve_runtime_deprecated_export(
    name: str,
    *,
    delivery_engine: Any,
    module_name: str,
) -> object:
    """Resolve deprecated runtime facade exports."""
    if name == "RecordDeliveryCoordinator":
        warnings.warn(
            "RecordDeliveryCoordinator is deprecated; use DeliveryEngine instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return delivery_engine
    raise AttributeError(f"module {module_name!r} has no attribute {name!r}")
