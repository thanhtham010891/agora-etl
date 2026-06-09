"""Core type variables and aliases."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

T = TypeVar("T")
U = TypeVar("U")
K = TypeVar("K")
P = TypeVar("P")

SqlRow = dict[str, Any]
SourceKey = str
PluginFactory = Callable[..., Any]
