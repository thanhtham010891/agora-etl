"""Lifetime markers for container registrations."""

from __future__ import annotations

import enum


class _Lifetime(enum.Enum):
    SINGLETON = "singleton"
    TRANSIENT = "transient"
