"""
agora/dedup/strategies/exact.py
================================
Exact string match dedup strategy — simplest possible dedup.

Two keys are duplicates iff they are equal strings.
"""

from __future__ import annotations


class ExactMatchStrategy:
    """Exact string equality dedup strategy."""

    def is_duplicate(self, key_a: str, key_b: str) -> bool:
        return key_a == key_b
