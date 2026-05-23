"""
agora/dedup/strategies/base.py
================================
``DedupStrategy`` — structural protocol for dedup comparison strategies.

Any object with an ``is_duplicate(key_a, key_b) -> bool`` method satisfies
this protocol.  No subclassing required (duck-typing via ``Protocol``).

Built-in implementations:
  - ``ExactMatchStrategy``  — string equality
  - ``FuzzyMatchStrategy``  — Jaro-Winkler similarity
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class DedupStrategy(Protocol):
    """Protocol for dedup comparison strategies."""

    def is_duplicate(self, key_a: str, key_b: str) -> bool:
        """Return True if *key_a* and *key_b* should be considered duplicates."""
        ...
