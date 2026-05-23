"""
agora/dedup/strategies/fuzzy.py
================================
Fuzzy dedup strategy using Jaro-Winkler string similarity + optional
geographic distance to determine if two records are duplicates.
"""

from __future__ import annotations

from agora.utils.math import haversine_meters as _haversine_meters
from agora.utils.math import jaro_winkler as _jaro_winkler

# _jaro_winkler and _haversine_meters imported from agora.utils.math


class FuzzyMatchStrategy:
    """Determine duplicates via name similarity + optional geo-distance.

    Two records are considered duplicates if:
    1. ``Jaro-Winkler(key_a, key_b) >= name_threshold``  AND
    2. (if coordinates provided) ``haversine(a, b) <= distance_threshold_m``

    Usage with DedupMiddleware::

        DedupMiddleware(
            key=lambda place: place.name.lower(),
            strategy=FuzzyMatchStrategy(threshold=0.85),
            # For geo-aware dedup, use a compound key or override
        )

    Parameters
    ----------
    threshold:
        Jaro-Winkler similarity threshold (0.0-1.0). Default: 0.82.
    distance_threshold_m:
        Maximum distance in meters for two records to be considered
        co-located.  Only used when coordinate extractors are provided.
    """

    def __init__(
        self,
        threshold: float = 0.82,
        distance_threshold_m: float = 80.0,
    ) -> None:
        self._threshold = threshold
        self._distance_m = distance_threshold_m

    def is_duplicate(self, key_a: str, key_b: str) -> bool:
        """Return True if key_a and key_b are similar enough to be duplicates."""
        return _jaro_winkler(key_a, key_b) >= self._threshold

    def is_duplicate_with_coords(
        self,
        key_a: str,
        lat_a: float,
        lon_a: float,
        key_b: str,
        lat_b: float,
        lon_b: float,
    ) -> bool:
        """Geo-aware duplicate check: name similarity AND proximity."""
        if _jaro_winkler(key_a, key_b) < self._threshold:
            return False
        dist = _haversine_meters(lat_a, lon_a, lat_b, lon_b)
        return dist <= self._distance_m
