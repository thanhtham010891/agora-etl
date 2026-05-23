# agora.dedup.strategies
"""
Dedup strategy implementations — comparison algorithms for duplicates.

Registry
--------
``dedup_strategy_registry`` provides plugin-style access::

    from agora.middlewares.dedup.strategies import dedup_strategy_registry

    cls = dedup_strategy_registry.get_or_raise("fuzzy")
    strategy = cls(threshold=0.85)
"""

from agora.core.registry import Registry
from agora.middlewares.dedup.strategies.base import DedupStrategy
from agora.middlewares.dedup.strategies.exact import ExactMatchStrategy
from agora.middlewares.dedup.strategies.fuzzy import FuzzyMatchStrategy

# ======================================================================
# Dedup Strategy Registry
# ======================================================================

dedup_strategy_registry: Registry[type] = Registry(name="dedup_strategy")

# Register built-in strategies
dedup_strategy_registry.register("exact", ExactMatchStrategy)
dedup_strategy_registry.register("fuzzy", FuzzyMatchStrategy)

__all__ = ["DedupStrategy", "ExactMatchStrategy", "FuzzyMatchStrategy", "dedup_strategy_registry"]
