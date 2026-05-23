"""
agora/utils/__init__.py
=======================
agora utility functions — general purpose, no framework dependencies.

Provided:
    slugify.to_slug              — Vietnamese/Unicode text → URL-safe slug
    slugify.sanitize_path        — safe filesystem path component
    records.merge_into_record    — merge updates into dict/Pydantic/dataclass
    math.cosine_similarity       — cosine similarity between float vectors
    math.jaro_winkler            — Jaro-Winkler string similarity
    math.haversine_meters        — great-circle distance (WGS-84)
"""
