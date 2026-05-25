"""
agora/core/constants.py
========================
Named constants replacing magic numbers scattered throughout the codebase.

Centralising these values here means:
  - They can be found with a single grep
  - They can be overridden via settings when needed
  - Tests can reference the same values
"""

from __future__ import annotations

# ── HTTP Source ──────────────────────────────────────────────────────────
HTTP_BACKOFF_BASE: float = 2.0
"""Base for exponential backoff in HTTPSource retries."""

HTTP_MAX_BACKOFF_S: float = 600.0
"""Maximum backoff cap (10 minutes) for HTTPSource retries."""

# ── Scheduler ────────────────────────────────────────────────────────────
SCHEDULER_MAX_BACKOFF_S: float = 600.0
"""Maximum error-backoff cap (10 minutes) for ScheduledPipeline."""

# ── Redis ────────────────────────────────────────────────────────────────
REDIS_DEFAULT_URL: str = "redis://localhost:6379"
"""Default Redis connection URL, shared by Redis-backed plugin integrations."""

# ── SQLite Cache ─────────────────────────────────────────────────────────
SQLITE_CACHE_SIZE_KB: int = 32_000
"""SQLite PRAGMA cache_size value (in KB, negative = KB).
Used by HttpCache: ``PRAGMA cache_size=-32000`` = 32 MB."""

# ── AI Provider env var names ────────────────────────────────────────────
GEMINI_ENV_VAR: str = "GEMINI_API_KEY"
OPENAI_ENV_VAR: str = "OPENAI_API_KEY"
ANTHROPIC_ENV_VAR: str = "ANTHROPIC_API_KEY"

# ── AI Provider batch sizes ──────────────────────────────────────────────
GEMINI_EMBEDDING_BATCH_SIZE: int = 100
"""Gemini embedding API batch limit."""

OPENAI_EMBEDDING_BATCH_SIZE: int = 2048
"""OpenAI embedding API batch limit."""

# ── LLM Cache defaults ──────────────────────────────────────────────────
LLM_CACHE_DEFAULT_TTL_S: int = 86_400
"""Default LLM response cache TTL: 24 hours."""

HTTP_CACHE_DEFAULT_TTL_S: int = 7 * 24 * 3600
"""Default HTTP response cache TTL: 7 days."""
