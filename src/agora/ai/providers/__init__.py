"""
agora/ai/providers/__init__.py
===============================
Base AI provider types for Agora.

Concrete provider implementations live in plugin packages and register
through the ``agora.ai.providers`` entry-point group.
"""

from __future__ import annotations

from agora.ai.providers.base import AIProvider, CompletionResponse, EmbeddingResponse

__all__ = [
    "AIProvider",
    "CompletionResponse",
    "EmbeddingResponse",
]
