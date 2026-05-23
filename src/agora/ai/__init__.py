"""
agora/ai/__init__.py
====================
AI integration layer for the Agora ETL framework.

Registry
--------
``ai_provider_registry`` provides plugin-style access to LLM providers::

    from agora.ai import ai_provider_registry

    cls = ai_provider_registry.get_or_raise("gemini")
    provider = cls(api_key="...")

    # Third-party providers via entry-points
    ai_provider_registry.load_entrypoints("agora.ai.providers")

Provides a provider-agnostic interface over popular LLM/embedding APIs.
AI providers are discovered from installed plugin packages.

Public API
----------
Cache:
    InMemoryLLMCache, SQLiteLLMCache, StateBackendLLMCache

Types:
    AIProvider, LLMCache, CompletionResponse, EmbeddingResponse
"""

from agora.ai.cache import (
    InMemoryLLMCache,
    LLMCache,
    SQLiteLLMCache,
    StateBackendLLMCache,
    ai_cache_registry,
    build_llm_cache,
)
from agora.ai.providers.base import AIProvider, CompletionResponse, EmbeddingResponse
from agora.core.registry import Registry

# ======================================================================
# AI Provider Registry
# ======================================================================

ai_provider_registry: Registry[type] = Registry(name="ai_provider")


def _register_lazy_providers() -> None:
    """Discover AI providers from installed plugin entry points."""

    ai_provider_registry.load_entrypoints("agora.ai.providers")


_register_lazy_providers()

__all__ = [
    "AIProvider",
    "CompletionResponse",
    "EmbeddingResponse",
    "InMemoryLLMCache",
    "LLMCache",
    "SQLiteLLMCache",
    "StateBackendLLMCache",
    "ai_cache_registry",
    "ai_provider_registry",
    "build_llm_cache",
]
