"""
agora/middlewares/__init__.py
==============================
Built-in agora middleware collection.

Registry
--------
``middleware_registry`` provides plugin-style access::

    from agora.middlewares import middleware_registry

    cls = middleware_registry.get_or_raise("validate")
    mw = cls(schema=MyModel)

All middlewares are generic — they work with any record type T.

Provided (built-in):
    ValidateMiddleware  — validate records via pydantic or custom validator
    EnrichMiddleware    — async lookup / enrichment (external API, DB, cache)

Provided (AI-powered, require an installed AI provider plugin):
    AIEnrichMiddleware    — LLM-based field enrichment
    AIClassifyMiddleware  — classify records into categories
    AIExtractMiddleware   — extract structured data from text
    AIValidateMiddleware  — validate data quality with LLM reasoning
    AITranslateMiddleware — translate text fields via LLM

Usage::

    from agora.middlewares import EnrichMiddleware, ValidateMiddleware
"""

from typing import Any

from agora.core.middleware import Middleware
from agora.core.registry import Registry
from agora.middlewares.enrich import EnrichMiddleware
from agora.middlewares.validate import ValidateMiddleware

# ======================================================================
# Middleware Registry
# ======================================================================

middleware_registry: Registry[type[Middleware[Any, Any]]] = Registry(name="middleware")

# Register built-in middlewares
middleware_registry.register("validate", ValidateMiddleware)
middleware_registry.register("enrich", EnrichMiddleware)


def _register_ai_middlewares() -> None:
    """Register AI middlewares as lazy factories (avoids importing AI SDKs at startup)."""

    def _ai_enrich_factory(**kwargs: Any) -> Any:
        from agora.middlewares.ai.enrich import AIEnrichMiddleware

        return AIEnrichMiddleware(**kwargs)

    def _ai_classify_factory(**kwargs: Any) -> Any:
        from agora.middlewares.ai.classify import AIClassifyMiddleware

        return AIClassifyMiddleware(**kwargs)

    def _ai_extract_factory(**kwargs: Any) -> Any:
        from agora.middlewares.ai.extract import AIExtractMiddleware

        return AIExtractMiddleware(**kwargs)

    def _ai_validate_factory(**kwargs: Any) -> Any:
        from agora.middlewares.ai.validate import AIValidateMiddleware

        return AIValidateMiddleware(**kwargs)

    def _ai_translate_factory(**kwargs: Any) -> Any:
        from agora.middlewares.ai.translate import AITranslateMiddleware

        return AITranslateMiddleware(**kwargs)

    def _ai_batch_factory(**kwargs: Any) -> Any:
        from agora.middlewares.ai.batch import AIBatchMiddleware

        return AIBatchMiddleware(**kwargs)

    middleware_registry.register_factory("ai_enrich", _ai_enrich_factory)
    middleware_registry.register_factory("ai_classify", _ai_classify_factory)
    middleware_registry.register_factory("ai_extract", _ai_extract_factory)
    middleware_registry.register_factory("ai_validate", _ai_validate_factory)
    middleware_registry.register_factory("ai_translate", _ai_translate_factory)
    middleware_registry.register_factory("ai_batch", _ai_batch_factory)


_register_ai_middlewares()
middleware_registry.load_entrypoints("agora.middlewares")

__all__ = [
    "EnrichMiddleware",
    "ValidateMiddleware",
    "middleware_registry",
]
