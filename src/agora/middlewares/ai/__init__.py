"""
agora/middlewares/ai/__init__.py
==================================
AI-powered middleware collection.

Completion-driven middlewares require a completion-capable provider.
Embedding mode and semantic dedup flows require embedding support.
Import what you need::

    from agora.middlewares.ai import (
        AIEnrichMiddleware,
        AIExtractMiddleware,
        AIClassifyMiddleware,
        AIValidateMiddleware,
        AITranslateMiddleware,
        AIBatchMiddleware,
    )
"""

from agora.middlewares.ai.batch import AIBatchMiddleware
from agora.middlewares.ai.classify import AIClassifyMiddleware
from agora.middlewares.ai.enrich import AIEnrichMiddleware
from agora.middlewares.ai.extract import AIExtractMiddleware
from agora.middlewares.ai.translate import AITranslateMiddleware
from agora.middlewares.ai.validate import AIValidateMiddleware, DataQualityError

__all__ = [
    "AIBatchMiddleware",
    "AIClassifyMiddleware",
    "AIEnrichMiddleware",
    "AIExtractMiddleware",
    "AITranslateMiddleware",
    "AIValidateMiddleware",
    "DataQualityError",
]
