"""
agora/middlewares/validate.py
==============================
``ValidateMiddleware[T, U]`` — validate and optionally coerce records.

Supports two validation modes:
  1. Pydantic model — pass a pydantic ``BaseModel`` class as ``schema``.
  2. Custom validator — pass a callable ``(T) -> U | None``.

Records failing validation are dropped (return None) and counted as
``records_dropped`` in pipeline metrics.

Usage — pydantic::

    middleware = ValidateMiddleware(schema=MyPydanticModel)

Usage — custom::

    def validate_poi(record) -> POI | None:
        if not record.get("name"):
            return None
        return POI(**record)

    middleware = ValidateMiddleware(validator=validate_poi)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

import logstruct

from agora.core.middleware import Middleware
from agora.core.types import OnError

if TYPE_CHECKING:
    from collections.abc import Callable

    from pydantic import BaseModel

    from agora.core.context import PipelineContext

T = TypeVar("T")
U = TypeVar("U")
logger = logstruct.getLogger(__name__)


class ValidateMiddleware(Middleware[T, U], Generic[T, U]):
    """Validate / coerce records.  Drop invalid ones.

    Parameters
    ----------
    schema:
        A Pydantic ``BaseModel`` subclass.  Records are passed as kwargs
        to the model constructor.  Validation errors cause the record to
        be dropped.
    validator:
        A callable ``(record: T) -> U | None``.  Return None to drop.
        Mutually exclusive with ``schema``.
    on_error:
        ``"drop"`` (default) — silently drop invalid records.
        ``"raise"`` — re-raise validation errors (stops the pipeline).
        ``"log"`` — log a warning and drop.
    """

    name = "validate"

    def __init__(
        self,
        schema: type[BaseModel] | None = None,
        validator: Callable[[T], U | None] | None = None,
        on_error: OnError = OnError.DROP,
    ) -> None:
        if schema is None and validator is None:
            raise ValueError("ValidateMiddleware requires either 'schema' or 'validator'")
        if schema is not None and validator is not None:
            raise ValueError("Pass either 'schema' or 'validator', not both")
        self._schema = schema
        self._validator = validator
        self._on_error = on_error

    async def process(self, record: T, ctx: PipelineContext) -> U | None:
        try:
            if self._validator is not None:
                return self._validator(record)

            # Pydantic path
            if isinstance(record, dict):
                return self._schema(**record)  # type: ignore[misc, return-value]
            return self._schema.model_validate(record)  # type: ignore[union-attr, return-value]

        except Exception as exc:
            if self._on_error == "raise":
                raise
            if self._on_error == "log":
                ctx.log.warning(
                    "validate_middleware_error",
                    error=str(exc),
                    record_type=type(record).__name__,
                )
            return None
