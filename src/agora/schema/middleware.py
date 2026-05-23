"""
agora/schema/middleware.py
===========================
SchemaMiddleware — automatic schema inference and evolution.

Observes records during pipeline execution, evolves schema incrementally,
and stores the latest schema in ctx.extras for downstream consumption.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

import logstruct

from agora.core.middleware import Middleware
from agora.schema.runtime import SchemaProcessor
from agora.schema.types import SchemaContract

if TYPE_CHECKING:
    from agora.core.context import PipelineContext
    from agora.schema.store import SchemaStore

T = TypeVar("T")

logger = logstruct.getLogger(__name__)


class SchemaMiddleware(Middleware[T, T], Generic[T]):
    """Passthrough middleware that infers and evolves schema incrementally."""

    def __init__(
        self,
        table: str,
        contract: SchemaContract = SchemaContract.EVOLVE,
        store: SchemaStore | None = None,
        name: str = "schema",
    ) -> None:
        self.name = name
        self._table = table
        self._store = store
        self._processor = SchemaProcessor[T](table=table, contract=contract)

    async def on_start(self, ctx: PipelineContext) -> None:
        """Load existing schema from store (if available)."""
        self._processor.load_schema(None)

        if self._store is None:
            return

        try:
            loaded_schema = self._store.load(ctx.pipeline_id, self._table)
            self._processor.load_schema(loaded_schema)
            if loaded_schema is not None:
                ctx.extras["schema"] = loaded_schema
                ctx.log.info(
                    "schema_loaded",
                    table=self._table,
                    version=loaded_schema.version,
                    columns=len(loaded_schema.columns),
                    middleware=self.name,
                )
        except Exception as exc:
            ctx.log.warning(
                "schema_load_failed",
                table=self._table,
                error=str(exc),
                middleware=self.name,
            )

    async def process(self, record: T, ctx: PipelineContext) -> T | None:
        """Observe a record, enforce the configured contract, and publish schema."""
        try:
            result = self._processor.process(record)
        except Exception as exc:
            ctx.log.warning(
                "schema_observe_failed",
                error=str(exc),
                middleware=self.name,
            )
            return record

        if result.record is None:
            if self._processor.pending_error is not None:
                ctx.log.error(
                    "schema_evolution_failed",
                    error=str(self._processor.pending_error),
                    middleware=self.name,
                )
            else:
                ctx.log.info(
                    "schema_row_discarded",
                    table=self._table,
                    middleware=self.name,
                )
            return None

        if result.schema is not None:
            ctx.extras["schema"] = result.schema

        current_schema = self._processor.current_schema
        is_first_inference = (
            current_schema is not None
            and current_schema.version == 1
            and self._processor.metrics.columns_added == len(current_schema.columns)
            and bool(result.changes)
            and self._processor.metrics.records_observed == 1
        )
        if is_first_inference:
            ctx.log.info(
                "schema_inferred",
                table=self._table,
                columns=len(current_schema.columns),
                middleware=self.name,
            )

        if result.changes and current_schema is not None:
            if not is_first_inference:
                ctx.log.info(
                    "schema_evolved",
                    table=self._table,
                    changes=len(result.changes),
                    version=current_schema.version,
                    middleware=self.name,
                )
            for change in result.changes:
                ctx.log.debug(
                    "schema_change",
                    column=change.column_name,
                    message=change.message,
                    middleware=self.name,
                )

        return result.record

    async def on_stop(self, ctx: PipelineContext) -> None:
        """Persist the latest schema and surface metrics."""
        current_schema = self._processor.current_schema
        if current_schema is not None:
            ctx.extras["schema"] = current_schema

        if self._store is not None and current_schema is not None:
            try:
                self._store.save(ctx.pipeline_id, self._table, current_schema)
                ctx.log.info(
                    "schema_saved",
                    table=self._table,
                    version=current_schema.version,
                    middleware=self.name,
                )
            except Exception as exc:
                ctx.log.warning(
                    "schema_save_failed",
                    table=self._table,
                    error=str(exc),
                    middleware=self.name,
                )

        ctx.metrics.middleware(self.name).schema = self._processor.metrics

        if self._store is not None:
            try:
                self._store.close()
            except Exception as exc:
                ctx.log.warning(
                    "schema_store_close_failed",
                    error=str(exc),
                    middleware=self.name,
                )

        if self._processor.pending_error is not None:
            raise self._processor.pending_error
