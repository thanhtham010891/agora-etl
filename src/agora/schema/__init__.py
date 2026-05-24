"""
agora/schema
=============
Schema inference and evolution for Agora ETL pipelines.

Automatically infers schemas from records and handles schema changes gracefully.
Inspired by dlt (data load tool) but simpler and Agora-native.

Usage
-----

**Basic inference:**

    from agora.schema import infer_schema

    records = [
        {"id": 1, "name": "Alice", "score": 95.5},
        {"id": 2, "name": "Bob", "score": 87},
    ]
    schema = infer_schema(records, table="students")

**Generate a Pydantic model:**

    from agora.schema import infer_schema

    schema = infer_schema(records, table="students")
    StudentModel = schema.to_pydantic_model()

**With middleware and same-run sink application:**

    from agora import Pipeline
    from agora.core.source import IterableSource
    from agora.schema import BackendSchemaStore, SchemaMiddleware
    from agora_plugins.postgres import PostgresSchemaAdapter, PostgresSink
    from agora.state import SQLiteBackend

    pipeline = (
        Pipeline(
            IterableSource(
                [
                    {"id": 1},
                    {"id": 2, "name": "Alice"},
                ]
            ),
            id="users",
        )
        .pipe(
            SchemaMiddleware(
                table="public.users",
                store=BackendSchemaStore(SQLiteBackend(".agora_schemas.db")),
            )
        )
        .build(
            PostgresSchemaAdapter(
                PostgresSink(
                    dsn="postgresql://localhost/mydb",
                    table="public.users",
                    row_mapper=lambda record: record,
                    conflict_key="id",
                )
            )
        )
    )

**Schema evolution:**

    from agora.schema import evolve_schema, SchemaContract

    evolved, changes = evolve_schema(old_schema, new_schema, SchemaContract.EVOLVE)
    for change in changes:
        print(change.message)

**Contracts:**

    SchemaContract.EVOLVE         # add columns, widen compatible types
    SchemaContract.FREEZE         # reject schema changes
    SchemaContract.DISCARD_COLUMN # strip unknown columns before sink writes
    SchemaContract.DISCARD_ROW    # drop records that violate current schema
"""

from agora.schema.evolution import SchemaEvolution, SchemaEvolutionError, evolve_schema
from agora.schema.inference import SchemaInferrer, infer_schema
from agora.schema.middleware import SchemaMiddleware
from agora.schema.pydantic import schema_to_pydantic_model
from agora.schema.runtime import SchemaProcessor, SchemaProcessResult
from agora.schema.store import BackendSchemaStore, InMemorySchemaStore, SchemaStore
from agora.schema.types import Column, DataType, Schema, SchemaContract

__all__ = [
    "BackendSchemaStore",
    "Column",
    "DataType",
    "InMemorySchemaStore",
    "Schema",
    "SchemaContract",
    "SchemaEvolution",
    "SchemaEvolutionError",
    "SchemaInferrer",
    "SchemaMiddleware",
    "SchemaProcessResult",
    "SchemaProcessor",
    "SchemaStore",
    "evolve_schema",
    "infer_schema",
    "schema_to_pydantic_model",
]
