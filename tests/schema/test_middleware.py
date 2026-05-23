from __future__ import annotations

import pytest

from agora.core.context import PipelineContext
from agora.core.metrics import PipelineMetrics
from agora.schema.evolution import SchemaEvolutionError
from agora.schema.middleware import SchemaMiddleware
from agora.schema.store import InMemorySchemaStore
from agora.schema.types import Column, DataType, Schema, SchemaContract


def _make_ctx() -> PipelineContext:
    """Create test context."""
    return PipelineContext(pipeline_id="test_pipeline", metrics=PipelineMetrics())


# ============================================================================
# SchemaMiddleware Basic Tests
# ============================================================================


@pytest.mark.asyncio
async def test_middleware_infers_schema_from_dicts() -> None:
    """SchemaMiddleware infers schema from dict records."""
    middleware = SchemaMiddleware(table="users")
    ctx = _make_ctx()

    await middleware.on_start(ctx)
    await middleware.process({"id": 1, "name": "Alice"}, ctx)
    await middleware.process({"id": 2, "name": "Bob"}, ctx)
    await middleware.on_stop(ctx)

    schema = ctx.extras.get("schema")
    assert schema is not None
    assert schema.table == "users"
    assert len(schema.columns) == 2
    assert "id" in schema.columns
    assert "name" in schema.columns
    assert schema.columns["id"].data_type == DataType.INTEGER
    assert schema.columns["name"].data_type == DataType.STRING


@pytest.mark.asyncio
async def test_middleware_passthrough() -> None:
    """SchemaMiddleware passes records through unchanged."""
    middleware = SchemaMiddleware(table="users")
    ctx = _make_ctx()

    await middleware.on_start(ctx)
    record = {"id": 1, "name": "Alice"}
    result = await middleware.process(record, ctx)
    await middleware.on_stop(ctx)

    assert result == record


@pytest.mark.asyncio
async def test_middleware_tracks_records_observed() -> None:
    """SchemaMiddleware tracks number of records observed."""
    middleware = SchemaMiddleware(table="users")
    ctx = _make_ctx()

    await middleware.on_start(ctx)
    await middleware.process({"id": 1}, ctx)
    await middleware.process({"id": 2}, ctx)
    await middleware.process({"id": 3}, ctx)
    await middleware.on_stop(ctx)

    m_metrics = ctx.metrics.middleware("schema")
    assert m_metrics.schema is not None
    assert m_metrics.schema.records_observed == 3


@pytest.mark.asyncio
async def test_middleware_stores_metrics_in_context() -> None:
    """SchemaMiddleware stores metrics in context."""
    middleware = SchemaMiddleware(table="users")
    ctx = _make_ctx()

    await middleware.on_start(ctx)
    await middleware.process({"id": 1, "name": "Alice"}, ctx)
    await middleware.on_stop(ctx)

    m_metrics = ctx.metrics.middleware("schema")
    assert m_metrics.schema is not None
    assert m_metrics.schema.columns_added == 2
    assert m_metrics.schema.schema_version == 1


# ============================================================================
# SchemaMiddleware with Store Tests
# ============================================================================


@pytest.mark.asyncio
async def test_middleware_saves_to_store() -> None:
    """SchemaMiddleware saves schema to store."""
    store = InMemorySchemaStore()
    middleware = SchemaMiddleware(table="users", store=store)
    ctx = _make_ctx()

    await middleware.on_start(ctx)
    await middleware.process({"id": 1, "name": "Alice"}, ctx)
    await middleware.on_stop(ctx)

    loaded = store.load("test_pipeline", "users")
    assert loaded is not None
    assert loaded.table == "users"
    assert len(loaded.columns) == 2


@pytest.mark.asyncio
async def test_middleware_loads_from_store() -> None:
    """SchemaMiddleware loads existing schema from store."""
    store = InMemorySchemaStore()
    existing_schema = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER)},
        version=1,
    )
    store.save("test_pipeline", "users", existing_schema)

    middleware = SchemaMiddleware(table="users", store=store)
    ctx = _make_ctx()

    await middleware.on_start(ctx)
    await middleware.process({"id": 1, "name": "Alice"}, ctx)
    await middleware.on_stop(ctx)

    schema = ctx.extras.get("schema")
    assert schema is not None
    assert schema.version == 2  # evolved
    assert len(schema.columns) == 2  # id + name


@pytest.mark.asyncio
async def test_middleware_evolves_schema_add_column() -> None:
    """SchemaMiddleware evolves schema when new column appears."""
    store = InMemorySchemaStore()
    existing_schema = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER)},
        version=1,
    )
    store.save("test_pipeline", "users", existing_schema)

    middleware = SchemaMiddleware(table="users", store=store, contract=SchemaContract.EVOLVE)
    ctx = _make_ctx()

    await middleware.on_start(ctx)
    await middleware.process({"id": 1, "name": "Alice", "age": 30}, ctx)
    await middleware.on_stop(ctx)

    schema = ctx.extras.get("schema")
    assert schema is not None
    assert len(schema.columns) == 3
    assert "name" in schema.columns
    assert "age" in schema.columns

    m_metrics = ctx.metrics.middleware("schema")
    assert m_metrics.schema is not None
    assert m_metrics.schema.columns_added == 2


@pytest.mark.asyncio
async def test_middleware_evolves_schema_widen_type() -> None:
    """SchemaMiddleware widens type when INTEGER → FLOAT."""
    store = InMemorySchemaStore()
    existing_schema = Schema(
        table="users",
        columns={"score": Column("score", DataType.INTEGER)},
        version=1,
    )
    store.save("test_pipeline", "users", existing_schema)

    middleware = SchemaMiddleware(table="users", store=store, contract=SchemaContract.EVOLVE)
    ctx = _make_ctx()

    await middleware.on_start(ctx)
    await middleware.process({"score": 95.5}, ctx)
    await middleware.on_stop(ctx)

    schema = ctx.extras.get("schema")
    assert schema is not None
    assert schema.columns["score"].data_type == DataType.FLOAT

    m_metrics = ctx.metrics.middleware("schema")
    assert m_metrics.schema is not None
    assert m_metrics.schema.types_widened >= 1


# ============================================================================
# SchemaMiddleware Contract Tests
# ============================================================================


@pytest.mark.asyncio
async def test_middleware_contract_evolve() -> None:
    """SchemaMiddleware with EVOLVE contract adds new columns."""
    store = InMemorySchemaStore()
    existing_schema = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER)},
        version=1,
    )
    store.save("test_pipeline", "users", existing_schema)

    middleware = SchemaMiddleware(table="users", store=store, contract=SchemaContract.EVOLVE)
    ctx = _make_ctx()

    await middleware.on_start(ctx)
    await middleware.process({"id": 1, "name": "Alice"}, ctx)
    await middleware.on_stop(ctx)

    schema = ctx.extras.get("schema")
    assert schema is not None
    assert "name" in schema.columns


@pytest.mark.asyncio
async def test_middleware_contract_freeze() -> None:
    """SchemaMiddleware with FREEZE contract rejects new columns."""
    store = InMemorySchemaStore()
    existing_schema = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER)},
        version=1,
    )
    store.save("test_pipeline", "users", existing_schema)

    middleware = SchemaMiddleware(table="users", store=store, contract=SchemaContract.FREEZE)
    ctx = _make_ctx()

    await middleware.on_start(ctx)
    result = await middleware.process({"id": 1, "name": "Alice"}, ctx)
    assert result is None

    with pytest.raises(SchemaEvolutionError):
        await middleware.on_stop(ctx)


@pytest.mark.asyncio
async def test_middleware_contract_discard_column() -> None:
    """SchemaMiddleware with DISCARD_COLUMN drops new columns."""
    store = InMemorySchemaStore()
    existing_schema = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER)},
        version=1,
    )
    store.save("test_pipeline", "users", existing_schema)

    middleware = SchemaMiddleware(
        table="users", store=store, contract=SchemaContract.DISCARD_COLUMN
    )
    ctx = _make_ctx()

    await middleware.on_start(ctx)
    result = await middleware.process({"id": 1, "name": "Alice"}, ctx)
    await middleware.on_stop(ctx)

    assert result == {"id": 1}
    schema = ctx.extras.get("schema")
    assert schema is not None
    assert "name" not in schema.columns
    assert len(schema.columns) == 1


@pytest.mark.asyncio
async def test_middleware_contract_discard_row() -> None:
    """SchemaMiddleware with DISCARD_ROW drops records with new columns."""
    store = InMemorySchemaStore()
    existing_schema = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER)},
        version=1,
    )
    store.save("test_pipeline", "users", existing_schema)

    middleware = SchemaMiddleware(table="users", store=store, contract=SchemaContract.DISCARD_ROW)
    ctx = _make_ctx()

    await middleware.on_start(ctx)
    result = await middleware.process({"id": 1, "name": "Alice"}, ctx)
    await middleware.on_stop(ctx)

    assert result is None
    schema = ctx.extras.get("schema")
    assert schema is not None
    assert list(schema.columns) == ["id"]


@pytest.mark.asyncio
async def test_middleware_marks_initial_columns_nullable_for_sparse_data() -> None:
    """Initial inferred columns stay nullable-safe for semi-structured input."""
    middleware = SchemaMiddleware(table="users")
    ctx = _make_ctx()

    await middleware.on_start(ctx)
    await middleware.process({"id": 1, "name": "Alice"}, ctx)
    await middleware.on_stop(ctx)

    schema = ctx.extras.get("schema")
    assert schema is not None
    assert schema.columns["id"].nullable is True
    assert schema.columns["name"].nullable is True


# ============================================================================
# SchemaMiddleware Error Handling Tests
# ============================================================================


@pytest.mark.asyncio
async def test_middleware_handles_observe_error() -> None:
    """SchemaMiddleware logs error but continues when observe fails."""
    middleware = SchemaMiddleware(table="users")
    ctx = _make_ctx()

    await middleware.on_start(ctx)

    # Pass invalid record that might cause observe to fail
    # (SchemaInferrer should handle most cases, but test error path)
    result = await middleware.process(None, ctx)  # type: ignore

    # Should return record unchanged even on error
    assert result is None


@pytest.mark.asyncio
async def test_middleware_handles_store_load_error() -> None:
    """SchemaMiddleware continues when store.load() fails."""

    class FailingStore:
        def load(self, pipeline_id: str, table: str):
            raise RuntimeError("Store load failed")

        def save(self, pipeline_id: str, table: str, schema):
            pass

        def close(self):
            pass

    middleware = SchemaMiddleware(table="users", store=FailingStore())  # type: ignore
    ctx = _make_ctx()

    # Should not raise — logs warning and continues
    await middleware.on_start(ctx)
    await middleware.process({"id": 1}, ctx)
    await middleware.on_stop(ctx)

    # Schema should still be inferred
    schema = ctx.extras.get("schema")
    assert schema is not None


@pytest.mark.asyncio
async def test_middleware_handles_store_save_error() -> None:
    """SchemaMiddleware continues when store.save() fails."""

    class FailingStore:
        def load(self, pipeline_id: str, table: str):
            return None

        def save(self, pipeline_id: str, table: str, schema):
            raise RuntimeError("Store save failed")

        def close(self):
            pass

    middleware = SchemaMiddleware(table="users", store=FailingStore())  # type: ignore
    ctx = _make_ctx()

    await middleware.on_start(ctx)
    await middleware.process({"id": 1}, ctx)

    # Should not raise — logs warning and continues
    await middleware.on_stop(ctx)

    # Schema should still be in ctx.extras
    schema = ctx.extras.get("schema")
    assert schema is not None


@pytest.mark.asyncio
async def test_middleware_custom_name() -> None:
    """SchemaMiddleware uses custom name in metrics."""
    middleware = SchemaMiddleware(table="users", name="custom_schema")
    ctx = _make_ctx()

    await middleware.on_start(ctx)
    await middleware.process({"id": 1}, ctx)
    await middleware.on_stop(ctx)

    m_metrics = ctx.metrics.middleware("custom_schema")
    assert m_metrics.schema is not None
