from __future__ import annotations

from agora.schema.store import BackendSchemaStore, InMemorySchemaStore
from agora.schema.types import Column, DataType, Schema
from agora.state import MemoryBackend, SQLiteBackend

# ============================================================================
# InMemorySchemaStore Tests
# ============================================================================


def test_in_memory_store_save_and_load() -> None:
    """InMemorySchemaStore saves and loads schemas."""
    store = InMemorySchemaStore()
    schema = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER)},
    )

    store.save("pipeline1", "users", schema)
    loaded = store.load("pipeline1", "users")

    assert loaded is not None
    assert loaded.table == "users"
    assert "id" in loaded.columns
    assert loaded.columns["id"].data_type == DataType.INTEGER


def test_in_memory_store_load_nonexistent() -> None:
    """InMemorySchemaStore returns None for nonexistent schema."""
    store = InMemorySchemaStore()
    loaded = store.load("pipeline1", "users")
    assert loaded is None


def test_in_memory_store_overwrite() -> None:
    """InMemorySchemaStore overwrites existing schema."""
    store = InMemorySchemaStore()
    schema1 = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER)},
        version=1,
    )
    schema2 = Schema(
        table="users",
        columns={
            "id": Column("id", DataType.INTEGER),
            "name": Column("name", DataType.STRING),
        },
        version=2,
    )

    store.save("pipeline1", "users", schema1)
    store.save("pipeline1", "users", schema2)
    loaded = store.load("pipeline1", "users")

    assert loaded is not None
    assert loaded.version == 2
    assert len(loaded.columns) == 2


def test_in_memory_store_multiple_pipelines() -> None:
    """InMemorySchemaStore isolates schemas by pipeline_id."""
    store = InMemorySchemaStore()
    schema1 = Schema(table="users", columns={"id": Column("id", DataType.INTEGER)})
    schema2 = Schema(table="users", columns={"name": Column("name", DataType.STRING)})

    store.save("pipeline1", "users", schema1)
    store.save("pipeline2", "users", schema2)

    loaded1 = store.load("pipeline1", "users")
    loaded2 = store.load("pipeline2", "users")

    assert loaded1 is not None
    assert loaded2 is not None
    assert "id" in loaded1.columns
    assert "name" in loaded2.columns
    assert "name" not in loaded1.columns


def test_in_memory_store_clear() -> None:
    """InMemorySchemaStore.clear() removes all schemas."""
    store = InMemorySchemaStore()
    schema = Schema(table="users", columns={"id": Column("id", DataType.INTEGER)})

    store.save("pipeline1", "users", schema)
    store.clear()
    loaded = store.load("pipeline1", "users")

    assert loaded is None


def test_in_memory_store_close() -> None:
    """InMemorySchemaStore.close() is a no-op."""
    store = InMemorySchemaStore()
    store.close()  # Should not raise


# ============================================================================
# BackendSchemaStore Tests (MemoryBackend)
# ============================================================================


def test_backend_store_memory_save_and_load() -> None:
    """BackendSchemaStore with MemoryBackend saves and loads schemas."""
    backend = MemoryBackend()
    store = BackendSchemaStore(backend)
    schema = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER)},
    )

    store.save("pipeline1", "users", schema)
    loaded = store.load("pipeline1", "users")

    assert loaded is not None
    assert loaded.table == "users"
    assert "id" in loaded.columns


def test_backend_store_memory_load_nonexistent() -> None:
    """BackendSchemaStore returns None for nonexistent schema."""
    backend = MemoryBackend()
    store = BackendSchemaStore(backend)
    loaded = store.load("pipeline1", "users")
    assert loaded is None


def test_backend_store_memory_key_format() -> None:
    """BackendSchemaStore uses correct key format: schema:{pipeline_id}:{table}."""
    backend = MemoryBackend()
    store = BackendSchemaStore(backend)
    schema = Schema(table="users", columns={"id": Column("id", DataType.INTEGER)})

    store.save("pipeline1", "users", schema)

    # Check backend directly
    key = "schema:pipeline1:users"
    stored = backend.get(key)
    assert stored is not None
    assert isinstance(stored.value, dict)
    assert stored.value["table"] == "users"


def test_backend_store_memory_serialization() -> None:
    """BackendSchemaStore serializes/deserializes schema correctly."""
    backend = MemoryBackend()
    store = BackendSchemaStore(backend)
    schema = Schema(
        table="users",
        columns={
            "id": Column("id", DataType.INTEGER, nullable=False, inferred_from=10),
            "name": Column("name", DataType.STRING, nullable=True, inferred_from=8),
        },
        version=2,
    )

    store.save("pipeline1", "users", schema)
    loaded = store.load("pipeline1", "users")

    assert loaded is not None
    assert loaded.table == "users"
    assert loaded.version == 2
    assert len(loaded.columns) == 2
    assert loaded.columns["id"].data_type == DataType.INTEGER
    assert loaded.columns["id"].nullable is False
    assert loaded.columns["id"].inferred_from == 10
    assert loaded.columns["name"].data_type == DataType.STRING
    assert loaded.columns["name"].nullable is True
    assert loaded.columns["name"].inferred_from == 8


def test_backend_store_memory_close() -> None:
    """BackendSchemaStore.close() closes backend."""
    backend = MemoryBackend()
    store = BackendSchemaStore(backend)
    store.close()
    # MemoryBackend.close() is a no-op, so just verify it doesn't raise


# ============================================================================
# BackendSchemaStore Tests (SQLiteBackend)
# ============================================================================


def test_backend_store_sqlite_save_and_load(tmp_path) -> None:
    """BackendSchemaStore with SQLiteBackend persists schemas to disk."""
    db_path = tmp_path / "schemas.db"
    backend = SQLiteBackend(str(db_path))
    store = BackendSchemaStore(backend)
    schema = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER)},
    )

    store.save("pipeline1", "users", schema)
    store.close()

    # Reopen and load
    backend2 = SQLiteBackend(str(db_path))
    store2 = BackendSchemaStore(backend2)
    loaded = store2.load("pipeline1", "users")

    assert loaded is not None
    assert loaded.table == "users"
    assert "id" in loaded.columns
    store2.close()


def test_backend_store_sqlite_multiple_tables(tmp_path) -> None:
    """BackendSchemaStore isolates schemas by table name."""
    db_path = tmp_path / "schemas.db"
    backend = SQLiteBackend(str(db_path))
    store = BackendSchemaStore(backend)

    schema1 = Schema(table="users", columns={"id": Column("id", DataType.INTEGER)})
    schema2 = Schema(table="orders", columns={"order_id": Column("order_id", DataType.STRING)})

    store.save("pipeline1", "users", schema1)
    store.save("pipeline1", "orders", schema2)

    loaded1 = store.load("pipeline1", "users")
    loaded2 = store.load("pipeline1", "orders")

    assert loaded1 is not None
    assert loaded2 is not None
    assert loaded1.table == "users"
    assert loaded2.table == "orders"
    assert "id" in loaded1.columns
    assert "order_id" in loaded2.columns
    store.close()


def test_backend_store_sqlite_overwrite(tmp_path) -> None:
    """BackendSchemaStore overwrites existing schema in SQLite."""
    db_path = tmp_path / "schemas.db"
    backend = SQLiteBackend(str(db_path))
    store = BackendSchemaStore(backend)

    schema1 = Schema(table="users", columns={"id": Column("id", DataType.INTEGER)}, version=1)
    schema2 = Schema(
        table="users",
        columns={
            "id": Column("id", DataType.INTEGER),
            "name": Column("name", DataType.STRING),
        },
        version=2,
    )

    store.save("pipeline1", "users", schema1)
    store.save("pipeline1", "users", schema2)
    loaded = store.load("pipeline1", "users")

    assert loaded is not None
    assert loaded.version == 2
    assert len(loaded.columns) == 2
    store.close()


def test_backend_store_load_corrupted_data() -> None:
    """BackendSchemaStore returns None when stored data is not a dict."""
    backend = MemoryBackend()
    store = BackendSchemaStore(backend)

    # Manually insert corrupted data
    backend.set("schema:pipeline1:users", "not a dict")

    loaded = store.load("pipeline1", "users")
    assert loaded is None
