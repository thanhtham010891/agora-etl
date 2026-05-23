from __future__ import annotations

from datetime import date, datetime

from agora.schema.types import (
    Column,
    DataType,
    Schema,
    SchemaContract,
    can_widen,
    infer_python_type,
    widen_type,
)

# ============================================================================
# DataType Tests
# ============================================================================


def test_data_type_enum_values() -> None:
    """DataType enum has expected values."""
    assert DataType.STRING == "string"
    assert DataType.INTEGER == "integer"
    assert DataType.FLOAT == "float"
    assert DataType.BOOLEAN == "boolean"
    assert DataType.TIMESTAMP == "timestamp"
    assert DataType.JSON == "json"
    assert DataType.BYTES == "bytes"
    assert DataType.NULL == "null"


def test_schema_contract_enum_values() -> None:
    """SchemaContract enum has expected values."""
    assert SchemaContract.EVOLVE == "evolve"
    assert SchemaContract.FREEZE == "freeze"
    assert SchemaContract.DISCARD_COLUMN == "discard_column"
    assert SchemaContract.DISCARD_ROW == "discard_row"


# ============================================================================
# infer_python_type Tests
# ============================================================================


def test_infer_python_type_none() -> None:
    """None infers to NULL."""
    assert infer_python_type(None) == DataType.NULL


def test_infer_python_type_bool() -> None:
    """bool infers to BOOLEAN (before int check)."""
    assert infer_python_type(True) == DataType.BOOLEAN
    assert infer_python_type(False) == DataType.BOOLEAN


def test_infer_python_type_int() -> None:
    """int infers to INTEGER."""
    assert infer_python_type(42) == DataType.INTEGER
    assert infer_python_type(0) == DataType.INTEGER
    assert infer_python_type(-100) == DataType.INTEGER


def test_infer_python_type_float() -> None:
    """float infers to FLOAT."""
    assert infer_python_type(3.14) == DataType.FLOAT
    assert infer_python_type(0.0) == DataType.FLOAT
    assert infer_python_type(-2.5) == DataType.FLOAT


def test_infer_python_type_str() -> None:
    """str infers to STRING."""
    assert infer_python_type("hello") == DataType.STRING
    assert infer_python_type("") == DataType.STRING


def test_infer_python_type_bytes() -> None:
    """bytes infers to BYTES."""
    assert infer_python_type(b"data") == DataType.BYTES
    assert infer_python_type(b"") == DataType.BYTES


def test_infer_python_type_datetime() -> None:
    """datetime infers to TIMESTAMP."""
    assert infer_python_type(datetime.now()) == DataType.TIMESTAMP
    assert infer_python_type(date.today()) == DataType.TIMESTAMP


def test_infer_python_type_dict() -> None:
    """dict infers to JSON."""
    assert infer_python_type({"key": "value"}) == DataType.JSON
    assert infer_python_type({}) == DataType.JSON


def test_infer_python_type_list() -> None:
    """list infers to JSON."""
    assert infer_python_type([1, 2, 3]) == DataType.JSON
    assert infer_python_type([]) == DataType.JSON


def test_infer_python_type_fallback() -> None:
    """Unknown types fall back to STRING."""

    class CustomClass:
        pass

    assert infer_python_type(CustomClass()) == DataType.STRING


# ============================================================================
# can_widen Tests
# ============================================================================


def test_can_widen_same_type() -> None:
    """Same type can always widen."""
    assert can_widen(DataType.STRING, DataType.STRING)
    assert can_widen(DataType.INTEGER, DataType.INTEGER)


def test_can_widen_null_to_any() -> None:
    """NULL can widen to any type."""
    assert can_widen(DataType.NULL, DataType.STRING)
    assert can_widen(DataType.NULL, DataType.INTEGER)
    assert can_widen(DataType.NULL, DataType.FLOAT)


def test_can_widen_integer_to_float() -> None:
    """INTEGER can widen to FLOAT."""
    assert can_widen(DataType.INTEGER, DataType.FLOAT)


def test_can_widen_any_to_string() -> None:
    """Any type can widen to STRING (fallback)."""
    assert can_widen(DataType.INTEGER, DataType.STRING)
    assert can_widen(DataType.BOOLEAN, DataType.STRING)
    assert can_widen(DataType.TIMESTAMP, DataType.STRING)


def test_cannot_widen_incompatible() -> None:
    """Incompatible types cannot widen."""
    assert not can_widen(DataType.STRING, DataType.INTEGER)
    assert not can_widen(DataType.FLOAT, DataType.INTEGER)
    assert not can_widen(DataType.BOOLEAN, DataType.INTEGER)


# ============================================================================
# widen_type Tests
# ============================================================================


def test_widen_type_same() -> None:
    """Widening same type returns same type."""
    assert widen_type(DataType.STRING, DataType.STRING) == DataType.STRING


def test_widen_type_null() -> None:
    """NULL widens to other type."""
    assert widen_type(DataType.NULL, DataType.INTEGER) == DataType.INTEGER
    assert widen_type(DataType.INTEGER, DataType.NULL) == DataType.INTEGER


def test_widen_type_integer_float() -> None:
    """INTEGER widens to FLOAT."""
    assert widen_type(DataType.INTEGER, DataType.FLOAT) == DataType.FLOAT
    assert widen_type(DataType.FLOAT, DataType.INTEGER) == DataType.FLOAT


def test_widen_type_conflict_to_string() -> None:
    """Type conflicts widen to STRING."""
    assert widen_type(DataType.INTEGER, DataType.BOOLEAN) == DataType.STRING
    assert widen_type(DataType.TIMESTAMP, DataType.INTEGER) == DataType.STRING


# ============================================================================
# Column Tests
# ============================================================================


def test_column_creation() -> None:
    """Column can be created with required fields."""
    col = Column(name="id", data_type=DataType.INTEGER)
    assert col.name == "id"
    assert col.data_type == DataType.INTEGER
    assert col.nullable is True  # default
    assert col.inferred_from == 0  # default


def test_column_to_dict() -> None:
    """Column serializes to dict."""
    col = Column(name="score", data_type=DataType.FLOAT, nullable=False, inferred_from=10)
    data = col.to_dict()
    assert data == {
        "name": "score",
        "data_type": "float",
        "nullable": False,
        "inferred_from": 10,
    }


def test_column_from_dict() -> None:
    """Column deserializes from dict."""
    data = {
        "name": "score",
        "data_type": "float",
        "nullable": False,
        "inferred_from": 10,
    }
    col = Column.from_dict(data)
    assert col.name == "score"
    assert col.data_type == DataType.FLOAT
    assert col.nullable is False
    assert col.inferred_from == 10


# ============================================================================
# Schema Tests
# ============================================================================


def test_schema_creation() -> None:
    """Schema can be created with table and columns."""
    schema = Schema(
        table="users",
        columns={
            "id": Column("id", DataType.INTEGER),
            "name": Column("name", DataType.STRING),
        },
    )
    assert schema.table == "users"
    assert len(schema.columns) == 2
    assert schema.version == 1
    assert schema.hash != ""  # hash computed automatically


def test_schema_hash_computed() -> None:
    """Schema hash is computed automatically."""
    schema = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER)},
    )
    assert len(schema.hash) == 16  # truncated SHA256


def test_schema_hash_changes_with_content() -> None:
    """Schema hash changes when content changes."""
    schema1 = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER)},
    )
    schema2 = Schema(
        table="users",
        columns={"id": Column("id", DataType.STRING)},  # different type
    )
    assert schema1.hash != schema2.hash


def test_schema_to_dict() -> None:
    """Schema serializes to dict."""
    schema = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER, nullable=False)},
        version=2,
    )
    data = schema.to_dict()
    assert data["table"] == "users"
    assert "id" in data["columns"]
    assert data["version"] == 2
    assert "hash" in data


def test_schema_from_dict() -> None:
    """Schema deserializes from dict."""
    data = {
        "table": "users",
        "columns": {
            "id": {
                "name": "id",
                "data_type": "integer",
                "nullable": False,
                "inferred_from": 5,
            }
        },
        "version": 2,
        "hash": "abc123",
    }
    schema = Schema.from_dict(data)
    assert schema.table == "users"
    assert "id" in schema.columns
    assert schema.columns["id"].data_type == DataType.INTEGER
    assert schema.version == 2


def test_schema_column_names() -> None:
    """Schema returns sorted column names."""
    schema = Schema(
        table="users",
        columns={
            "name": Column("name", DataType.STRING),
            "id": Column("id", DataType.INTEGER),
            "age": Column("age", DataType.INTEGER),
        },
    )
    assert schema.column_names() == ["age", "id", "name"]


def test_schema_has_column() -> None:
    """Schema checks if column exists."""
    schema = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER)},
    )
    assert schema.has_column("id")
    assert not schema.has_column("name")


def test_schema_get_column() -> None:
    """Schema retrieves column by name."""
    col = Column("id", DataType.INTEGER)
    schema = Schema(table="users", columns={"id": col})
    assert schema.get_column("id") == col
    assert schema.get_column("name") is None
