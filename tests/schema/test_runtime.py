from __future__ import annotations

from agora.schema.runtime import SchemaProcessor
from agora.schema.types import Column, DataType, Schema, SchemaContract


def test_processor_initial_inference() -> None:
    processor = SchemaProcessor[dict[str, object]](table="users")

    result = processor.process({"id": 1, "name": "Alice"})

    assert result.record == {"id": 1, "name": "Alice"}
    assert result.schema is not None
    assert result.schema.columns["id"].data_type == DataType.INTEGER
    assert len(result.changes) == 2
    assert processor.metrics.columns_added == 2
    assert processor.metrics.records_observed == 1


def test_processor_discards_unknown_columns() -> None:
    processor = SchemaProcessor[dict[str, object]](
        table="users",
        contract=SchemaContract.DISCARD_COLUMN,
    )
    processor.load_schema(
        Schema(
            table="users",
            columns={"id": Column("id", DataType.INTEGER)},
            version=2,
        )
    )

    result = processor.process({"id": 1, "name": "Alice"})

    assert result.record == {"id": 1}
    assert result.schema is not None
    assert list(result.schema.columns) == ["id"]


def test_processor_discards_row_on_violation() -> None:
    processor = SchemaProcessor[dict[str, object]](
        table="users",
        contract=SchemaContract.DISCARD_ROW,
    )
    processor.load_schema(
        Schema(
            table="users",
            columns={"id": Column("id", DataType.INTEGER)},
            version=2,
        )
    )

    result = processor.process({"id": 1, "name": "Alice"})

    assert result.record is None
    assert result.schema is not None
    assert list(result.schema.columns) == ["id"]


def test_processor_freeze_stashes_error_until_stop() -> None:
    processor = SchemaProcessor[dict[str, object]](
        table="users",
        contract=SchemaContract.FREEZE,
    )
    processor.load_schema(
        Schema(
            table="users",
            columns={"id": Column("id", DataType.INTEGER)},
            version=2,
        )
    )

    result = processor.process({"id": 1, "name": "Alice"})

    assert result.record is None
    assert processor.pending_error is not None


def test_processor_tracks_type_widening() -> None:
    processor = SchemaProcessor[dict[str, object]](table="users")
    processor.load_schema(
        Schema(
            table="users",
            columns={"score": Column("score", DataType.INTEGER)},
            version=1,
        )
    )

    result = processor.process({"score": 95.5})

    assert result.schema is not None
    assert result.schema.columns["score"].data_type == DataType.FLOAT
    assert processor.metrics.types_widened == 1
