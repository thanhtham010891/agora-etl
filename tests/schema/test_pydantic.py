from __future__ import annotations

from datetime import date, datetime

import pytest
from pydantic import ValidationError

from agora.schema import schema_to_pydantic_model
from agora.schema.types import Column, DataType, Schema


def test_schema_to_pydantic_model_generates_expected_types() -> None:
    schema = Schema(
        table="users",
        columns={
            "id": Column("id", DataType.INTEGER, nullable=False),
            "name": Column("name", DataType.STRING, nullable=True),
            "created_at": Column("created_at", DataType.TIMESTAMP, nullable=False),
        },
    )

    model = schema_to_pydantic_model(schema, model_name="UserRecord")
    record = model.model_validate(
        {
            "id": 1,
            "name": "Alice",
            "created_at": datetime(2026, 5, 20, 10, 0, 0),
        }
    )

    assert model.__name__ == "UserRecord"
    assert record.id == 1
    assert record.name == "Alice"
    assert record.created_at == datetime(2026, 5, 20, 10, 0, 0)


def test_schema_to_pydantic_model_accepts_date_for_timestamp() -> None:
    schema = Schema(
        table="events",
        columns={"event_date": Column("event_date", DataType.TIMESTAMP, nullable=False)},
    )

    model = schema.to_pydantic_model()
    record = model.model_validate({"event_date": date(2026, 5, 20)})

    assert record.event_date == date(2026, 5, 20)


def test_schema_to_pydantic_model_supports_aliases_for_invalid_identifiers() -> None:
    schema = Schema(
        table="raw-events",
        columns={
            "user-id": Column("user-id", DataType.INTEGER, nullable=False),
            "class": Column("class", DataType.STRING, nullable=True),
        },
    )

    model = schema.to_pydantic_model()
    record = model.model_validate({"user-id": 42, "class": "vip"})

    assert record.user_id == 42
    assert record.class_ == "vip"
    assert record.model_dump(by_alias=True) == {"user-id": 42, "class": "vip"}


def test_schema_to_pydantic_model_requires_non_nullable_fields() -> None:
    schema = Schema(
        table="users",
        columns={"id": Column("id", DataType.INTEGER, nullable=False)},
    )

    model = schema.to_pydantic_model()

    with pytest.raises(ValidationError):
        model.model_validate({})
