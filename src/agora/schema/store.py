"""
agora/schema/store.py
======================
Schema storage and persistence.

Stores schemas in StateBackend (SQLite, Redis, etc.) for persistence across runs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from agora.schema.types import Schema, SchemaContract

if TYPE_CHECKING:
    from agora.state.backend import StateBackend


@runtime_checkable
class SchemaStore(Protocol):
    """Protocol for schema storage.

    Allows pluggable storage backends.
    """

    def load(self, pipeline_id: str, table: str) -> Schema | None:
        """Load schema from store.

        Parameters
        ----------
        pipeline_id:
            Pipeline identifier.
        table:
            Table name.

        Returns
        -------
        Schema | None
            Stored schema, or None if not found.
        """
        ...

    def save(self, pipeline_id: str, table: str, schema: Schema) -> None:
        """Save schema to store.

        Parameters
        ----------
        pipeline_id:
            Pipeline identifier.
        table:
            Table name.
        schema:
            Schema to save.
        """
        ...

    def close(self) -> None:
        """Close store and release resources."""
        ...


class BackendSchemaStore:
    """Schema store backed by StateBackend.

    Wraps any StateBackend (SQLite, Redis, Memory) for schema persistence.
    Follows the same pattern as BackendCheckpointStore.

    Parameters
    ----------
    backend:
        State backend for storage.
    contract:
        Schema contract — when FREEZE, hash mismatches on load raise ValueError
        instead of logging a warning.
    """

    def __init__(
        self,
        backend: StateBackend,
        contract: SchemaContract = SchemaContract.EVOLVE,
    ) -> None:
        self._backend = backend
        self._strict_hash = contract == SchemaContract.FREEZE

    def load(self, pipeline_id: str, table: str) -> Schema | None:
        key = self._make_key(pipeline_id, table)
        stored = self._backend.get(key)
        if stored is None:
            return None

        value = stored.value
        if not isinstance(value, dict):
            return None

        return Schema.from_dict(value, strict_hash=self._strict_hash)

    def save(self, pipeline_id: str, table: str, schema: Schema) -> None:
        """Save schema to backend.

        Parameters
        ----------
        pipeline_id:
            Pipeline identifier.
        table:
            Table name.
        schema:
            Schema to save.
        """
        key = self._make_key(pipeline_id, table)
        value = schema.to_dict()
        self._backend.set(key, value)

    def close(self) -> None:
        """Close backend and release resources."""
        self._backend.close()

    def _make_key(self, pipeline_id: str, table: str) -> str:
        """Generate storage key.

        Format: schema:{pipeline_id}:{table}

        Parameters
        ----------
        pipeline_id:
            Pipeline identifier.
        table:
            Table name.

        Returns
        -------
        str
            Storage key.
        """
        return f"schema:{pipeline_id}:{table}"


class InMemorySchemaStore:
    """In-memory schema store for testing.

    Does not persist across process restarts.
    """

    def __init__(self) -> None:
        """Initialize empty store."""
        self._schemas: dict[str, Schema] = {}

    def load(self, pipeline_id: str, table: str) -> Schema | None:
        """Load schema from memory.

        Parameters
        ----------
        pipeline_id:
            Pipeline identifier.
        table:
            Table name.

        Returns
        -------
        Schema | None
            Stored schema, or None if not found.
        """
        key = f"{pipeline_id}:{table}"
        return self._schemas.get(key)

    def save(self, pipeline_id: str, table: str, schema: Schema) -> None:
        """Save schema to memory.

        Parameters
        ----------
        pipeline_id:
            Pipeline identifier.
        table:
            Table name.
        schema:
            Schema to save.
        """
        key = f"{pipeline_id}:{table}"
        self._schemas[key] = schema

    def close(self) -> None:
        """No-op for in-memory store."""

    def clear(self) -> None:
        """Clear all stored schemas (for testing)."""
        self._schemas.clear()
