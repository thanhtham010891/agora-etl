from __future__ import annotations

import time
from typing import TYPE_CHECKING

from agora import (
    MembershipKeyStore,
    MemoryBackend,
    SQLiteBackend,
    TTLKeyValueStore,
    state_backend_registry,
)
from agora.state.backend import StoredValue

if TYPE_CHECKING:
    from pathlib import Path


def test_memory_backend_stores_and_expires_values() -> None:
    backend = MemoryBackend()
    backend.set("alpha", {"ok": True})
    backend.set("expired", "gone", expires_at=time.time() - 1)

    assert backend.get("alpha") == StoredValue(value={"ok": True}, expires_at=None)
    assert backend.get("expired") is None


def test_sqlite_backend_persists_values(tmp_path: Path) -> None:
    path = tmp_path / "state.db"

    first = SQLiteBackend(path)
    first.set("alpha", {"count": 1})
    first.close()

    second = SQLiteBackend(path)
    try:
        assert second.get("alpha") == StoredValue(value={"count": 1}, expires_at=None)
    finally:
        second.close()


def test_memory_backend_set_if_absent_is_atomic_for_existing_key() -> None:
    backend = MemoryBackend()

    assert backend.set_if_absent("alpha", {"count": 1}) is True
    assert backend.set_if_absent("alpha", {"count": 2}) is False
    assert backend.get("alpha") == StoredValue(value={"count": 1}, expires_at=None)


def test_sqlite_backend_set_if_absent_persists_first_value(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path / "state.db")
    try:
        assert backend.set_if_absent("alpha", {"count": 1}) is True
        assert backend.set_if_absent("alpha", {"count": 2}) is False
        assert backend.get("alpha") == StoredValue(value={"count": 1}, expires_at=None)
    finally:
        backend.close()


def test_ttl_key_value_store_namespaces_keys() -> None:
    backend = MemoryBackend()
    store = TTLKeyValueStore(backend=backend, namespace="http", default_ttl_s=60)

    store.set("request", {"status": 200})

    assert store.get("request") == {"status": 200}
    assert backend.get("http:request") is not None


def test_membership_key_store_uses_namespaced_set_if_absent() -> None:
    backend = MemoryBackend()
    store = MembershipKeyStore(backend=backend, namespace="dedup", default_ttl_s=60)

    assert store.mark_if_new("a") is True
    assert store.mark_if_new("a") is False
    assert store.contains("a") is True
    assert backend.get("dedup:a") is not None


def test_state_backend_registry_exposes_memory_and_sqlite() -> None:
    assert state_backend_registry.has("memory")
    assert state_backend_registry.has("sqlite")
