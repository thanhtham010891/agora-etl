"""Storage backends shared by checkpoint, cache, and future stateful runtime features."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

StateValue = dict[str, Any] | list[Any] | str | int | float | bool | None


@dataclass(frozen=True)
class StoredValue:
    """A stored value plus optional expiration metadata."""

    value: StateValue
    expires_at: float | None = None


class StateBackend(ABC):
    """Synchronous backend contract for persisted runtime state."""

    backend_name: str = "state"

    @abstractmethod
    def get(self, key: str) -> StoredValue | None:
        """Return the stored value for *key*, or None if missing/expired."""

    @abstractmethod
    def set(self, key: str, value: StateValue, *, expires_at: float | None = None) -> None:
        """Store *value* under *key*."""

    @abstractmethod
    def set_if_absent(
        self,
        key: str,
        value: StateValue,
        *,
        expires_at: float | None = None,
    ) -> bool:
        """Store *value* under *key* only if *key* is currently absent.

        Returns:
            True if the key was newly stored.
            False if the key already existed.
        """

    @abstractmethod
    def delete(self, key: str) -> None:
        """Remove *key* if present."""

    @abstractmethod
    def count_prefix(self, prefix: str) -> int:
        """Return the number of keys that start with *prefix*."""

    @abstractmethod
    def delete_prefix(self, prefix: str) -> int:
        """Delete all keys that start with *prefix* and return the count."""

    def close(self) -> None:
        """Release any held resources."""
        return


class MemoryBackend(StateBackend):
    """Single-process in-memory state backend."""

    backend_name = "memory"

    def __init__(self) -> None:
        self._values: dict[str, StoredValue] = {}

    def get(self, key: str) -> StoredValue | None:
        entry = self._values.get(key)
        if entry is None:
            return None
        if entry.expires_at is not None and time.time() >= entry.expires_at:
            self._values.pop(key, None)
            return None
        return entry

    def set(self, key: str, value: StateValue, *, expires_at: float | None = None) -> None:
        self._values[key] = StoredValue(value=value, expires_at=expires_at)

    def set_if_absent(
        self,
        key: str,
        value: StateValue,
        *,
        expires_at: float | None = None,
    ) -> bool:
        entry = self.get(key)
        if entry is not None:
            return False
        self._values[key] = StoredValue(value=value, expires_at=expires_at)
        return True

    def delete(self, key: str) -> None:
        self._values.pop(key, None)

    def count_prefix(self, prefix: str) -> int:
        return sum(1 for key in self._values if key.startswith(prefix))

    def delete_prefix(self, prefix: str) -> int:
        keys = [key for key in self._values if key.startswith(prefix)]
        for key in keys:
            self._values.pop(key, None)
        return len(keys)


class SQLiteBackend(StateBackend):
    """SQLite-backed state backend with optional per-key expiry."""

    backend_name = "sqlite"

    def __init__(self, path: str | Path = ".agora_state.db") -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.RLock()

    def get(self, key: str) -> StoredValue | None:
        with self._lock:
            conn = self._raw_conn()
            row = conn.execute(
                "SELECT value, expires_at FROM state_store WHERE key = ?",
                (key,),
            ).fetchone()
            if row is None:
                return None
            raw_value, expires_at = row
            if expires_at is not None and time.time() >= expires_at:
                conn.execute("DELETE FROM state_store WHERE key = ?", (key,))
                conn.commit()
                return None
            return StoredValue(value=json.loads(raw_value), expires_at=expires_at)

    def set(self, key: str, value: StateValue, *, expires_at: float | None = None) -> None:
        with self._lock:
            conn = self._raw_conn()
            conn.execute(
                """
                INSERT OR REPLACE INTO state_store (key, value, expires_at)
                VALUES (?, ?, ?)
                """,
                (key, json.dumps(value, ensure_ascii=False), expires_at),
            )
            conn.commit()

    def set_if_absent(
        self,
        key: str,
        value: StateValue,
        *,
        expires_at: float | None = None,
    ) -> bool:
        with self._lock:
            conn = self._raw_conn()
            now = time.time()
            with conn:
                conn.execute(
                    """
                    DELETE FROM state_store
                    WHERE key = ?
                      AND expires_at IS NOT NULL
                      AND expires_at <= ?
                    """,
                    (key, now),
                )
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO state_store (key, value, expires_at)
                    VALUES (?, ?, ?)
                    """,
                    (key, json.dumps(value, ensure_ascii=False), expires_at),
                )
            return cur.rowcount > 0

    def delete(self, key: str) -> None:
        with self._lock:
            conn = self._raw_conn()
            conn.execute("DELETE FROM state_store WHERE key = ?", (key,))
            conn.commit()

    def count_prefix(self, prefix: str) -> int:
        # Escape LIKE special chars so prefix matches are exact
        escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        with self._lock:
            row = (
                self._raw_conn()
                .execute(
                    "SELECT COUNT(*) FROM state_store WHERE key LIKE ? ESCAPE '\\'",
                    (f"{escaped}%",),
                )
                .fetchone()
            )
        return int(row[0]) if row is not None else 0

    def delete_prefix(self, prefix: str) -> int:
        # Escape LIKE special chars so prefix matches are exact
        escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        with self._lock:
            conn = self._raw_conn()
            cur = conn.execute(
                "DELETE FROM state_store WHERE key LIKE ? ESCAPE '\\'",
                (f"{escaped}%",),
            )
            conn.commit()
        return cur.rowcount

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def _raw_conn(self) -> sqlite3.Connection:
        # Must be called under self._lock to avoid a race on first connection.
        if self._conn is None:
            self._conn = sqlite3.connect(str(self._path), check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA cache_size=-32000")
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS state_store (
                    key        TEXT PRIMARY KEY,
                    value      TEXT NOT NULL,
                    expires_at REAL
                );
                CREATE INDEX IF NOT EXISTS idx_state_store_expires
                    ON state_store(expires_at);
                """
            )
            self._conn.commit()
        return self._conn
