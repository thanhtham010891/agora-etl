"""
agora/sources/cache.py
=======================
``HttpCache`` — SQLite-backed HTTP response cache for ``HTTPSource``.

Prevents redundant API calls by caching responses keyed on request semantics.
Works transparently inside ``HTTPSource`` when ``cache=True`` is set.

Features
--------
  - TTL-based expiry (default: 7 days)
  - Per-source scoped eviction
  - Bulk write context manager (single commit per batch)
  - KV store for arbitrary pipeline state

Usage (standalone)::

    cache = HttpCache(db_path=Path(".cache/http.db"), ttl=3600)
    cached = cache.get("https://api.example.com/search", params={"q": "hanoi"})
    if cached is None:
        raw = await http_client.get(...)
        cache.set("https://api.example.com/search", raw.text, params={"q": "hanoi"})

Usage (via HTTPSource)::

    class MyExtractor(HTTPSource):
        def __init__(self):
            super().__init__(..., cache_ttl_seconds=86400)

Async note
----------
All cache methods are **synchronous** (SQLite has no async driver).
Use ``asyncio.to_thread(cache.get, url, params)`` in async contexts, or
call from a sync helper — see ``HTTPSource._cached_get()``.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import logstruct

from agora.state import SQLiteBackend, TTLKeyValueStore

logger = logstruct.getLogger(__name__)

DEFAULT_TTL = 7 * 24 * 3600  # 7 days

_TABLES = ("http_cache", "kv_store")
_KV_NAMESPACE = "http_kv"


class HttpCache:
    """SQLite HTTP response cache.

    Parameters
    ----------
    db_path:
        Path to the SQLite database file.
        Defaults to ``.cache/agora_http.db`` relative to cwd.
    ttl:
        Default time-to-live in seconds.
    """

    def __init__(
        self,
        db_path: Path | str | None = None,
        ttl: int = DEFAULT_TTL,
    ) -> None:
        if db_path is None:
            db_path = Path(".cache") / "agora_http.db"
        self.db_path = Path(db_path)
        self.ttl = ttl
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = None
        self._lock = (
            threading.RLock()
        )  # RLock allows bulk_write to hold lock while callers re-enter
        self._in_bulk: bool = False
        self._kv_store = TTLKeyValueStore(
            backend=SQLiteBackend(self.db_path),
            namespace=_KV_NAMESPACE,
            default_ttl_s=self.ttl,
        )
        self._init_db()

    # ------------------------------------------------------------------ #
    # HTTP response cache                                                  #
    # ------------------------------------------------------------------ #

    def get(
        self,
        url: str,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        source: str = "",
    ) -> str | None:
        """Return cached response body, or None if missing/expired."""
        key = _make_key(url, params, headers, source)
        with self._lock:
            row = self._execute(
                "SELECT body, cached_at FROM http_cache WHERE key = ?", (key,)
            ).fetchone()
            if not row:
                return None
            body, cached_at = row
            if time.time() - cached_at > self.ttl:
                self._execute("DELETE FROM http_cache WHERE key = ?", (key,))
                self._commit_if_not_bulk()
                return None
            return str(body)

    def set(
        self,
        url: str,
        body: str,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        source: str = "",
    ) -> None:
        """Cache a response body."""
        key = _make_key(url, params, headers, source)
        with self._lock:
            self._execute(
                "INSERT OR REPLACE INTO http_cache (key, url, body, source, cached_at)"
                " VALUES (?, ?, ?, ?, ?)",
                (key, url, body, source, time.time()),
            )
            self._commit_if_not_bulk()

    def invalidate(
        self,
        url: str,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        source: str = "",
    ) -> None:
        """Remove a single cached entry."""
        key = _make_key(url, params, headers, source)
        with self._lock:
            self._execute("DELETE FROM http_cache WHERE key = ?", (key,))
            self._commit_if_not_bulk()

    def evict_source(self, source: str, older_than_days: int = 0) -> int:
        """Delete all entries for a specific source, optionally age-filtered."""
        with self._lock:
            if older_than_days:
                cutoff = time.time() - older_than_days * 86400
                cur = self._execute(
                    "DELETE FROM http_cache WHERE source = ? AND cached_at < ?",
                    (source, cutoff),
                )
            else:
                cur = self._execute("DELETE FROM http_cache WHERE source = ?", (source,))
            self._commit_if_not_bulk()
        return cur.rowcount

    def purge_expired(self) -> int:
        """Remove all expired entries.  Returns count deleted."""
        cutoff = time.time() - self.ttl
        with self._lock:
            cur = self._execute("DELETE FROM http_cache WHERE cached_at < ?", (cutoff,))
            self._commit_if_not_bulk()
        return cur.rowcount

    # ------------------------------------------------------------------ #
    # KV store (arbitrary pipeline state)                                  #
    # ------------------------------------------------------------------ #

    def kv_set(self, key: str, value: Any, ttl: int | None = None) -> None:
        self._kv_store.set(key, value, ttl_s=ttl)

    def kv_get(self, key: str) -> Any | None:
        return self._kv_store.get(key)

    # ------------------------------------------------------------------ #
    # Bulk write                                                           #
    # ------------------------------------------------------------------ #

    @contextmanager
    def bulk_write(self) -> Any:
        """Context manager: batch multiple writes into one commit.

        Usage::

            with cache.bulk_write():
                for url, body in responses:
                    cache.set(url, body, source="my_api")
        """
        with self._lock:
            self._in_bulk = True
            try:
                yield
                self._raw_conn().commit()
            except Exception:
                self._raw_conn().rollback()
                raise
            finally:
                self._in_bulk = False

    # ------------------------------------------------------------------ #
    # Stats / maintenance                                                  #
    # ------------------------------------------------------------------ #

    def stats(self) -> dict[str, int]:
        cutoff = time.time() - self.ttl
        with self._lock:
            count = self._execute(
                "SELECT COUNT(*) FROM http_cache WHERE cached_at >= ?",
                (cutoff,),
            ).fetchone()[0]
        return {
            "http_cache": count,
            "kv_store": self._kv_store.count(),
        }

    def reset(self) -> None:
        """Delete ALL cached data."""
        with self._lock:
            for table in _TABLES:
                assert table in _TABLES, f"Unexpected table name: {table!r}"
                self._execute(f"DELETE FROM {table}")  # table is whitelisted above
            self._raw_conn().commit()
        self._kv_store.clear()
        logger.warning("http_cache_reset")

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
        self._kv_store.close()

    def __enter__(self) -> HttpCache:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    def _raw_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA cache_size=-32000")
            self._conn.execute("PRAGMA temp_store=MEMORY")
        return self._conn

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        return self._raw_conn().execute(sql, params)

    def _commit_if_not_bulk(self) -> None:
        """Commit unless inside a ``bulk_write()`` context."""
        if not self._in_bulk:
            self._raw_conn().commit()

    def _init_db(self) -> None:
        self._raw_conn().executescript("""
            CREATE TABLE IF NOT EXISTS http_cache (
                key        TEXT PRIMARY KEY,
                url        TEXT NOT NULL,
                body       TEXT NOT NULL,
                source     TEXT NOT NULL DEFAULT '',
                cached_at  REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS kv_store (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL,
                expires_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_http_cached_at ON http_cache(cached_at);
            CREATE INDEX IF NOT EXISTS idx_http_source    ON http_cache(source, cached_at);
            CREATE INDEX IF NOT EXISTS idx_kv_expires     ON kv_store(expires_at);
        """)
        self._raw_conn().commit()


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #


def _normalize_mapping(mapping: dict[str, str] | None) -> str:
    if not mapping:
        return ""
    return json.dumps(sorted(mapping.items()), ensure_ascii=False, separators=(",", ":"))


def _make_key(
    url: str,
    params: dict[str, str] | None,
    headers: dict[str, str] | None = None,
    source: str = "",
) -> str:
    """SHA-256 cache key from source + URL + params + semantic headers."""
    raw = "||".join(
        [
            source,
            url,
            _normalize_mapping(params),
            _normalize_mapping(headers),
        ]
    )
    return hashlib.sha256(raw.encode()).hexdigest()
