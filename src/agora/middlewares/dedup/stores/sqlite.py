"""
agora/dedup/stores/sqlite.py
============================
SQLite-backed exact dedup store built on the shared ``SQLiteBackend``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from agora.middlewares.dedup.stores.backend import BackendDedupStore
from agora.state import MembershipKeyStore, SQLiteBackend

if TYPE_CHECKING:
    from pathlib import Path


class SQLiteDedupStore(BackendDedupStore):
    """Durable exact dedup store for single-node workers.

    Parameters
    ----------
    path:
        Path to the SQLite state database file.
    namespace:
        Namespace prefix for dedup keys in the shared state store.
    ttl_seconds:
        Optional TTL for dedup entries.
    """

    def __init__(
        self,
        path: str | Path = ".agora_dedup.db",
        *,
        namespace: str = "dedup",
        ttl_seconds: int | None = None,
    ) -> None:
        backend = SQLiteBackend(path)
        super().__init__(
            MembershipKeyStore(backend=backend, namespace=namespace),
            default_ttl_seconds=ttl_seconds,
        )
