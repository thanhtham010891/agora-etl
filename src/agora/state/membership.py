"""Reusable exact-membership stores built on top of state backends."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agora.state.backend import StateBackend


@dataclass
class MembershipKeyStore:
    """Namespaced exact-membership store with optional TTL semantics."""

    backend: StateBackend
    namespace: str
    default_ttl_s: int | None = None

    def contains(self, key: str) -> bool:
        return self.backend.get(self._full_key(key)) is not None

    def add(self, key: str, *, ttl_s: int | None = None) -> None:
        expires_at = self._expires_at(ttl_s)
        self.backend.set(self._full_key(key), 1, expires_at=expires_at)

    def mark_if_new(self, key: str, *, ttl_s: int | None = None) -> bool:
        expires_at = self._expires_at(ttl_s)
        return self.backend.set_if_absent(self._full_key(key), 1, expires_at=expires_at)

    def delete(self, key: str) -> None:
        self.backend.delete(self._full_key(key))

    def count(self) -> int:
        return self.backend.count_prefix(self._prefix())

    def clear(self) -> int:
        return self.backend.delete_prefix(self._prefix())

    def close(self) -> None:
        self.backend.close()

    def _expires_at(self, ttl_s: int | None) -> float | None:
        ttl = self.default_ttl_s if ttl_s is None else ttl_s
        if ttl is None:
            return None
        return time.time() + ttl

    def _full_key(self, key: str) -> str:
        return f"{self._prefix()}{key}"

    def _prefix(self) -> str:
        if not self.namespace:
            return ""
        return f"{self.namespace}:"
