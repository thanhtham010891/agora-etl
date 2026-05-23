"""Small reusable key-value stores built on top of state backends."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agora.state.backend import StateBackend, StateValue


@dataclass
class TTLKeyValueStore:
    """Namespaced key-value store with optional TTL semantics."""

    backend: StateBackend
    namespace: str
    default_ttl_s: int | None = None

    def get(self, key: str) -> StateValue:
        entry = self.backend.get(self._full_key(key))
        if entry is None:
            return None
        return entry.value

    def set(self, key: str, value: StateValue, *, ttl_s: int | None = None) -> None:
        ttl = self.default_ttl_s if ttl_s is None else ttl_s
        expires_at = None if ttl is None else time.time() + ttl
        self.backend.set(self._full_key(key), value, expires_at=expires_at)

    def delete(self, key: str) -> None:
        self.backend.delete(self._full_key(key))

    def count(self) -> int:
        return self.backend.count_prefix(self._prefix())

    def clear(self) -> int:
        return self.backend.delete_prefix(self._prefix())

    def close(self) -> None:
        self.backend.close()

    def _full_key(self, key: str) -> str:
        return f"{self._prefix()}{key}"

    def _prefix(self) -> str:
        return f"{self.namespace}:"
