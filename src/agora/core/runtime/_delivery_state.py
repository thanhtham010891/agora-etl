"""Checkpoint-state helpers for runtime delivery."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agora.core.checkpoint import CheckpointValue

try:
    from agora_rs import CheckpointState as _RustCheckpointState

    try:
        _test = _RustCheckpointState()
        del _test
        _RUST_CHECKPOINT = True
    except Exception:
        _RUST_CHECKPOINT = False
except ImportError:
    _RUST_CHECKPOINT = False


@dataclass
class CheckpointState:
    """Encapsulates mutable checkpoint state during pipeline execution."""

    processed_count: int = 0
    last_saved_value: CheckpointValue = None

    def increment(self) -> None:
        self.processed_count += 1

    def increment_by(self, count: int) -> None:
        self.processed_count += max(0, count)

    def should_save(self, current_value: CheckpointValue, every: int) -> bool:
        if current_value is None or current_value == self.last_saved_value:
            return False
        return self.processed_count % every == 0

    def mark_saved(self, value: CheckpointValue) -> None:
        self.last_saved_value = value


def make_checkpoint_state() -> Any:
    """Return a Rust-backed ``CheckpointState`` when available, Python otherwise."""
    if _RUST_CHECKPOINT:
        return _RustCheckpointState()
    return CheckpointState()
