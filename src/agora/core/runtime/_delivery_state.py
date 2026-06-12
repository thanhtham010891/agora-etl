"""Checkpoint-state helpers for runtime delivery."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from agora.core.acceleration import make_checkpoint_state as _make_accelerated_checkpoint_state

if TYPE_CHECKING:
    from agora.core.acceleration import AccelerationMode
    from agora.core.checkpoint import CheckpointValue


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


def make_checkpoint_state(mode: AccelerationMode | str = "auto") -> Any:
    """Return a Rust-backed ``CheckpointState`` when available, Python otherwise."""
    return _make_accelerated_checkpoint_state(CheckpointState, mode=mode)
