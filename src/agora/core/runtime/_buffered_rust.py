"""Rust extension integration helpers for buffered runtime paths."""

from __future__ import annotations

from agora.core.acceleration import acceleration_available, linear_batch_buffer_class

RUST_AVAILABLE = acceleration_available()
LinearBatchBuffer = linear_batch_buffer_class()
