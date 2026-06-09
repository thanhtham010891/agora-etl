"""Rust extension integration helpers for buffered runtime paths."""

from __future__ import annotations

try:
    from agora_rs import LinearBatchBuffer as _LinearBatchBuffer

    try:
        _test = _LinearBatchBuffer(1, 1)
        del _test
        RUST_AVAILABLE = True
    except Exception:
        RUST_AVAILABLE = False
except ImportError:
    RUST_AVAILABLE = False

    class _LinearBatchBuffer:  # type: ignore[no-redef]
        """Placeholder — agora-rs not installed. Allows monkeypatching in tests."""

        def __init__(self, batch_size: int, metrics_flush_interval: int) -> None:
            raise ImportError("agora-etl-rs is not installed.")


LinearBatchBuffer = _LinearBatchBuffer
