"""
agora/core/runtime/_process_codec.py
====================================
Internal batch codec abstractions for process-isolated batch execution.

These codecs are not part of the public API. They let the process runner work
with more than one batch representation without changing the meaning of the
existing ProcessBatchMiddleware contract.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast


class BatchCodecError(Exception):
    """Raised when a batch cannot be encoded, decoded, or validated."""


class BatchCodec(Protocol):
    """Internal codec interface for process-batch transport."""

    @property
    def name(self) -> str:
        """Human-readable codec name for logs and diagnostics."""

    def batch_size(self, batch: Any) -> int:
        """Return the number of logical rows in *batch*."""

    def encode_for_worker(self, batch: Any) -> Any:
        """Prepare *batch* for worker-process execution."""

    def decode_in_worker(self, payload: Any) -> Any:
        """Decode worker input payload back into the user-facing batch object."""

    def encode_from_worker(self, batch: Any) -> Any:
        """Prepare the user function result for transport back to the main process."""

    def decode_from_worker(self, payload: Any, *, expected_rows: int) -> Any:
        """Decode and validate the worker result in the main process."""


def _coerce_sequence(value: Any, *, label: str) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, (str, bytes, bytearray)):
        raise BatchCodecError(
            f"{label} returned non-sequence result of type {type(value).__name__}; "
            "expected a list or sequence."
        )
    if isinstance(value, Sequence):
        return list(value)
    raise BatchCodecError(
        f"{label} returned non-sequence result of type {type(value).__name__}; "
        "expected a list or sequence."
    )


@dataclass(frozen=True)
class PythonObjectCodec:
    """Stable compatibility codec for Python-object list batches."""

    name: str = "python_object"

    def batch_size(self, batch: Any) -> int:
        self._validate_input(batch)
        return len(batch)

    def encode_for_worker(self, batch: Any) -> Any:
        self._validate_input(batch)
        return batch

    def decode_in_worker(self, payload: Any) -> list[Any]:
        self._validate_input(payload)
        return cast("list[Any]", payload)

    def encode_from_worker(self, batch: Any) -> list[Any]:
        return _coerce_sequence(batch, label="worker fn")

    def decode_from_worker(self, payload: Any, *, expected_rows: int) -> list[Any]:
        result = _coerce_sequence(payload, label="worker fn")
        if len(result) != expected_rows:
            raise BatchCodecError(
                f"fn returned {len(result)} results for {expected_rows} inputs — lengths must match."
            )
        return result

    def _validate_input(self, batch: Any) -> None:
        if not isinstance(batch, list):
            raise BatchCodecError(f"expected batch input to be list, got {type(batch).__name__}.")


@dataclass(frozen=True)
class ArrowBatchCodec:
    """Internal Arrow-native codec using Arrow IPC bytes across the worker boundary."""

    name: str = "arrow_ipc"
    preserve_row_count: bool = True

    def batch_size(self, batch: Any) -> int:
        record_batch = self._validate_input(batch)
        return int(record_batch.num_rows)

    def encode_for_worker(self, batch: Any) -> bytes:
        record_batch = self._validate_input(batch)
        return self._record_batch_to_ipc_bytes(record_batch)

    def decode_in_worker(self, payload: Any) -> Any:
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise BatchCodecError(
                f"expected Arrow worker payload to be bytes-like, got {type(payload).__name__}."
            )
        return self._ipc_bytes_to_record_batch(bytes(payload))

    def encode_from_worker(self, batch: Any) -> bytes:
        record_batch = self._validate_input(batch)
        return self._record_batch_to_ipc_bytes(record_batch)

    def decode_from_worker(self, payload: Any, *, expected_rows: int) -> Any:
        record_batch = self.decode_in_worker(payload)
        if self.preserve_row_count and record_batch.num_rows != expected_rows:
            raise BatchCodecError(
                f"fn returned {record_batch.num_rows} rows for {expected_rows} input rows — "
                "row count must match for Arrow process batches."
            )
        return record_batch

    def _validate_input(self, batch: Any) -> Any:
        try:
            import pyarrow as pa
        except ImportError as exc:  # pragma: no cover - exercised only without optional dep
            raise ImportError(
                "Arrow process batch execution requires pyarrow. Install via: "
                "pip install 'agora-etl[file]'"
            ) from exc

        if not isinstance(batch, pa.RecordBatch):
            raise BatchCodecError(
                f"expected batch input to be pyarrow.RecordBatch, got {type(batch).__name__}."
            )
        return batch

    def _record_batch_to_ipc_bytes(self, batch: Any) -> bytes:
        import pyarrow as pa
        import pyarrow.ipc as ipc

        ipc_module = cast("Any", ipc)
        sink = pa.BufferOutputStream()
        with ipc_module.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)
        return cast("bytes", sink.getvalue().to_pybytes())

    def _ipc_bytes_to_record_batch(self, payload: bytes) -> Any:
        import pyarrow.ipc as ipc

        ipc_module = cast("Any", ipc)
        with ipc_module.open_stream(payload) as reader:
            return reader.read_next_batch()


__all__ = ["ArrowBatchCodec", "BatchCodec", "BatchCodecError", "PythonObjectCodec"]
