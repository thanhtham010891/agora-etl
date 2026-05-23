"""
agora/core/writer.py
====================
``Writer`` protocol — unified write interface for pipeline output.

Replaces the ``isinstance(self._writer, SinkFanOut)`` check in
``BoundPipeline.run()`` with a polymorphic ``Writer.write()`` call
that returns a ``WriteResult``.

Both ``SinkFanOut`` and ``SinkRouter`` will implement this protocol
(Phase 2), eliminating the OCP/LSP violation.

Design notes
------------
- ``WriteResult`` is a frozen dataclass — immutable, hashable, cheap.
- ``Writer`` is a Protocol — ``SinkFanOut`` and ``SinkRouter`` satisfy
  it structurally without needing to inherit from it.
- ``errors`` in ``WriteResult`` collects per-sink exceptions without
  stopping writes to other sinks (fan-out semantics preserved).

Usage::

    result = await writer.write(record)
    if not result.written:
        metrics.records_dropped += 1
    elif result.errors:
        metrics.records_errored += len(result.errors)
    else:
        metrics.records_written += 1
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, TypeVar, runtime_checkable

T = TypeVar("T")


# ======================================================================
# WriteResult — immutable outcome of a write operation
# ======================================================================


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Outcome of a ``Writer.write()`` call.

    Attributes
    ----------
    written:
        ``True`` if the record was accepted by at least one sink.
    errors:
        List of exceptions raised by individual sinks during fan-out.
        Empty on full success or when no sinks matched (router case).
    """

    written: bool = True
    errors: list[Exception] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """``True`` if the record was written without any errors."""
        return self.written and not self.errors

    @property
    def partial(self) -> bool:
        """``True`` if written but with some sink errors (fan-out)."""
        return self.written and bool(self.errors)


# ======================================================================
# Writer — structural protocol
# ======================================================================


@runtime_checkable
class Writer(Protocol[T]):
    """Unified write interface for pipeline sinks.

    Implementations:
    - ``SinkFanOut``  — writes to ALL sinks, collects errors
    - ``SinkRouter``  — writes to FIRST matching sink

    ``BoundPipeline`` depends on this protocol instead of concrete classes.
    """

    async def open(self) -> None:
        """Open all sinks before first write (e.g. establish connections)."""
        ...

    async def write(self, record: T) -> WriteResult:
        """Write *record* to sink(s).  Return outcome."""
        ...

    async def write_batch(self, records: list[T]) -> list[WriteResult]:
        """Write *records* to sink(s). Return per-record outcomes."""
        ...

    async def flush(self) -> None:
        """Flush any buffered data across all sinks."""
        ...

    async def close(self) -> None:
        """Flush and release all resources."""
        ...
