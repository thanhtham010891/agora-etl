"""
agora/core/sink.py
==================
Abstract async sink — the exit point of every agora pipeline.

A sink receives normalized records and persists them to external storage.
The pipeline fans-out to ALL registered sinks (every sink sees every record).

Built-in sinks live in ``agora.sinks.*``.  Custom sinks subclass ``BaseSink``.

Implementing a custom sink
---------------------------
1. Subclass ``BaseSink[T]``.
2. Implement ``write(record)`` — or ``write_batch(records)`` for bulk ops.
3. Override ``flush()`` / ``close()`` if buffering is involved.

Example::

    class MyS3Sink(BaseSink[Place]):
        async def write(self, record: Place) -> None:
            await self._client.put_object(Body=record.json(), ...)
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, runtime_checkable

from agora.core.writer import WriteResult

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from types import TracebackType

T = TypeVar("T")


@runtime_checkable
class ContextBindable(Protocol):
    """Capability protocol for sinks/writers that accept run-scoped context."""

    def bind_context(self, ctx: Any) -> None:
        """Attach run-scoped context."""
        ...


@runtime_checkable
class BatchWritable(Protocol[T]):
    """Capability protocol for sinks that support explicit batch writes."""

    async def write_batch(self, records: list[T]) -> None:
        """Persist a batch of records."""
        ...


@dataclass(frozen=True, slots=True)
class SinkCapabilities:
    """Execution hints advertised by sinks to the runtime/writer."""

    batch_writable_native: bool = False
    parallel_writes_safe: bool = False
    ordered_writes_required: bool = True


def bind_context_if_supported(target: object, ctx: Any) -> None:
    """Bind context when the target advertises that capability."""
    if isinstance(target, ContextBindable):
        target.bind_context(ctx)


def sink_capabilities(target: object) -> SinkCapabilities:
    """Return execution capabilities for *target*.

    Explicit ``sink_capabilities()`` implementations take precedence. For
    ``BaseSink`` subclasses, class attributes define the contract. For
    duck-typed sinks, native batch support falls back to ``write_batch()``
    presence so existing tests and simple sinks keep working.
    """
    advertised = getattr(target, "sink_capabilities", None)
    if callable(advertised):
        value = advertised()
        if isinstance(value, SinkCapabilities):
            return value
        raise TypeError("sink_capabilities() must return SinkCapabilities")

    batch_native = False
    parallel_safe = False
    ordered_required = True

    if isinstance(target, BaseSink):
        batch_native = bool(getattr(target, "batch_writable_native", False))
        parallel_safe = bool(getattr(target, "parallel_writes_safe", False))
        ordered_required = bool(getattr(target, "ordered_writes_required", True))
        if not batch_native and type(target).write_batch is not BaseSink.write_batch:
            batch_native = True
    elif isinstance(target, BatchWritable):
        batch_native = True

    return SinkCapabilities(
        batch_writable_native=batch_native,
        parallel_writes_safe=parallel_safe,
        ordered_writes_required=ordered_required,
    )


# ======================================================================
# BaseSink
# ======================================================================


class BaseSink(ABC, Generic[T]):
    """Abstract async sink.

    Subclasses must implement ``write()``.

    ``write_batch()`` has a default loop implementation; batched sinks
    should override it for better performance.

    ``flush()`` / ``close()`` are no-ops by default — override for
    buffered sinks (e.g. PostgresSink accumulates rows in memory).
    """

    # Subclasses must set a meaningful name (shown in logs and metrics).
    sink_name: str = "sink"
    batch_writable_native: bool = False
    parallel_writes_safe: bool = False
    ordered_writes_required: bool = True

    @abstractmethod
    async def write(self, record: T) -> None:
        """Persist a single record."""

    async def open(self) -> None:
        """Called once before the pipeline loop starts.  Override to set up connections."""

    def bind_context(self, ctx: Any) -> None:
        """Attach run-scoped context when a sink needs pipeline metadata."""

    def sink_capabilities(self) -> SinkCapabilities:
        """Execution hints used by writer/runtime strategy selection."""
        batch_writable_native = self.batch_writable_native
        if not batch_writable_native and type(self).write_batch is not BaseSink.write_batch:
            batch_writable_native = True
        return SinkCapabilities(
            batch_writable_native=batch_writable_native,
            parallel_writes_safe=self.parallel_writes_safe,
            ordered_writes_required=self.ordered_writes_required,
        )

    async def write_batch(self, records: list[T]) -> None:
        """Persist a batch of records.

        Default: calls ``write()`` for each record.
        Override for batch-optimized sinks (e.g. bulk INSERT).
        """
        for record in records:
            await self.write(record)

    async def flush(self) -> None:
        """Flush any buffered data.  No-op default for non-buffered sinks."""

    async def close(self) -> None:
        """Flush and release all resources."""
        await self.flush()

    # ------------------------------------------------------------------ #
    # Async context manager                                                #
    # ------------------------------------------------------------------ #

    async def __aenter__(self) -> BaseSink[T]:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()


# ======================================================================
# SinkFanOut — write to ALL sinks (default pipeline behaviour)
# ======================================================================


class SinkFanOut(Generic[T]):
    """Writes each record to ALL registered sinks.

    This is the default strategy used by ``Pipeline.fan_out()``.
    Errors in one sink do NOT prevent writes to the others.

    Satisfies the ``Writer`` protocol.
    """

    def __init__(
        self,
        sinks: list[BaseSink[T]],
        *,
        concurrent_writes: bool = False,
        max_concurrency: int | None = None,
    ) -> None:
        if not sinks:
            raise ValueError("SinkFanOut requires at least one sink")
        self._sinks = sinks
        self._concurrent_writes = concurrent_writes
        self._max_concurrency = max_concurrency
        # Cache per-sink capabilities — never changes after construction.
        self._sink_capabilities = [sink_capabilities(s) for s in sinks]
        self._sink_batch_writable = [
            cap.batch_writable_native and isinstance(s, BatchWritable)
            for s, cap in zip(sinks, self._sink_capabilities, strict=True)
        ]

    def with_concurrency(self, max_concurrency: int | None = None) -> SinkFanOut[T]:
        """Return a copy with opt-in concurrent sink writes enabled."""
        return SinkFanOut(
            list(self._sinks),
            concurrent_writes=True,
            max_concurrency=max_concurrency,
        )

    async def _run_sink_calls(
        self,
        calls: list[tuple[BaseSink[T], Awaitable[object]]],
    ) -> list[object]:
        """Execute sink coroutines, optionally limiting fan-out concurrency."""
        if self._max_concurrency == 1:
            results: list[object] = []
            for _, call in calls:
                try:
                    results.append(await call)
                except Exception as exc:
                    results.append(exc)
            return results

        semaphore = None
        if self._max_concurrency is not None and self._max_concurrency > 1:
            semaphore = asyncio.Semaphore(self._max_concurrency)

        async def _execute(call):
            if semaphore is None:
                return await call
            async with semaphore:
                return await call

        tasks = [asyncio.create_task(_execute(call)) for _, call in calls]
        return await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    def _raise_first_exception(results: list[object]) -> None:
        for result in results:
            if isinstance(result, Exception):
                raise result

    async def _write_batch_to_sink(
        self,
        sink: BaseSink[T],
        records: list[T],
        *,
        batch_writable: bool,
        capabilities: SinkCapabilities,
    ) -> object:
        if batch_writable:
            await sink.write_batch(records)  # type: ignore[attr-defined]
            return None

        if (
            self._concurrent_writes
            and capabilities.parallel_writes_safe
            and not capabilities.ordered_writes_required
        ):

            async def _write_one(index: int, record: T) -> tuple[int, Exception | None]:
                try:
                    await sink.write(record)
                except Exception as exc:
                    return index, exc
                return index, None

            per_record_errors: list[Exception | None] = [None] * len(records)
            results = await asyncio.gather(
                *(_write_one(index, record) for index, record in enumerate(records))
            )
            for index, error in results:
                per_record_errors[index] = error
            return per_record_errors

        per_record_errors: list[Exception | None] = [None] * len(records)
        for index, record in enumerate(records):
            try:
                await sink.write(record)
            except Exception as exc:
                per_record_errors[index] = exc
        return per_record_errors

    async def open(self) -> None:
        """Open all sinks (called before first write)."""
        if not self._concurrent_writes:
            for sink in self._sinks:
                await sink.open()
            return

        results = await self._run_sink_calls([(sink, sink.open()) for sink in self._sinks])
        self._raise_first_exception(results)

    async def write(self, record: T) -> WriteResult:
        """Write *record* to all sinks.  Returns ``WriteResult``."""
        errors: list[Exception] = []
        successful_writes = 0
        if not self._concurrent_writes:
            for sink in self._sinks:
                try:
                    await sink.write(record)
                    successful_writes += 1
                except Exception as exc:
                    errors.append(exc)
            return WriteResult(written=successful_writes > 0, errors=errors)

        results = await self._run_sink_calls([(sink, sink.write(record)) for sink in self._sinks])
        for result in results:
            if isinstance(result, Exception):
                errors.append(result)
            else:
                successful_writes += 1
        return WriteResult(written=successful_writes > 0, errors=errors)

    async def write_batch(self, records: list[T]) -> list[WriteResult]:
        """Write *records* to all sinks, returning per-record outcomes."""
        if not records:
            return []

        written_flags = [False] * len(records)
        errors_by_record: list[list[Exception]] = [[] for _ in records]
        sink_calls: list[tuple[BaseSink[T], Awaitable[object]]] = []
        for sink, cap, bw in zip(
            self._sinks, self._sink_capabilities, self._sink_batch_writable, strict=True
        ):
            sink_calls.append(
                (
                    sink,
                    self._write_batch_to_sink(sink, records, batch_writable=bw, capabilities=cap),
                )
            )

        if not self._concurrent_writes:
            results: list[object] = []
            for _, call in sink_calls:
                try:
                    results.append(await call)
                except Exception as exc:
                    results.append(exc)
        else:
            results = await self._run_sink_calls(sink_calls)

        for result in results:
            if isinstance(result, Exception):
                for errors in errors_by_record:
                    errors.append(result)
            elif isinstance(result, list):
                for index, error in enumerate(result):
                    if error is not None:
                        errors_by_record[index].append(error)
                    else:
                        written_flags[index] = True
            else:
                for index in range(len(records)):
                    written_flags[index] = True
        return [
            WriteResult(written=written_flags[index], errors=errors_by_record[index])
            for index in range(len(records))
        ]

    async def flush(self) -> None:
        """Flush all sinks."""
        if not self._concurrent_writes:
            for sink in self._sinks:
                await sink.flush()
            return

        results = await self._run_sink_calls([(sink, sink.flush()) for sink in self._sinks])
        self._raise_first_exception(results)

    async def close(self) -> None:
        """Close all sinks (flush + release resources)."""
        if not self._concurrent_writes:
            for sink in self._sinks:
                await sink.close()
            return

        results = await self._run_sink_calls([(sink, sink.close()) for sink in self._sinks])
        self._raise_first_exception(results)

    # Backward-compatible aliases
    async def flush_all(self) -> None:
        await self.flush()

    async def close_all(self) -> None:
        await self.close()

    def __iter__(self):
        return iter(self._sinks)

    def bind_context(self, ctx: Any) -> None:
        for sink in self._sinks:
            bind_context_if_supported(sink, ctx)


# ======================================================================
# SinkRouter — conditional routing to specific sinks
# ======================================================================


class SinkRoute(Generic[T]):
    """A predicate + sink pair."""

    def __init__(self, predicate, sink: BaseSink[T]) -> None:
        self.predicate = predicate
        self.sink = sink


class SinkRouter(Generic[T]):
    """Route each record to the FIRST matching sink.

    Satisfies the ``Writer`` protocol.

    Usage::

        router = (
            SinkRouter()
            .route(lambda r: r.source == "source_a", sink_a)
            .route(lambda r: r.source == "source_b", sink_b)
            .default(fallback_sink)
        )
    """

    def __init__(self) -> None:
        self._routes: list[SinkRoute[T]] = []
        self._default: BaseSink[T] | None = None

    def route(self, predicate, sink: BaseSink[T]) -> SinkRouter[T]:
        """Add a conditional route."""
        self._routes.append(SinkRoute(predicate, sink))
        return self

    def default(self, sink: BaseSink[T]) -> SinkRouter[T]:
        """Set the fallback sink for unmatched records."""
        self._default = sink
        return self

    async def open(self) -> None:
        """Open all routed sinks."""
        for route in self._routes:
            await route.sink.open()
        if self._default is not None:
            await self._default.open()

    async def write(self, record: T) -> WriteResult:
        """Route *record* to first matching sink.  Returns ``WriteResult``."""
        for route in self._routes:
            if route.predicate(record):
                try:
                    await route.sink.write(record)
                    return WriteResult(written=True)
                except Exception as exc:
                    return WriteResult(written=False, errors=[exc])
        if self._default is not None:
            try:
                await self._default.write(record)
                return WriteResult(written=True)
            except Exception as exc:
                return WriteResult(written=False, errors=[exc])
        return WriteResult(written=False)

    async def write_batch(self, records: list[T]) -> list[WriteResult]:
        """Route *records* and write them in sink-local batches."""
        if not records:
            return []

        written_flags = [False] * len(records)
        errors_by_record: list[list[Exception]] = [[] for _ in records]
        grouped: dict[int, tuple[BaseSink[T], list[tuple[int, T]]]] = {}

        for index, record in enumerate(records):
            target: BaseSink[T] | None = None
            for route in self._routes:
                if route.predicate(record):
                    target = route.sink
                    break
            if target is None:
                target = self._default
            if target is None:
                continue

            # Do NOT set written_flags[index] here — only set it after a
            # confirmed successful write to avoid counting failed writes.
            target_id = id(target)
            if target_id not in grouped:
                grouped[target_id] = (target, [])
            grouped[target_id][1].append((index, record))

        for sink, entries in grouped.values():
            capabilities = sink_capabilities(sink)
            if capabilities.batch_writable_native and isinstance(sink, BatchWritable):
                try:
                    await sink.write_batch([record for _, record in entries])
                    for index, _ in entries:
                        written_flags[index] = True
                except Exception as exc:
                    for index, _ in entries:
                        errors_by_record[index].append(exc)
                continue

            if capabilities.parallel_writes_safe and not capabilities.ordered_writes_required:

                async def _write_one(
                    index: int,
                    record: T,
                    *,
                    sink_instance: BaseSink[T] = sink,
                ) -> tuple[int, Exception | None]:
                    try:
                        await sink_instance.write(record)
                    except Exception as exc:
                        return index, exc
                    return index, None

                results = await asyncio.gather(
                    *(_write_one(index, record) for index, record in entries)
                )
                for index, error in results:
                    if error is not None:
                        errors_by_record[index].append(error)
                    else:
                        written_flags[index] = True
                continue

            for index, record in entries:
                try:
                    await sink.write(record)
                    written_flags[index] = True
                except Exception as exc:
                    errors_by_record[index].append(exc)

        return [
            WriteResult(written=written_flags[index], errors=errors_by_record[index])
            for index in range(len(records))
        ]

    async def flush(self) -> None:
        """Flush all routed sinks."""
        for route in self._routes:
            await route.sink.flush()
        if self._default is not None:
            await self._default.flush()

    async def close(self) -> None:
        """Close all routed sinks."""
        for route in self._routes:
            await route.sink.close()
        if self._default is not None:
            await self._default.close()

    # Backward-compatible alias
    async def close_all(self) -> None:
        await self.close()

    def bind_context(self, ctx: Any) -> None:
        for route in self._routes:
            bind_context_if_supported(route.sink, ctx)
        if self._default is not None:
            bind_context_if_supported(self._default, ctx)
