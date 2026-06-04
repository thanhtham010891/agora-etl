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
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, cast, runtime_checkable

from agora.core.batch import is_arrow_native_sink
from agora.core.data_plane import DataPlane, SinkDataPlaneSpec, ordered_unique_planes
from agora.core.writer import WriteResult

# Singleton reused for every successfully written record in the fast path.
# Invariant: never mutate _WRITE_OK.errors — the empty list is shared across
# every record returned via `[_WRITE_OK] * n`, so an append would corrupt all of them.
_WRITE_OK = WriteResult(written=True, errors=[])

if TYPE_CHECKING:
    from collections.abc import Awaitable, Iterator
    from types import TracebackType

T = TypeVar("T")
_WARNED_LEGACY_SINK_TYPES: set[type[object]] = set()


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
    arrow_passthrough_native: bool = False
    parallel_writes_safe: bool = False
    ordered_writes_required: bool = True
    accepted_data_planes: tuple[DataPlane, ...] = ()
    native_data_planes: tuple[DataPlane, ...] = ()


def bind_context_if_supported(target: object, ctx: Any) -> None:
    """Bind context when the target advertises that capability."""
    if isinstance(target, ContextBindable):
        target.bind_context(ctx)


def _default_sink_data_planes(
    *,
    batch_native: bool,
    arrow_native: bool,
) -> tuple[tuple[DataPlane, ...], tuple[DataPlane, ...]]:
    accepted = [DataPlane.PYTHON_ROWS]
    native = [DataPlane.PYTHON_ROWS]
    if batch_native:
        accepted.append(DataPlane.PYTHON_BATCHES)
        native.append(DataPlane.PYTHON_BATCHES)
    if arrow_native:
        accepted.append(DataPlane.ARROW_BATCHES)
        native.append(DataPlane.ARROW_BATCHES)
    return ordered_unique_planes(accepted), ordered_unique_planes(native)


def _normalized_sink_capabilities(
    capabilities: SinkCapabilities,
    *,
    batch_native: bool,
    arrow_native: bool,
) -> SinkCapabilities:
    accepted_data_planes, native_data_planes = _default_sink_data_planes(
        batch_native=batch_native,
        arrow_native=arrow_native,
    )
    if capabilities.accepted_data_planes:
        accepted_data_planes = capabilities.accepted_data_planes
    if capabilities.native_data_planes:
        native_data_planes = capabilities.native_data_planes
    batch_native = batch_native or DataPlane.PYTHON_BATCHES in native_data_planes
    arrow_native = arrow_native or DataPlane.ARROW_BATCHES in native_data_planes
    return replace(
        capabilities,
        batch_writable_native=batch_native,
        arrow_passthrough_native=arrow_native,
        accepted_data_planes=accepted_data_planes,
        native_data_planes=native_data_planes,
    )


def _warn_legacy_sink_flags_once(target: object) -> None:
    sink_type = type(target)
    if sink_type in _WARNED_LEGACY_SINK_TYPES:
        return
    _WARNED_LEGACY_SINK_TYPES.add(sink_type)
    warnings.warn(
        f"{sink_type.__name__} uses legacy sink data-plane bool flags; "
        "advertise accepted_data_planes/native_data_planes or override "
        "sink_capabilities() with explicit data planes instead. "
        "Legacy flags remain supported in 0.3.x and are planned for removal in 0.4.0.",
        DeprecationWarning,
        stacklevel=3,
    )


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
            if (
                (value.batch_writable_native or value.arrow_passthrough_native)
                and not value.accepted_data_planes
                and not value.native_data_planes
            ):
                _warn_legacy_sink_flags_once(target)
            return _normalized_sink_capabilities(
                value,
                batch_native=bool(getattr(value, "batch_writable_native", False))
                or DataPlane.PYTHON_BATCHES in value.native_data_planes,
                arrow_native=(
                    bool(getattr(value, "arrow_passthrough_native", False))
                    or DataPlane.ARROW_BATCHES in value.native_data_planes
                    or is_arrow_native_sink(target)
                ),
            )
        raise TypeError("sink_capabilities() must return SinkCapabilities")

    batch_native = False
    arrow_native = False
    parallel_safe = False
    ordered_required = True

    if isinstance(target, BaseSink):
        accepted_planes = tuple(getattr(target, "accepted_data_planes", ()))
        native_planes = tuple(getattr(target, "native_data_planes", ()))
        batch_native = bool(getattr(target, "batch_writable_native", False))
        arrow_native = bool(getattr(target, "arrow_passthrough_native", False))
        parallel_safe = bool(getattr(target, "parallel_writes_safe", False))
        ordered_required = bool(getattr(target, "ordered_writes_required", True))
        if (batch_native or arrow_native) and not accepted_planes and not native_planes:
            _warn_legacy_sink_flags_once(target)
        if not batch_native and type(target).write_batch is not BaseSink.write_batch:
            batch_native = True
    elif isinstance(target, BatchWritable):
        batch_native = True
        arrow_native = is_arrow_native_sink(target)
    else:
        legacy_batch_native = bool(getattr(target, "batch_writable_native", False))
        legacy_arrow_native = bool(getattr(target, "arrow_passthrough_native", False))
        arrow_native = is_arrow_native_sink(target)
        if (
            (legacy_batch_native or legacy_arrow_native)
            and not getattr(target, "accepted_data_planes", ())
            and not getattr(target, "native_data_planes", ())
        ):
            _warn_legacy_sink_flags_once(target)
        batch_native = legacy_batch_native
        arrow_native = legacy_arrow_native or arrow_native

    return _normalized_sink_capabilities(
        SinkCapabilities(
            batch_writable_native=batch_native,
            arrow_passthrough_native=arrow_native,
            parallel_writes_safe=parallel_safe,
            ordered_writes_required=ordered_required,
            accepted_data_planes=tuple(getattr(target, "accepted_data_planes", ())),
            native_data_planes=tuple(getattr(target, "native_data_planes", ())),
        ),
        batch_native=batch_native,
        arrow_native=arrow_native,
    )


def sink_data_plane_spec(target: object) -> SinkDataPlaneSpec:
    """Return the sink-side data-plane contract for *target*."""
    capabilities = sink_capabilities(target)
    return SinkDataPlaneSpec(
        sink_name=str(getattr(target, "sink_name", type(target).__name__)),
        accepted_planes=capabilities.accepted_data_planes,
        native_planes=capabilities.native_data_planes,
    )


def writer_target_data_plane_specs(writer: object) -> tuple[SinkDataPlaneSpec, ...]:
    """Return sink-level data-plane specs visible behind *writer*."""
    inner_sinks = getattr(writer, "_sinks", None)
    if inner_sinks is not None:
        return tuple(sink_data_plane_spec(sink) for sink in inner_sinks)

    routes = getattr(writer, "_routes", None)
    default_sink = getattr(writer, "_default", None)
    if routes is not None:
        seen: set[int] = set()
        specs: list[SinkDataPlaneSpec] = []
        for route in routes:
            sink = route.sink
            sink_id = id(sink)
            if sink_id in seen:
                continue
            seen.add(sink_id)
            specs.append(sink_data_plane_spec(sink))
        if default_sink is not None and id(default_sink) not in seen:
            specs.append(sink_data_plane_spec(default_sink))
        return tuple(specs)

    advertised = getattr(writer, "sink_capabilities", None)
    if callable(advertised):
        return (sink_data_plane_spec(writer),)
    return ()


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
    arrow_passthrough_native: bool = False
    parallel_writes_safe: bool = False
    ordered_writes_required: bool = True
    accepted_data_planes: tuple[DataPlane, ...] = ()
    native_data_planes: tuple[DataPlane, ...] = ()

    @abstractmethod
    async def write(self, record: T) -> None:
        """Persist a single record."""

    async def open(self) -> None:
        """Called once before the pipeline loop starts.  Override to set up connections."""

    def bind_context(self, ctx: Any) -> None:
        """Attach run-scoped context when a sink needs pipeline metadata."""

    def sink_capabilities(self) -> SinkCapabilities:
        """Execution hints used by writer/runtime strategy selection."""
        accepted_planes = self.accepted_data_planes
        native_planes = self.native_data_planes
        batch_writable_native = DataPlane.PYTHON_BATCHES in native_planes
        arrow_passthrough_native = DataPlane.ARROW_BATCHES in native_planes
        if not accepted_planes and not native_planes:
            batch_writable_native = self.batch_writable_native
            arrow_passthrough_native = self.arrow_passthrough_native
            if batch_writable_native or arrow_passthrough_native:
                _warn_legacy_sink_flags_once(self)
        if not batch_writable_native and type(self).write_batch is not BaseSink.write_batch:
            batch_writable_native = True
        return _normalized_sink_capabilities(
            SinkCapabilities(
                batch_writable_native=batch_writable_native,
                arrow_passthrough_native=arrow_passthrough_native,
                parallel_writes_safe=self.parallel_writes_safe,
                ordered_writes_required=self.ordered_writes_required,
                accepted_data_planes=accepted_planes,
                native_data_planes=native_planes,
            ),
            batch_native=batch_writable_native,
            arrow_native=arrow_passthrough_native,
        )

    def data_plane_spec(self) -> SinkDataPlaneSpec:
        """Return the sink-side data-plane contract used by runtime planning."""
        return sink_data_plane_spec(self)

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
        self._sink_arrow_writable = [is_arrow_native_sink(s) for s in sinks]

    def with_concurrency(self, max_concurrency: int | None = None) -> SinkFanOut[T]:
        """Return a copy with opt-in concurrent sink writes enabled."""
        return SinkFanOut(
            list(self._sinks),
            concurrent_writes=True,
            max_concurrency=max_concurrency,
        )

    @staticmethod
    def _arrow_fallback_chunk_size(sink: BaseSink[T]) -> int:
        """Best-effort chunk size for row-materialized Arrow fallback.

        Text sinks such as CSV/JSONL often buffer around ``flush_every`` rows.
        Reusing that size avoids turning one large Arrow batch into one giant
        Python-object write burst.
        """
        candidates = (
            getattr(sink, "_flush_every", None),
            getattr(sink, "_batch_size", None),
        )
        for value in candidates:
            if isinstance(value, int) and value > 0:
                return value
        return 1000

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

        async def _execute(call: Awaitable[object]) -> object:
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
            await sink.write_batch(records)
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

            per_record_errors_concurrent: list[Exception | None] = [None] * len(records)
            results = await asyncio.gather(
                *(_write_one(index, record) for index, record in enumerate(records))
            )
            for index, error in results:
                per_record_errors_concurrent[index] = error
            return per_record_errors_concurrent

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
        # Fast path: single sink, no concurrent-writes overhead.
        if len(self._sinks) == 1 and not self._concurrent_writes:
            try:
                await self._sinks[0].write(record)
                return _WRITE_OK
            except Exception as exc:
                return WriteResult(written=False, errors=[exc])

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

        # Fast path: single batch-writable sink — skip fanout overhead entirely.
        if len(self._sinks) == 1 and self._sink_batch_writable[0]:
            try:
                await self._sinks[0].write_batch(records)
                n = len(records)
                return [_WRITE_OK] * n
            except Exception as exc:
                err = WriteResult(written=False, errors=[exc])
                return [err] * len(records)

        # Fast path: single non-batch-writable sink, no concurrency.
        # Sequential write, keeping per-record outcomes without fanout bookkeeping.
        if len(self._sinks) == 1 and not self._concurrent_writes:
            sink = self._sinks[0]
            fast_results: list[WriteResult] = [_WRITE_OK] * len(records)
            for i, record in enumerate(records):
                try:
                    await sink.write(record)
                except Exception as exc:
                    fast_results[i] = WriteResult(written=False, errors=[exc])
            return fast_results

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

        results: list[object]
        if not self._concurrent_writes:
            seq: list[object] = []
            for _, call in sink_calls:
                try:
                    seq.append(await call)
                except Exception as exc:
                    seq.append(exc)
            results = seq
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

    async def write_arrow_batch(self, batch: Any) -> None:
        """Write one Arrow batch to every sink using the best path per sink.

        - Arrow-capable sinks receive the original ``RecordBatch``.
        - Non-Arrow sinks receive a materialized ``list[dict]`` through their
          existing batch/write contract.
        """
        rows: list[Any] | None = None
        sink_calls: list[tuple[BaseSink[T], Awaitable[object]]] = []

        for sink, cap, batch_writable, arrow_writable in zip(
            self._sinks,
            self._sink_capabilities,
            self._sink_batch_writable,
            self._sink_arrow_writable,
            strict=True,
        ):
            if arrow_writable:
                arrow_sink = cast("Any", sink)
                sink_calls.append((sink, arrow_sink.write_arrow_batch(batch)))
                continue

            if rows is None:
                rows = await asyncio.to_thread(batch.to_pylist)
            assert rows is not None
            materialized_rows = rows
            chunk_size = self._arrow_fallback_chunk_size(sink)
            if chunk_size >= len(materialized_rows):
                sink_calls.append(
                    (
                        sink,
                        self._write_batch_to_sink(
                            sink,
                            materialized_rows,
                            batch_writable=batch_writable,
                            capabilities=cap,
                        ),
                    )
                )
                continue

            async def _write_chunks(
                target_sink: BaseSink[T],
                target_rows: list[Any],
                *,
                target_batch_writable: bool,
                target_capabilities: SinkCapabilities,
                target_chunk_size: int,
            ) -> object:
                for start in range(0, len(target_rows), target_chunk_size):
                    chunk = target_rows[start : start + target_chunk_size]
                    result = await self._write_batch_to_sink(
                        target_sink,
                        chunk,
                        batch_writable=target_batch_writable,
                        capabilities=target_capabilities,
                    )
                    if isinstance(result, list):
                        for error in result:
                            if error is not None:
                                return [error]
                return None

            sink_calls.append(
                (
                    sink,
                    _write_chunks(
                        sink,
                        materialized_rows,
                        target_batch_writable=batch_writable,
                        target_capabilities=cap,
                        target_chunk_size=chunk_size,
                    ),
                )
            )

        if not sink_calls:
            return

        results: list[object]
        if not self._concurrent_writes:
            seq: list[object] = []
            for _, call in sink_calls:
                try:
                    seq.append(await call)
                except Exception as exc:
                    seq.append(exc)
            results = seq
        else:
            results = await self._run_sink_calls(sink_calls)

        for result in results:
            if isinstance(result, Exception):
                raise result
            if isinstance(result, list):
                for error in result:
                    if error is not None:
                        raise error

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

    def __iter__(self) -> Iterator[BaseSink[T]]:
        return iter(self._sinks)

    def bind_context(self, ctx: Any) -> None:
        for sink in self._sinks:
            bind_context_if_supported(sink, ctx)


# ======================================================================
# SinkRouter — conditional routing to specific sinks
# ======================================================================


class SinkRoute(Generic[T]):
    """A predicate + sink pair."""

    def __init__(self, predicate: Any, sink: BaseSink[T]) -> None:
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

    def route(self, predicate: Any, sink: BaseSink[T]) -> SinkRouter[T]:
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

    def bind_context(self, ctx: Any) -> None:
        for route in self._routes:
            bind_context_if_supported(route.sink, ctx)
        if self._default is not None:
            bind_context_if_supported(self._default, ctx)
