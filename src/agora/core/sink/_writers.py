"""Internal sink writer implementations."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from agora.core.batch import is_arrow_native_sink
from agora.core.data_plane import SinkDataPlaneSpec
from agora.core.sink._support import (
    BatchWritable,
    SinkCapabilities,
    bind_context_if_supported,
    sink_capabilities,
)
from agora.core.writer import WriteResult

if TYPE_CHECKING:
    from collections.abc import Awaitable, Iterator

    from agora.core.sink.base import BaseSink

T = TypeVar("T")


def _write_ok() -> WriteResult:
    """Return a fresh success result with isolated mutable state."""
    return WriteResult(written=True, errors=[])


def _normalize_batch_write_results(
    result: object,
    *,
    expected: int,
) -> list[WriteResult]:
    """Normalize batch-write return values across legacy and writer-style sinks.

    Native batch sinks historically returned ``None`` on success because
    ``BaseSink.write_batch()`` models a side-effecting sink API. Newer
    writer-style sinks may instead return one ``WriteResult`` per input record.
    """

    if result is None:
        return [_write_ok() for _ in range(expected)]
    if isinstance(result, list) and all(isinstance(item, WriteResult) for item in result):
        if len(result) != expected:
            raise RuntimeError(
                "Sink.write_batch() must return one WriteResult per input record. "
                f"Expected {expected}, got {len(result)}."
            )
        return result
    raise RuntimeError(
        "Sink.write_batch() must return None or a list[WriteResult] matching the input batch."
    )


async def _close_opened_sinks(sinks: list[BaseSink[Any]]) -> None:
    first_error: Exception | None = None
    for sink in reversed(sinks):
        try:
            await sink.close()
        except Exception as exc:
            if first_error is None:
                first_error = exc
    if first_error is not None:
        raise first_error


class SinkFanOut(Generic[T]):
    """Writes each record to ALL registered sinks."""

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
        self._open_rolled_back = False
        self._sink_capabilities = [sink_capabilities(s) for s in sinks]
        self._sink_data_plane_specs = tuple(
            SinkDataPlaneSpec(
                sink_name=str(getattr(sink, "sink_name", type(sink).__name__)),
                accepted_planes=cap.accepted_data_planes,
                native_planes=cap.native_data_planes,
            )
            for sink, cap in zip(sinks, self._sink_capabilities, strict=True)
        )
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
            batch_sink = cast("BatchWritable[T]", sink)
            batch_result = await batch_sink.write_batch(records)
            return _normalize_batch_write_results(batch_result, expected=len(records))

        if (
            self._concurrent_writes
            and capabilities.parallel_writes_safe
            and not capabilities.ordered_writes_required
        ):
            semaphore = None
            if self._max_concurrency is not None and self._max_concurrency > 0:
                semaphore = asyncio.Semaphore(self._max_concurrency)

            async def _write_one(index: int, record: T) -> tuple[int, Exception | None]:
                try:
                    if semaphore is None:
                        await sink.write(record)
                    else:
                        async with semaphore:
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
        self._open_rolled_back = False
        opened: list[BaseSink[T]] = []
        if not self._concurrent_writes:
            try:
                for sink in self._sinks:
                    await sink.open()
                    opened.append(sink)
            except Exception:
                self._open_rolled_back = True
                await _close_opened_sinks(opened)
                raise
            return

        results = await self._run_sink_calls([(sink, sink.open()) for sink in self._sinks])
        errors = [result for result in results if isinstance(result, Exception)]
        if not errors:
            return
        opened.extend(
            sink
            for sink, result in zip(self._sinks, results, strict=True)
            if not isinstance(result, Exception)
        )
        close_error: Exception | None = None
        try:
            self._open_rolled_back = True
            await _close_opened_sinks(opened)
        except Exception as exc:
            close_error = exc
        if close_error is not None:
            raise errors[0] from close_error
        raise errors[0]

    async def write(self, record: T) -> WriteResult:
        if len(self._sinks) == 1 and not self._concurrent_writes:
            try:
                await self._sinks[0].write(record)
                return _write_ok()
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
        if not records:
            return []

        if len(self._sinks) == 1 and self._sink_batch_writable[0]:
            try:
                sink = cast("BatchWritable[T]", self._sinks[0])
                batch_result = await sink.write_batch(records)
                return _normalize_batch_write_results(batch_result, expected=len(records))
            except Exception as exc:
                return [WriteResult(written=False, errors=[exc]) for _ in range(len(records))]

        if len(self._sinks) == 1 and not self._concurrent_writes:
            sink = self._sinks[0]
            fast_results = [_write_ok() for _ in range(len(records))]
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
                if all(isinstance(item, WriteResult) for item in result):
                    for index, write_result in enumerate(result):
                        if write_result.written:
                            written_flags[index] = True
                        if write_result.errors:
                            errors_by_record[index].extend(write_result.errors)
                else:
                    per_record_errors = cast("list[Exception | None]", result)
                    for index, error in enumerate(per_record_errors):
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
        if len(self._sinks) == 1 and not self._concurrent_writes:
            sink = self._sinks[0]
            if self._sink_arrow_writable[0]:
                arrow_sink = cast("Any", sink)
                await arrow_sink.write_arrow_batch(batch)
                return

            batch_rows = await asyncio.to_thread(batch.to_pylist)
            chunk_size = self._arrow_fallback_chunk_size(sink)
            if chunk_size >= len(batch_rows):
                result = await self._write_batch_to_sink(
                    sink,
                    batch_rows,
                    batch_writable=self._sink_batch_writable[0],
                    capabilities=self._sink_capabilities[0],
                )
                if isinstance(result, list):
                    if all(isinstance(item, WriteResult) for item in result):
                        for write_result in result:
                            if write_result.errors:
                                raise write_result.errors[0]
                    else:
                        for error in cast("list[Exception | None]", result):
                            if error is not None:
                                raise error
                return

            for start in range(0, len(batch_rows), chunk_size):
                chunk = batch_rows[start : start + chunk_size]
                result = await self._write_batch_to_sink(
                    sink,
                    chunk,
                    batch_writable=self._sink_batch_writable[0],
                    capabilities=self._sink_capabilities[0],
                )
                if isinstance(result, list):
                    if all(isinstance(item, WriteResult) for item in result):
                        for write_result in result:
                            if write_result.errors:
                                raise write_result.errors[0]
                    else:
                        for error in cast("list[Exception | None]", result):
                            if error is not None:
                                raise error
            return

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
                        if all(isinstance(item, WriteResult) for item in result):
                            for write_result in result:
                                if write_result.errors:
                                    return list(write_result.errors)
                        else:
                            for error in cast("list[Exception | None]", result):
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
                if all(isinstance(item, WriteResult) for item in result):
                    for write_result in result:
                        if write_result.errors:
                            raise write_result.errors[0]
                else:
                    for error in cast("list[Exception | None]", result):
                        if error is not None:
                            raise error

    async def flush(self) -> None:
        if not self._concurrent_writes:
            for sink in self._sinks:
                await sink.flush()
            return

        results = await self._run_sink_calls([(sink, sink.flush()) for sink in self._sinks])
        self._raise_first_exception(results)

    async def close(self) -> None:
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


class SinkRoute(Generic[T]):
    """A predicate + sink pair."""

    def __init__(self, predicate: Any, sink: BaseSink[T]) -> None:
        self.predicate = predicate
        self.sink = sink


class SinkRouter(Generic[T]):
    """Route each record to the FIRST matching sink."""

    def __init__(self) -> None:
        self._routes: list[SinkRoute[T]] = []
        self._default: BaseSink[T] | None = None
        self._open_rolled_back = False

    def _unique_sinks(self) -> list[BaseSink[T]]:
        seen: set[int] = set()
        ordered: list[BaseSink[T]] = []
        for route in self._routes:
            sink_id = id(route.sink)
            if sink_id in seen:
                continue
            seen.add(sink_id)
            ordered.append(route.sink)
        if self._default is not None and id(self._default) not in seen:
            ordered.append(self._default)
        return ordered

    def route(self, predicate: Any, sink: BaseSink[T]) -> SinkRouter[T]:
        self._routes.append(SinkRoute(predicate, sink))
        return self

    def default(self, sink: BaseSink[T]) -> SinkRouter[T]:
        self._default = sink
        return self

    async def open(self) -> None:
        self._open_rolled_back = False
        opened: list[BaseSink[T]] = []
        try:
            for sink in self._unique_sinks():
                await sink.open()
                opened.append(sink)
        except Exception:
            self._open_rolled_back = True
            await _close_opened_sinks(opened)
            raise

    async def write(self, record: T) -> WriteResult:
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

            target_id = id(target)
            if target_id not in grouped:
                grouped[target_id] = (target, [])
            grouped[target_id][1].append((index, record))

        for sink, entries in grouped.values():
            capabilities = sink_capabilities(sink)
            if capabilities.batch_writable_native and isinstance(sink, BatchWritable):
                try:
                    batch_sink = cast("BatchWritable[T]", sink)
                    batch_result = await batch_sink.write_batch([record for _, record in entries])
                    write_results = _normalize_batch_write_results(
                        batch_result,
                        expected=len(entries),
                    )
                    for (index, _), write_result in zip(entries, write_results, strict=True):
                        if write_result.written:
                            written_flags[index] = True
                        if write_result.errors:
                            errors_by_record[index].extend(write_result.errors)
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
        for sink in self._unique_sinks():
            await sink.flush()

    async def close(self) -> None:
        for sink in self._unique_sinks():
            await sink.close()

    def bind_context(self, ctx: Any) -> None:
        for sink in self._unique_sinks():
            bind_context_if_supported(sink, ctx)
