"""
agora/core/runtime/_process.py
===============================
Internal process-pool runner for ProcessBatchMiddleware.

Not part of the public API. Owns process lifecycle, submission,
timeout enforcement, and exception wrapping.
"""

from __future__ import annotations

import asyncio
import contextlib
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from agora.core.runtime._process_codec import BatchCodec


class ProcessBatchError(Exception):
    """Worker-process failure surfaced back to the main pipeline.

    Wraps the original exception with enough context for the runtime
    to produce a useful failure record.
    """

    def __init__(
        self,
        middleware_name: str,
        batch_index: int,
        cause: BaseException,
        *,
        timed_out: bool = False,
        invalidated: bool = False,
        generation: int | None = None,
    ) -> None:
        self.middleware_name = middleware_name
        self.batch_index = batch_index
        self.timed_out = timed_out
        self.invalidated = invalidated
        self.generation = generation
        if timed_out:
            kind = "timed out"
        elif invalidated:
            kind = "was invalidated"
        else:
            kind = "raised"
        super().__init__(
            f"ProcessBatchMiddleware '{middleware_name}' batch #{batch_index} {kind}: "
            f"{type(cause).__name__}: {cause}"
        )
        self.__cause__ = cause


def _run_codec_batch(
    fn: Callable[[Any], Any],
    codec: BatchCodec,
    encoded_batch: Any,
) -> Any:
    """Worker entrypoint for codec-aware process batch execution."""

    batch = codec.decode_in_worker(encoded_batch)
    result = fn(batch)
    return codec.encode_from_worker(result)


@dataclass
class ProcessPoolRunner:
    """Owns a ProcessPoolExecutor and provides async batch submission.

    One runner per ProcessBatchMiddleware instance. Created on open(),
    shut down on close().

    Shutdown policy:
    - ``close(wait=True)``  — drain in-flight work, then shut down (graceful)
    - ``close(wait=False)`` — cancel pending futures immediately (forced)
    """

    max_workers: int | None
    middleware_name: str
    _pool: ProcessPoolExecutor = field(init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)
    _in_flight: set[asyncio.Future[list[Any]]] = field(default_factory=set, init=False, repr=False)

    def open(self) -> None:
        self._pool = ProcessPoolExecutor(max_workers=self.max_workers)
        self._closed = False
        self._in_flight.clear()

    def close(self, *, wait: bool = True, force: bool = False) -> None:
        """Shut down the process pool.

        Parameters
        ----------
        wait:
            If True (default), wait for submitted work to finish before
            returning. If False, cancel pending futures and return immediately.
        force:
            If True, terminate worker processes before shutting the pool down.
        """
        if self._closed:
            return
        self._closed = True
        if force:
            self._terminate_workers()
        self._pool.shutdown(wait=wait, cancel_futures=force or not wait)
        self._in_flight.clear()

    async def drain(self, *, timeout_s: float | None = None) -> bool:
        """Wait for tracked in-flight futures to complete.

        Returns True when all work completed before the timeout.
        """
        if not self._in_flight:
            return True
        _done, pending = await asyncio.wait(self._in_flight, timeout=timeout_s)
        return not pending

    async def submit(
        self,
        fn: Callable[[Any], Any],
        batch: Any,
        batch_index: int,
        timeout_s: float | None,
        *,
        codec: BatchCodec,
    ) -> Any:
        """Submit *batch* to the process pool and await the result.

        Raises ProcessBatchError on worker exception or timeout.
        """
        if self._closed:
            raise RuntimeError(f"ProcessPoolRunner for '{self.middleware_name}' is already closed")

        encoded_batch = codec.encode_for_worker(batch)
        expected_rows = codec.batch_size(batch)
        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(self._pool, _run_codec_batch, fn, codec, encoded_batch)
        self._track_future(fut)

        try:
            if timeout_s is not None:
                # Shield the underlying future so the pool task is not
                # cancelled on timeout — the worker process keeps running
                # but the main pipeline treats this batch as failed.
                result = await asyncio.wait_for(asyncio.shield(fut), timeout=timeout_s)
            else:
                result = await fut
        except TimeoutError as exc:
            raise ProcessBatchError(self.middleware_name, batch_index, exc, timed_out=True) from exc
        except Exception as exc:
            raise ProcessBatchError(self.middleware_name, batch_index, exc) from exc

        return codec.decode_from_worker(result, expected_rows=expected_rows)

    def has_in_flight_work(self) -> bool:
        return any(not fut.done() for fut in self._in_flight)

    def _track_future(self, fut: asyncio.Future[list[Any]]) -> None:
        self._in_flight.add(fut)
        fut.add_done_callback(self._discard_future)

    def _discard_future(self, fut: asyncio.Future[list[Any]]) -> None:
        self._in_flight.discard(fut)
        with contextlib.suppress(BaseException):
            fut.exception()

    def _terminate_workers(self) -> None:
        processes = getattr(self._pool, "_processes", None)
        if not processes:
            return
        for process in list(processes.values()):
            try:
                if process.is_alive():
                    process.terminate()
            except Exception:
                continue
