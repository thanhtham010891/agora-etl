"""
agora/middlewares/ai/batch.py
==============================
``AIBatchMiddleware`` — amortize LLM costs by batching N records per call.

Problem
-------
Each AI middleware currently fires one LLM API call per record.
At 10k records with 300ms latency each → 50 minutes, and full per-record cost.

Solution
--------
Buffer records into a queue, flush when ``batch_size`` is reached OR
``flush_timeout_ms`` elapses (whichever comes first), then send a single
LLM call with a JSON array prompt.  Results are matched back by index.

The LLM response **must** be a JSON array of the same length as the input.
If the response cannot be parsed or lengths mismatch, the middleware falls
back to processing each record individually via the ``inner`` middleware.

Usage::

    pipeline.pipe(
        AIBatchMiddleware(
            provider=GeminiProvider(),
            prompt_fn=lambda records: (
                f"Enrich each of the following {len(records)} POI records.\\n"
                f"Return a JSON array of objects with keys: summary, tags, price_level.\\n"
                f"Input: {json.dumps(records)}"
            ),
            output_fields=["summary", "tags", "price_level"],
            batch_size=20,
            flush_timeout_ms=500,
        )
    )

    # Or wrap an existing AIMiddleware (delegates single-record fallback):
    pipeline.pipe(
        AIBatchMiddleware.wrapping(
            inner=AIEnrichMiddleware(provider=..., prompt_template=...),
            batch_size=20,
        )
    )

Design notes
------------
- Uses ``asyncio.Queue`` + a background flush task driven by ``on_start``.
- Each ``process()`` call enqueues the record and awaits a ``Future`` that
  is resolved by the flush task when the batch is complete.
- Flush is triggered on size OR timeout — whichever happens first.
- ``on_stop`` drains any remaining buffered records before the pipeline ends.
- Thread safety: all state access is within a single asyncio event loop.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct

from agora.middlewares.ai.base import AIMiddleware, OnError
from agora.utils.records import merge_into_record

if TYPE_CHECKING:
    from collections.abc import Callable

    from agora.ai.cache import LLMCache
    from agora.ai.providers.base import AIProvider
    from agora.core.context import PipelineContext

T = TypeVar("T")

logger = logstruct.getLogger(__name__)


class AIBatchMiddleware(AIMiddleware[T], Generic[T]):
    """Buffer records and process them in a single batched LLM call.

    Parameters
    ----------
    provider:
        Any ``AIProvider``.
    prompt_fn:
        Callable that receives ``list[dict]`` (serialized records) and returns
        the prompt string to send to the LLM.
        The LLM **must** return a JSON array of the same length.
    output_fields:
        Keys to extract from each LLM response dict and merge into the record.
        ``None`` = merge all keys returned by the LLM.
    batch_size:
        Maximum number of records per LLM call.  Default: 20.
    flush_timeout_ms:
        Maximum milliseconds to wait before flushing a partial batch.
        Default: 500 ms.
    system:
        Optional system instruction for the LLM.
    max_tokens:
        Max tokens for the LLM response.
    cache / cache_ttl / on_error:
        See ``AIMiddleware``.
    """

    name = "ai_batch"

    def __init__(
        self,
        provider: AIProvider,
        prompt_fn: Callable[[list[dict]], str],
        *,
        output_fields: list[str] | None = None,
        batch_size: int = 20,
        flush_timeout_ms: int = 500,
        system: str | None = None,
        max_tokens: int = 4096,
        cache: LLMCache | None = None,
        cache_ttl: int = 86_400,
        on_error: OnError = "passthrough",
    ) -> None:
        super().__init__(provider, cache=cache, cache_ttl=cache_ttl, on_error=on_error)
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        self._prompt_fn = prompt_fn
        self._output_fields = output_fields
        self._batch_size = batch_size
        self._flush_timeout_s = flush_timeout_ms / 1000.0
        self._system = system
        self._max_tokens = max_tokens
        self.min_concurrency = batch_size

        # Runtime state (initialized in on_start)
        self._queue: asyncio.Queue[tuple[Any, asyncio.Future]] | None = None
        self._flush_task: asyncio.Task | None = None
        self._size_flush_task: asyncio.Task | None = None
        self._flush_lock: asyncio.Lock | None = None
        self._ctx: PipelineContext | None = None

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def on_start(self, ctx: PipelineContext) -> None:
        self._ctx = ctx
        self._queue = asyncio.Queue()
        self._flush_lock = asyncio.Lock()
        self._flush_task = asyncio.create_task(self._flush_loop())
        logger.debug(
            "ai_batch_started",
            batch_size=self._batch_size,
            flush_timeout_ms=int(self._flush_timeout_s * 1000),
        )

    async def on_stop(self, ctx: PipelineContext) -> None:
        # Yield once so any process() tasks that were create_task'd
        # but not yet run can enqueue their records before we drain.
        await asyncio.sleep(0)

        await self.drain_pending(ctx)

        # Cancel background flush tasks
        if self._flush_task is not None and not self._flush_task.done():
            self._flush_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._flush_task
        if self._size_flush_task is not None and not self._size_flush_task.done():
            self._size_flush_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._size_flush_task

        # Resolve any futures still in the queue (late-arriving enqueues that
        # slipped in after drain_pending) with passthrough so callers don't hang.
        if self._queue is not None:
            while not self._queue.empty():
                try:
                    record, future = self._queue.get_nowait()
                    if not future.done():
                        future.set_result(record)
                except asyncio.QueueEmpty:
                    break

        self._ctx = None
        await super().on_stop(ctx)

    # ------------------------------------------------------------------ #
    # process                                                              #
    # ------------------------------------------------------------------ #

    async def process(self, record: T, ctx: PipelineContext) -> T | None:
        future = await self.submit(record, ctx)
        return await future

    async def submit(self, record: T, ctx: PipelineContext) -> asyncio.Future[T | None]:
        """Enqueue a record and return a future for its eventual batch result."""
        if self._queue is None:
            loop = asyncio.get_running_loop()
            future: asyncio.Future[T | None] = loop.create_future()
            future.set_result(record)
            return future

        loop = asyncio.get_running_loop()
        future: asyncio.Future[T | None] = loop.create_future()
        await self._queue.put((record, future))

        # Schedule flush as a background task (not awaited here) to avoid
        # deadlock: process() cannot both trigger flush AND await the future
        # that flush will resolve.
        if self._queue.qsize() >= self._batch_size and (
            self._size_flush_task is None or self._size_flush_task.done()
        ):
            self._size_flush_task = asyncio.create_task(self._flush_pending(flush_partial=False))

        return future

    async def drain_pending(self, ctx: PipelineContext | None = None) -> None:
        """Flush any partial batch waiting in the queue."""
        if self._queue is not None and not self._queue.empty():
            await self._flush_pending()

    # ------------------------------------------------------------------ #
    # Background flush loop                                               #
    # ------------------------------------------------------------------ #

    async def _flush_loop(self) -> None:
        """Periodically flush partial batches based on timeout."""
        try:
            while True:
                await asyncio.sleep(self._flush_timeout_s)
                if self._queue and not self._queue.empty():
                    await self._flush_pending()
        except asyncio.CancelledError:
            pass

    async def _flush_pending(self, *, flush_partial: bool = True) -> None:
        """Drain pending items, optionally waiting for a full batch."""
        if self._queue is None or self._queue.empty():
            return
        if self._flush_lock is None:
            self._flush_lock = asyncio.Lock()

        async with self._flush_lock:
            while self._queue is not None and not self._queue.empty():
                if not flush_partial and self._queue.qsize() < self._batch_size:
                    break

                batch: list[tuple[Any, asyncio.Future]] = []
                while not self._queue.empty() and len(batch) < self._batch_size:
                    try:
                        item = self._queue.get_nowait()
                        batch.append(item)
                    except asyncio.QueueEmpty:
                        break

                if not batch:
                    return

                records = [item[0] for item in batch]
                futures = [item[1] for item in batch]

                try:
                    results = await self._process_batch(records, ctx=self._ctx)
                except Exception as exc:
                    logger.warning(
                        "ai_batch_flush_error",
                        error=str(exc),
                        batch_len=len(batch),
                    )
                    for record, future in zip(records, futures, strict=True):
                        if not future.done():
                            result = await self._handle_error(exc, record, self._ctx)
                            future.set_result(result)
                    continue

                for _record, future, result in zip(records, futures, results, strict=True):
                    if not future.done():
                        future.set_result(result)

    # ------------------------------------------------------------------ #
    # Batch LLM call                                                       #
    # ------------------------------------------------------------------ #

    async def _process_batch(
        self, records: list[T], *, ctx: PipelineContext | None = None
    ) -> list[T | None]:
        """Send batch to LLM and merge results back into records."""
        serialized = [self._serialize_record(r) for r in records]
        prompt = self._prompt_fn(serialized)

        response = await self._cached_complete(
            prompt,
            system=self._system,
            temperature=0.0,
            max_tokens=self._max_tokens,
            ctx=ctx,
        )

        raw_text = response.content.strip()
        if raw_text.startswith("```"):
            lines = raw_text.splitlines()
            raw_text = "\n".join(lines[1:-1]) if len(lines) > 2 else raw_text

        llm_results: list[dict[str, Any]] = json.loads(raw_text)

        if len(llm_results) != len(records):
            raise ValueError(f"LLM returned {len(llm_results)} results for {len(records)} records")

        output: list[T | None] = []
        for record, enrichment in zip(records, llm_results, strict=True):
            if self._output_fields is not None:
                enrichment = {k: v for k, v in enrichment.items() if k in self._output_fields}
            output.append(merge_into_record(record, enrichment))

        logger.debug(
            "ai_batch_flushed",
            batch_size=len(records),
            model=response.model,
        )
        return output

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _serialize_record(record: Any) -> dict:
        """Convert any record type to a plain dict for the prompt."""
        if isinstance(record, dict):
            return record
        if hasattr(record, "model_dump"):
            return record.model_dump()
        if hasattr(record, "__dict__"):
            return record.__dict__
        return {"record": str(record)}

    # ``process()`` is overridden above and satisfies the abstract requirement
    # from ``AIMiddleware``. The process() override handles queueing + batching.
