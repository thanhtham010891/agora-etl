"""
agora/middlewares/process.py
=============================
ProcessBatchMiddleware — run heavy user-defined batch transforms in a
separate process pool while preserving Agora's checkpoint, DLQ, and
sink-write semantics in the main process.

Batch-only. The user function must be pickleable (module-level def or
importable callable). The batch must consist of pickleable objects.

Usage::

    from agora.middlewares.process import ProcessBatchMiddleware

    def transform(batch: list[dict]) -> list[dict]:
        return [{**r, "x": r["x"] * 2} for r in batch]

    pipeline = (
        Pipeline(source)
        .pipe(
            ProcessBatchMiddleware(
                fn=transform,
                max_workers=4,
                timeout_s=60,
                max_in_flight_batches=8,
            )
        )
        .build(sink)
    )
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from agora.core.batch import ArrowBatchMiddleware, BatchMiddleware
from agora.core.runtime._process import ProcessBatchError, ProcessPoolRunner
from agora.core.runtime._process_codec import ArrowBatchCodec, BatchCodecError, PythonObjectCodec

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from agora.core.context import PipelineContext

T = TypeVar("T")
U = TypeVar("U")

# How long to wait for in-flight work to drain on graceful shutdown before
# force-cancelling. None means wait indefinitely.
_DEFAULT_DRAIN_TIMEOUT_S: float = 30.0


class _ProcessPoolLifecycle:
    """Shared process-pool lifecycle for process-isolated middleware variants."""

    name: str
    _max_workers: int | None
    _max_in_flight: int
    _ordered: bool
    _batch_index: int
    _runner_generation: int
    _invalidated_generations: dict[int, str]
    _generation_tasks: dict[int, set[asyncio.Task[Any]]]
    _runner: ProcessPoolRunner | None

    @property
    def batch_in_flight_limit(self) -> int:
        if self._max_workers == 1:
            return 1
        return self._max_in_flight

    @property
    def ordered_batch_commits(self) -> bool:
        return self._ordered

    async def on_start(self, ctx: PipelineContext) -> None:
        if not self._ordered and self._max_in_flight > 1:
            raise NotImplementedError(
                f"{type(self).__name__} currently supports pipelined execution only with "
                "ordered=True. Unordered batch commit is not available yet."
            )
        self._runner = self._open_runner()
        self._batch_index = 0
        self._runner_generation = 0
        self._invalidated_generations = {}
        self._generation_tasks = {}
        ctx.log.info(
            "process_batch_middleware_started",
            middleware=self.name,
            max_workers=self._max_workers,
            ordered=self._ordered,
        )

    async def on_stop(self, ctx: PipelineContext) -> None:
        """Shut down the process pool.

        Attempts graceful drain first: waits up to _DEFAULT_DRAIN_TIMEOUT_S
        for any in-flight batch to complete, then force-cancels.
        """
        if self._runner is None:
            return

        drained = True
        if self._runner.has_in_flight_work():
            ctx.log.info(
                "process_batch_middleware_draining",
                middleware=self.name,
                drain_timeout_s=_DEFAULT_DRAIN_TIMEOUT_S,
            )
            try:
                drained = await self._runner.drain(
                    timeout_s=_DEFAULT_DRAIN_TIMEOUT_S,
                )
                if drained:
                    ctx.log.info(
                        "process_batch_middleware_drain_ok",
                        middleware=self.name,
                    )
                else:
                    ctx.log.warning(
                        "process_batch_middleware_drain_timeout",
                        middleware=self.name,
                    )
            except Exception:
                drained = False
                ctx.log.warning(
                    "process_batch_middleware_drain_failed",
                    middleware=self.name,
                )

        self._runner.close(wait=drained, force=not drained)
        self._runner = None
        ctx.log.info("process_batch_middleware_stopped", middleware=self.name)

    async def drain_pending_batches(self, ctx: PipelineContext) -> None:
        del ctx
        await asyncio.sleep(0)

    async def abort_in_flight_batches(self, ctx: PipelineContext, *, reason: str) -> None:
        self._abort_runner(ctx, reason=reason)
        await asyncio.sleep(0)

    def _open_runner(self) -> ProcessPoolRunner:
        runner = ProcessPoolRunner(
            max_workers=self._max_workers,
            middleware_name=self.name,
        )
        runner.open()
        return runner

    def _recycle_runner(
        self,
        ctx: PipelineContext,
        *,
        reason: str,
        batch_index: int,
        generation: int,
    ) -> None:
        old_runner = self._runner
        if old_runner is None:
            return
        if generation != self._runner_generation:
            return
        self._invalidated_generations[generation] = reason

        ctx.log.warning(
            "process_batch_middleware_recycling_pool",
            middleware=self.name,
            reason=reason,
            batch_index=batch_index,
            generation=generation,
        )
        self._cancel_generation_tasks(generation)
        old_runner.close(wait=False, force=True)
        self._runner_generation += 1
        self._runner = self._open_runner()
        ctx.log.info(
            "process_batch_middleware_pool_recycled",
            middleware=self.name,
            reason=reason,
            batch_index=batch_index,
            previous_generation=generation,
            generation=self._runner_generation,
        )

    def _abort_runner(self, ctx: PipelineContext, *, reason: str) -> None:
        old_runner = self._runner
        if old_runner is None:
            return
        generation = self._runner_generation
        self._invalidated_generations[generation] = reason
        ctx.log.warning(
            "process_batch_middleware_aborting_pool",
            middleware=self.name,
            reason=reason,
            generation=generation,
        )
        self._cancel_generation_tasks(generation, include_current=True)
        old_runner.close(wait=False, force=True)
        self._runner = None
        self._runner_generation += 1

    def _require_live_runner(self) -> tuple[ProcessPoolRunner, int]:
        runner = self._runner
        if runner is None:
            raise RuntimeError(f"{type(self).__name__} '{self.name}' used before on_start()")
        return runner, self._runner_generation

    def _maybe_raise_invalidated_generation(self, batch_index: int, generation: int) -> None:
        reason = self._invalidated_generations.get(generation)
        if reason is None:
            return
        raise ProcessBatchError(
            self.name,
            batch_index,
            RuntimeError(f"worker pool generation {generation} was invalidated due to {reason}"),
            invalidated=True,
            generation=generation,
        )

    def _register_generation_task(self, generation: int) -> asyncio.Task[Any] | None:
        task = asyncio.current_task()
        if task is None:
            return None
        self._generation_tasks.setdefault(generation, set()).add(task)
        return task

    def _unregister_generation_task(
        self,
        generation: int,
        task: asyncio.Task[Any] | None,
    ) -> None:
        if task is None:
            return
        tasks = self._generation_tasks.get(generation)
        if not tasks:
            return
        tasks.discard(task)
        if not tasks:
            self._generation_tasks.pop(generation, None)

    def _cancel_generation_tasks(self, generation: int, *, include_current: bool = False) -> None:
        tasks = list(self._generation_tasks.get(generation, ()))
        current = asyncio.current_task()
        for task in tasks:
            if task.done():
                continue
            if not include_current and current is not None and task is current:
                continue
            task.cancel()


class ProcessBatchMiddleware(_ProcessPoolLifecycle, BatchMiddleware[T, U], Generic[T, U]):
    """Offload a synchronous batch transform to a managed process pool.

    The user function runs in a separate process; the main pipeline handles
    checkpoint, DLQ, and sink writes as normal after results come back.

    On the batch lane, Agora can either:

    - run one batch at a time when ``max_workers == 1`` or
      ``max_in_flight_batches == 1``
    - keep multiple batches in flight when ``max_workers > 1`` and
      ``max_in_flight_batches > 1``

    Even in pipelined mode, commits stay in source order and checkpoint does
    not advance until the result is written by the delivery engine downstream.

    Shutdown policy: on_stop() attempts a graceful drain (waiting up to
    ``_DEFAULT_DRAIN_TIMEOUT_S`` seconds) before force-cancelling the pool.
    This ensures in-flight work completes cleanly under normal pipeline
    teardown, while still terminating promptly on cancellation or error.

    Parameters
    ----------
    fn:
        A pickleable sync callable ``(list[T]) -> list[U]``. Must be
        importable from worker processes (module-level def recommended).
    max_workers:
        Number of worker processes. Defaults to ``os.cpu_count()``.
    ordered:
        Keep sink commits in source order. Required for pipelined mode in
        ``0.3.x``. Asking for ``ordered=False`` together with
        ``max_in_flight_batches > 1`` is rejected explicitly.
    timeout_s:
        Per-batch timeout in seconds. None means no timeout.
    max_in_flight_batches:
        Upper bound on batches submitted to the worker pool before Agora drains
        ready results back in source order. Effective only when
        ``max_workers > 1``.
    name:
        Middleware name shown in logs and metrics.
    """

    def __init__(
        self,
        fn: Callable[[list[T]], list[U] | Sequence[U]],
        *,
        max_workers: int | None = None,
        ordered: bool = True,
        timeout_s: float | None = None,
        max_in_flight_batches: int = 4,
        name: str = "process_batch",
    ) -> None:
        if max_workers is not None and max_workers < 1:
            raise ValueError(f"max_workers must be >= 1, got {max_workers}")
        if max_in_flight_batches < 1:
            raise ValueError(f"max_in_flight_batches must be >= 1, got {max_in_flight_batches}")
        if timeout_s is not None and timeout_s <= 0:
            raise ValueError(f"timeout_s must be > 0, got {timeout_s}")

        self.name = name
        self._fn = fn
        self._max_workers = max_workers
        self._ordered = ordered
        self._timeout_s = timeout_s
        self._max_in_flight = max_in_flight_batches
        self._batch_index: int = 0
        self._codec = PythonObjectCodec()
        self._runner: ProcessPoolRunner | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def process_batch(
        self,
        records: list[T],
        ctx: PipelineContext,
    ) -> list[U | None]:
        """Submit *records* to the process pool and return transformed results.

        Blocks until the worker process completes. Checkpoint and DLQ
        handling remain in the main pipeline delivery engine.
        """
        runner, generation = self._require_live_runner()
        current_task = self._register_generation_task(generation)

        batch_index = self._batch_index
        self._batch_index += 1

        try:
            result = await runner.submit(
                self._fn,
                records,
                batch_index=batch_index,
                timeout_s=self._timeout_s,
                codec=self._codec,
            )
        except asyncio.CancelledError:
            if generation in self._invalidated_generations:
                self._maybe_raise_invalidated_generation(batch_index, generation)
            self._abort_runner(ctx, reason="cancellation")
            raise
        except BatchCodecError as exc:
            raise RuntimeError(f"ProcessBatchMiddleware '{self.name}' {exc}") from exc
        except ProcessBatchError as exc:
            if exc.timed_out:
                self._recycle_runner(
                    ctx,
                    reason="timeout",
                    batch_index=batch_index,
                    generation=generation,
                )
                raise
            self._maybe_raise_invalidated_generation(batch_index, generation)
            raise ProcessBatchError(
                self.name,
                batch_index,
                exc,
                generation=generation,
            ) from exc
        except Exception as exc:
            raise ProcessBatchError(self.name, batch_index, exc) from exc
        finally:
            self._unregister_generation_task(generation, current_task)

        self._maybe_raise_invalidated_generation(batch_index, generation)
        return list(result)

    async def process(self, record: T, ctx: PipelineContext) -> U | None:
        raise RuntimeError(
            f"ProcessBatchMiddleware '{self.name}' requires a batch-capable source; "
            "per-record execution is not supported."
        )

    async def submit_batch(
        self, records: list[T], ctx: PipelineContext
    ) -> asyncio.Task[list[U | None]]:
        task = asyncio.create_task(self.process_batch(records, ctx))
        await asyncio.sleep(0)
        return task


class ArrowProcessBatchMiddleware(_ProcessPoolLifecycle, ArrowBatchMiddleware):
    """Run an Arrow-native batch transform in a managed process pool.

    The batch stays columnar across the process boundary using Arrow IPC bytes.
    This middleware must be used on an Arrow-native batch lane, typically with
    sources like ``ArrowCsvSource`` or ``ParquetSource(use_arrow_batches=True)``.

    For the first public cut, the worker function must preserve row count:
    ``pa.RecordBatch -> pa.RecordBatch`` with the same number of rows. When
    ``max_workers > 1`` and ``max_in_flight_batches > 1``, Agora can keep
    multiple Arrow batches in flight while still committing them in source
    order.
    """

    name = "arrow_process_batch"

    def __init__(
        self,
        fn: Callable[[Any], Any],
        *,
        max_workers: int | None = None,
        ordered: bool = True,
        timeout_s: float | None = None,
        max_in_flight_batches: int = 4,
        name: str = "arrow_process_batch",
    ) -> None:
        if max_workers is not None and max_workers < 1:
            raise ValueError(f"max_workers must be >= 1, got {max_workers}")
        if max_in_flight_batches < 1:
            raise ValueError(f"max_in_flight_batches must be >= 1, got {max_in_flight_batches}")
        if timeout_s is not None and timeout_s <= 0:
            raise ValueError(f"timeout_s must be > 0, got {timeout_s}")

        self.name = name
        self._fn = fn
        self._max_workers = max_workers
        self._ordered = ordered
        self._timeout_s = timeout_s
        self._max_in_flight = max_in_flight_batches
        self._batch_index: int = 0
        self._codec = ArrowBatchCodec()
        self._runner: ProcessPoolRunner | None = None

    async def process_arrow_batch(self, batch: Any, ctx: PipelineContext) -> Any:
        runner, generation = self._require_live_runner()
        current_task = self._register_generation_task(generation)

        batch_index = self._batch_index
        self._batch_index += 1

        try:
            result = await runner.submit(
                self._fn,
                batch,
                batch_index=batch_index,
                timeout_s=self._timeout_s,
                codec=self._codec,
            )
        except asyncio.CancelledError:
            if generation in self._invalidated_generations:
                self._maybe_raise_invalidated_generation(batch_index, generation)
            self._abort_runner(ctx, reason="cancellation")
            raise
        except BatchCodecError as exc:
            raise RuntimeError(f"ArrowProcessBatchMiddleware '{self.name}' {exc}") from exc
        except ProcessBatchError as exc:
            if exc.timed_out:
                self._recycle_runner(
                    ctx,
                    reason="timeout",
                    batch_index=batch_index,
                    generation=generation,
                )
                raise
            self._maybe_raise_invalidated_generation(batch_index, generation)
            raise ProcessBatchError(
                self.name,
                batch_index,
                exc,
                generation=generation,
            ) from exc
        except Exception as exc:
            raise ProcessBatchError(self.name, batch_index, exc) from exc
        finally:
            self._unregister_generation_task(generation, current_task)
        self._maybe_raise_invalidated_generation(batch_index, generation)
        return result

    async def process(self, record: Any, ctx: PipelineContext) -> Any:
        raise RuntimeError(
            f"ArrowProcessBatchMiddleware '{self.name}' requires an Arrow batch lane; "
            "per-record execution is not supported."
        )

    async def apply_in_batch(
        self,
        current: list[Any],
        ctx: PipelineContext,
        chain: Any,
        idx: int,
    ) -> Any:
        del current, ctx, chain, idx
        raise RuntimeError(
            f"ArrowProcessBatchMiddleware '{self.name}' requires an all-Arrow batch chain; "
            "mixed list-batch execution is not supported."
        )

    async def submit_batch(self, batch: Any, ctx: PipelineContext) -> asyncio.Task[Any]:
        task = asyncio.create_task(self.process_arrow_batch(batch, ctx))
        await asyncio.sleep(0)
        return task
