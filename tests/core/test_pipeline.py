"""
Tests for the core pipeline builder and BoundPipeline runner.
"""

from __future__ import annotations

import asyncio

import pytest

from agora import (
    DeliveryConfig,
    FilterMiddleware,
    IterableSource,
    MapMiddleware,
    Pipeline,
    SinkFailurePolicy,
)
from agora.core.middleware import Middleware
from agora.core.source import BaseSource
from agora.sinks.io.stdout import StdoutSink

# ======================================================================
# Fixtures
# ======================================================================


def make_source(n: int = 5) -> IterableSource:
    return IterableSource(list(range(n)))


# ======================================================================
# Pipeline builder tests
# ======================================================================


def test_pipeline_from_source_sets_id():
    source = IterableSource([])
    source.source_name = "test_source"
    pipeline = Pipeline(source)
    assert pipeline._pipeline_id == "test_source"


def test_pipeline_from_source_custom_id():
    pipeline = Pipeline(IterableSource([]), id="my_pipe")
    assert pipeline._pipeline_id == "my_pipe"


def test_pipeline_pipe_is_immutable():
    p1 = Pipeline(IterableSource([]))
    middleware = FilterMiddleware(lambda x: True)
    p2 = p1.pipe(middleware)
    assert p1._middlewares == []
    assert len(p2._middlewares) == 1


def test_pipeline_filter_shorthand():
    p = Pipeline(IterableSource([])).filter(lambda x: x > 0)
    assert len(p._middlewares) == 1
    assert isinstance(p._middlewares[0], FilterMiddleware)


# ======================================================================
# BoundPipeline.run() tests
# ======================================================================


@pytest.mark.asyncio
async def test_run_consumes_all_records():
    source = make_source(10)
    pipeline = Pipeline(source).build(StdoutSink())
    summary = await pipeline.run()
    assert summary.records_consumed == 10
    assert summary.records_written == 10
    assert summary.records_dropped == 0
    assert summary.records_errored == 0
    assert summary.by_source == {"iterable": 10}


@pytest.mark.asyncio
async def test_run_with_max_records():
    source = make_source(100)
    pipeline = Pipeline(source).build(StdoutSink())
    summary = await pipeline.run(max_records=5)
    assert summary.records_consumed == 5
    assert summary.records_written == 5


class InfiniteCounterSource(BaseSource[int]):
    source_name = "infinite_counter"

    async def stream(self):
        value = 0
        while True:
            yield value
            value += 1


class CollectSink:
    sink_name = "collect"

    def __init__(self) -> None:
        self.records: list[int] = []

    async def open(self) -> None:
        pass

    async def write(self, record: int) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


class BatchCollectSink:
    sink_name = "batch_collect"

    def __init__(self) -> None:
        self.records: list[int] = []
        self.batches: list[list[int]] = []
        self.single_write_calls = 0

    async def open(self) -> None:
        pass

    async def write(self, record: int) -> None:
        self.single_write_calls += 1
        self.records.append(record)

    async def write_batch(self, records: list[int]) -> None:
        self.batches.append(list(records))
        self.records.extend(records)

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


class BufferedPassThroughMiddleware(Middleware[int, int]):
    name = "buffered_passthrough"

    def __init__(self, batch_size: int = 2) -> None:
        self.min_concurrency = batch_size
        self._batch_size = batch_size
        self._pending: list[tuple[int, asyncio.Future[int | None]]] = []

    async def process(self, record: int, ctx) -> int | None:
        return record

    async def submit(self, record: int, ctx) -> asyncio.Future[int | None]:
        future: asyncio.Future[int | None] = asyncio.get_running_loop().create_future()
        self._pending.append((record, future))
        if len(self._pending) >= self._batch_size:
            await self._flush_pending()
        return future

    async def drain_pending(self, ctx) -> None:
        await self._flush_pending()

    async def _flush_pending(self) -> None:
        batch, self._pending = self._pending, []
        for record, future in batch:
            if not future.done():
                future.set_result(record)


class DelayedBufferedTransformMiddleware(Middleware[int, int]):
    def __init__(
        self,
        *,
        add: int,
        delays: dict[int, float] | None = None,
        name: str = "delayed_buffered",
        min_concurrency: int = 2,
    ) -> None:
        self.name = name
        self.min_concurrency = min_concurrency
        self._add = add
        self._delays = delays or {}

    async def process(self, record: int, ctx) -> int | None:
        del ctx
        return record

    async def submit(self, record: int, ctx) -> asyncio.Future[int | None]:
        del ctx
        future: asyncio.Future[int | None] = asyncio.get_running_loop().create_future()
        resolve_task: asyncio.Task[None] | None = None

        async def _resolve() -> None:
            delay = self._delays.get(record, 0.0)
            if delay > 0:
                await asyncio.sleep(delay)
            future.set_result(record + self._add)

        resolve_task = asyncio.create_task(_resolve())
        future.add_done_callback(lambda _: resolve_task)
        return future

    async def drain_pending(self, ctx) -> None:
        del ctx


class BlockingBufferedMiddleware(Middleware[int, int]):
    name = "blocking_buffered"

    def __init__(self, expected_records: int) -> None:
        self.min_concurrency = expected_records
        self._expected_records = expected_records
        self._started = 0
        self.all_started = asyncio.Event()
        self.cancelled: list[int] = []
        self.stopped = False

    async def process(self, record: int, ctx) -> int | None:
        del ctx
        return record

    async def submit(self, record: int, ctx) -> asyncio.Task[int]:
        del ctx
        self._started += 1
        if self._started >= self._expected_records:
            self.all_started.set()

        async def _resolve() -> int:
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                self.cancelled.append(record)
                raise

        return asyncio.create_task(_resolve())

    async def drain_pending(self, ctx) -> None:
        del ctx

    async def on_stop(self, ctx) -> None:
        del ctx
        self.stopped = True


class PrefetchSafeSource(BaseSource[int]):
    source_name = "prefetch_safe"
    supports_prefetch = True
    prefetch_limit = 2

    def __init__(self, count: int) -> None:
        self._count = count
        self.yielded_count = 0

    async def stream(self):
        for value in range(self._count):
            self.yielded_count += 1
            yield value


class HookTrackingSource(BaseSource[int]):
    source_name = "hook_tracking"

    def __init__(self, records: list[int], target: list[int]) -> None:
        self._records = records
        self._target = target
        self._current: int | None = None

    def delivery_success_callback(self):
        record = self._current
        if record is None:
            return None

        async def _ack() -> None:
            self._target.append(record)

        return _ack

    async def stream(self):
        for record in self._records:
            self._current = record
            yield record


class SlowCollectSink:
    sink_name = "slow_collect"

    def __init__(self, source: PrefetchSafeSource) -> None:
        self._source = source
        self.records: list[int] = []
        self.max_gap = 0

    async def open(self) -> None:
        pass

    async def write(self, record: int) -> None:
        self.max_gap = max(self.max_gap, self._source.yielded_count - len(self.records))
        await asyncio.sleep(0.01)
        self.records.append(record)

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


@pytest.mark.asyncio
async def test_run_with_max_records_stops_infinite_source_without_waiting_for_next_record():
    sink = CollectSink()
    summary = await asyncio.wait_for(
        Pipeline(InfiniteCounterSource()).build(sink).run(max_records=3),
        timeout=1.0,
    )

    assert summary.records_consumed == 3
    assert summary.records_written == 3
    assert sink.records == [0, 1, 2]


@pytest.mark.asyncio
async def test_run_with_max_records_stops_infinite_buffered_pipeline_without_waiting():
    sink = CollectSink()
    summary = await asyncio.wait_for(
        (
            Pipeline(InfiniteCounterSource())
            .pipe(BufferedPassThroughMiddleware(batch_size=2))
            .build(sink)
            .run(max_records=3)
        ),
        timeout=1.0,
    )

    assert summary.records_consumed == 3
    assert summary.records_written == 3
    assert sink.records == [0, 1, 2]


@pytest.mark.asyncio
async def test_safe_prefetch_keeps_runner_bounded_while_preserving_order():
    source = PrefetchSafeSource(count=6)
    sink = SlowCollectSink(source)

    summary = await Pipeline(source).build(sink).run()

    assert summary.records_consumed == 6
    assert summary.records_written == 6
    assert sink.records == [0, 1, 2, 3, 4, 5]
    assert sink.max_gap <= 3
    assert summary.runtime.source_prefetch_enabled is True
    assert summary.runtime.source_prefetch_limit == 2
    assert summary.runtime.source_prefetch_max_depth <= 2
    assert summary.runtime.source_prefetch_block_count >= 1


@pytest.mark.asyncio
async def test_source_delivery_success_hook_runs_after_successful_write() -> None:
    acknowledged: list[int] = []
    sink = CollectSink()

    summary = (
        await Pipeline(HookTrackingSource([1, 2, 3], acknowledged))
        .build(
            sink  # type: ignore[arg-type]
        )
        .run()
    )

    assert summary.records_written == 3
    assert sink.records == [1, 2, 3]
    assert acknowledged == [1, 2, 3]


@pytest.mark.asyncio
async def test_buffered_pipeline_cancellation_cancels_pending_tasks_and_preserves_shutdown() -> (
    None
):
    middleware = BlockingBufferedMiddleware(expected_records=4)
    events: list[str] = []

    class _FailingCloseSink:
        sink_name = "failing_close"

        async def open(self) -> None:
            events.append("sink.open")

        async def write(self, record: int) -> None:
            events.append(f"sink.write:{record}")

        async def flush(self) -> None:
            events.append("sink.flush")

        async def close(self) -> None:
            events.append("sink.close")
            raise RuntimeError("close broke")

    task = asyncio.create_task(
        Pipeline(IterableSource([1, 2, 3, 4]))
        .pipe(middleware)
        .build(_FailingCloseSink())  # type: ignore[arg-type]
        .run()
    )

    await asyncio.wait_for(middleware.all_started.wait(), timeout=1.0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert sorted(middleware.cancelled) == [1, 2, 3, 4]
    assert middleware.stopped is True
    assert events == ["sink.open", "sink.flush", "sink.close"]


@pytest.mark.asyncio
async def test_buffered_pipeline_drain_failure_cancels_pending_tasks() -> None:
    events: list[str] = []

    class FailingDrainBufferedMiddleware(Middleware[int, int]):
        name = "failing_drain_buffered"

        def __init__(self, expected_records: int) -> None:
            self.min_concurrency = expected_records
            self._expected_records = expected_records
            self._started = 0
            self._release = asyncio.Event()
            self.cancelled: list[int] = []
            self.all_started = asyncio.Event()

        async def process(self, record: int, ctx) -> int | None:
            del ctx
            return record

        async def submit(self, record: int, ctx) -> asyncio.Task[int]:
            del ctx
            self._started += 1
            if self._started >= self._expected_records:
                self.all_started.set()

            async def _resolve() -> int:
                try:
                    await self._release.wait()
                    return record
                except asyncio.CancelledError:
                    self.cancelled.append(record)
                    raise

            return asyncio.create_task(_resolve())

        async def drain_pending(self, ctx) -> None:
            del ctx
            await asyncio.sleep(0)
            raise RuntimeError("drain broke")

    class TrackingSink:
        sink_name = "tracking"

        async def open(self) -> None:
            events.append("sink.open")

        async def write(self, record: int) -> None:
            events.append(f"sink.write:{record}")

        async def flush(self) -> None:
            events.append("sink.flush")

        async def close(self) -> None:
            events.append("sink.close")

    middleware = FailingDrainBufferedMiddleware(expected_records=3)

    with pytest.raises(RuntimeError, match="drain broke"):
        await (
            Pipeline(IterableSource([1, 2]))
            .pipe(middleware)
            .build(TrackingSink())  # type: ignore[arg-type]
            .run()
        )

    assert sorted(middleware.cancelled) == [1, 2]
    assert events == ["sink.open", "sink.flush", "sink.close"]


@pytest.mark.asyncio
async def test_writer_batch_size_uses_sink_batch_path_and_flushes_tail() -> None:
    sink = BatchCollectSink()

    summary = await (
        Pipeline(IterableSource(list(range(7))))
        .build(sink, config=DeliveryConfig(batch_size=3))  # type: ignore[arg-type]
        .run()
    )

    assert summary.records_written == 7
    assert sink.records == [0, 1, 2, 3, 4, 5, 6]
    assert sink.batches == [[0, 1, 2], [3, 4, 5], [6]]
    assert sink.single_write_calls == 0
    assert summary.runtime.writer_flush_count == 3
    assert summary.runtime.writer_flush_max_batch_size == 3
    assert summary.runtime.writer_flush_time_ms >= 0.0


@pytest.mark.asyncio
async def test_writer_batch_size_preserves_delivery_success_hooks_with_batch_sink() -> None:
    acknowledged: list[int] = []
    sink = BatchCollectSink()

    summary = await (
        Pipeline(HookTrackingSource([1, 2, 3, 4], acknowledged))
        .build(sink, config=DeliveryConfig(batch_size=2))  # type: ignore[arg-type]
        .run()
    )

    assert summary.records_written == 4
    assert sink.records == [1, 2, 3, 4]
    assert acknowledged == [1, 2, 3, 4]


@pytest.mark.asyncio
async def test_buffered_stage_runtime_metrics_capture_in_flight_pressure():
    sink = CollectSink()

    summary = await (
        Pipeline(IterableSource([1, 2, 3, 4]))
        .pipe(BufferedPassThroughMiddleware(batch_size=2))
        .build(sink)
        .run()
    )

    assert summary.records_written == 4
    assert summary.runtime.buffered_stage_limit == 2
    assert summary.runtime.buffered_stage_max_in_flight == 2
    # Ready buffered tasks are now drained in groups instead of one-by-one.
    assert summary.runtime.buffered_stage_drain_count == 2


@pytest.mark.asyncio
async def test_multiple_buffered_stages_flush_tail_records_at_end_of_stream() -> None:
    sink = CollectSink()

    summary = await (
        Pipeline(IterableSource([1, 2, 3]))
        .pipe(BufferedPassThroughMiddleware(batch_size=2))
        .pipe(BufferedPassThroughMiddleware(batch_size=2))
        .build(sink)
        .run()
    )

    assert summary.records_consumed == 3
    assert summary.records_written == 3
    assert sink.records == [1, 2, 3]


@pytest.mark.asyncio
async def test_multiple_buffered_stages_preserve_output_order_with_out_of_order_completion() -> (
    None
):
    sink = CollectSink()

    summary = await (
        Pipeline(IterableSource([0, 1, 2, 3]))
        .pipe(
            DelayedBufferedTransformMiddleware(
                add=10,
                delays={0: 0.03},
                name="stage_one",
                min_concurrency=2,
            )
        )
        .pipe(
            DelayedBufferedTransformMiddleware(
                add=100,
                name="stage_two",
                min_concurrency=2,
            )
        )
        .build(sink)
        .run()
    )

    assert summary.records_consumed == 4
    assert summary.records_written == 4
    assert sink.records == [110, 111, 112, 113]


@pytest.mark.asyncio
async def test_filter_drops_records():
    source = make_source(10)
    pipeline = Pipeline(source).filter(lambda x: x % 2 == 0, name="even_only").build(StdoutSink())
    summary = await pipeline.run()
    assert summary.records_consumed == 10
    assert summary.records_written == 5  # 0, 2, 4, 6, 8
    assert summary.records_dropped == 5


@pytest.mark.asyncio
async def test_map_transforms_records():
    collected: list[int] = []

    class CollectSink:
        sink_name = "collect"

        async def open(self) -> None:
            pass

        async def write(self, record):
            collected.append(record)

        async def flush(self):
            pass

        async def close(self):
            pass

    source = make_source(5)
    pipeline = (
        Pipeline(source).pipe(MapMiddleware(lambda x: x * 10, name="multiply")).build(CollectSink())  # type: ignore[arg-type]
    )
    await pipeline.run()
    assert collected == [0, 10, 20, 30, 40]


@pytest.mark.asyncio
async def test_run_returns_summary_with_elapsed():
    pipeline = Pipeline(make_source(3)).build(StdoutSink())
    summary = await pipeline.run()
    assert summary.elapsed_seconds >= 0.0
    assert str(summary).startswith("PipelineRunSummary(")


@pytest.mark.asyncio
async def test_empty_source():
    pipeline = Pipeline(IterableSource([])).build(StdoutSink())
    summary = await pipeline.run()
    assert summary.records_consumed == 0
    assert summary.records_written == 0


@pytest.mark.asyncio
async def test_partial_sink_errors_fail_closed_by_default() -> None:
    class OkSink:
        sink_name = "ok"

        async def open(self) -> None:
            pass

        async def write(self, record):
            return None

        async def flush(self):
            pass

        async def close(self):
            pass

    class FailingSink:
        sink_name = "failing"

        async def open(self) -> None:
            pass

        async def write(self, record):
            raise RuntimeError("boom")

        async def flush(self):
            pass

        async def close(self):
            pass

    with pytest.raises(RuntimeError, match="boom"):
        await (
            Pipeline(IterableSource([1]))
            .fan_out([OkSink(), FailingSink()])  # type: ignore[list-item]
            .run()
        )


@pytest.mark.asyncio
async def test_partial_sink_errors_can_log_and_continue_when_opted_in() -> None:
    class OkSink:
        sink_name = "ok"

        async def open(self) -> None:
            pass

        async def write(self, record):
            return None

        async def flush(self):
            pass

        async def close(self):
            pass

    class FailingSink:
        sink_name = "failing"

        async def open(self) -> None:
            pass

        async def write(self, record):
            raise RuntimeError("boom")

        async def flush(self):
            pass

        async def close(self):
            pass

    summary = await (
        Pipeline(IterableSource([1]))
        .fan_out([OkSink(), FailingSink()], config=DeliveryConfig(sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE))  # type: ignore[list-item]
        .run()
    )

    assert summary.records_consumed == 1
    assert summary.records_written == 1
    assert summary.records_errored == 1
    assert summary.records_dropped == 0


@pytest.mark.asyncio
async def test_sink_open_failure_still_stops_started_middlewares() -> None:
    events: list[str] = []

    class TrackingMiddleware(Middleware[int, int]):
        name = "tracking"

        async def on_start(self, ctx) -> None:
            events.append("middleware.start")

        async def on_stop(self, ctx) -> None:
            events.append("middleware.stop")

        async def process(self, record: int, ctx) -> int | None:
            return record

    class FailingOpenSink:
        sink_name = "failing_open"

        async def open(self) -> None:
            events.append("sink.open")
            raise RuntimeError("sink open broke")

        async def write(self, record: int) -> None:
            events.append(f"sink.write:{record}")

        async def flush(self) -> None:
            events.append("sink.flush")

        async def close(self) -> None:
            events.append("sink.close")

    with pytest.raises(RuntimeError, match="sink open broke"):
        await (
            Pipeline(IterableSource([1]))
            .pipe(TrackingMiddleware())
            .build(FailingOpenSink())  # type: ignore[arg-type]
            .run()
        )

    assert events == ["middleware.start", "sink.open", "middleware.stop"]


@pytest.mark.asyncio
async def test_middleware_start_failure_rolls_back_started_middlewares() -> None:
    events: list[str] = []

    class TrackingMiddleware(Middleware[int, int]):
        name = "tracking"

        async def on_start(self, ctx) -> None:
            del ctx
            events.append("tracking.start")

        async def on_stop(self, ctx) -> None:
            del ctx
            events.append("tracking.stop")

        async def process(self, record: int, ctx) -> int | None:
            del ctx
            return record

    class FailingStartMiddleware(Middleware[int, int]):
        name = "failing_start"

        async def on_start(self, ctx) -> None:
            del ctx
            events.append("failing.start")
            raise RuntimeError("middleware start broke")

        async def on_stop(self, ctx) -> None:
            del ctx
            events.append("failing.stop")

        async def process(self, record: int, ctx) -> int | None:
            del ctx
            return record

    with pytest.raises(RuntimeError, match="middleware start broke"):
        await (
            Pipeline(IterableSource([1]))
            .pipe(TrackingMiddleware())
            .pipe(FailingStartMiddleware())
            .build(CollectSink())  # type: ignore[arg-type]
            .run()
        )

    assert events == [
        "tracking.start",
        "failing.start",
        "failing.stop",
        "tracking.stop",
    ]


@pytest.mark.asyncio
async def test_dlq_open_failure_closes_started_writer() -> None:
    events: list[str] = []

    class TrackingWriterSink:
        sink_name = "writer"

        async def open(self) -> None:
            events.append("writer.open")

        async def write(self, record: int) -> None:
            events.append(f"writer.write:{record}")

        async def flush(self) -> None:
            events.append("writer.flush")

        async def close(self) -> None:
            events.append("writer.close")

    class FailingDLQSink:
        sink_name = "dlq"

        async def open(self) -> None:
            events.append("dlq.open")
            raise RuntimeError("dlq open broke")

        async def write(self, record) -> None:
            events.append(f"dlq.write:{record}")

        async def flush(self) -> None:
            events.append("dlq.flush")

        async def close(self) -> None:
            events.append("dlq.close")

    with pytest.raises(RuntimeError, match="dlq open broke"):
        await (
            Pipeline(IterableSource([1]))
            .build(TrackingWriterSink(), config=DeliveryConfig(dlq=FailingDLQSink()))  # type: ignore[arg-type]
            .run()
        )

    assert events == ["writer.open", "dlq.open", "writer.close"]


@pytest.mark.asyncio
async def test_shutdown_closes_sink_even_if_middleware_stop_fails() -> None:
    events: list[str] = []

    class FailingStopMiddleware(Middleware[int, int]):
        name = "failing_stop"

        async def process(self, record: int, ctx) -> int | None:
            return record

        async def on_stop(self, ctx) -> None:
            events.append("middleware.stop")
            raise RuntimeError("middleware stop broke")

    class TrackingSink:
        sink_name = "tracking_sink"

        async def open(self) -> None:
            events.append("sink.open")

        async def write(self, record: int) -> None:
            events.append(f"sink.write:{record}")

        async def flush(self) -> None:
            events.append("sink.flush")

        async def close(self) -> None:
            events.append("sink.close")

    # stop_all now logs and continues on error instead of raising,
    # so the pipeline completes and sink is still flushed/closed.
    summary = await (
        Pipeline(IterableSource([1]))
        .pipe(FailingStopMiddleware())
        .build(TrackingSink())  # type: ignore[arg-type]
        .run()
    )

    assert summary.records_written == 1
    assert events == [
        "sink.open",
        "sink.write:1",
        "middleware.stop",
        "sink.flush",
        "sink.close",
    ]


@pytest.mark.asyncio
async def test_stop_all_continues_stopping_remaining_middlewares_after_one_fails() -> None:
    events: list[str] = []

    class _FailingStopMiddleware(Middleware[int, int]):
        name = "failing_stop"

        async def on_stop(self, ctx) -> None:
            events.append("failing.stop")
            raise RuntimeError("stop broke")

        async def process(self, record: int, ctx) -> int | None:
            return record

    class _TrackingMiddleware(Middleware[int, int]):
        name = "tracking"

        async def on_stop(self, ctx) -> None:
            events.append("tracking.stop")

        async def process(self, record: int, ctx) -> int | None:
            return record

    sink = CollectSink()
    # _FailingStopMiddleware is first, _TrackingMiddleware is second.
    # stop_all iterates reversed, so tracking.stop runs first, then failing.stop.
    # Both must run regardless of failure order.
    summary = await (
        Pipeline(IterableSource([1]))
        .pipe(_TrackingMiddleware())
        .pipe(_FailingStopMiddleware())
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert summary.records_written == 1
    # Both middlewares must have been stopped even though one raised.
    assert "tracking.stop" in events
    assert "failing.stop" in events
