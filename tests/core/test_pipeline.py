"""
Tests for the core pipeline builder and BoundPipeline runner.
"""

from __future__ import annotations

import asyncio
from typing import ClassVar

import pytest

from agora import (
    DeliveryConfig,
    FilterMiddleware,
    InMemoryTracer,
    IterableSource,
    MapMiddleware,
    Pipeline,
    SinkFailurePolicy,
)
from agora.core.context import PipelineContext
from agora.core.data_plane import DataPlane, SourceDataPlaneSpec
from agora.core.errors import PipelineError
from agora.core.fencing import FenceLostError, RunFence
from agora.core.metrics import PipelineMetrics
from agora.core.middleware import Middleware, MiddlewareChain, MiddlewareFailure
from agora.core.runtime import _buffered, _lanes, _source_adapter
from agora.core.runtime._delivery import SourceRecord
from agora.core.runtime._plan import BufferedStageSpec
from agora.core.sink import BaseSink
from agora.core.source import BaseSource
from agora.sinks.file.csv import CsvSink
from agora.sinks.file.jsonlines import JsonLinesSink
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
    pipeline = Pipeline(source).build()
    assert pipeline.pipeline_id == "test_source"


def test_pipeline_from_source_custom_id():
    pipeline = Pipeline(IterableSource([]), id="my_pipe").build()
    assert pipeline.pipeline_id == "my_pipe"


def test_pipeline_pipe_is_immutable():
    p1 = Pipeline(IterableSource([]))
    middleware = FilterMiddleware(lambda x: True)
    p2 = p1.pipe(middleware)
    assert len(p1.build().explain().middleware_matrix) == 0
    assert len(p2.build().explain().middleware_matrix) == 1


def test_pipeline_filter_shorthand():
    p = Pipeline(IterableSource([])).filter(lambda x: x > 0)
    explain = p.build().explain()
    assert len(explain.middleware_matrix) == 1
    assert explain.middleware_matrix[0].name == "filter"


async def test_bound_pipeline_with_sink_preserves_concurrency_and_live_metrics_callback() -> None:
    callback_calls = 0

    async def _callback(ctx) -> None:
        nonlocal callback_calls
        del ctx
        callback_calls += 1

    class RecordingSink(BaseSink[int]):
        def __init__(self) -> None:
            self.records: list[int] = []

        async def write(self, record: int) -> None:
            self.records.append(record)

    bound = Pipeline(IterableSource([1, 2, 3])).build(
        StdoutSink(),
        config=DeliveryConfig(sink_concurrency=3),
    )
    bound.set_live_metrics_callback(_callback)
    sink = RecordingSink()

    replaced = bound.with_sink(sink)
    await replaced.run()

    assert replaced is not bound
    assert replaced.config.sink_concurrency == 3
    assert sink.records == [1, 2, 3]
    assert callback_calls >= 1


async def test_bound_pipeline_run_fence_blocks_stale_sink_write() -> None:
    class RecordingSink(BaseSink[int]):
        def __init__(self) -> None:
            self.records: list[int] = []

        async def write(self, record: int) -> None:
            self.records.append(record)

    async def _stale() -> bool:
        return False

    sink = RecordingSink()
    bound = Pipeline(IterableSource([1]), id="fenced").build(sink)
    bound.set_run_fence(
        RunFence(
            pipeline_id="fenced",
            worker_id="worker-a",
            fencing_token=7,
            validate=_stale,
        )
    )

    with pytest.raises(FenceLostError):
        await bound.run()

    assert sink.records == []


async def test_bound_pipeline_run_fence_is_preserved_when_replacing_sink() -> None:
    class RecordingSink(BaseSink[int]):
        def __init__(self) -> None:
            self.records: list[int] = []

        async def write(self, record: int) -> None:
            self.records.append(record)

    async def _stale() -> bool:
        return False

    bound = Pipeline(IterableSource([1]), id="fenced_replace").build(StdoutSink())
    bound.set_run_fence(
        RunFence(
            pipeline_id="fenced_replace",
            worker_id="worker-a",
            fencing_token=7,
            validate=_stale,
        )
    )
    sink = RecordingSink()
    replaced = bound.with_sink(sink)

    with pytest.raises(FenceLostError):
        await replaced.run()

    assert sink.records == []


def test_bound_pipeline_explain_reports_pre_run_shape() -> None:
    pipeline = (
        Pipeline(make_source(5))
        .pipe(MapMiddleware(lambda x: x * 2, name="double"))
        .build(
            StdoutSink(),
            config=DeliveryConfig(acceleration_mode="off", performance_profile="low_latency"),
        )
    )

    explain = pipeline.explain(max_records=2)

    assert explain.pipeline_id == "iterable"
    assert explain.source_limit == 2
    assert explain.planned_lane == "linear"
    assert "no buffered stage requires submit() concurrency" in explain.lane_reason
    assert explain.source_data_plane == DataPlane.PYTHON_ROWS
    assert explain.writer_input_data_plane == DataPlane.PYTHON_ROWS
    assert (
        explain.writer_input_data_plane_reason == "writer receives middleware output as python_rows"
    )
    assert explain.middleware_matrix[0].name == "double"
    assert explain.middleware_matrix[0].data_plane == DataPlane.PYTHON_ROWS
    assert explain.sinks[0].sink_name == "stdout"
    assert explain.sinks[0].selection_reason == "sink accepts python_rows natively"
    assert explain.sink_downgrade_count == 0
    assert explain.acceleration.mode == "off"
    assert explain.acceleration.profile == "low_latency"
    assert explain.acceleration.available is False
    assert explain.acceleration.direct_flush_eligible is False
    assert explain.acceleration.direct_flush_inactive_reason == "writer batch size is 1"
    assert explain.to_dict()["lane_reason"] == explain.lane_reason
    assert explain.to_dict()["source_limit"] == 2
    assert explain.to_dict()["acceleration"]["mode"] == "off"
    assert "PipelineExplain(" in str(explain)
    assert "acceleration=off/unavailable" in str(explain)


def test_throughput_profile_resolves_explicit_runtime_settings() -> None:
    bound = Pipeline(make_source(5)).build(
        StdoutSink(),
        config=DeliveryConfig(performance_profile="throughput"),
    )

    assert bound.config.batch_size == 1_000
    assert bound.config.batch_flush_interval_ms == 100
    assert bound.config.max_buffer_size == 1_024
    assert bound.config.backpressure is not None
    assert bound.config.backpressure.max_buffer_size == 4_096

    explain = bound.explain()
    settings = explain.acceleration.profile_settings
    assert settings["profile"] == "throughput"
    assert settings["writer_batch_size"] == 1_000
    assert settings["flush_cadence_ms"] == 100
    assert settings["max_in_flight_batches"] == 1_024


def test_manual_delivery_settings_win_over_throughput_profile() -> None:
    bound = Pipeline(make_source(5)).build(
        StdoutSink(),
        config=DeliveryConfig(
            performance_profile="throughput",
            batch_size=25,
            batch_flush_interval_ms=7,
            max_buffer_size=9,
        ),
    )

    assert bound.config.batch_size == 25
    assert bound.config.batch_flush_interval_ms == 7
    assert bound.config.max_buffer_size == 9


def test_explain_reports_source_prefetch_benchmark_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _SyncPrefetchSource(BaseSource[int]):
        source_name = "sync_prefetch"
        supports_rust_prefetch = True
        prefetch_limit = 2

        async def stream(self):
            yield 1

        def stream_sync_batches(self):
            yield 1

    monkeypatch.setattr(
        "agora.core.explain._builders.acceleration_status",
        lambda mode: type(
            "Status",
            (),
            {
                "mode": type("Mode", (), {"value": "auto"})(),
                "enabled": True,
                "version": "0.2.0",
                "compatible": True,
                "capabilities": frozenset(),
                "reason": None,
                "supports": lambda self, capability: False,
            },
        )(),
    )

    balanced = Pipeline(_SyncPrefetchSource()).build(StdoutSink()).explain()
    throughput = (
        Pipeline(_SyncPrefetchSource())
        .build(StdoutSink(), config=DeliveryConfig(performance_profile="throughput"))
        .explain()
    )

    assert balanced.acceleration.source_prefetch_eligible is True
    assert balanced.acceleration.source_prefetch_active is False
    assert (
        balanced.acceleration.source_prefetch_inactive_reason
        == "benchmark gate not enabled for that lane"
    )
    assert throughput.acceleration.source_prefetch_active is True
    assert throughput.acceleration.source_prefetch_inactive_reason is None


def test_pipeline_explain_reports_arrow_fanout_sink_downgrades() -> None:
    class _ArrowBatchSource(BaseSource[int]):
        source_name = "arrow_batch_source"

        async def stream(self):
            yield 1

        async def stream_batches(self):  # type: ignore[override]
            yield []

        def data_plane_spec(self) -> SourceDataPlaneSpec:
            return SourceDataPlaneSpec(
                source_name=self.source_name,
                emitted_plane=DataPlane.ARROW_BATCHES,
                supports_batch_emit=True,
                emits_arrow_batches=True,
            )

    class _ArrowSink(BaseSink[int]):
        sink_name = "arrow_sink"
        accepted_data_planes = (
            DataPlane.PYTHON_ROWS,
            DataPlane.ARROW_BATCHES,
        )
        native_data_planes = accepted_data_planes

        async def write(self, record: int) -> None:
            del record

        async def write_arrow_batch(self, batch) -> None:
            del batch

    class _RowSink(BaseSink[int]):
        sink_name = "row_sink"

        async def write(self, record: int) -> None:
            del record

    explain = Pipeline(_ArrowBatchSource()).fan_out([_ArrowSink(), _RowSink()]).explain()

    assert explain.planned_lane == "batch"
    assert explain.source_data_plane == DataPlane.ARROW_BATCHES
    assert explain.writer_input_data_plane == DataPlane.ARROW_BATCHES
    assert explain.arrow_chain_eligible is True
    assert explain.arrow_fast_path_eligible is True
    assert "only downgrades for sink paths" in explain.writer_input_data_plane_reason
    assert explain.sink_downgrade_count == 1
    assert [sink.selected_data_plane for sink in explain.sinks] == [
        DataPlane.ARROW_BATCHES,
        DataPlane.PYTHON_ROWS,
    ]
    assert explain.sinks[0].selection_reason == "sink accepts arrow_batches natively"
    assert "writer downgrades to python_rows" in explain.sinks[1].selection_reason
    assert explain.acceleration.expected_row_materialization_points == (
        f"{explain.sinks[1].sink_name}: {explain.sinks[1].selection_reason}",
    )
    assert "(downgraded)" in str(explain)


def test_pipeline_explain_file_fanout_keeps_arrow_boundary(tmp_path) -> None:
    class _ArrowBatchSource(BaseSource[int]):
        source_name = "arrow_batch_source"

        async def stream(self):
            yield 1

        async def stream_batches(self):  # type: ignore[override]
            yield []

        def data_plane_spec(self) -> SourceDataPlaneSpec:
            return SourceDataPlaneSpec(
                source_name=self.source_name,
                emitted_plane=DataPlane.ARROW_BATCHES,
                supports_batch_emit=True,
                emits_arrow_batches=True,
            )

    explain = (
        Pipeline(_ArrowBatchSource())
        .fan_out(
            [
                JsonLinesSink(path=tmp_path / "out.jsonl", serializer=lambda row: row),
                CsvSink(path=tmp_path / "out.csv", row_mapper=lambda row: row),
            ]
        )
        .explain()
    )

    assert explain.writer_input_data_plane == DataPlane.ARROW_BATCHES
    assert [sink.selected_data_plane for sink in explain.sinks] == [
        DataPlane.ARROW_BATCHES,
        DataPlane.ARROW_BATCHES,
    ]


def test_pipeline_explain_reports_arrow_materialization_reason_for_row_chain() -> None:
    class _ArrowBatchSource(BaseSource[int]):
        source_name = "arrow_batch_source"

        async def stream(self):
            yield 1

        async def stream_batches(self):  # type: ignore[override]
            yield []

        def data_plane_spec(self) -> SourceDataPlaneSpec:
            return SourceDataPlaneSpec(
                source_name=self.source_name,
                emitted_plane=DataPlane.ARROW_BATCHES,
                supports_batch_emit=True,
                emits_arrow_batches=True,
            )

    class _RowMW(Middleware[int, int]):
        name = "row_stage"

        async def process(self, record: int, ctx) -> int | None:
            del ctx
            return record

    explain = Pipeline(_ArrowBatchSource()).pipe(_RowMW()).build(StdoutSink()).explain()

    assert explain.middleware_materializes_arrow_to_rows is True
    assert explain.middleware_materialization_reason is not None
    assert (
        "materialize once before middleware execution" in explain.middleware_materialization_reason
    )
    assert explain.writer_input_data_plane == DataPlane.PYTHON_BATCHES
    assert (
        "writer receives middleware output as python_batches"
        in explain.writer_input_data_plane_reason
    )


def test_pipeline_explain_tracks_arrow_batch_sink_boundary_materialization() -> None:
    class _ArrowBatchSource(BaseSource[int]):
        source_name = "arrow_batch_source"

        async def stream(self):
            yield 1

        async def stream_batches(self):  # type: ignore[override]
            yield []

        def data_plane_spec(self) -> SourceDataPlaneSpec:
            return SourceDataPlaneSpec(
                source_name=self.source_name,
                emitted_plane=DataPlane.ARROW_BATCHES,
                supports_batch_emit=True,
                emits_arrow_batches=True,
            )

    class _BatchSink(BaseSink[int]):
        sink_name = "batch_sink"
        accepted_data_planes = (
            DataPlane.PYTHON_ROWS,
            DataPlane.PYTHON_BATCHES,
        )
        native_data_planes = accepted_data_planes

        async def write(self, record: int) -> None:
            del record

        async def write_batch(self, records: list[int]) -> None:
            del records

    explain = Pipeline(_ArrowBatchSource()).fan_out([_BatchSink()]).explain()

    assert explain.writer_input_data_plane == DataPlane.ARROW_BATCHES
    assert "keeps arrow_batches until sink dispatch" in explain.writer_input_data_plane_reason
    assert explain.sink_downgrade_count == 1
    assert explain.sinks[0].selected_data_plane == DataPlane.PYTHON_BATCHES
    assert "writer downgrades to python_batches" in explain.sinks[0].selection_reason
    assert explain.acceleration.expected_row_materialization_points == (
        f"{explain.sinks[0].sink_name}: {explain.sinks[0].selection_reason}",
    )


def test_pipeline_explain_fail_fast_on_invalid_mixed_chain() -> None:
    from agora.core.batch import ArrowBatchMiddleware

    class _ArrowBatchSource(BaseSource[int]):
        source_name = "arrow_batch_source"

        async def stream(self):
            yield 1

        async def stream_batches(self):  # type: ignore[override]
            yield []

        def data_plane_spec(self) -> SourceDataPlaneSpec:
            return SourceDataPlaneSpec(
                source_name=self.source_name,
                emitted_plane=DataPlane.ARROW_BATCHES,
                supports_batch_emit=True,
                emits_arrow_batches=True,
            )

    class _ArrowMW(ArrowBatchMiddleware):
        name = "arrow_stage"

        async def process_arrow_batch(self, batch, ctx):
            del ctx
            return batch

    class _RowMW(Middleware[int, int]):
        name = "row_stage"

        async def process(self, record: int, ctx) -> int | None:
            del ctx
            return record

    pipeline = Pipeline(_ArrowBatchSource()).pipe(_ArrowMW()).pipe(_RowMW()).build(StdoutSink())

    with pytest.raises(PipelineError, match="mixes incompatible data planes"):
        pipeline.explain()


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


@pytest.mark.asyncio
async def test_fan_out_with_max_records_limits_at_source_boundary() -> None:
    seen_left: list[int] = []
    seen_right: list[int] = []

    class _CollectSink:
        sink_name = "collect"

        def __init__(self, target: list[int]) -> None:
            self._target = target

        async def open(self) -> None:
            return None

        async def write(self, record: int) -> None:
            self._target.append(record)

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    summary = await (
        Pipeline(IterableSource(list(range(100))))
        .fan_out([_CollectSink(seen_left), _CollectSink(seen_right)])  # type: ignore[list-item]
        .run(max_records=3)
    )

    assert summary.records_consumed == 3
    assert summary.records_written == 3
    assert seen_left == [0, 1, 2]
    assert seen_right == [0, 1, 2]


@pytest.mark.asyncio
async def test_batch_flush_interval_flushes_partial_batches_for_long_lived_sources():
    source = TimedBatchSource(values=[1, 2], delays_after_yield=[0.03, 0.0])
    sink = StrictBatchCollectSink()

    summary = await (
        Pipeline(source)
        .build(
            sink,
            config=DeliveryConfig(batch_size=100, batch_flush_interval_ms=10),
        )
        .run()
    )

    assert summary.records_consumed == 2
    assert summary.records_written == 2
    assert sink.batches == [[1], [2]]


@pytest.mark.asyncio
async def test_batch_flush_interval_does_not_duplicate_records_when_timeout_and_new_data_are_close():
    source = TimedBatchSource(values=[1, 2], delays_after_yield=[0.011, 0.0])
    sink = StrictBatchCollectSink()

    summary = await (
        Pipeline(source)
        .build(
            sink,
            config=DeliveryConfig(batch_size=2, batch_flush_interval_ms=10),
        )
        .run()
    )

    flattened = [record for batch in sink.batches for record in batch]

    assert summary.records_consumed == 2
    assert summary.records_written == 2
    assert flattened == [1, 2]
    assert len(flattened) == 2


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


class TimedBatchSource(BaseSource[int]):
    source_name = "timed_batch"

    def __init__(self, values: list[int], delays_after_yield: list[float]) -> None:
        self._values = values
        self._delays_after_yield = delays_after_yield

    async def stream(self):
        for value, delay in zip(self._values, self._delays_after_yield, strict=True):
            yield value
            if delay > 0:
                await asyncio.sleep(delay)


class BlockingAfterRecordsSource(BaseSource[int]):
    source_name = "blocking_after_records"

    def __init__(self, records: list[int]) -> None:
        self._records = records
        self.blocked = asyncio.Event()

    async def stream(self):
        for record in self._records:
            yield record
        self.blocked.set()
        await asyncio.Future()


class StrictBatchCollectSink:
    sink_name = "strict_batch_collect"

    def __init__(self) -> None:
        self.batches: list[list[int]] = []

    async def open(self) -> None:
        pass

    async def write(self, record: int) -> None:
        raise AssertionError(f"single-record write should not be used: {record}")

    async def write_batch(self, records: list[int]) -> None:
        self.batches.append(list(records))

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


class PartiallyBlockingBufferedMiddleware(Middleware[int, int]):
    name = "partially_blocking_buffered"

    def __init__(self) -> None:
        self.min_concurrency = 2
        self._started = 0
        self.all_started = asyncio.Event()
        self.first_resolved = asyncio.Event()
        self.cancelled: list[int] = []

    async def process(self, record: int, ctx) -> int | None:
        del ctx
        return record

    async def submit(self, record: int, ctx) -> asyncio.Task[int]:
        del ctx
        self._started += 1
        if self._started >= 2:
            self.all_started.set()

        async def _resolve() -> int:
            if record == 1:
                await asyncio.sleep(0)
                self.first_resolved.set()
                return record
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                self.cancelled.append(record)
                raise

        return asyncio.create_task(_resolve())

    async def drain_pending(self, ctx) -> None:
        del ctx


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
async def test_run_with_max_records_does_not_pull_blocking_fourth_record() -> None:
    source = BlockingAfterRecordsSource([0, 1, 2])
    sink = CollectSink()

    summary = await asyncio.wait_for(
        Pipeline(source).build(sink).run(max_records=3),
        timeout=1.0,
    )

    assert summary.records_consumed == 3
    assert summary.records_written == 3
    assert sink.records == [0, 1, 2]
    assert source.blocked.is_set() is False


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
async def test_python_prefetch_adapter_aclose_does_not_hang_when_queue_is_full() -> None:
    from types import SimpleNamespace

    class _InfinitePrefetchSource(BaseSource[int]):
        source_name = "infinite_prefetch"
        supports_prefetch = True
        prefetch_limit = 2

        async def stream(self):
            value = 0
            while True:
                yield value
                value += 1

    ctx = SimpleNamespace(
        metrics=SimpleNamespace(
            runtime=SimpleNamespace(
                source_prefetch_enabled=False,
                source_prefetch_limit=0,
                source_prefetch_block_count=0,
                source_prefetch_max_depth=0,
                rust_prefetch_active=False,
                rust_prefetch_wait_count=0,
                rust_prefetch_batch_drain_count=0,
                rust_prefetch_push_batch_count=0,
                source_record_error_count=0,
                source_record_drop_count=0,
            )
        ),
        log=SimpleNamespace(info=lambda *args, **kwargs: None),
    )

    adapter = _source_adapter.SourceRuntimeAdapter(
        source=_InfinitePrefetchSource(),
        has_buffered_stages=False,
    )
    stream = adapter.iter_source_records(ctx)

    first = await anext(stream)
    assert first.raw == 0

    await asyncio.wait_for(stream.aclose(), timeout=1.0)


@pytest.mark.asyncio
async def test_buffered_prefetch_source_without_rust_capability_uses_python_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        _source_adapter,
        "acceleration_status",
        lambda mode: type("Status", (), {"enabled": True, "reason": None})(),
    )

    source = PrefetchSafeSource(count=6)
    sink = CollectSink()

    summary = await (
        Pipeline(source).pipe(BufferedPassThroughMiddleware(batch_size=2)).build(sink).run()
    )

    assert summary.records_consumed == 6
    assert summary.records_written == 6
    assert sink.records == [0, 1, 2, 3, 4, 5]
    assert summary.runtime.source_prefetch_enabled is True
    assert summary.runtime.source_prefetch_limit == 2
    assert summary.runtime.execution_lane == "buffered"
    assert summary.runtime.rust_prefetch_active is False


@pytest.mark.asyncio
async def test_buffered_rust_prefetch_uses_blocking_wait_and_batch_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import threading
    from types import SimpleNamespace

    class _FakeRecordBuffer:
        instances: ClassVar[list[_FakeRecordBuffer]] = []

        def __init__(self, capacity: int) -> None:
            self.capacity = capacity
            self._items: list[object] = []
            self._closed = False
            self._cancelled = False
            self._cond = threading.Condition()
            self.push_batch_calls: list[int] = []
            self.wait_calls: list[int] = []
            self.pop_batch_calls: list[int] = []
            self.try_pop_calls = 0
            type(self).instances.append(self)

        def push(self, item: object) -> bool:
            with self._cond:
                while (
                    len(self._items) >= self.capacity and not self._closed and not self._cancelled
                ):
                    self._cond.wait(timeout=0.05)
                if self._cancelled:
                    raise RuntimeError("cancelled")
                if self._closed:
                    return False
                self._items.append(item)
                self._cond.notify_all()
                return True

        def push_batch(self, items: list[object]) -> int:
            self.push_batch_calls.append(len(items))
            pushed = 0
            for item in items:
                if not self.push(item):
                    return pushed
                pushed += 1
            return pushed

        def wait_for_item(self, timeout_ms: int) -> bool:
            with self._cond:
                self.wait_calls.append(timeout_ms)
                if not self._items and not self._closed and not self._cancelled:
                    self._cond.wait(timeout=timeout_ms / 1000)
                if self._cancelled:
                    raise RuntimeError("cancelled")
                return bool(self._items)

        def pop_batch(self, max_items: int) -> list[object]:
            with self._cond:
                self.pop_batch_calls.append(max_items)
                n = min(max_items, len(self._items))
                if n == 0:
                    return []
                batch = list(self._items[:n])
                del self._items[:n]
                self._cond.notify_all()
                return batch

        def try_pop(self) -> object | None:
            self.try_pop_calls += 1
            raise AssertionError("Prefetch v2 should drain via pop_batch(), not try_pop().")

        def close(self) -> None:
            with self._cond:
                self._closed = True
                self._cond.notify_all()

        def cancel(self) -> None:
            with self._cond:
                self._cancelled = True
                self._cond.notify_all()

        def size(self) -> int:
            with self._cond:
                return len(self._items)

        def is_done(self) -> bool:
            with self._cond:
                return self._closed and not self._items

    class _RustPrefetchSource(BaseSource[int]):
        source_name = "rust_prefetch_test"
        supports_prefetch = True
        supports_rust_prefetch = True
        prefetch_limit = 2

        async def stream(self):
            for value in range(6):
                yield value

        def stream_sync_batches(self):
            yield from range(6)

    monkeypatch.setattr(
        _source_adapter,
        "acceleration_status",
        lambda mode: type("Status", (), {"enabled": True, "reason": None})(),
    )
    monkeypatch.setattr(
        _source_adapter,
        "make_record_buffer",
        lambda capacity, *, mode: _FakeRecordBuffer(capacity),
    )
    _FakeRecordBuffer.instances.clear()

    ctx = SimpleNamespace(
        metrics=SimpleNamespace(
            runtime=SimpleNamespace(
                source_prefetch_enabled=False,
                source_prefetch_limit=0,
                source_prefetch_block_count=0,
                source_prefetch_max_depth=0,
                rust_prefetch_active=False,
                rust_prefetch_wait_count=0,
                rust_prefetch_batch_drain_count=0,
                rust_prefetch_push_batch_count=0,
                source_record_error_count=0,
                source_record_drop_count=0,
            )
        ),
        log=SimpleNamespace(info=lambda *args, **kwargs: None),
    )

    adapter = _source_adapter.SourceRuntimeAdapter(
        source=_RustPrefetchSource(),
        has_buffered_stages=True,
    )
    records = [record.raw async for record in adapter.iter_source_records(ctx)]

    assert records == [0, 1, 2, 3, 4, 5]
    assert ctx.metrics.runtime.source_prefetch_enabled is True
    assert ctx.metrics.runtime.source_prefetch_limit == 2
    assert ctx.metrics.runtime.source_prefetch_max_depth <= 2
    assert ctx.metrics.runtime.rust_prefetch_active is True
    assert ctx.metrics.runtime.rust_prefetch_wait_count >= 1
    assert ctx.metrics.runtime.rust_prefetch_batch_drain_count >= 1
    assert ctx.metrics.runtime.rust_prefetch_push_batch_count >= 1

    assert len(_FakeRecordBuffer.instances) == 1
    fake_buffer = _FakeRecordBuffer.instances[0]
    assert fake_buffer.push_batch_calls, "Rust prefetch producer should batch pushes."
    assert fake_buffer.wait_calls, "Rust prefetch should block via wait_for_item() when empty."
    assert fake_buffer.pop_batch_calls, "Rust prefetch should drain via pop_batch()."
    assert fake_buffer.try_pop_calls == 0


@pytest.mark.asyncio
async def test_python_prefetch_does_not_call_current_checkpoint_without_opt_in() -> None:
    from types import SimpleNamespace

    class _NoCheckpointPrefetchSource(BaseSource[int]):
        source_name = "no_checkpoint_prefetch"
        supports_prefetch = True
        prefetch_limit = 2

        def __init__(self) -> None:
            self.checkpoint_calls = 0

        def current_checkpoint(self):
            self.checkpoint_calls += 1
            return {"unexpected": self.checkpoint_calls}

        async def stream(self):
            for value in range(4):
                yield value

    ctx = SimpleNamespace(
        metrics=SimpleNamespace(
            runtime=SimpleNamespace(
                source_prefetch_enabled=False,
                source_prefetch_limit=0,
                source_prefetch_block_count=0,
                source_prefetch_max_depth=0,
                rust_prefetch_active=False,
                rust_prefetch_wait_count=0,
                rust_prefetch_batch_drain_count=0,
                rust_prefetch_push_batch_count=0,
                source_record_error_count=0,
                source_record_drop_count=0,
            )
        ),
        log=SimpleNamespace(info=lambda *args, **kwargs: None),
    )

    source = _NoCheckpointPrefetchSource()
    adapter = _source_adapter.SourceRuntimeAdapter(source=source, has_buffered_stages=False)
    records = [record async for record in adapter.iter_source_records(ctx)]

    assert [record.raw for record in records] == [0, 1, 2, 3]
    assert all(record.checkpoint is None for record in records)
    assert source.checkpoint_calls == 0


@pytest.mark.asyncio
async def test_rust_prefetch_does_not_call_current_checkpoint_without_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import threading
    from types import SimpleNamespace

    class _FakeRecordBuffer:
        def __init__(self, capacity: int) -> None:
            self.capacity = capacity
            self._items: list[object] = []
            self._closed = False
            self._cond = threading.Condition()

        def push(self, item: object) -> bool:
            with self._cond:
                if self._closed or len(self._items) >= self.capacity:
                    return False
                self._items.append(item)
                self._cond.notify_all()
                return True

        def push_batch(self, items: list[object]) -> int:
            pushed = 0
            for item in items:
                if not self.push(item):
                    return pushed
                pushed += 1
            return pushed

        def wait_for_item(self, timeout_ms: int) -> bool:
            with self._cond:
                if not self._items and not self._closed:
                    self._cond.wait(timeout=timeout_ms / 1000)
                return bool(self._items)

        def pop_batch(self, max_items: int) -> list[object]:
            with self._cond:
                n = min(max_items, len(self._items))
                if n == 0:
                    return []
                batch = list(self._items[:n])
                del self._items[:n]
                return batch

        def close(self) -> None:
            with self._cond:
                self._closed = True
                self._cond.notify_all()

        def is_done(self) -> bool:
            with self._cond:
                return self._closed and not self._items

    class _NoCheckpointRustSource(BaseSource[int]):
        source_name = "no_checkpoint_rust_prefetch"
        supports_rust_prefetch = True
        prefetch_limit = 2

        def __init__(self) -> None:
            self.checkpoint_calls = 0

        def current_checkpoint(self):
            self.checkpoint_calls += 1
            return {"unexpected": self.checkpoint_calls}

        async def stream(self):
            for value in range(4):
                yield value

        def stream_sync_batches(self):
            yield from range(4)

    monkeypatch.setattr(
        _source_adapter,
        "acceleration_status",
        lambda mode: type("Status", (), {"enabled": True, "reason": None})(),
    )
    monkeypatch.setattr(
        _source_adapter,
        "make_record_buffer",
        lambda capacity, *, mode: _FakeRecordBuffer(capacity),
    )

    ctx = SimpleNamespace(
        metrics=SimpleNamespace(
            runtime=SimpleNamespace(
                source_prefetch_enabled=False,
                source_prefetch_limit=0,
                source_prefetch_block_count=0,
                source_prefetch_max_depth=0,
                rust_prefetch_active=False,
                rust_prefetch_wait_count=0,
                rust_prefetch_batch_drain_count=0,
                rust_prefetch_push_batch_count=0,
                source_record_error_count=0,
                source_record_drop_count=0,
            )
        ),
        log=SimpleNamespace(info=lambda *args, **kwargs: None),
    )

    source = _NoCheckpointRustSource()
    adapter = _source_adapter.SourceRuntimeAdapter(source=source, has_buffered_stages=True)
    records = [record async for record in adapter.iter_source_records(ctx)]

    assert [record.raw for record in records] == [0, 1, 2, 3]
    assert all(record.checkpoint is None for record in records)
    assert source.checkpoint_calls == 0
    assert ctx.metrics.runtime.rust_prefetch_active is True


@pytest.mark.asyncio
async def test_python_prefetch_preserves_delivery_success_callbacks() -> None:
    from types import SimpleNamespace

    acknowledged: list[int] = []

    class _PrefetchHookSource(BaseSource[int]):
        source_name = "prefetch_hook"
        supports_prefetch = True
        prefetch_limit = 2

        def __init__(self) -> None:
            self._current: int | None = None

        def delivery_success_callback(self):
            current = self._current
            if current is None:
                return None

            async def _ack() -> None:
                acknowledged.append(current)

            return _ack

        async def stream(self):
            for value in range(4):
                self._current = value
                yield value

    ctx = SimpleNamespace(
        metrics=SimpleNamespace(
            runtime=SimpleNamespace(
                source_prefetch_enabled=False,
                source_prefetch_limit=0,
                source_prefetch_block_count=0,
                source_prefetch_max_depth=0,
                rust_prefetch_active=False,
                rust_prefetch_wait_count=0,
                rust_prefetch_batch_drain_count=0,
                rust_prefetch_push_batch_count=0,
                source_record_error_count=0,
                source_record_drop_count=0,
            )
        ),
        log=SimpleNamespace(info=lambda *args, **kwargs: None),
    )

    adapter = _source_adapter.SourceRuntimeAdapter(
        source=_PrefetchHookSource(),
        has_buffered_stages=False,
    )
    records = [record async for record in adapter.iter_source_records(ctx)]

    for record in records:
        assert record.on_success is not None
        await record.on_success()

    assert acknowledged == [0, 1, 2, 3]


@pytest.mark.asyncio
async def test_rust_prefetch_preserves_delivery_success_callbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import threading
    from types import SimpleNamespace

    acknowledged: list[int] = []

    class _FakeRecordBuffer:
        def __init__(self, capacity: int) -> None:
            self.capacity = capacity
            self._items: list[object] = []
            self._closed = False
            self._cond = threading.Condition()

        def push(self, item: object) -> bool:
            with self._cond:
                if self._closed or len(self._items) >= self.capacity:
                    return False
                self._items.append(item)
                self._cond.notify_all()
                return True

        def push_batch(self, items: list[object]) -> int:
            pushed = 0
            for item in items:
                if not self.push(item):
                    return pushed
                pushed += 1
            return pushed

        def wait_for_item(self, timeout_ms: int) -> bool:
            with self._cond:
                if not self._items and not self._closed:
                    self._cond.wait(timeout=timeout_ms / 1000)
                return bool(self._items)

        def pop_batch(self, max_items: int) -> list[object]:
            with self._cond:
                n = min(max_items, len(self._items))
                if n == 0:
                    return []
                batch = list(self._items[:n])
                del self._items[:n]
                return batch

        def close(self) -> None:
            with self._cond:
                self._closed = True
                self._cond.notify_all()

        def is_done(self) -> bool:
            with self._cond:
                return self._closed and not self._items

    class _RustPrefetchHookSource(BaseSource[int]):
        source_name = "rust_prefetch_hook"
        supports_rust_prefetch = True
        prefetch_limit = 2

        def __init__(self) -> None:
            self._current: int | None = None

        def delivery_success_callback(self):
            current = self._current
            if current is None:
                return None

            async def _ack() -> None:
                acknowledged.append(current)

            return _ack

        async def stream(self):
            for value in range(4):
                self._current = value
                yield value

        def stream_sync_batches(self):
            for value in range(4):
                self._current = value
                yield value

    monkeypatch.setattr(
        _source_adapter,
        "acceleration_status",
        lambda mode: type("Status", (), {"enabled": True, "reason": None})(),
    )
    monkeypatch.setattr(
        _source_adapter,
        "make_record_buffer",
        lambda capacity, *, mode: _FakeRecordBuffer(capacity),
    )

    ctx = SimpleNamespace(
        metrics=SimpleNamespace(
            runtime=SimpleNamespace(
                source_prefetch_enabled=False,
                source_prefetch_limit=0,
                source_prefetch_block_count=0,
                source_prefetch_max_depth=0,
                rust_prefetch_active=False,
                rust_prefetch_wait_count=0,
                rust_prefetch_batch_drain_count=0,
                rust_prefetch_push_batch_count=0,
                source_record_error_count=0,
                source_record_drop_count=0,
            )
        ),
        log=SimpleNamespace(info=lambda *args, **kwargs: None),
    )

    adapter = _source_adapter.SourceRuntimeAdapter(
        source=_RustPrefetchHookSource(),
        has_buffered_stages=True,
    )
    records = [record async for record in adapter.iter_source_records(ctx)]

    for record in records:
        assert record.on_success is not None
        await record.on_success()

    assert acknowledged == [0, 1, 2, 3]


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
async def test_buffered_pipeline_cancellation_closes_pending_write_owner_task() -> None:
    middleware = PartiallyBlockingBufferedMiddleware()
    pipeline_task = asyncio.create_task(
        Pipeline(IterableSource([1, 2]), id="buffered_owner_cleanup")
        .pipe(middleware)
        .build(
            BatchCollectSink(),
            config=DeliveryConfig(batch_size=100, batch_flush_interval_ms=1000),
        )  # type: ignore[arg-type]
        .run()
    )

    await asyncio.wait_for(middleware.all_started.wait(), timeout=1.0)
    await asyncio.wait_for(middleware.first_resolved.wait(), timeout=1.0)
    await asyncio.sleep(0)

    owner_tasks = [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
        and task.get_name() == "buffered_owner_cleanup-pending-write-owner"
    ]
    assert owner_tasks, "pending write owner should be active before cancellation"

    pipeline_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pipeline_task

    await asyncio.sleep(0)
    leaked_owner_tasks = [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
        and task.get_name() == "buffered_owner_cleanup-pending-write-owner"
    ]
    assert leaked_owner_tasks == []
    assert middleware.cancelled == [2]


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
async def test_writer_batch_size_direct_flush_preserves_delivery_success_hooks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeLinearBatchBuffer:
        instances: ClassVar[list[_FakeLinearBatchBuffer]] = []

        def __init__(self, batch_size: int, metrics_flush_interval: int) -> None:
            self.batch_size = batch_size
            self.metrics_flush_interval = metrics_flush_interval
            self.pending: list[tuple[object, object, object, object]] = []
            self.take_flush_batch_calls = 0
            self.take_batch_calls = 0
            type(self).instances.append(self)

        def push(self, processed, raw, checkpoint, on_success):
            self.pending.append((processed, raw, checkpoint, on_success))
            return len(self.pending) >= self.batch_size

        def take_flush_batch(self):
            self.take_flush_batch_calls += 1
            batch = list(self.pending)
            self.pending.clear()
            processed = [item[0] for item in batch]
            raw = [item[1] for item in batch]
            checkpoints = [item[2] for item in batch]
            on_successes = [item[3] for item in batch]
            return processed, raw, checkpoints, on_successes

        def take_batch(self):
            self.take_batch_calls += 1
            batch = list(self.pending)
            self.pending.clear()
            return batch

        def len(self):
            return len(self.pending)

        def inc_consumed(self, source_name: str) -> bool:
            del source_name
            return False

        def flush_metrics(self, metrics) -> None:
            del metrics

        def flush_metrics_final(self, metrics) -> None:
            del metrics

    monkeypatch.setattr(
        _buffered,
        "acceleration_status",
        lambda mode: type("Status", (), {"enabled": True, "reason": None})(),
    )
    monkeypatch.setattr(
        _buffered,
        "make_linear_batch_buffer",
        lambda batch_size, flush_interval, *, mode: _FakeLinearBatchBuffer(
            batch_size,
            flush_interval,
        ),
    )
    _FakeLinearBatchBuffer.instances.clear()

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
    assert len(_FakeLinearBatchBuffer.instances) == 1
    fake_buf = _FakeLinearBatchBuffer.instances[0]
    assert summary.runtime.execution_lane == "linear"
    assert summary.runtime.direct_flush_active is True
    assert fake_buf.take_flush_batch_calls == 2
    assert fake_buf.take_batch_calls == 0


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
async def test_buffered_stage_failure_metrics_not_double_counted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    class _TraceSpan:
        def __call__(self, *args, **kwargs):
            del args, kwargs
            return self

        def __enter__(self):
            return None

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

    class _FailingBufferedMiddleware(Middleware[int, int]):
        name = "failing_buffered"
        min_concurrency = 2

        async def process(self, record: int, ctx) -> int | None:
            del ctx
            return record

        async def submit(self, record: int, ctx) -> asyncio.Future[int]:
            del record, ctx
            future: asyncio.Future[int] = asyncio.get_running_loop().create_future()
            future.set_exception(RuntimeError("boom"))
            return future

    metrics = SimpleNamespace(
        records_in=0,
        records_out=0,
        records_dropped=0,
        records_errored=0,
        total_time_ms=0.0,
    )
    ctx = SimpleNamespace(
        metrics=SimpleNamespace(middleware=lambda _name: metrics),
        trace_span=_TraceSpan(),
    )

    async def _unexpected_process_range(*args, **kwargs):
        del args, kwargs
        raise AssertionError("process_range should not run after buffered middleware failure")

    strategy = _lanes.BufferedLaneStrategy(
        coordinator=SimpleNamespace(chain=SimpleNamespace(process_range=_unexpected_process_range))
    )
    stage = BufferedStageSpec(
        index=0,
        middleware=_FailingBufferedMiddleware(),
        name="failing_buffered",
        concurrency=2,
    )
    monotonic_values = iter([100.0, 100.25])

    def _fake_monotonic() -> float:
        return next(monotonic_values, 100.25)

    monkeypatch.setattr(_lanes.time, "monotonic", _fake_monotonic)

    result = await strategy.process_record_through_buffered_stages(
        SourceRecord(raw=1),
        ctx,
        (stage,),
        1,
    )

    assert result.failure is not None
    assert metrics.records_in == 1
    assert metrics.records_out == 0
    assert metrics.records_dropped == 0
    assert metrics.records_errored == 1
    assert metrics.total_time_ms == pytest.approx(250.0)


@pytest.mark.asyncio
async def test_single_buffered_stage_failure_metrics_not_double_counted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    class _TraceSpan:
        def __call__(self, *args, **kwargs):
            del args, kwargs
            return self

        def __enter__(self):
            return None

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

    class _FailingBufferedMiddleware(Middleware[int, int]):
        name = "failing_buffered"
        min_concurrency = 2

        async def process(self, record: int, ctx) -> int | None:
            del ctx
            return record

        async def submit(self, record: int, ctx) -> asyncio.Future[int]:
            del record, ctx
            future: asyncio.Future[int] = asyncio.get_running_loop().create_future()
            future.set_exception(RuntimeError("boom"))
            return future

    metrics = SimpleNamespace(
        records_in=0,
        records_out=0,
        records_dropped=0,
        records_errored=0,
        total_time_ms=0.0,
    )
    ctx = SimpleNamespace(
        metrics=SimpleNamespace(middleware=lambda _name: metrics),
        trace_span=_TraceSpan(),
    )
    dispatched: list[tuple[object | None, MiddlewareFailure | None]] = []

    async def _dispatch_processed_result(
        state,
        result,
        raw_record,
        checkpoint_value,
        writer_batch_size,
        *,
        failure=None,
        on_success=None,
    ) -> None:
        del state, raw_record, checkpoint_value, writer_batch_size, on_success
        dispatched.append((result, failure))

    async def _unexpected_process_range(*args, **kwargs):
        del args, kwargs
        raise AssertionError("process_range should not run after buffered middleware failure")

    strategy = _lanes.BufferedLaneStrategy(
        coordinator=SimpleNamespace(
            chain=SimpleNamespace(
                process_range=_unexpected_process_range,
                middleware_count=lambda: 1,
            ),
            delivery=SimpleNamespace(dispatch_processed_result=_dispatch_processed_result),
            writer_batch_size=1,
        )
    )
    stage = BufferedStageSpec(
        index=0,
        middleware=_FailingBufferedMiddleware(),
        name="failing_buffered",
        concurrency=2,
    )
    monotonic_values = iter([100.0, 100.25])

    def _fake_monotonic() -> float:
        return next(monotonic_values, 100.25)

    monkeypatch.setattr(_lanes.time, "monotonic", _fake_monotonic)

    entry = await strategy._submit_single_buffered_record(
        stage,
        SourceRecord(raw=1),
        ctx,
        1,
        suffix_start=1,
    )
    await strategy._resolve_single_buffered_record(SimpleNamespace(ctx=ctx), entry)

    assert len(dispatched) == 1
    assert dispatched[0][0] is None
    assert dispatched[0][1] is not None
    assert metrics.records_in == 1
    assert metrics.records_out == 0
    assert metrics.records_dropped == 0
    assert metrics.records_errored == 1
    assert metrics.total_time_ms == pytest.approx(250.0)


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
async def test_single_buffered_stage_preserves_suffix_middleware_order() -> None:
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
        .pipe(MapMiddleware(lambda record: record * 10, name="suffix_map"))
        .build(sink)
        .run()
    )

    assert summary.records_consumed == 4
    assert summary.records_written == 4
    assert sink.records == [100, 110, 120, 130]


@pytest.mark.asyncio
async def test_sync_builtin_row_middlewares_use_fast_path(monkeypatch: pytest.MonkeyPatch) -> None:
    sink = CollectSink()
    add_one = MapMiddleware(lambda record: record + 1, name="add_one")
    keep_lt_four = FilterMiddleware(lambda record: record < 4, name="keep_lt_four")

    async def _unexpected_map(record, ctx):
        del record, ctx
        raise AssertionError("sync MapMiddleware should use the row fast path")

    async def _unexpected_filter(record, ctx):
        del record, ctx
        raise AssertionError("sync FilterMiddleware should use the row fast path")

    monkeypatch.setattr(add_one, "process", _unexpected_map)
    monkeypatch.setattr(keep_lt_four, "process", _unexpected_filter)

    summary = await (
        Pipeline(IterableSource([1, 2, 3, 4])).pipe(add_one).pipe(keep_lt_four).build(sink).run()
    )

    assert summary.records_written == 2
    assert summary.records_dropped == 2
    assert sink.records == [2, 3]


@pytest.mark.asyncio
async def test_sync_builtin_row_middlewares_use_rust_executor_when_noop_tracing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executors: list[object] = []

    class _FakeRustExecutor:
        def __init__(self, callables: list[object], names: list[str]) -> None:
            self.callables = callables
            self.names = names
            self.calls: list[tuple[int, int, int]] = []

        def process_range(self, start: int, stop: int, record: int, ctx) -> int | None:
            del ctx
            self.calls.append((start, stop, record))
            current: int | None = record
            for idx in range(start, stop):
                current = self.callables[idx](current)
                if current is None:
                    return None
            return current

    monkeypatch.setattr(
        "agora.core.middleware._chain.acceleration_supports",
        lambda capability, *, mode: capability == "sync_builtin_chain_executor",
    )
    monkeypatch.setattr(
        "agora.core.middleware._chain._rust_sync_builtin_executor_enabled",
        lambda: True,
    )
    monkeypatch.setattr(
        "agora.core.middleware._chain.make_sync_builtin_chain_executor",
        lambda callables, names, *, mode: (
            executors.append(_FakeRustExecutor(callables, names)) or executors[-1]
        ),
    )

    chain = MiddlewareChain(
        [
            MapMiddleware(lambda record: record + 1, name="add_one"),
            FilterMiddleware(lambda record: record < 4, name="keep_lt_four"),
        ]
    )
    ctx = PipelineContext(pipeline_id="pipe", metrics=PipelineMetrics())

    value, failure = await chain.process_outcome(1, ctx)

    assert failure is None
    assert value == 2
    assert len(executors) == 1
    assert executors[0].calls == [(0, 2, 1)]


@pytest.mark.asyncio
async def test_sync_builtin_rust_executor_skips_traced_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executors: list[object] = []

    class _FakeRustExecutor:
        def __init__(self, callables: list[object], names: list[str]) -> None:
            del callables, names
            self.calls: list[tuple[int, int, int]] = []

        def process_range(self, start: int, stop: int, record: int, ctx) -> int | None:
            del ctx
            self.calls.append((start, stop, record))
            return record

    monkeypatch.setattr(
        "agora.core.middleware._chain.acceleration_supports",
        lambda capability, *, mode: capability == "sync_builtin_chain_executor",
    )
    monkeypatch.setattr(
        "agora.core.middleware._chain._rust_sync_builtin_executor_enabled",
        lambda: True,
    )
    monkeypatch.setattr(
        "agora.core.middleware._chain.make_sync_builtin_chain_executor",
        lambda callables, names, *, mode: (
            executors.append(_FakeRustExecutor(callables, names)) or executors[-1]
        ),
    )

    chain = MiddlewareChain(
        [
            MapMiddleware(lambda record: record + 1, name="add_one"),
            FilterMiddleware(lambda record: record < 4, name="keep_lt_four"),
        ]
    )
    tracer = InMemoryTracer()
    ctx = PipelineContext(pipeline_id="pipe", metrics=PipelineMetrics(), tracer=tracer)

    value, failure = await chain.process_outcome(1, ctx)

    assert failure is None
    assert value == 2
    assert len(executors) == 1
    assert executors[0].calls == []
    assert [span.name for span in tracer.spans] == ["middleware.process", "middleware.process"]


@pytest.mark.asyncio
async def test_sync_builtin_rust_executor_skips_single_stage_fast_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executors: list[object] = []

    class _FakeRustExecutor:
        def __init__(self, callables: list[object], names: list[str]) -> None:
            del callables, names
            self.calls: list[tuple[int, int, int]] = []

        def process_range(self, start: int, stop: int, record: int, ctx) -> int | None:
            del ctx
            self.calls.append((start, stop, record))
            return record

    monkeypatch.setattr(
        "agora.core.middleware._chain.acceleration_supports",
        lambda capability, *, mode: capability == "sync_builtin_chain_executor",
    )
    monkeypatch.setattr(
        "agora.core.middleware._chain._rust_sync_builtin_executor_enabled",
        lambda: True,
    )
    monkeypatch.setattr(
        "agora.core.middleware._chain.make_sync_builtin_chain_executor",
        lambda callables, names, *, mode: (
            executors.append(_FakeRustExecutor(callables, names)) or executors[-1]
        ),
    )

    chain = MiddlewareChain([MapMiddleware(lambda record: record + 1, name="add_one")])
    ctx = PipelineContext(pipeline_id="pipe", metrics=PipelineMetrics())

    value, failure = await chain.process_outcome(1, ctx)

    assert failure is None
    assert value == 2
    assert len(executors) == 1
    assert executors[0].calls == []


@pytest.mark.asyncio
async def test_sync_builtin_rust_executor_preserves_on_error_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handled: list[tuple[int, str]] = []

    class _FakeRustExecutor:
        def __init__(self, callables: list[object], names: list[str]) -> None:
            self.callables = callables
            self.names = names

        def process_range(self, start: int, stop: int, record: int, ctx) -> int | None:
            del ctx
            current: int | None = record
            for idx in range(start, stop):
                try:
                    current = self.callables[idx](current)
                except Exception as exc:
                    exc._agora_stage_index = idx
                    raise
                if current is None:
                    return None
            return current

    monkeypatch.setattr(
        "agora.core.middleware._chain.acceleration_supports",
        lambda capability, *, mode: capability == "sync_builtin_chain_executor",
    )
    monkeypatch.setattr(
        "agora.core.middleware._chain._rust_sync_builtin_executor_enabled",
        lambda: True,
    )
    monkeypatch.setattr(
        "agora.core.middleware._chain.make_sync_builtin_chain_executor",
        lambda callables, names, *, mode: _FakeRustExecutor(callables, names),
    )

    add_one = MapMiddleware(lambda record: record + 1, name="add_one")
    boom_filter = FilterMiddleware(
        lambda record: (_ for _ in ()).throw(RuntimeError("boom")) if record == 2 else True,
        name="boom_filter",
    )

    async def _on_error(record, exc, ctx) -> None:
        del ctx
        handled.append((record, str(exc)))

    monkeypatch.setattr(boom_filter, "on_error", _on_error)

    chain = MiddlewareChain([add_one, boom_filter])
    ctx = PipelineContext(pipeline_id="pipe", metrics=PipelineMetrics())

    value, failure = await chain.process_outcome(1, ctx)

    assert value is None
    assert failure is not None
    assert failure.middleware == "boom_filter"
    assert str(failure.exception) == "boom"
    assert handled == [(1, "boom")]


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
    pipeline = Pipeline(make_source(3)).build(
        StdoutSink(),
        config=DeliveryConfig(acceleration_mode="off", performance_profile="low_latency"),
    )
    summary = await pipeline.run()
    assert summary.elapsed_seconds >= 0.0
    assert summary.runtime.acceleration_mode == "off"
    assert summary.runtime.acceleration_profile == "low_latency"
    assert summary.runtime.acceleration_available is False
    assert summary.runtime.direct_flush_inactive_reason == "writer batch size is 1"
    assert str(summary).startswith("PipelineRunSummary(")
    assert "acceleration=off" in str(summary)


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
        .fan_out(
            [OkSink(), FailingSink()],
            config=DeliveryConfig(sink_failure_policy=SinkFailurePolicy.LOG_AND_CONTINUE),
        )  # type: ignore[list-item]
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
async def test_fan_out_partial_open_failure_rolls_back_opened_sink_before_middleware_stop() -> None:
    events: list[str] = []

    class TrackingMiddleware(Middleware[int, int]):
        name = "tracking"

        async def on_start(self, ctx) -> None:
            del ctx
            events.append("middleware.start")

        async def on_stop(self, ctx) -> None:
            del ctx
            events.append("middleware.stop")

        async def process(self, record: int, ctx) -> int | None:
            del ctx
            return record

    class OpenedSink:
        sink_name = "opened"

        async def open(self) -> None:
            events.append("opened.open")

        async def write(self, record: int) -> None:
            events.append(f"opened.write:{record}")

        async def flush(self) -> None:
            events.append("opened.flush")

        async def close(self) -> None:
            events.append("opened.close")

    class FailingOpenSink:
        sink_name = "failing_open"

        async def open(self) -> None:
            events.append("failing.open")
            raise RuntimeError("fanout open broke")

        async def write(self, record: int) -> None:
            events.append(f"failing.write:{record}")

        async def flush(self) -> None:
            events.append("failing.flush")

        async def close(self) -> None:
            events.append("failing.close")

    with pytest.raises(RuntimeError, match="fanout open broke"):
        await (
            Pipeline(IterableSource([1]))
            .pipe(TrackingMiddleware())
            .fan_out([OpenedSink(), FailingOpenSink()])  # type: ignore[list-item]
            .run()
        )

    assert events == [
        "middleware.start",
        "opened.open",
        "failing.open",
        "opened.close",
        "middleware.stop",
    ]


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

    assert events == ["writer.open", "dlq.open", "dlq.close", "writer.close"]


@pytest.mark.asyncio
async def test_partial_dlq_open_failure_closes_dlq_and_writer() -> None:
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

    class PartiallyOpenedDLQSink:
        sink_name = "dlq"

        async def open(self) -> None:
            events.append("dlq.open")
            raise RuntimeError("dlq partial open broke")

        async def write(self, record) -> None:
            events.append(f"dlq.write:{record}")

        async def flush(self) -> None:
            events.append("dlq.flush")

        async def close(self) -> None:
            events.append("dlq.close")

    with pytest.raises(RuntimeError, match="dlq partial open broke"):
        await (
            Pipeline(IterableSource([1]))
            .build(
                TrackingWriterSink(),
                config=DeliveryConfig(dlq=PartiallyOpenedDLQSink()),
            )  # type: ignore[arg-type]
            .run()
        )

    assert events == ["writer.open", "dlq.open", "dlq.close", "writer.close"]


@pytest.mark.asyncio
async def test_source_close_failure_propagates_on_clean_run() -> None:
    events: list[str] = []

    class FailingCloseSource(BaseSource[int]):
        source_name = "failing_close_source"

        async def open(self) -> None:
            events.append("source.open")

        async def close(self) -> None:
            events.append("source.close")
            raise RuntimeError("source close broke")

        async def stream(self):
            yield 1

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

    with pytest.raises(RuntimeError, match="source close broke"):
        await Pipeline(FailingCloseSource()).build(TrackingSink()).run()  # type: ignore[arg-type]

    assert events == [
        "sink.open",
        "source.open",
        "sink.write:1",
        "source.close",
        "sink.flush",
        "sink.close",
    ]


@pytest.mark.asyncio
async def test_source_close_failure_does_not_mask_stream_error() -> None:
    events: list[str] = []

    class FailingSource(BaseSource[int]):
        source_name = "failing_source"

        async def open(self) -> None:
            events.append("source.open")

        async def close(self) -> None:
            events.append("source.close")
            raise RuntimeError("source close broke")

        async def stream(self):
            yield 1
            raise RuntimeError("source stream broke")

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

    with pytest.raises(RuntimeError, match="source stream broke"):
        await Pipeline(FailingSource()).build(TrackingSink()).run()  # type: ignore[arg-type]

    assert events == [
        "sink.open",
        "source.open",
        "sink.write:1",
        "source.close",
        "sink.flush",
        "sink.close",
    ]


@pytest.mark.asyncio
async def test_source_close_failure_does_not_mask_cancellation() -> None:
    events: list[str] = []
    source_started = asyncio.Event()

    class BlockingSource(BaseSource[int]):
        source_name = "blocking_source"

        async def open(self) -> None:
            events.append("source.open")

        async def close(self) -> None:
            events.append("source.close")
            raise RuntimeError("source close broke")

        async def stream(self):
            source_started.set()
            await asyncio.Event().wait()
            yield 1

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

    task = asyncio.create_task(
        Pipeline(BlockingSource()).build(TrackingSink()).run()  # type: ignore[arg-type]
    )

    await asyncio.wait_for(source_started.wait(), timeout=1.0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert events == [
        "sink.open",
        "source.open",
        "source.close",
        "sink.flush",
        "sink.close",
    ]


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
