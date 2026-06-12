from __future__ import annotations

import asyncio
import warnings

import pytest

from agora import IterableSource, Pipeline
from agora.core.data_plane import DataPlane
from agora.core.sink import (
    BaseSink,
    SinkCapabilities,
    SinkFanOut,
    SinkRouter,
    sink_capabilities,
    sink_data_plane_spec,
)
from agora.sinks.file.csv import CsvSink
from agora.sinks.file.jsonlines import JsonLinesSink


class _BlockingSink:
    sink_name = "blocking"

    def __init__(
        self,
        entered: asyncio.Event,
        release: asyncio.Event,
        active: list[int],
        max_active: list[int],
    ) -> None:
        self._entered = entered
        self._release = release
        self._active = active
        self._max_active = max_active

    async def open(self) -> None:
        return None

    async def write(self, record: str) -> None:
        del record
        self._active[0] += 1
        self._max_active[0] = max(self._max_active[0], self._active[0])
        self._entered.set()
        await self._release.wait()
        self._active[0] -= 1

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _BlockingLifecycleSink:
    sink_name = "blocking_lifecycle"

    def __init__(
        self,
        phase: str,
        entered: asyncio.Event,
        release: asyncio.Event,
        active: list[int],
        max_active: list[int],
    ) -> None:
        self._phase = phase
        self._entered = entered
        self._release = release
        self._active = active
        self._max_active = max_active

    async def _block(self) -> None:
        self._active[0] += 1
        self._max_active[0] = max(self._max_active[0], self._active[0])
        self._entered.set()
        await self._release.wait()
        self._active[0] -= 1

    async def open(self) -> None:
        if self._phase == "open":
            await self._block()

    async def write(self, record: str) -> None:
        del record

    async def flush(self) -> None:
        if self._phase == "flush":
            await self._block()

    async def close(self) -> None:
        if self._phase == "close":
            await self._block()


class _FailingSink:
    sink_name = "failing"

    def __init__(self, message: str) -> None:
        self._message = message

    async def open(self) -> None:
        return None

    async def write(self, record: str) -> None:
        del record
        raise RuntimeError(self._message)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _SingleWriteSink:
    sink_name = "single_write"

    async def open(self) -> None:
        return None

    async def write(self, record: str) -> None:
        if record == "b":
            raise RuntimeError("single-write-broke")

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _BatchFailingSink:
    sink_name = "batch_failing"

    async def open(self) -> None:
        return None

    async def write(self, record: str) -> None:
        del record
        raise AssertionError("batch path should be used")

    async def write_batch(self, records: list[str]) -> None:
        del records
        raise RuntimeError("batch-broke")

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _ArrowBatchCollectSink:
    sink_name = "arrow_batch_collect"

    def __init__(self) -> None:
        self.batches: list[object] = []

    async def open(self) -> None:
        return None

    async def write(self, record: str) -> None:
        raise AssertionError("arrow batch path should bypass write()")

    async def write_arrow_batch(self, batch: object) -> None:
        self.batches.append(batch)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _ListBatchCollectSink:
    sink_name = "list_batch_collect"

    def __init__(self) -> None:
        self.batches: list[list[object]] = []

    async def open(self) -> None:
        return None

    async def write(self, record: object) -> None:
        self.batches.append([record])

    async def write_batch(self, records: list[object]) -> None:
        self.batches.append(list(records))

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _ArrowSerializeOnlySink(BaseSink[object]):
    sink_name = "arrow_serialize_only"

    def __init__(self) -> None:
        self.arrow_batches: list[object] = []
        self.list_batches: list[list[object]] = []

    async def write(self, record: object) -> None:
        self.list_batches.append([record])

    async def write_batch(self, records: list[object]) -> None:
        self.list_batches.append(list(records))

    async def write_arrow_batch(self, batch: object) -> None:
        self.arrow_batches.append(batch)


class _ParallelCapableFallbackSink(BaseSink[str]):
    sink_name = "parallel_capable_fallback"
    parallel_writes_safe = True
    ordered_writes_required = False

    def __init__(self, expected_active: int) -> None:
        self._expected_active = expected_active
        self._release = asyncio.Event()
        self._all_entered = asyncio.Event()
        self.active = 0
        self.max_active = 0

    async def write(self, record: str) -> None:
        del record
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        if self.max_active >= self._expected_active:
            self._all_entered.set()
        await self._release.wait()
        self.active -= 1

    async def wait_for_all_entered(self) -> None:
        await self._all_entered.wait()

    def release(self) -> None:
        self._release.set()


class _OrderedFallbackSink(BaseSink[str]):
    sink_name = "ordered_fallback"
    parallel_writes_safe = True
    ordered_writes_required = True

    def __init__(self) -> None:
        self._release = asyncio.Event()
        self._first_entered = asyncio.Event()
        self.started_records: list[str] = []
        self.active = 0
        self.max_active = 0

    async def write(self, record: str) -> None:
        self.started_records.append(record)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self._first_entered.set()
        await self._release.wait()
        self.active -= 1

    async def wait_for_first_entered(self) -> None:
        await self._first_entered.wait()

    def release(self) -> None:
        self._release.set()


@pytest.mark.asyncio
async def test_sink_fan_out_can_write_to_independent_sinks_concurrently() -> None:
    release = asyncio.Event()
    entered_one = asyncio.Event()
    entered_two = asyncio.Event()
    active = [0]
    max_active = [0]
    fan_out = SinkFanOut(
        [
            _BlockingSink(entered_one, release, active, max_active),
            _BlockingSink(entered_two, release, active, max_active),
        ]
    ).with_concurrency()

    write_task = asyncio.create_task(fan_out.write("record"))

    await asyncio.wait_for(asyncio.gather(entered_one.wait(), entered_two.wait()), timeout=1.0)
    assert max_active[0] == 2

    release.set()
    result = await asyncio.wait_for(write_task, timeout=1.0)
    assert result.ok


@pytest.mark.asyncio
async def test_sink_fan_out_concurrent_errors_preserve_sink_order() -> None:
    fan_out = SinkFanOut(
        [
            _FailingSink("first"),
            _FailingSink("second"),
        ]
    ).with_concurrency()

    result = await fan_out.write("record")

    assert result.written is False
    assert [str(error) for error in result.errors] == ["first", "second"]


@pytest.mark.asyncio
async def test_sink_fan_out_concurrent_batch_path_preserves_per_record_errors() -> None:
    fan_out = SinkFanOut(
        [
            _BatchFailingSink(),
            _SingleWriteSink(),
        ]
    ).with_concurrency()

    results = await fan_out.write_batch(["a", "b", "c"])

    assert [[str(error) for error in result.errors] for result in results] == [
        ["batch-broke"],
        ["batch-broke", "single-write-broke"],
        ["batch-broke"],
    ]


@pytest.mark.asyncio
async def test_sink_fan_out_arrow_batch_writes_to_all_sinks() -> None:
    batch = object()
    sink_one = _ArrowBatchCollectSink()
    sink_two = _ArrowBatchCollectSink()
    fan_out = SinkFanOut([sink_one, sink_two])  # type: ignore[list-item]

    await fan_out.write_arrow_batch(batch)

    assert sink_one.batches == [batch]
    assert sink_two.batches == [batch]


@pytest.mark.asyncio
async def test_sink_fan_out_arrow_batch_falls_back_to_list_batch_for_non_arrow_sink() -> None:
    class _ArrowBatch:
        def __init__(self) -> None:
            self.rows = [{"id": 1}, {"id": 2}]

        def to_pylist(self) -> list[dict[str, int]]:
            return list(self.rows)

    batch = _ArrowBatch()
    arrow_sink = _ArrowBatchCollectSink()
    list_sink = _ListBatchCollectSink()
    fan_out = SinkFanOut([arrow_sink, list_sink])  # type: ignore[list-item]

    await fan_out.write_arrow_batch(batch)

    assert arrow_sink.batches == [batch]
    assert list_sink.batches == [[{"id": 1}, {"id": 2}]]


@pytest.mark.asyncio
async def test_sink_fan_out_arrow_batch_uses_arrow_path_for_sink_with_write_arrow_batch() -> None:
    class _ArrowBatch:
        def __init__(self) -> None:
            self.rows = [{"id": 1}, {"id": 2}]

        def to_pylist(self) -> list[dict[str, int]]:
            return list(self.rows)

    batch = _ArrowBatch()
    sink = _ArrowSerializeOnlySink()
    fan_out = SinkFanOut([sink])

    await fan_out.write_arrow_batch(batch)

    assert sink.arrow_batches == [batch]
    assert sink.list_batches == []


@pytest.mark.asyncio
async def test_sink_fan_out_arrow_batch_single_arrow_sink_skips_row_materialization() -> None:
    class _ArrowBatch:
        def to_pylist(self) -> list[dict[str, int]]:
            raise AssertionError("single arrow sink should not materialize rows")

    batch = _ArrowBatch()
    sink = _ArrowBatchCollectSink()
    fan_out = SinkFanOut([sink])  # type: ignore[list-item]

    await fan_out.write_arrow_batch(batch)

    assert sink.batches == [batch]


@pytest.mark.asyncio
async def test_sink_router_with_max_records_limits_at_source_boundary() -> None:
    class _CollectRouteSink:
        sink_name = "collect_route"

        def __init__(self) -> None:
            self.records: list[int] = []

        async def open(self) -> None:
            return None

        async def write(self, record: int) -> None:
            self.records.append(record)

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    sink = _CollectRouteSink()
    router = SinkRouter[int]().default(sink)  # type: ignore[arg-type]

    summary = await Pipeline(IterableSource(list(range(100)))).route(router).run(max_records=4)

    assert summary.records_consumed == 4
    assert summary.records_written == 4
    assert sink.records == [0, 1, 2, 3]


@pytest.mark.asyncio
async def test_sink_fan_out_single_sink_batch_path_continues_after_mid_batch_failure() -> None:
    class _TrackingSingleWriteSink:
        sink_name = "tracking_single_write"

        def __init__(self) -> None:
            self.seen: list[str] = []

        async def open(self) -> None:
            return None

        async def write(self, record: str) -> None:
            self.seen.append(record)
            if record == "b":
                raise RuntimeError("single-write-broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    sink = _TrackingSingleWriteSink()
    fan_out = SinkFanOut([sink])  # type: ignore[list-item]

    results = await fan_out.write_batch(["a", "b", "c"])

    assert sink.seen == ["a", "b", "c"]
    assert [result.ok for result in results] == [True, False, True]
    assert [str(error) for error in results[1].errors] == ["single-write-broke"]


@pytest.mark.asyncio
async def test_sink_fan_out_uses_parallel_fallback_for_capability_advertised_sink() -> None:
    sink = _ParallelCapableFallbackSink(expected_active=3)
    fan_out = SinkFanOut([sink]).with_concurrency()  # type: ignore[list-item]

    write_task = asyncio.create_task(fan_out.write_batch(["a", "b", "c"]))

    await asyncio.wait_for(sink.wait_for_all_entered(), timeout=1.0)
    assert sink.max_active == 3

    sink.release()
    results = await asyncio.wait_for(write_task, timeout=1.0)
    assert all(result.ok for result in results)


@pytest.mark.asyncio
async def test_sink_fan_out_keeps_serial_fallback_for_ordered_sink() -> None:
    sink = _OrderedFallbackSink()
    fan_out = SinkFanOut([sink]).with_concurrency()  # type: ignore[list-item]

    write_task = asyncio.create_task(fan_out.write_batch(["a", "b", "c"]))

    await asyncio.wait_for(sink.wait_for_first_entered(), timeout=1.0)
    await asyncio.sleep(0)
    assert sink.max_active == 1
    assert sink.started_records == ["a"]

    sink.release()
    results = await asyncio.wait_for(write_task, timeout=1.0)
    assert sink.started_records == ["a", "b", "c"]
    assert all(result.ok for result in results)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["open", "flush", "close"])
async def test_sink_fan_out_can_run_lifecycle_concurrently(phase: str) -> None:
    release = asyncio.Event()
    entered_one = asyncio.Event()
    entered_two = asyncio.Event()
    active = [0]
    max_active = [0]
    fan_out = SinkFanOut(
        [
            _BlockingLifecycleSink(phase, entered_one, release, active, max_active),
            _BlockingLifecycleSink(phase, entered_two, release, active, max_active),
        ]
    ).with_concurrency()

    method = getattr(fan_out, phase)
    lifecycle_task = asyncio.create_task(method())

    await asyncio.wait_for(asyncio.gather(entered_one.wait(), entered_two.wait()), timeout=1.0)
    assert max_active[0] == 2

    release.set()
    await asyncio.wait_for(lifecycle_task, timeout=1.0)


@pytest.mark.asyncio
async def test_sink_fan_out_open_rolls_back_already_opened_sinks_on_failure() -> None:
    events: list[str] = []

    class _OpenedSink:
        sink_name = "opened"

        async def open(self) -> None:
            events.append("opened.open")

        async def write(self, record: str) -> None:
            del record

        async def flush(self) -> None:
            events.append("opened.flush")

        async def close(self) -> None:
            events.append("opened.close")

    class _FailingSink:
        sink_name = "failing"

        async def open(self) -> None:
            events.append("failing.open")
            raise RuntimeError("fanout open failed")

        async def write(self, record: str) -> None:
            del record

        async def flush(self) -> None:
            events.append("failing.flush")

        async def close(self) -> None:
            events.append("failing.close")

    fan_out = SinkFanOut([_OpenedSink(), _FailingSink()])  # type: ignore[list-item]

    with pytest.raises(RuntimeError, match="fanout open failed"):
        await fan_out.open()

    assert events == ["opened.open", "failing.open", "opened.close"]


@pytest.mark.asyncio
async def test_sink_router_dedupes_shared_sink_lifecycle_and_rolls_back_on_failure() -> None:
    events: list[str] = []

    class _SharedSink:
        sink_name = "shared"

        async def open(self) -> None:
            events.append("shared.open")

        async def write(self, record: str) -> None:
            del record

        async def flush(self) -> None:
            events.append("shared.flush")

        async def close(self) -> None:
            events.append("shared.close")

        def bind_context(self, ctx: object) -> None:
            del ctx
            events.append("shared.bind")

    class _FailingSink:
        sink_name = "failing"

        async def open(self) -> None:
            events.append("failing.open")
            raise RuntimeError("router open failed")

        async def write(self, record: str) -> None:
            del record

        async def flush(self) -> None:
            events.append("failing.flush")

        async def close(self) -> None:
            events.append("failing.close")

    shared = _SharedSink()
    router = (
        SinkRouter[str]()
        .route(lambda record: record.startswith("a"), shared)
        .route(lambda record: record.startswith("b"), shared)
        .default(_FailingSink())
    )

    router.bind_context(object())
    with pytest.raises(RuntimeError, match="router open failed"):
        await router.open()

    assert events == ["shared.bind", "shared.open", "failing.open", "shared.close"]


def test_with_sink_concurrency_requires_fan_out_pipeline() -> None:
    class _CollectSink:
        sink_name = "collect"

        async def open(self) -> None:
            return None

        async def write(self, record: int) -> None:
            del record

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    class _RouteSink(_CollectSink):
        pass

    router = SinkRouter[int]().default(_RouteSink())  # type: ignore[arg-type]
    # sink_concurrency is only valid for fan_out, not route
    # route() does not accept sink_concurrency kwarg — it's excluded by design
    pipeline = Pipeline(IterableSource([1])).route(router)
    assert pipeline is not None


def test_sink_fan_out_binds_context_only_for_context_bindable_sinks() -> None:
    calls: list[object] = []

    class _ContextAwareSink:
        sink_name = "context_aware"

        async def open(self) -> None:
            return None

        async def write(self, record: int) -> None:
            del record

        def bind_context(self, ctx: object) -> None:
            calls.append(ctx)

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    class _PlainSink:
        sink_name = "plain"

        async def open(self) -> None:
            return None

        async def write(self, record: int) -> None:
            del record

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    fan_out = SinkFanOut([_ContextAwareSink(), _PlainSink()])  # type: ignore[list-item]
    ctx = object()
    fan_out.bind_context(ctx)
    assert calls == [ctx]


def test_sink_router_binds_context_for_routes_and_default_sink() -> None:
    calls: list[tuple[str, object]] = []

    class _ContextAwareSink:
        sink_name = "context_aware"

        def __init__(self, name: str) -> None:
            self._name = name

        async def open(self) -> None:
            return None

        async def write(self, record: int) -> None:
            del record

        def bind_context(self, ctx: object) -> None:
            calls.append((self._name, ctx))

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    router = (
        SinkRouter[int]()
        .route(lambda value: value > 0, _ContextAwareSink("route"))  # type: ignore[arg-type]
        .default(_ContextAwareSink("default"))  # type: ignore[arg-type]
    )
    ctx = object()
    router.bind_context(ctx)
    assert calls == [("route", ctx), ("default", ctx)]


def test_sink_capabilities_prefers_explicit_contracts_and_detects_native_batch() -> None:
    class _ExplicitSink(BaseSink[int]):
        sink_name = "explicit"

        async def write(self, record: int) -> None:
            del record

        def sink_capabilities(self) -> SinkCapabilities:
            return SinkCapabilities(
                batch_writable_native=False,
                parallel_writes_safe=True,
                ordered_writes_required=False,
            )

    class _BatchOverrideSink(BaseSink[int]):
        sink_name = "batch_override"

        async def write(self, record: int) -> None:
            del record

        async def write_batch(self, records: list[int]) -> None:
            del records

    assert sink_capabilities(_ExplicitSink()) == SinkCapabilities(
        batch_writable_native=False,
        parallel_writes_safe=True,
        ordered_writes_required=False,
        accepted_data_planes=(DataPlane.PYTHON_ROWS,),
        native_data_planes=(DataPlane.PYTHON_ROWS,),
    )
    assert sink_capabilities(_BatchOverrideSink()) == SinkCapabilities(
        batch_writable_native=True,
        parallel_writes_safe=False,
        ordered_writes_required=True,
        accepted_data_planes=(DataPlane.PYTHON_ROWS, DataPlane.PYTHON_BATCHES),
        native_data_planes=(DataPlane.PYTHON_ROWS, DataPlane.PYTHON_BATCHES),
    )

    assert sink_data_plane_spec(_BatchOverrideSink()).native_planes == (
        DataPlane.PYTHON_ROWS,
        DataPlane.PYTHON_BATCHES,
    )


def test_file_sinks_advertise_arrow_batch_boundary_when_supported() -> None:
    csv_sink = CsvSink(path="out.csv", row_mapper=lambda row: row)
    jsonl_sink = JsonLinesSink(path="out.jsonl", serializer=lambda row: row)

    assert sink_data_plane_spec(csv_sink).native_planes == (
        DataPlane.PYTHON_ROWS,
        DataPlane.PYTHON_BATCHES,
        DataPlane.ARROW_BATCHES,
    )
    assert sink_data_plane_spec(jsonl_sink).native_planes == (
        DataPlane.PYTHON_ROWS,
        DataPlane.PYTHON_BATCHES,
        DataPlane.ARROW_BATCHES,
    )


def test_sink_capabilities_warn_once_for_legacy_bool_flags() -> None:
    class _LegacyArrowSink(BaseSink[int]):
        sink_name = "legacy_arrow"
        batch_writable_native = True
        arrow_passthrough_native = True

        async def write(self, record: int) -> None:
            del record

        async def write_batch(self, records: list[int]) -> None:
            del records

        async def write_arrow_batch(self, batch: object) -> None:
            del batch

    sink = _LegacyArrowSink()
    with pytest.raises(TypeError, match="legacy sink data-plane bool flags"):
        sink_capabilities(sink)


def test_sink_capabilities_do_not_warn_for_explicit_data_planes() -> None:
    class _ExplicitArrowSink(BaseSink[int]):
        sink_name = "explicit_arrow"
        accepted_data_planes = (
            DataPlane.PYTHON_ROWS,
            DataPlane.PYTHON_BATCHES,
            DataPlane.ARROW_BATCHES,
        )
        native_data_planes = accepted_data_planes

        async def write(self, record: int) -> None:
            del record

        async def write_batch(self, records: list[int]) -> None:
            del records

        async def write_arrow_batch(self, batch: object) -> None:
            del batch

    sink = _ExplicitArrowSink()
    with warnings.catch_warnings(record=True) as record:
        capabilities = sink_capabilities(sink)

    assert capabilities.native_data_planes == sink.native_data_planes
    assert len(record) == 0


@pytest.mark.asyncio
async def test_sink_fanout_write_single_sink_returns_write_ok_singleton() -> None:
    from agora.core.sink._writers import _WRITE_OK

    written: list[str] = []

    class _TrackSink:
        sink_name = "track"

        async def open(self) -> None:
            pass

        async def write(self, record: str) -> None:
            written.append(record)

        async def flush(self) -> None:
            pass

        async def close(self) -> None:
            pass

    fanout = SinkFanOut([_TrackSink()])  # type: ignore[list-item]
    result = await fanout.write("hello")

    assert result is _WRITE_OK
    assert written == ["hello"]


@pytest.mark.asyncio
async def test_sink_fanout_write_single_sink_error_returns_write_result_not_ok() -> None:
    fanout = SinkFanOut([_FailingSink("boom")])  # type: ignore[list-item]
    result = await fanout.write("x")

    assert result.written is False
    assert len(result.errors) == 1
    assert str(result.errors[0]) == "boom"


@pytest.mark.asyncio
async def test_sink_fanout_write_multi_sink_does_not_use_fast_path() -> None:
    from agora.core.sink._writers import _WRITE_OK

    written: list[str] = []

    class _TrackSink:
        sink_name = "track"

        async def open(self) -> None:
            pass

        async def write(self, record: str) -> None:
            written.append(record)

        async def flush(self) -> None:
            pass

        async def close(self) -> None:
            pass

    fanout = SinkFanOut([_TrackSink(), _TrackSink()])  # type: ignore[list-item]
    result = await fanout.write("hello")

    assert result is not _WRITE_OK
    assert result.written is True
    assert written == ["hello", "hello"]
