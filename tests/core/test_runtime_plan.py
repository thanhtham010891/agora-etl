from __future__ import annotations

import pytest

from agora.core.data_plane import DataPlane, SourceDataPlaneSpec
from agora.core.errors import PipelineError
from agora.core.middleware import Middleware, MiddlewareChain
from agora.core.runtime import RuntimeLane, build_runtime_plan
from agora.core.sink import BaseSink, SinkFanOut, SinkRouter
from agora.core.source import BaseSource, IterableSource


class _BufferedPassThrough(Middleware[int, int]):
    name = "buffered_test"
    min_concurrency = 2

    async def process(self, record: int, ctx) -> int | None:
        del ctx
        return record

    async def submit(self, record: int, ctx):
        del ctx
        loop = __import__("asyncio").get_running_loop()
        future = loop.create_future()
        future.set_result(record)
        return future


class _BatchSource(BaseSource[int]):
    source_name = "batch_source"

    async def stream(self):
        for record in [1, 2]:
            yield record

    async def stream_batches(self):  # type: ignore[override]
        yield [1, 2]

    def data_plane_spec(self) -> SourceDataPlaneSpec:
        return SourceDataPlaneSpec(
            source_name=self.source_name,
            emitted_plane=DataPlane.PYTHON_BATCHES,
            supports_batch_emit=True,
            emits_arrow_batches=False,
        )


class _HookSource(BaseSource[int]):
    source_name = "hook_source"

    async def stream(self):
        for record in [1, 2]:
            yield record

    def delivery_success_callback(self):
        async def _ack() -> None:
            return None

        return _ack


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


class _RowOnlySink(BaseSink[int]):
    sink_name = "row_only_sink"

    async def write(self, record: int) -> None:
        del record


def test_runtime_plan_selects_linear_lane_by_default() -> None:
    plan = build_runtime_plan(
        IterableSource([1, 2]),
        MiddlewareChain([]),
        SinkFanOut([_BatchSink()]),
        writer_batch_size=1,
    )

    assert plan.lane == RuntimeLane.LINEAR
    assert "no buffered stage requires submit() concurrency" in plan.lane_reason
    assert plan.source.emitted_plane == DataPlane.PYTHON_ROWS
    assert plan.writer.input_data_plane == DataPlane.PYTHON_ROWS
    assert plan.writer.input_data_plane_reason == "writer receives middleware output as python_rows"
    assert plan.writer.direct_flush_eligible is False


def test_runtime_plan_selects_buffered_lane_when_submit_stage_exists() -> None:
    plan = build_runtime_plan(
        IterableSource([1, 2]),
        MiddlewareChain([_BufferedPassThrough()]),
        SinkFanOut([_BatchSink()]),
        writer_batch_size=1,
    )

    assert plan.lane == RuntimeLane.BUFFERED
    assert "buffered_test(min_concurrency=2)" in plan.lane_reason
    assert len(plan.buffered_stages) == 1
    assert plan.buffered_stages[0].name == "buffered_test"


class _BufferedConcurrency1(Middleware[int, int]):
    """submit()-capable but min_concurrency == 1 — gains nothing from the buffered lane."""

    name = "buffered_c1"
    min_concurrency = 1

    async def process(self, record: int, ctx) -> int | None:
        del ctx
        return record

    async def submit(self, record: int, ctx):
        del ctx
        loop = __import__("asyncio").get_running_loop()
        future = loop.create_future()
        future.set_result(record)
        return future


def test_runtime_plan_keeps_concurrency1_submit_stage_on_linear_lane() -> None:
    plan = build_runtime_plan(
        IterableSource([1, 2]),
        MiddlewareChain([_BufferedConcurrency1()]),
        SinkFanOut([_BatchSink()]),
        writer_batch_size=1,
    )

    assert plan.lane == RuntimeLane.LINEAR
    assert plan.buffered_stages == ()


def test_runtime_plan_selects_batch_lane_for_batch_source() -> None:
    plan = build_runtime_plan(
        _BatchSource(),
        MiddlewareChain([]),
        SinkFanOut([_BatchSink()]),
        writer_batch_size=1,
    )

    assert plan.lane == RuntimeLane.BATCH
    assert "source advertises batch emission" in plan.lane_reason
    assert plan.batch_source is True
    assert plan.source.emitted_plane == DataPlane.PYTHON_BATCHES
    assert plan.writer.input_data_plane == DataPlane.PYTHON_BATCHES


def test_runtime_plan_keeps_direct_flush_when_delivery_hooks_exist() -> None:
    plan = build_runtime_plan(
        _HookSource(),
        MiddlewareChain([]),
        SinkFanOut([_BatchSink()]),
        writer_batch_size=2,
    )

    assert plan.has_delivery_hooks is True
    assert plan.writer.direct_flush_eligible is True


class _ArrowNativeSink(BaseSink[int]):
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


class _ArrowBatchSource(_BatchSource):
    """A batch source that emits native arrow RecordBatch objects."""

    source_name = "arrow_batch_source"

    def data_plane_spec(self) -> SourceDataPlaneSpec:
        return SourceDataPlaneSpec(
            source_name=self.source_name,
            emitted_plane=DataPlane.ARROW_BATCHES,
            supports_batch_emit=True,
            emits_arrow_batches=True,
        )


def test_arrow_fast_path_not_selected_for_list_batch_source() -> None:
    # CSV/JSONL-style source (supports_batch_emit but emits list[dict], NOT arrow)
    # paired with an arrow-native sink must NOT take the arrow fast path —
    # otherwise write_arrow_batch() would receive a list and crash.
    plan = build_runtime_plan(
        _BatchSource(),
        MiddlewareChain([]),
        SinkFanOut([_ArrowNativeSink()]),
        writer_batch_size=5000,
    )

    assert plan.lane == RuntimeLane.BATCH
    assert plan.writer.arrow_fast_path is False
    assert plan.writer.input_data_plane == DataPlane.PYTHON_BATCHES


def test_arrow_fast_path_selected_for_arrow_emitting_source() -> None:
    plan = build_runtime_plan(
        _ArrowBatchSource(),
        MiddlewareChain([]),
        SinkFanOut([_ArrowNativeSink()]),
        writer_batch_size=5000,
    )

    assert plan.lane == RuntimeLane.BATCH
    assert plan.writer.arrow_fast_path is True
    assert plan.source.emitted_plane == DataPlane.ARROW_BATCHES
    assert plan.writer.input_data_plane == DataPlane.ARROW_BATCHES
    assert "keeps arrow_batches" in plan.writer.input_data_plane_reason


def test_arrow_fast_path_selected_for_arrow_emitting_source_with_multiple_arrow_sinks() -> None:
    plan = build_runtime_plan(
        _ArrowBatchSource(),
        MiddlewareChain([]),
        SinkFanOut([_ArrowNativeSink(), _ArrowNativeSink()]),
        writer_batch_size=5000,
    )

    assert plan.lane == RuntimeLane.BATCH
    assert plan.writer.arrow_fast_path is True


def test_arrow_fast_path_selected_when_any_fan_out_sink_is_arrow_native() -> None:
    plan = build_runtime_plan(
        _ArrowBatchSource(),
        MiddlewareChain([]),
        SinkFanOut([_ArrowNativeSink(), _BatchSink()]),
        writer_batch_size=5000,
    )

    assert plan.lane == RuntimeLane.BATCH
    assert plan.writer.arrow_fast_path is True
    assert plan.writer.arrow_chain is True


def test_arrow_chain_selected_even_without_arrow_native_sink() -> None:
    plan = build_runtime_plan(
        _ArrowBatchSource(),
        MiddlewareChain([]),
        SinkFanOut([_BatchSink()]),
        writer_batch_size=5000,
    )

    assert plan.lane == RuntimeLane.BATCH
    assert plan.writer.arrow_fast_path is False
    assert plan.writer.arrow_chain is True


def test_arrow_chain_selected_when_all_stages_are_arrow_native() -> None:
    from agora.core.batch import ArrowBatchMiddleware

    class _IdentityArrowMW(ArrowBatchMiddleware):
        name = "identity_arrow"

        async def process_arrow_batch(self, batch, ctx):
            return batch

    plan = build_runtime_plan(
        _ArrowBatchSource(),
        MiddlewareChain([_IdentityArrowMW()]),
        SinkFanOut([_ArrowNativeSink()]),
        writer_batch_size=5000,
    )

    assert plan.writer.arrow_fast_path is True
    assert plan.writer.arrow_chain is True


def test_arrow_process_batch_middleware_keeps_arrow_chain_selected() -> None:
    from agora.middlewares.process import ArrowProcessBatchMiddleware

    def _identity(batch):
        return batch

    plan = build_runtime_plan(
        _ArrowBatchSource(),
        MiddlewareChain([ArrowProcessBatchMiddleware(fn=_identity, max_workers=1)]),
        SinkFanOut([_ArrowNativeSink()]),
        writer_batch_size=5000,
    )

    assert plan.writer.arrow_fast_path is True
    assert plan.writer.arrow_chain is True


def test_arrow_chain_validation_rejects_mixed_chain() -> None:
    from agora.core.batch import ArrowBatchMiddleware
    from agora.core.middleware import Middleware

    class _IdentityArrowMW(ArrowBatchMiddleware):
        name = "identity_arrow"

        async def process_arrow_batch(self, batch, ctx):
            return batch

    class _RegularMW(Middleware):
        name = "regular"

        async def process(self, record, ctx):
            return record

    with pytest.raises(PipelineError, match="mixes incompatible data planes"):
        build_runtime_plan(
            _ArrowBatchSource(),
            MiddlewareChain([_IdentityArrowMW(), _RegularMW()]),
            SinkFanOut([_ArrowNativeSink()]),
            writer_batch_size=5000,
        )


def test_arrow_chain_validation_rejects_non_arrow_source() -> None:
    from agora.core.batch import ArrowBatchMiddleware

    class _IdentityArrowMW(ArrowBatchMiddleware):
        name = "identity_arrow"

        async def process_arrow_batch(self, batch, ctx):
            return batch

    with pytest.raises(PipelineError, match="Arrow-emitting batch source"):
        build_runtime_plan(
            _BatchSource(),
            MiddlewareChain([_IdentityArrowMW()]),
            SinkFanOut([_ArrowNativeSink()]),
            writer_batch_size=5000,
        )


def test_arrow_chain_materializes_at_writer_boundary_when_writer_has_no_arrow_path() -> None:
    plan = build_runtime_plan(
        _ArrowBatchSource(),
        MiddlewareChain([]),
        SinkFanOut([_BatchSink()]),
        writer_batch_size=5000,
    )

    assert plan.source.emitted_plane == DataPlane.ARROW_BATCHES
    assert plan.middleware.output_data_plane == DataPlane.ARROW_BATCHES
    assert plan.writer.arrow_chain is True
    assert plan.writer.arrow_fast_path is False
    assert plan.writer.input_data_plane == DataPlane.ARROW_BATCHES
    assert plan.writer.sink_plans[0].selected_data_plane == DataPlane.PYTHON_BATCHES
    assert "keeps arrow_batches until sink dispatch" in plan.writer.input_data_plane_reason
    assert "writer downgrades to python_batches" in plan.writer.sink_plans[0].selection_reason


def test_arrow_source_with_python_row_chain_tracks_single_materialization() -> None:
    class _RegularMW(Middleware[int, int]):
        name = "regular"

        async def process(self, record: int, ctx) -> int | None:
            del ctx
            return record

    plan = build_runtime_plan(
        _ArrowBatchSource(),
        MiddlewareChain([_RegularMW()]),
        SinkFanOut([_BatchSink()]),
        writer_batch_size=5000,
    )

    assert plan.middleware.input_data_plane == DataPlane.ARROW_BATCHES
    assert plan.middleware.output_data_plane == DataPlane.PYTHON_BATCHES
    assert plan.middleware.materializes_arrow_to_rows is True
    assert plan.middleware.materialization_reason is not None
    assert "materialize once before middleware execution" in plan.middleware.materialization_reason
    assert plan.writer.input_data_plane == DataPlane.PYTHON_BATCHES


def test_runtime_plan_tracks_sink_downgrades_for_mixed_arrow_fanout() -> None:
    plan = build_runtime_plan(
        _ArrowBatchSource(),
        MiddlewareChain([]),
        SinkFanOut([_ArrowNativeSink(), _RowOnlySink()]),
        writer_batch_size=5000,
    )

    assert plan.writer.input_data_plane == DataPlane.ARROW_BATCHES
    assert plan.writer.downgraded_sink_count == 1
    assert "only downgrades for sink paths" in plan.writer.input_data_plane_reason
    assert [sink.selected_data_plane for sink in plan.writer.sink_plans] == [
        DataPlane.ARROW_BATCHES,
        DataPlane.PYTHON_ROWS,
    ]
    assert plan.writer.sink_plans[0].selection_reason == "sink accepts arrow_batches natively"
    assert "writer downgrades to python_rows" in plan.writer.sink_plans[1].selection_reason


def test_router_does_not_advertise_arrow_writer_path_from_arrow_capable_sink() -> None:
    router = SinkRouter[int]().default(_ArrowNativeSink())  # type: ignore[arg-type]

    plan = build_runtime_plan(
        _ArrowBatchSource(),
        MiddlewareChain([]),
        router,
        writer_batch_size=5000,
    )

    assert plan.writer.arrow_chain is True
    assert plan.writer.arrow_fast_path is False
    assert plan.writer.input_data_plane == DataPlane.PYTHON_BATCHES
    assert plan.writer.sink_plans[0].selected_data_plane == DataPlane.PYTHON_ROWS


class _HintedBatchSource(_BatchSource):
    """List-batch source that advertises an Arrow-native counterpart."""

    source_name = "hinted_batch_source"
    arrow_alternative_hint = "ArrowFakeSource"


class _RowOnlySink(BaseSink[int]):
    sink_name = "row_only_sink"

    async def write(self, record: int) -> None:
        del record


def test_arrow_advisory_fires_for_hinted_source_with_arrow_sink(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from agora.core.runtime import _plan_middleware

    _plan_middleware._ADVISED_SOURCE_TYPES.discard(_HintedBatchSource)
    with caplog.at_level("INFO"):
        build_runtime_plan(
            _HintedBatchSource(),
            MiddlewareChain([]),
            SinkFanOut([_ArrowNativeSink()]),
            writer_batch_size=5000,
        )

    hits = [r for r in caplog.records if r.msg == "arrow_fast_path_available"]
    assert len(hits) == 1


def test_arrow_advisory_silent_for_arrow_emitting_source(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from agora.core.runtime import _plan_middleware

    _plan_middleware._ADVISED_SOURCE_TYPES.discard(_ArrowBatchSource)
    with caplog.at_level("INFO"):
        build_runtime_plan(
            _ArrowBatchSource(),
            MiddlewareChain([]),
            SinkFanOut([_ArrowNativeSink()]),
            writer_batch_size=5000,
        )

    assert not [r for r in caplog.records if r.msg == "arrow_fast_path_available"]


def test_arrow_advisory_silent_with_middleware(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from agora.core.runtime import _plan_middleware

    _plan_middleware._ADVISED_SOURCE_TYPES.discard(_HintedBatchSource)
    with caplog.at_level("INFO"):
        build_runtime_plan(
            _HintedBatchSource(),
            MiddlewareChain([_BufferedConcurrency1()]),
            SinkFanOut([_ArrowNativeSink()]),
            writer_batch_size=5000,
        )

    assert not [r for r in caplog.records if r.msg == "arrow_fast_path_available"]


def test_arrow_advisory_silent_when_sink_not_arrow(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from agora.core.runtime import _plan_middleware

    _plan_middleware._ADVISED_SOURCE_TYPES.discard(_HintedBatchSource)
    with caplog.at_level("INFO"):
        build_runtime_plan(
            _HintedBatchSource(),
            MiddlewareChain([]),
            SinkFanOut([_RowOnlySink()]),
            writer_batch_size=5000,
        )

    assert not [r for r in caplog.records if r.msg == "arrow_fast_path_available"]


def test_arrow_advisory_fires_once_per_source_type(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from agora.core.runtime import _plan_middleware

    _plan_middleware._ADVISED_SOURCE_TYPES.discard(_HintedBatchSource)
    with caplog.at_level("INFO"):
        for _ in range(3):
            build_runtime_plan(
                _HintedBatchSource(),
                MiddlewareChain([]),
                SinkFanOut([_ArrowNativeSink()]),
                writer_batch_size=5000,
            )

    hits = [r for r in caplog.records if r.msg == "arrow_fast_path_available"]
    assert len(hits) == 1
