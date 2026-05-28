from __future__ import annotations

from agora.core.middleware import Middleware, MiddlewareChain
from agora.core.runtime import RuntimeLane, build_runtime_plan
from agora.core.sink import BaseSink, SinkFanOut
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
    supports_batch_emit = True

    async def stream(self):
        for record in [1, 2]:
            yield record

    async def stream_batches(self):  # type: ignore[override]
        yield [1, 2]


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
    batch_writable_native = True

    async def write(self, record: int) -> None:
        del record

    async def write_batch(self, records: list[int]) -> None:
        del records


def test_runtime_plan_selects_linear_lane_by_default() -> None:
    plan = build_runtime_plan(
        IterableSource([1, 2]),
        MiddlewareChain([]),
        SinkFanOut([_BatchSink()]),
        writer_batch_size=1,
    )

    assert plan.lane == RuntimeLane.LINEAR
    assert plan.writer.direct_flush_eligible is False


def test_runtime_plan_selects_buffered_lane_when_submit_stage_exists() -> None:
    plan = build_runtime_plan(
        IterableSource([1, 2]),
        MiddlewareChain([_BufferedPassThrough()]),
        SinkFanOut([_BatchSink()]),
        writer_batch_size=1,
    )

    assert plan.lane == RuntimeLane.BUFFERED
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
    assert plan.batch_source is True


def test_runtime_plan_disables_direct_flush_when_delivery_hooks_exist() -> None:
    plan = build_runtime_plan(
        _HookSource(),
        MiddlewareChain([]),
        SinkFanOut([_BatchSink()]),
        writer_batch_size=2,
    )

    assert plan.has_delivery_hooks is True
    assert plan.writer.direct_flush_eligible is False


class _ArrowNativeSink(BaseSink[int]):
    sink_name = "arrow_sink"

    async def write(self, record: int) -> None:
        del record

    async def write_arrow_batch(self, batch) -> None:
        del batch


class _ArrowBatchSource(_BatchSource):
    """A batch source that emits native arrow RecordBatch objects."""

    source_name = "arrow_batch_source"
    emits_arrow_batches = True


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


def test_arrow_fast_path_selected_for_arrow_emitting_source() -> None:
    plan = build_runtime_plan(
        _ArrowBatchSource(),
        MiddlewareChain([]),
        SinkFanOut([_ArrowNativeSink()]),
        writer_batch_size=5000,
    )

    assert plan.lane == RuntimeLane.BATCH
    assert plan.writer.arrow_fast_path is True


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


def test_arrow_chain_not_selected_for_mixed_chain() -> None:
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

    plan = build_runtime_plan(
        _ArrowBatchSource(),
        MiddlewareChain([_IdentityArrowMW(), _RegularMW()]),
        SinkFanOut([_ArrowNativeSink()]),
        writer_batch_size=5000,
    )

    assert plan.writer.arrow_fast_path is False
    assert plan.writer.arrow_chain is False
