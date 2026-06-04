from __future__ import annotations

import pytest

from agora import DeliveryConfig, InMemoryCheckpointStore, InMemoryTracer, Pipeline
from agora.core.middleware import Middleware
from agora.core.source import BaseSource


class _CheckpointedSource(BaseSource[dict[str, int]]):
    source_name = "checkpointed"
    supports_checkpoint = True

    def __init__(self, records: list[dict[str, int]]) -> None:
        self._records = records
        self._last_index = -1

    async def prepare_resume(self, checkpoint) -> None:
        del checkpoint
        return

    def current_checkpoint(self) -> dict[str, int] | None:
        if self._last_index < 0:
            return None
        return {"index": self._last_index}

    async def stream(self):
        for index, record in enumerate(self._records):
            self._last_index = index
            yield record


class _ArrowBatchSource(BaseSource[dict[str, int]]):
    source_name = "arrow_batch"
    supports_batch_emit = True
    emits_arrow_batches = True

    def __init__(self, rows: list[dict[str, int]]) -> None:
        self._rows = rows

    async def stream_batches(self):
        pa = pytest.importorskip("pyarrow")
        yield pa.RecordBatch.from_pylist(self._rows)

    async def stream(self):
        for row in self._rows:
            yield row


class _TagMiddleware(Middleware[dict[str, int], dict[str, int]]):
    name = "tag"

    async def process(self, record: dict[str, int], ctx):
        del ctx
        return {**record, "tagged": 1}


class _CollectSink:
    sink_name = "collect"

    def __init__(self) -> None:
        self.records: list[dict[str, int]] = []

    async def open(self) -> None:
        return None

    async def write(self, record: dict[str, int]) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _CollectDLQSink:
    sink_name = "collect_dlq"

    def __init__(self) -> None:
        self.records = []

    async def open(self) -> None:
        return None

    async def write(self, record) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _ArrowCollectSink:
    sink_name = "arrow_collect"

    def __init__(self) -> None:
        self.batches = []

    async def open(self) -> None:
        return None

    async def write_arrow_batch(self, batch) -> None:
        self.batches.append(batch)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


def _span_by_name(tracer: InMemoryTracer, name: str):
    return [span for span in tracer.spans if span.name == name]


@pytest.mark.asyncio
async def test_pipeline_tracing_captures_core_runtime_spans() -> None:
    tracer = InMemoryTracer()
    sink = _CollectSink()
    store = InMemoryCheckpointStore()

    summary = await (
        Pipeline(_CheckpointedSource([{"id": 1}]))
        .pipe(_TagMiddleware())
        .build(
            sink,  # type: ignore[arg-type]
            config=DeliveryConfig(
                checkpoint=store,
                tracer=tracer,
            ),
        )
        .run(run_id="trace-run")
    )

    assert summary.records_written == 1
    assert sink.records == [{"id": 1, "tagged": 1}]

    pipeline_span = _span_by_name(tracer, "pipeline.run")[0]
    assert pipeline_span.attributes["pipeline_id"] == "checkpointed"
    assert pipeline_span.attributes["run_id"] == "trace-run"
    assert pipeline_span.attributes["planned_lane"] == "linear"
    assert pipeline_span.attributes["direct_flush_eligible"] is False
    assert pipeline_span.attributes["arrow_fast_path_eligible"] is False
    assert pipeline_span.attributes["execution_lane"] == "linear"
    assert pipeline_span.attributes["direct_flush_active"] is False
    assert pipeline_span.ended is True

    source_span = _span_by_name(tracer, "source.stream")[0]
    assert source_span.parent_name == "pipeline.run"
    assert source_span.attributes["source"] == "checkpointed"
    assert source_span.attributes["lane"] == "linear"
    assert source_span.attributes["batch_source"] is False
    assert source_span.attributes["buffered_stage_count"] == 0
    assert source_span.attributes["direct_flush_eligible"] is False
    assert source_span.attributes["arrow_fast_path_eligible"] is False

    middleware_span = _span_by_name(tracer, "middleware.process")[0]
    assert middleware_span.parent_name == "source.stream"
    assert middleware_span.attributes["middleware"] == "tag"
    assert middleware_span.attributes["execution_mode"] == "linear"

    writer_span = _span_by_name(tracer, "writer.write")[0]
    assert writer_span.parent_name == "source.stream"
    assert writer_span.attributes["writer"] == "SinkFanOut"

    checkpoint_load_span = _span_by_name(tracer, "checkpoint.load")[0]
    assert checkpoint_load_span.attributes["checkpoint.loaded"] is False
    checkpoint_save_span = _span_by_name(tracer, "checkpoint.save")[0]
    assert checkpoint_save_span.attributes["checkpoint_key"] == "checkpointed"


@pytest.mark.asyncio
async def test_pipeline_tracing_records_writer_failures_and_dlq_writes() -> None:
    tracer = InMemoryTracer()
    dlq = _CollectDLQSink()

    class _BoomSink:
        sink_name = "boom"

        async def open(self) -> None:
            return None

        async def write(self, record: dict[str, int]) -> None:
            del record
            raise RuntimeError("sink broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    summary = await (
        Pipeline(_CheckpointedSource([{"id": 1}]))
        .build(
            _BoomSink(),  # type: ignore[arg-type]
            config=DeliveryConfig(
                dlq=dlq,  # type: ignore[arg-type]
                tracer=tracer,
            ),
        )
        .run(run_id="trace-fail")
    )

    assert summary.records_errored == 1
    assert len(dlq.records) == 1

    writer_span = _span_by_name(tracer, "writer.write")[0]
    assert writer_span.exceptions == []

    dlq_span = _span_by_name(tracer, "dlq.write")[0]
    assert dlq_span.attributes["stage"] == "sink_write"
    assert dlq_span.attributes["sink"] == "collect_dlq"
    assert dlq_span.parent_name == "source.stream"


@pytest.mark.asyncio
async def test_pipeline_tracing_captures_arrow_fast_path_metadata() -> None:
    tracer = InMemoryTracer()
    sink = _ArrowCollectSink()

    pa = pytest.importorskip("pyarrow")

    from agora import ArrowMapMiddleware

    summary = await (
        Pipeline(_ArrowBatchSource([{"id": 1}, {"id": 2}]))
        .pipe(ArrowMapMiddleware(lambda batch: batch))
        .build(
            sink,  # type: ignore[arg-type]
            config=DeliveryConfig(
                tracer=tracer,
            ),
        )
        .run(run_id="trace-arrow")
    )

    assert summary.records_written == 2
    assert len(sink.batches) == 1
    assert isinstance(sink.batches[0], pa.RecordBatch)

    pipeline_span = _span_by_name(tracer, "pipeline.run")[0]
    assert pipeline_span.attributes["planned_lane"] == "batch"
    assert pipeline_span.attributes["batch_source"] is True
    assert pipeline_span.attributes["source_data_plane"] == "arrow_batches"
    assert pipeline_span.attributes["writer_input_data_plane"] == "arrow_batches"
    assert pipeline_span.attributes["downgraded_sink_count"] == 0
    assert pipeline_span.attributes["arrow_fast_path_eligible"] is True
    assert pipeline_span.attributes["arrow_chain_eligible"] is True
    assert pipeline_span.attributes["execution_lane"] == "batch"
    assert pipeline_span.attributes["source_data_plane"] == "arrow_batches"
    assert pipeline_span.attributes["writer_input_data_plane"] == "arrow_batches"
    assert pipeline_span.attributes["arrow_fast_path_active"] is True
    assert pipeline_span.attributes["arrow_chain_active"] is True

    source_span = _span_by_name(tracer, "source.stream")[0]
    assert source_span.attributes["lane"] == "batch"
    assert source_span.attributes["batch_source"] is True
    assert source_span.attributes["source_data_plane"] == "arrow_batches"
    assert source_span.attributes["writer_input_data_plane"] == "arrow_batches"
    assert source_span.attributes["arrow_fast_path_eligible"] is True
    assert source_span.attributes["arrow_chain_eligible"] is True


@pytest.mark.asyncio
async def test_noop_tracer_trace_span_returns_singleton_and_allocates_no_spans() -> None:
    from agora.core.context import _NOOP_SPAN_SCOPE, PipelineContext
    from agora.core.metrics import PipelineMetrics
    from agora.core.tracing import NoopTracer

    ctx = PipelineContext(
        pipeline_id="test",
        metrics=PipelineMetrics(),
        tracer=NoopTracer(),
    )

    scope1 = ctx.trace_span("middleware.process", middleware="m1")
    scope2 = ctx.trace_span("writer.write", writer="SinkFanOut")

    assert scope1 is _NOOP_SPAN_SCOPE
    assert scope2 is _NOOP_SPAN_SCOPE
    assert ctx._trace_stack == []

    with ctx.trace_span("some.span"):
        assert ctx._trace_stack == []


@pytest.mark.asyncio
async def test_real_tracer_trace_span_pushes_and_pops_stack() -> None:
    from agora.core.context import PipelineContext
    from agora.core.metrics import PipelineMetrics

    tracer = InMemoryTracer()
    ctx = PipelineContext(
        pipeline_id="test",
        metrics=PipelineMetrics(),
        tracer=tracer,
    )

    with ctx.trace_span("outer", key="val") as outer_span:
        assert len(ctx._trace_stack) == 1
        assert ctx.current_span() is outer_span
        with ctx.trace_span("inner"):
            assert len(ctx._trace_stack) == 2
        assert len(ctx._trace_stack) == 1
    assert ctx._trace_stack == []
    assert len(tracer.spans) == 2
    assert tracer.spans[0].name == "outer"
    assert tracer.spans[0].attributes["key"] == "val"
