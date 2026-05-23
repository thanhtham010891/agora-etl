"""
agora/core/pipeline.py
======================
``Pipeline`` — fluent builder + ``BoundPipeline`` — async runner.

Usage::

    summary = await (
        Pipeline(KafkaSource(topics=["raw_events"], config=kafka_cfg))
        .pipe(NormalizerMiddleware())
        .filter(lambda r: r.confidence > 0.8)
        .build(
            PostgresSink(dsn=dsn),
            dlq=SQLiteDLQSink(path=".dlq.db"),
            checkpoint=store,
            checkpoint_every=50,
            batch_size=100,
            sink_concurrency=4,
            backpressure=Backpressure.adaptive(max_buffer_size=500),
        )
        .run(max_records=10_000)
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct

from agora.core.executor import PipelineExecutor, PipelineRuntimeSpec
from agora.core.middleware import FilterMiddleware, MiddlewareChain
from agora.core.sink import BaseSink, SinkFanOut, SinkRouter
from agora.core.tracing import NoopTracer
from agora.core.types import (
    Backpressure,
    CheckpointFailurePolicy,
    DLQFailurePolicy,
    SinkFailurePolicy,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from agora.core.checkpoint import CheckpointStore
    from agora.core.dlq import DLQRecord
    from agora.core.metrics import PipelineRunSummary
    from agora.core.source import BaseSource
    from agora.core.tracing import PipelineTracer
    from agora.core.writer import Writer

T = TypeVar("T")
U = TypeVar("U")

logger = logstruct.getLogger(__name__)


# ======================================================================
# Pipeline — immutable fluent builder
# ======================================================================


class Pipeline(Generic[T]):
    """Immutable fluent pipeline builder.

    Create with ``Pipeline(source)`` or ``Pipeline(source, id="my-pipe")``.
    """

    def __init__(
        self,
        source: BaseSource[T],
        id: str | None = None,
        *,
        _middlewares: list[Any] | None = None,
    ) -> None:
        self._source = source
        self._middlewares = list(_middlewares) if _middlewares else []
        self._pipeline_id = id or source.source_name

    # ------------------------------------------------------------------ #
    # Fluent builders (return new Pipeline — immutable)                   #
    # ------------------------------------------------------------------ #

    def pipe(self, middleware: Any) -> Pipeline[Any]:
        return Pipeline(
            self._source,
            id=self._pipeline_id,
            _middlewares=[*self._middlewares, middleware],
        )

    def filter(self, predicate: Callable[[Any], bool], name: str = "filter") -> Pipeline[Any]:
        return self.pipe(FilterMiddleware(predicate=predicate, name=name))

    # ------------------------------------------------------------------ #
    # Terminal builders — produce BoundPipeline                           #
    # ------------------------------------------------------------------ #

    def _build_bound_pipeline(
        self,
        writer: Writer[Any],
        *,
        dlq: BaseSink[DLQRecord] | None = None,
        dlq_failure_policy: DLQFailurePolicy = DLQFailurePolicy.LOG_ONLY,
        checkpoint: CheckpointStore | None = None,
        checkpoint_key: str | None = None,
        checkpoint_every: int = 1,
        checkpoint_failure_policy: CheckpointFailurePolicy = CheckpointFailurePolicy.FAIL_CLOSED,
        batch_size: int = 1,
        sink_failure_policy: SinkFailurePolicy = SinkFailurePolicy.FAIL_CLOSED,
        max_buffer_size: int | None = None,
        backpressure: Backpressure | None = None,
        tracer: PipelineTracer | None = None,
    ) -> BoundPipeline[Any]:
        return BoundPipeline(
            source=self._source,
            chain=MiddlewareChain(self._middlewares),
            writer=writer,
            pipeline_id=self._pipeline_id,
            dlq=dlq,
            dlq_failure_policy=dlq_failure_policy,
            checkpoint=checkpoint,
            checkpoint_key=checkpoint_key or self._pipeline_id,
            checkpoint_every=max(checkpoint_every, 1),
            checkpoint_failure_policy=checkpoint_failure_policy,
            batch_size=max(batch_size, 1),
            sink_failure_policy=sink_failure_policy,
            max_buffer_size=max_buffer_size,
            backpressure=backpressure,
            tracer=tracer or NoopTracer(),
        )

    def build(
        self,
        sink: BaseSink[Any] | None = None,
        *,
        dlq: BaseSink[DLQRecord] | None = None,
        dlq_failure_policy: DLQFailurePolicy = DLQFailurePolicy.LOG_ONLY,
        checkpoint: CheckpointStore | None = None,
        checkpoint_key: str | None = None,
        checkpoint_every: int = 1,
        checkpoint_failure_policy: CheckpointFailurePolicy = CheckpointFailurePolicy.FAIL_CLOSED,
        batch_size: int = 1,
        sink_failure_policy: SinkFailurePolicy = SinkFailurePolicy.FAIL_CLOSED,
        sink_concurrency: int | None = None,
        max_buffer_size: int | None = None,
        backpressure: Backpressure | None = None,
        tracer: PipelineTracer | None = None,
    ) -> BoundPipeline[Any]:
        if sink is not None:
            sinks: list[BaseSink[Any]] = [sink]
        else:
            from agora.sinks.io.stdout import StdoutSink

            sinks = [StdoutSink()]

        writer: SinkFanOut[Any] = SinkFanOut(sinks)
        if sink_concurrency is not None:
            writer = writer.with_concurrency(sink_concurrency)

        return self._build_bound_pipeline(
            writer,
            dlq=dlq,
            dlq_failure_policy=dlq_failure_policy,
            checkpoint=checkpoint,
            checkpoint_key=checkpoint_key,
            checkpoint_every=checkpoint_every,
            checkpoint_failure_policy=checkpoint_failure_policy,
            batch_size=batch_size,
            sink_failure_policy=sink_failure_policy,
            max_buffer_size=max_buffer_size,
            backpressure=backpressure,
            tracer=tracer,
        )

    def fan_out(
        self,
        sinks: list[BaseSink[Any]],
        *,
        dlq: BaseSink[DLQRecord] | None = None,
        dlq_failure_policy: DLQFailurePolicy = DLQFailurePolicy.LOG_ONLY,
        checkpoint: CheckpointStore | None = None,
        checkpoint_key: str | None = None,
        checkpoint_every: int = 1,
        checkpoint_failure_policy: CheckpointFailurePolicy = CheckpointFailurePolicy.FAIL_CLOSED,
        batch_size: int = 1,
        sink_failure_policy: SinkFailurePolicy = SinkFailurePolicy.FAIL_CLOSED,
        sink_concurrency: int | None = None,
        max_buffer_size: int | None = None,
        backpressure: Backpressure | None = None,
        tracer: PipelineTracer | None = None,
    ) -> BoundPipeline[Any]:
        writer: SinkFanOut[Any] = SinkFanOut(sinks)
        if sink_concurrency is not None:
            writer = writer.with_concurrency(sink_concurrency)

        return self._build_bound_pipeline(
            writer,
            dlq=dlq,
            dlq_failure_policy=dlq_failure_policy,
            checkpoint=checkpoint,
            checkpoint_key=checkpoint_key,
            checkpoint_every=checkpoint_every,
            checkpoint_failure_policy=checkpoint_failure_policy,
            batch_size=batch_size,
            sink_failure_policy=sink_failure_policy,
            max_buffer_size=max_buffer_size,
            backpressure=backpressure,
            tracer=tracer,
        )

    def route(
        self,
        router: SinkRouter[Any],
        *,
        dlq: BaseSink[DLQRecord] | None = None,
        dlq_failure_policy: DLQFailurePolicy = DLQFailurePolicy.LOG_ONLY,
        checkpoint: CheckpointStore | None = None,
        checkpoint_key: str | None = None,
        checkpoint_every: int = 1,
        checkpoint_failure_policy: CheckpointFailurePolicy = CheckpointFailurePolicy.FAIL_CLOSED,
        batch_size: int = 1,
        sink_failure_policy: SinkFailurePolicy = SinkFailurePolicy.FAIL_CLOSED,
        max_buffer_size: int | None = None,
        backpressure: Backpressure | None = None,
        tracer: PipelineTracer | None = None,
    ) -> BoundPipeline[Any]:
        return self._build_bound_pipeline(
            router,
            dlq=dlq,
            dlq_failure_policy=dlq_failure_policy,
            checkpoint=checkpoint,
            checkpoint_key=checkpoint_key,
            checkpoint_every=checkpoint_every,
            checkpoint_failure_policy=checkpoint_failure_policy,
            batch_size=batch_size,
            sink_failure_policy=sink_failure_policy,
            max_buffer_size=max_buffer_size,
            backpressure=backpressure,
            tracer=tracer,
        )


# ======================================================================
# BoundPipeline — async runner
# ======================================================================


class BoundPipeline(Generic[T]):
    """Fully configured, runnable pipeline. Call ``.run()`` to execute."""

    def __init__(
        self,
        source: BaseSource[Any],
        chain: MiddlewareChain[Any, Any],
        writer: Writer[Any],
        pipeline_id: str,
        *,
        dlq: BaseSink[DLQRecord] | None = None,
        dlq_failure_policy: DLQFailurePolicy = DLQFailurePolicy.LOG_ONLY,
        checkpoint: CheckpointStore | None = None,
        checkpoint_key: str | None = None,
        checkpoint_every: int = 1,
        checkpoint_failure_policy: CheckpointFailurePolicy = CheckpointFailurePolicy.FAIL_CLOSED,
        batch_size: int = 1,
        sink_failure_policy: SinkFailurePolicy = SinkFailurePolicy.FAIL_CLOSED,
        max_buffer_size: int | None = None,
        backpressure: Backpressure | None = None,
        tracer: PipelineTracer | None = None,
    ) -> None:
        self._source = source
        self._chain = chain
        self._writer = writer
        self._pipeline_id = pipeline_id
        self._dlq_sink = dlq
        self._dlq_failure_policy = dlq_failure_policy
        self._checkpoint_store = checkpoint
        self._checkpoint_key = checkpoint_key or pipeline_id
        self._checkpoint_every = checkpoint_every
        self._checkpoint_failure_policy = checkpoint_failure_policy
        self._writer_batch_size = batch_size
        self._sink_failure_policy = sink_failure_policy
        self._max_buffer_size = max_buffer_size
        self._backpressure = backpressure
        self._tracer: PipelineTracer = tracer or NoopTracer()

    @property
    def pipeline_id(self) -> str:
        return self._pipeline_id

    def with_sink(self, *sinks: BaseSink[Any]) -> BoundPipeline[Any]:
        """Replace sinks (used for dry-run mode)."""
        return BoundPipeline(
            source=self._source,
            chain=self._chain,
            writer=SinkFanOut(list(sinks)),
            pipeline_id=self._pipeline_id,
            dlq=self._dlq_sink,
            dlq_failure_policy=self._dlq_failure_policy,
            checkpoint=self._checkpoint_store,
            checkpoint_key=self._checkpoint_key,
            checkpoint_every=self._checkpoint_every,
            checkpoint_failure_policy=self._checkpoint_failure_policy,
            batch_size=self._writer_batch_size,
            sink_failure_policy=self._sink_failure_policy,
            max_buffer_size=self._max_buffer_size,
            backpressure=self._backpressure,
            tracer=self._tracer,
        )

    def _runtime_spec(self) -> PipelineRuntimeSpec:
        bp = self._backpressure
        return PipelineRuntimeSpec(
            source=self._source,
            chain=self._chain,
            writer=self._writer,
            pipeline_id=self._pipeline_id,
            dlq_sink=self._dlq_sink,
            dlq_failure_policy=self._dlq_failure_policy,
            checkpoint_store=self._checkpoint_store,
            checkpoint_failure_policy=self._checkpoint_failure_policy,
            checkpoint_key=self._checkpoint_key,
            checkpoint_every=self._checkpoint_every,
            writer_batch_size=self._writer_batch_size,
            sink_failure_policy=self._sink_failure_policy,
            tracer=self._tracer,
            max_buffer_size=self._max_buffer_size,
            adaptive_backpressure=bp is not None,
            adaptive_min_buffer_size=bp.min_buffer_size if bp else 1,
            adaptive_max_buffer_size=bp.max_buffer_size if bp else None,
            adaptive_scale_up_step=bp.scale_up_step if bp else 1,
            adaptive_scale_down_step=bp.scale_down_step if bp else 1,
            adaptive_writer_slow_ms=bp.writer_slow_ms if bp else 25.0,
            adaptive_checkpoint_slow_ms=bp.checkpoint_slow_ms if bp else 10.0,
        )

    async def run(
        self,
        max_records: int | None = None,
        run_id: str | None = None,
    ) -> PipelineRunSummary:
        executor = PipelineExecutor(self._runtime_spec())
        return await executor.execute(
            max_records=max_records,
            run_id=run_id,
        )
