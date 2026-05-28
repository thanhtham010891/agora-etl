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

from dataclasses import replace
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import logstruct

from agora.core.executor import PipelineExecutor, PipelineRuntimeSpec
from agora.core.middleware import FilterMiddleware, MiddlewareChain
from agora.core.sink import BaseSink, SinkFanOut, SinkRouter
from agora.core.tracing import NoopTracer
from agora.core.types import DeliveryConfig

if TYPE_CHECKING:
    from collections.abc import Callable

    from agora.core.metrics import PipelineRunSummary
    from agora.core.source import BaseSource
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
        config: DeliveryConfig,
    ) -> BoundPipeline[Any]:
        return BoundPipeline(
            source=self._source,
            chain=MiddlewareChain(self._middlewares),
            writer=writer,
            pipeline_id=self._pipeline_id,
            config=config,
        )

    def build(
        self,
        sink: BaseSink[Any] | None = None,
        *,
        config: DeliveryConfig | None = None,
    ) -> BoundPipeline[Any]:
        config = config or DeliveryConfig()
        if sink is not None:
            sinks: list[BaseSink[Any]] = [sink]
        else:
            from agora.sinks.io.stdout import StdoutSink

            sinks = [StdoutSink()]

        writer: SinkFanOut[Any] = SinkFanOut(sinks)
        if config.sink_concurrency is not None:
            writer = writer.with_concurrency(config.sink_concurrency)

        return self._build_bound_pipeline(writer, config)

    def fan_out(
        self,
        sinks: list[BaseSink[Any]],
        *,
        config: DeliveryConfig | None = None,
    ) -> BoundPipeline[Any]:
        config = config or DeliveryConfig()
        writer: SinkFanOut[Any] = SinkFanOut(sinks)
        if config.sink_concurrency is not None:
            writer = writer.with_concurrency(config.sink_concurrency)

        return self._build_bound_pipeline(writer, config)

    def route(
        self,
        router: SinkRouter[Any],
        *,
        config: DeliveryConfig | None = None,
    ) -> BoundPipeline[Any]:
        config = config or DeliveryConfig()
        return self._build_bound_pipeline(router, config)


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
        config: DeliveryConfig | None = None,
    ) -> None:
        self._source = source
        self._chain = chain
        self._writer = writer
        self._pipeline_id = pipeline_id
        config = config or DeliveryConfig()
        self._config = replace(
            config,
            checkpoint_key=config.checkpoint_key or pipeline_id,
            checkpoint_every=max(config.checkpoint_every, 1),
            batch_size=max(config.batch_size, 1),
            tracer=config.tracer or NoopTracer(),
        )

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
            config=self._config,
        )

    def _runtime_spec(self) -> PipelineRuntimeSpec:
        return PipelineRuntimeSpec(
            source=self._source,
            chain=self._chain,
            writer=self._writer,
            pipeline_id=self._pipeline_id,
            config=self._config,
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
