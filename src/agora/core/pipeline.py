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
            config=DeliveryConfig(
                dlq=SQLiteDLQSink(path=".dlq.db"),
                checkpoint=store,
                checkpoint_every=50,
                batch_size=100,
                sink_concurrency=4,
                backpressure=Backpressure.adaptive(max_buffer_size=500),
            ),
        )
        .run(max_records=10_000)
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

from agora.core._pipeline_support import (
    build_sink_fanout_writer,
    explain_pipeline,
    normalize_delivery_config,
    run_async_sync_bridge,
)
from agora.core.executor import PipelineExecutor, PipelineRuntimeSpec
from agora.core.middleware import FilterMiddleware, MiddlewareChain
from agora.core.types import DeliveryConfig

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from agora.core.context import PipelineContext
    from agora.core.explain import PipelineExplain
    from agora.core.metrics import PipelineRunSummary
    from agora.core.sink import BaseSink, SinkRouter
    from agora.core.source import BaseSource
    from agora.core.writer import Writer

T = TypeVar("T")
U = TypeVar("U")


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

        writer = build_sink_fanout_writer(
            sinks,
            sink_concurrency=config.sink_concurrency,
        )
        return self._build_bound_pipeline(writer, config)

    def fan_out(
        self,
        sinks: list[BaseSink[Any]],
        *,
        config: DeliveryConfig | None = None,
    ) -> BoundPipeline[Any]:
        config = config or DeliveryConfig()
        writer = build_sink_fanout_writer(
            sinks,
            sink_concurrency=config.sink_concurrency,
        )
        return self._build_bound_pipeline(writer, config)

    def route(
        self,
        router: SinkRouter[Any],
        *,
        config: DeliveryConfig | None = None,
    ) -> BoundPipeline[Any]:
        config = config or DeliveryConfig()
        return self._build_bound_pipeline(router, config)

    def explain(
        self,
        sink: BaseSink[Any] | None = None,
        *,
        config: DeliveryConfig | None = None,
        max_records: int | None = None,
    ) -> PipelineExplain:
        """Build the default writer and return the pre-run execution summary."""
        return self.build(sink, config=config).explain(max_records=max_records)


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
        self._config = normalize_delivery_config(
            config,
            pipeline_id=pipeline_id,
        )
        self._live_metrics_callback: Callable[[PipelineContext], Awaitable[None]] | None = None

    @property
    def pipeline_id(self) -> str:
        return self._pipeline_id

    def with_sink(self, *sinks: BaseSink[Any]) -> BoundPipeline[Any]:
        """Replace sinks (used for dry-run mode)."""
        writer = build_sink_fanout_writer(
            list(sinks),
            sink_concurrency=self._config.sink_concurrency,
        )

        bound: BoundPipeline[Any] = BoundPipeline(
            source=self._source,
            chain=self._chain,
            writer=writer,
            pipeline_id=self._pipeline_id,
            config=self._config,
        )
        bound._live_metrics_callback = self._live_metrics_callback
        return bound

    def set_live_metrics_callback(
        self,
        callback: Callable[[PipelineContext], Awaitable[None]] | None,
    ) -> None:
        self._live_metrics_callback = callback

    def _runtime_spec(self) -> PipelineRuntimeSpec:
        return PipelineRuntimeSpec(
            source=self._source,
            chain=self._chain,
            writer=self._writer,
            pipeline_id=self._pipeline_id,
            config=self._config,
            live_metrics_callback=self._live_metrics_callback,
        )

    def explain(self, max_records: int | None = None) -> PipelineExplain:
        """Return the resolved runtime plan without starting the pipeline."""
        return explain_pipeline(
            source=self._source,
            chain=self._chain,
            writer=self._writer,
            pipeline_id=self._pipeline_id,
            config=self._config,
            max_records=max_records,
        )

    async def run(
        self,
        max_records: int | None = None,
        run_id: str | None = None,
        live_metrics_callback: Callable[[PipelineContext], Awaitable[None]] | None = None,
    ) -> PipelineRunSummary:
        previous_live_metrics_callback = self._live_metrics_callback
        if live_metrics_callback is not None:
            self._live_metrics_callback = live_metrics_callback
        try:
            executor = PipelineExecutor(self._runtime_spec())
            return await executor.execute(
                max_records=max_records,
                run_id=run_id,
            )
        finally:
            self._live_metrics_callback = previous_live_metrics_callback

    def run_sync(
        self,
        max_records: int | None = None,
        run_id: str | None = None,
    ) -> PipelineRunSummary:
        """Run the pipeline synchronously — no ``asyncio.run()`` required.

        Behaviour by calling context
        ----------------------------
        - **Plain script / Django management command / no running loop**: calls
          ``asyncio.run()`` directly.
        - **Already-running event loop** (e.g. Jupyter, another async framework):
          runs the coroutine in a new background thread with its own event loop,
          then blocks the calling thread until it completes.  This avoids the
          ``asyncio.run()`` restriction that forbids nesting event loops.

        Notes
        -----
        - Notebook users should prefer ``await pipeline.run()`` for better
          integration with the kernel's event loop.  ``run_sync()`` works in
          notebooks but spawns a thread, which may interact poorly with widgets
          or kernel-level async tooling.
        - ``run_sync()`` is not thread-safe: do not call it from multiple threads
          on the same ``BoundPipeline`` instance simultaneously.
        """
        coro = self.run(max_records=max_records, run_id=run_id)
        return run_async_sync_bridge(coro)
