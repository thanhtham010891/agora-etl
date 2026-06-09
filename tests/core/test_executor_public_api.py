from __future__ import annotations

import importlib

from agora import IterableSource
from agora.core.executor import PipelineExecutor, PipelineRuntimeSpec
from agora.core.middleware import MiddlewareChain
from agora.core.sink import SinkFanOut
from agora.core.types import DeliveryConfig
from agora.sinks.io.stdout import StdoutSink


def test_executor_module_reexports_public_api() -> None:
    module = importlib.import_module("agora.core.executor")

    assert module.PipelineExecutor is PipelineExecutor
    assert module.PipelineRuntimeSpec is PipelineRuntimeSpec


def test_pipeline_runtime_spec_import_path_remains_stable() -> None:
    spec = PipelineRuntimeSpec(
        source=IterableSource([]),
        chain=MiddlewareChain([]),
        writer=SinkFanOut([StdoutSink()]),
        pipeline_id="pipe",
        config=DeliveryConfig(),
    )

    executor = PipelineExecutor(spec)

    assert executor is not None
