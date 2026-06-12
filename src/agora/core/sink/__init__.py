"""Public sink facade for Agora."""

from agora.core.sink._support import (
    BatchWritable,
    ContextBindable,
    SinkCapabilities,
    bind_context_if_supported,
    sink_capabilities,
    sink_data_plane_spec,
    writer_target_data_plane_specs,
)
from agora.core.sink._writers import SinkFanOut, SinkRoute, SinkRouter
from agora.core.sink.base import BaseSink
from agora.core.writer import WriteResult

__all__ = [
    "BaseSink",
    "BatchWritable",
    "ContextBindable",
    "SinkCapabilities",
    "SinkFanOut",
    "SinkRoute",
    "SinkRouter",
    "WriteResult",
    "bind_context_if_supported",
    "sink_capabilities",
    "sink_data_plane_spec",
    "writer_target_data_plane_specs",
]
