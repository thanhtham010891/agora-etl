"""Lane strategy facade for Agora runtime execution."""

import time

from agora.core.runtime._lanes._batch import BatchLaneStrategy
from agora.core.runtime._lanes._buffered import BufferedLaneStrategy
from agora.core.runtime._lanes._linear import LinearLaneStrategy

__all__ = [
    "BatchLaneStrategy",
    "BufferedLaneStrategy",
    "LinearLaneStrategy",
    "time",
]
