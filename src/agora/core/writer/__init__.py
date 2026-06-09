"""Unified writer protocol and write outcomes."""

from agora.core.writer._protocol import Writer
from agora.core.writer._result import WriteResult

__all__ = ["WriteResult", "Writer"]
