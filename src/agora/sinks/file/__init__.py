"""agora/sinks/file/__init__.py — file-based sinks."""

from agora.sinks.file.csv import CsvSink
from agora.sinks.file.jsonlines import JsonLinesSink
from agora.sinks.file.parquet import ParquetSink

__all__ = ["CsvSink", "JsonLinesSink", "ParquetSink"]
