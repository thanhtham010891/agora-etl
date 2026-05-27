"""
agora.sinks — built-in async sinks.

Registry
--------
``sink_registry`` provides plugin-style access to all built-in sinks::

    from agora.sinks import sink_registry

    # Lookup class
    cls = sink_registry.get_or_raise("stdout")
    sink = cls()

    # Installed plugins are discovered automatically via entry-points
    # when `agora.sinks` is imported.

    # Decorator registration
    @sink_registry.plugin("my_sink")
    class MySink(BaseSink[T]):
        ...
"""

from typing import Any

from agora.core.registry import Registry
from agora.core.sink import BaseSink
from agora.sinks.io.stdout import StdoutSink

# ======================================================================
# Sink Registry
# ======================================================================

sink_registry: Registry[type[BaseSink[Any]]] = Registry(name="sink")

# Register built-in sinks
sink_registry.register("stdout", StdoutSink)


def _register_lazy_sinks() -> None:
    """Register optional sinks as factories to avoid import-time errors."""

    def _jsonl_factory(**kwargs: Any) -> Any:
        from agora.sinks.file.jsonlines import JsonLinesSink

        return JsonLinesSink(**kwargs)

    def _csv_sink_factory(**kwargs: Any) -> Any:
        from agora.sinks.file.csv import CsvSink

        return CsvSink(**kwargs)

    def _parquet_sink_factory(**kwargs: Any) -> Any:
        from agora.sinks.file.parquet import ParquetSink

        return ParquetSink(**kwargs)

    def _log_factory(**kwargs: Any) -> Any:
        from agora.sinks.io.log import LogSink

        return LogSink(**kwargs)

    def _webhook_factory(**kwargs: Any) -> Any:
        from agora.sinks.http.webhook import WebhookSink

        return WebhookSink(**kwargs)

    def _sqlite_dlq_factory(**kwargs: Any) -> Any:
        from agora.core.dlq import SQLiteDLQSink

        return SQLiteDLQSink(**kwargs)

    sink_registry.register_factory("jsonl", _jsonl_factory)
    sink_registry.register_factory("csv", _csv_sink_factory)
    sink_registry.register_factory("parquet", _parquet_sink_factory)
    sink_registry.register_factory("log", _log_factory)
    sink_registry.register_factory("webhook", _webhook_factory)
    sink_registry.register_factory("sqlite_dlq", _sqlite_dlq_factory)


_register_lazy_sinks()
sink_registry.load_entrypoints("agora.sinks")

__all__ = ["StdoutSink", "sink_registry"]
