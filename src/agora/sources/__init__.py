"""
agora.sources — built-in async sources.

Registry
--------
``source_registry`` provides plugin-style access to built-in and installed plugin sources::

    from agora.sources import source_registry

    # Installed plugins are discovered automatically via entry-points
    # when `agora.sources` is imported.

    # Lookup class
    cls = source_registry.get_or_raise("websocket")
    source = cls(url="wss://example.com/stream")

    # Decorator registration
    @source_registry.plugin("my_source")
    class MySource(BaseSource[T]):
        ...
"""

from agora.core.registry import Registry
from agora.core.source import BaseSource

# ======================================================================
# Source Registry
# ======================================================================

source_registry: Registry[type[BaseSource]] = Registry(name="source")


def _register_lazy_sources() -> None:
    """Register optional sources as factories to avoid import-time errors."""

    def _http_factory(**kwargs):
        from agora.sources.http.http import HTTPSource

        return HTTPSource(**kwargs)

    def _jsonl_factory(**kwargs):
        from agora.sources.file.jsonlines import JsonLinesSource

        return JsonLinesSource(**kwargs)

    def _parquet_factory(**kwargs):
        from agora.sources.file.parquet import ParquetSource

        return ParquetSource(**kwargs)

    def _file_factory(**kwargs):
        from agora.sources.file.base import FileSource

        return FileSource(**kwargs)

    def _csv_factory(**kwargs):
        from agora.sources.file.csv import CsvSource

        return CsvSource(**kwargs)

    def _iterable_factory(**kwargs):
        from agora.core.source import IterableSource

        return IterableSource(kwargs["records"])

    def _sqlite_dlq_factory(**kwargs):
        from agora.core.dlq import SQLiteDLQSource

        return SQLiteDLQSource(**kwargs)

    source_registry.register_factory("http", _http_factory)
    source_registry.register_factory("jsonl", _jsonl_factory)
    source_registry.register_factory("parquet", _parquet_factory)
    source_registry.register_factory("file", _file_factory)
    source_registry.register_factory("csv", _csv_factory)
    source_registry.register_factory("iterable", _iterable_factory)
    source_registry.register_factory("sqlite_dlq_source", _sqlite_dlq_factory)


_register_lazy_sources()
source_registry.load_entrypoints("agora.sources")

__all__ = ["source_registry"]
"""agora.sources — built-in async sources."""
