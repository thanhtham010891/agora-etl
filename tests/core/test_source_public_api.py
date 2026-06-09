from __future__ import annotations

import importlib

from agora.core.source import (
    BaseSource,
    IterableSource,
    SourceRecordError,
    SourceRuntimeMetrics,
    prefetch_limit_for,
    source_data_plane_spec,
)


def test_source_module_reexports_public_api() -> None:
    module = importlib.import_module("agora.core.source")

    assert module.BaseSource is BaseSource
    assert module.IterableSource is IterableSource
    assert module.SourceRecordError is SourceRecordError
    assert module.SourceRuntimeMetrics is SourceRuntimeMetrics
    assert module.prefetch_limit_for is prefetch_limit_for
    assert module.source_data_plane_spec is source_data_plane_spec
