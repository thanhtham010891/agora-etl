"""Source-side data-plane validation helpers."""

from __future__ import annotations

from agora.core.data_plane import DataPlane, SourceDataPlaneSpec


def source_data_plane_spec(source: object) -> SourceDataPlaneSpec:
    """Return the emitted data plane for *source*."""
    from agora.core.source._base import BaseSource

    if isinstance(source, BaseSource):
        return validated_source_data_plane_spec(source.data_plane_spec())

    advertised = getattr(source, "data_plane_spec", None)
    if callable(advertised):
        return validated_source_data_plane_spec(advertised())

    return default_source_data_plane_spec(source)


def validated_source_data_plane_spec(spec: object) -> SourceDataPlaneSpec:
    if not isinstance(spec, SourceDataPlaneSpec):
        raise TypeError("data_plane_spec() must return SourceDataPlaneSpec")
    return spec


def default_source_data_plane_spec(source: object) -> SourceDataPlaneSpec:
    """Return the default row-oriented data-plane contract for *source*."""
    supports_batch_emit = bool(getattr(source, "supports_batch_emit", False))
    emits_arrow_batches = bool(getattr(source, "emits_arrow_batches", False))
    if supports_batch_emit or emits_arrow_batches:
        raise TypeError(
            f"{type(source).__name__} still uses legacy source data-plane bool flags. "
            "In 0.4.0, implement data_plane_spec() returning SourceDataPlaneSpec."
        )
    return SourceDataPlaneSpec(
        source_name=str(getattr(source, "source_name", type(source).__name__)),
        emitted_plane=DataPlane.PYTHON_ROWS,
        supports_batch_emit=False,
        emits_arrow_batches=False,
    )
