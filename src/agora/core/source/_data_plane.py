"""Source-side data-plane compatibility and validation helpers."""

from __future__ import annotations

import warnings

from agora.core.data_plane import DataPlane, SourceDataPlaneSpec

_WARNED_LEGACY_SOURCE_TYPES: set[type[object]] = set()


def source_data_plane_spec(source: object) -> SourceDataPlaneSpec:
    """Return the emitted data plane for *source*."""
    from agora.core.source._base import BaseSource

    if isinstance(source, BaseSource):
        return validated_source_data_plane_spec(source.data_plane_spec())

    advertised = getattr(source, "data_plane_spec", None)
    if callable(advertised):
        return validated_source_data_plane_spec(advertised())

    return source_data_plane_spec_from_legacy_flags(source, warn=True)


def validated_source_data_plane_spec(spec: object) -> SourceDataPlaneSpec:
    if not isinstance(spec, SourceDataPlaneSpec):
        raise TypeError("data_plane_spec() must return SourceDataPlaneSpec")
    return spec


def source_data_plane_spec_from_legacy_flags(
    source: object,
    *,
    warn: bool,
) -> SourceDataPlaneSpec:
    """Compatibility bridge for older source bool flags."""
    supports_batch_emit = bool(getattr(source, "supports_batch_emit", False))
    emits_arrow_batches = bool(getattr(source, "emits_arrow_batches", False))
    if warn and (supports_batch_emit or emits_arrow_batches):
        source_type = type(source)
        if source_type not in _WARNED_LEGACY_SOURCE_TYPES:
            _WARNED_LEGACY_SOURCE_TYPES.add(source_type)
            warnings.warn(
                f"{source_type.__name__} uses legacy source data-plane bool flags; "
                "override data_plane_spec() returning SourceDataPlaneSpec instead. "
                "Legacy flags remain supported in 0.3.x and are planned for removal in 0.4.0.",
                DeprecationWarning,
                stacklevel=3,
            )
    emitted_plane = DataPlane.PYTHON_ROWS
    if emits_arrow_batches:
        emitted_plane = DataPlane.ARROW_BATCHES
    elif supports_batch_emit:
        emitted_plane = DataPlane.PYTHON_BATCHES
    return SourceDataPlaneSpec(
        source_name=str(getattr(source, "source_name", type(source).__name__)),
        emitted_plane=emitted_plane,
        supports_batch_emit=supports_batch_emit,
        emits_arrow_batches=emits_arrow_batches,
    )
