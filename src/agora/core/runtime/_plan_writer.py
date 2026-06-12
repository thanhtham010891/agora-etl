"""Writer-side runtime planning helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from agora.core.data_plane import DataPlane
from agora.core.runtime._plan_types import WriterSinkPlan
from agora.core.sink import writer_target_data_plane_specs

if TYPE_CHECKING:
    from agora.core.source import BaseSource
    from agora.core.writer import Writer


def direct_flush_eligible(
    source: BaseSource[Any],
    writer: Writer[Any],
    writer_batch_size: int,
) -> bool:
    """Return whether the linear lane can skip PendingWrite objects."""
    del source
    if writer_batch_size <= 1:
        return False
    writer_caps = getattr(writer, "_sink_batch_writable", None)
    return bool(writer_caps is not None and len(writer_caps) == 1 and writer_caps[0])


def writer_has_arrow_batch_path(writer: Writer[Any]) -> bool:
    """Return whether the writer can preserve Arrow batches to any sink path."""
    sink_capability_cache = getattr(writer, "_sink_capabilities", None)
    if sink_capability_cache is not None:
        return any(
            DataPlane.ARROW_BATCHES in capability.native_data_planes
            for capability in sink_capability_cache
        )
    sink_specs = writer_target_data_plane_specs(writer)
    if getattr(writer, "_sinks", None) is not None and sink_specs:
        return any(DataPlane.ARROW_BATCHES in spec.native_planes for spec in sink_specs)
    return callable(getattr(writer, "write_arrow_batch", None))


def writer_accepts_arrow_batches(writer: Writer[Any]) -> bool:
    """Return whether the writer object itself can receive Arrow batches."""
    return callable(getattr(writer, "write_arrow_batch", None))


def sink_selection_reason(
    *,
    input_data_plane: DataPlane,
    selected_data_plane: DataPlane,
    native_data_planes: tuple[DataPlane, ...],
) -> str:
    """Explain any data-plane downgrade that happens at the sink boundary."""
    if selected_data_plane == input_data_plane:
        return f"sink accepts {input_data_plane.value} natively"
    native = ", ".join(plane.value for plane in native_data_planes) or "python_rows"
    return (
        f"sink has no native {input_data_plane.value} path; writer downgrades to "
        f"{selected_data_plane.value} at the sink boundary (native={native})"
    )


def writer_sink_plans(
    writer: Writer[Any],
    *,
    input_data_plane: DataPlane,
) -> tuple[WriterSinkPlan, ...]:
    """Resolve writer-to-sink data-plane selections."""
    sink_specs = writer_target_data_plane_specs(writer)
    if not sink_specs:
        return ()
    return tuple(
        WriterSinkPlan(
            sink_name=spec.sink_name,
            accepted_data_planes=spec.accepted_planes,
            native_data_planes=spec.native_planes,
            selected_data_plane=(selected := spec.selected_plane_for(input_data_plane)),
            downgraded_from_input=spec.downgraded_from(input_data_plane),
            selection_reason=sink_selection_reason(
                input_data_plane=input_data_plane,
                selected_data_plane=selected,
                native_data_planes=spec.native_planes,
            ),
        )
        for spec in sink_specs
    )


def writer_input_data_plane_reason(
    *,
    middleware_output_data_plane: DataPlane,
    writer_input_data_plane: DataPlane,
    arrow_fast_path: bool,
    sink_plans: tuple[WriterSinkPlan, ...],
) -> str:
    """Explain the writer input data plane chosen by the planner."""
    if middleware_output_data_plane != DataPlane.ARROW_BATCHES:
        return f"writer receives middleware output as {middleware_output_data_plane.value}"
    if writer_input_data_plane != DataPlane.ARROW_BATCHES:
        return (
            "writer materializes arrow_batches to python_batches before sink dispatch "
            "because the writer object has no Arrow batch entrypoint"
        )
    downgraded_sinks = tuple(sink for sink in sink_plans if sink.downgraded_from_input)
    if not downgraded_sinks:
        if arrow_fast_path:
            return (
                "writer keeps arrow_batches because the source/middleware chain stays "
                "Arrow-native and every resolved sink path accepts Arrow batches natively"
            )
        return "writer keeps arrow_batches through the sink boundary"
    if arrow_fast_path:
        return (
            "writer keeps arrow_batches through the writer boundary and only downgrades "
            "for sink paths that do not expose a native Arrow batch write path"
        )
    return (
        "writer keeps arrow_batches until sink dispatch, then downgrades every sink path "
        "because none of the resolved sinks exposes a native Arrow batch write path"
    )
