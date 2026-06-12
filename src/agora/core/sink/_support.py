"""Internal sink capability and data-plane helpers."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, runtime_checkable

from agora.core.batch import is_arrow_native_sink
from agora.core.data_plane import DataPlane, SinkDataPlaneSpec, ordered_unique_planes

T = TypeVar("T")

if TYPE_CHECKING:
    from agora.core.writer import WriteResult


@runtime_checkable
class ContextBindable(Protocol):
    """Capability protocol for sinks/writers that accept run-scoped context."""

    def bind_context(self, ctx: Any) -> None:
        """Attach run-scoped context."""
        ...


@runtime_checkable
class BatchWritable(Protocol[T]):
    """Capability protocol for sinks that support explicit batch writes."""

    async def write_batch(self, records: list[T]) -> None | list[WriteResult]:
        """Persist a batch of records."""
        ...


@dataclass(frozen=True, slots=True)
class SinkCapabilities:
    """Execution hints advertised by sinks to the runtime/writer."""

    batch_writable_native: bool = False
    arrow_passthrough_native: bool = False
    parallel_writes_safe: bool = False
    ordered_writes_required: bool = True
    accepted_data_planes: tuple[DataPlane, ...] = ()
    native_data_planes: tuple[DataPlane, ...] = ()


def bind_context_if_supported(target: object, ctx: Any) -> None:
    """Bind context when the target advertises that capability."""
    bind_context = getattr(target, "bind_context", None)
    if callable(bind_context):
        bind_context(ctx)


def _has_batch_write_method(target: object) -> bool:
    return callable(getattr(target, "write_batch", None))


def _default_sink_data_planes(
    *,
    batch_native: bool,
    arrow_native: bool,
) -> tuple[tuple[DataPlane, ...], tuple[DataPlane, ...]]:
    accepted = [DataPlane.PYTHON_ROWS]
    native = [DataPlane.PYTHON_ROWS]
    if batch_native:
        accepted.append(DataPlane.PYTHON_BATCHES)
        native.append(DataPlane.PYTHON_BATCHES)
    if arrow_native:
        accepted.append(DataPlane.ARROW_BATCHES)
        native.append(DataPlane.ARROW_BATCHES)
    return ordered_unique_planes(accepted), ordered_unique_planes(native)


def normalized_sink_capabilities(
    capabilities: SinkCapabilities,
    *,
    batch_native: bool,
    arrow_native: bool,
) -> SinkCapabilities:
    accepted_data_planes, native_data_planes = _default_sink_data_planes(
        batch_native=batch_native,
        arrow_native=arrow_native,
    )
    if capabilities.accepted_data_planes:
        accepted_data_planes = capabilities.accepted_data_planes
    if capabilities.native_data_planes:
        native_data_planes = capabilities.native_data_planes
    batch_native = batch_native or DataPlane.PYTHON_BATCHES in native_data_planes
    arrow_native = arrow_native or DataPlane.ARROW_BATCHES in native_data_planes
    return replace(
        capabilities,
        batch_writable_native=batch_native,
        arrow_passthrough_native=arrow_native,
        accepted_data_planes=accepted_data_planes,
        native_data_planes=native_data_planes,
    )


def _raise_legacy_sink_flag_error(target: object) -> None:
    raise TypeError(
        f"{type(target).__name__} still uses legacy sink data-plane bool flags. "
        "In 0.4.0, declare accepted_data_planes/native_data_planes explicitly."
    )


def sink_capabilities(target: object) -> SinkCapabilities:
    """Return execution capabilities for *target*.

    Explicit ``sink_capabilities()`` implementations take precedence. For
    ``BaseSink`` subclasses, class attributes define the contract. For
    duck-typed sinks, native batch support falls back to ``write_batch()``
    presence so existing tests and simple sinks keep working.
    """
    advertised = getattr(target, "sink_capabilities", None)
    if callable(advertised):
        value = advertised()
        if isinstance(value, SinkCapabilities):
            if (
                (value.batch_writable_native or value.arrow_passthrough_native)
                and not value.accepted_data_planes
                and not value.native_data_planes
            ):
                _raise_legacy_sink_flag_error(target)
            return normalized_sink_capabilities(
                value,
                batch_native=bool(getattr(value, "batch_writable_native", False))
                or DataPlane.PYTHON_BATCHES in value.native_data_planes,
                arrow_native=(
                    bool(getattr(value, "arrow_passthrough_native", False))
                    or DataPlane.ARROW_BATCHES in value.native_data_planes
                    or is_arrow_native_sink(target)
                ),
            )
        raise TypeError("sink_capabilities() must return SinkCapabilities")

    batch_native = False
    arrow_native = False
    parallel_safe = False
    ordered_required = True

    from agora.core.sink.base import BaseSink

    if isinstance(target, BaseSink):
        accepted_planes = tuple(getattr(target, "accepted_data_planes", ()))
        native_planes = tuple(getattr(target, "native_data_planes", ()))
        batch_native = bool(getattr(target, "batch_writable_native", False))
        arrow_native = bool(getattr(target, "arrow_passthrough_native", False))
        parallel_safe = bool(getattr(target, "parallel_writes_safe", False))
        ordered_required = bool(getattr(target, "ordered_writes_required", True))
        if (batch_native or arrow_native) and not accepted_planes and not native_planes:
            _raise_legacy_sink_flag_error(target)
        if not batch_native and type(target).write_batch is not BaseSink.write_batch:
            batch_native = True
    elif _has_batch_write_method(target):
        batch_native = True
        arrow_native = is_arrow_native_sink(target)
    else:
        legacy_batch_native = bool(getattr(target, "batch_writable_native", False))
        legacy_arrow_native = bool(getattr(target, "arrow_passthrough_native", False))
        arrow_native = is_arrow_native_sink(target)
        if (
            (legacy_batch_native or legacy_arrow_native)
            and not getattr(target, "accepted_data_planes", ())
            and not getattr(target, "native_data_planes", ())
        ):
            _raise_legacy_sink_flag_error(target)
        batch_native = legacy_batch_native
        arrow_native = legacy_arrow_native or arrow_native

    return normalized_sink_capabilities(
        SinkCapabilities(
            batch_writable_native=batch_native,
            arrow_passthrough_native=arrow_native,
            parallel_writes_safe=parallel_safe,
            ordered_writes_required=ordered_required,
            accepted_data_planes=tuple(getattr(target, "accepted_data_planes", ())),
            native_data_planes=tuple(getattr(target, "native_data_planes", ())),
        ),
        batch_native=batch_native,
        arrow_native=arrow_native,
    )


def sink_data_plane_spec(target: object) -> SinkDataPlaneSpec:
    """Return the sink-side data-plane contract for *target*."""
    capabilities = sink_capabilities(target)
    return SinkDataPlaneSpec(
        sink_name=str(getattr(target, "sink_name", type(target).__name__)),
        accepted_planes=capabilities.accepted_data_planes,
        native_planes=capabilities.native_data_planes,
    )


def writer_target_data_plane_specs(writer: object) -> tuple[SinkDataPlaneSpec, ...]:
    """Return sink-level data-plane specs visible behind *writer*."""
    cached_specs = getattr(writer, "_sink_data_plane_specs", None)
    if cached_specs is not None:
        return tuple(cached_specs)

    inner_sinks = getattr(writer, "_sinks", None)
    if inner_sinks is not None:
        return tuple(sink_data_plane_spec(sink) for sink in inner_sinks)

    routes = getattr(writer, "_routes", None)
    default_sink = getattr(writer, "_default", None)
    if routes is not None:
        seen: set[int] = set()
        specs: list[SinkDataPlaneSpec] = []
        for route in routes:
            sink = route.sink
            sink_id = id(sink)
            if sink_id in seen:
                continue
            seen.add(sink_id)
            specs.append(sink_data_plane_spec(sink))
        if default_sink is not None and id(default_sink) not in seen:
            specs.append(sink_data_plane_spec(default_sink))
        return tuple(specs)

    advertised = getattr(writer, "sink_capabilities", None)
    if callable(advertised):
        return (sink_data_plane_spec(writer),)
    return ()
