"""Optional acceleration boundary for runtime hot paths."""

from __future__ import annotations

import importlib
import importlib.metadata
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any, NoReturn, Protocol, cast

if TYPE_CHECKING:
    from collections.abc import Callable

_AGORA_RS_MODULE = "agora_rs"
_AGORA_RS_PACKAGE = "agora-etl-rs"
_MIN_AGORA_RS_VERSION = (0, 2, 0)


class AccelerationUnavailableError(RuntimeError):
    """Raised when an acceleration primitive is requested but unavailable."""


class AccelerationMode(StrEnum):
    """Runtime policy for optional acceleration."""

    AUTO = "auto"
    OFF = "off"
    REQUIRED = "required"


class AccelerationCapability(StrEnum):
    """Known optional acceleration capabilities."""

    RECORD_BUFFER = "record_buffer"
    METRICS_ACCUMULATOR = "metrics_accumulator"
    LINEAR_BATCH_BUFFER = "linear_batch_buffer"
    CHECKPOINT_STATE = "checkpoint_state"
    SYNC_BUILTIN_CHAIN_EXECUTOR = "sync_builtin_chain_executor"
    CSV_ARROW_WRITER = "csv_arrow_writer"
    JSONL_ARROW_WRITER = "jsonl_arrow_writer"
    JSONL_ARROW_READER = "jsonl_arrow_reader"


# `auto` is the release-facing policy, so keep only proven wins enabled there.
# `csv_arrow_writer` is a net win for the CSV Arrow sink boundary, but the
# current JSONL reader path still regresses the Arrow JSONL lanes badly enough
# that it should stay opt-in via `required` until the boundary is redesigned.
_AUTO_DISABLED_CAPABILITIES = frozenset({AccelerationCapability.JSONL_ARROW_READER})


class _ModuleLoader(Protocol):
    def __call__(self, name: str) -> Any: ...


class _VersionLoader(Protocol):
    def __call__(self, distribution_name: str) -> str: ...


@dataclass(frozen=True, slots=True)
class AccelerationStatus:
    """Structured view of the optional acceleration layer."""

    mode: AccelerationMode
    available: bool
    package_name: str = _AGORA_RS_PACKAGE
    module_name: str = _AGORA_RS_MODULE
    version: str | None = None
    compatible: bool = False
    capabilities: frozenset[AccelerationCapability] = frozenset()
    reason: str | None = None
    error: str | None = None

    @property
    def enabled(self) -> bool:
        """Return whether runtime code may use acceleration for this status."""
        return self.mode != AccelerationMode.OFF and self.available

    def supports(self, capability: AccelerationCapability | str) -> bool:
        """Return whether the capability is available and enabled."""
        try:
            normalized = (
                capability
                if isinstance(capability, AccelerationCapability)
                else AccelerationCapability(capability)
            )
        except ValueError:
            return False
        return self.enabled and normalized in self.capabilities


class _UnavailableRecordBuffer:
    def __init__(self, capacity: int) -> None:
        del capacity
        raise ImportError("agora-etl-rs is not installed.")


class _UnavailableMetricsAccumulator:
    def __init__(self, flush_interval: int = 100) -> None:
        del flush_interval
        raise ImportError("agora-etl-rs is not installed.")


class _UnavailableLinearBatchBuffer:
    def __init__(self, batch_size: int, metrics_flush_interval: int) -> None:
        del batch_size, metrics_flush_interval
        raise ImportError("agora-etl-rs is not installed.")


class _UnavailableCheckpointState:
    def __init__(self) -> None:
        raise ImportError("agora-etl-rs is not installed.")


class _UnavailableSyncBuiltinChainExecutor:
    def __init__(self, callables: list[Any], names: list[str]) -> None:
        del callables, names
        raise ImportError("agora-etl-rs is not installed.")


class _UnavailableCsvArrowWriter:
    def __init__(self, path: str, append: bool = False) -> None:
        del path, append
        raise ImportError("agora-etl-rs is not installed.")


class _UnavailableJsonlArrowWriter:
    def __init__(self, path: str, append: bool = False) -> None:
        del path, append
        raise ImportError("agora-etl-rs is not installed.")


def _unavailable_jsonl_arrow_reader(path: str, batch_size: int) -> Any:
    del path, batch_size
    raise ImportError("agora-etl-rs is not installed.")


def _metadata_version(distribution_name: str) -> str:
    return importlib.metadata.version(distribution_name)


def normalize_acceleration_mode(mode: AccelerationMode | str) -> AccelerationMode:
    """Normalize user-facing acceleration policy values."""
    if isinstance(mode, AccelerationMode):
        return mode
    try:
        return AccelerationMode(str(mode).strip().lower())
    except ValueError as exc:
        expected = ", ".join(policy.value for policy in AccelerationMode)
        raise ValueError(f"acceleration mode must be one of: {expected}") from exc


@dataclass(slots=True)
class AccelerationRuntime:
    """Detect and construct optional Rust-backed runtime primitives."""

    mode: AccelerationMode | str = AccelerationMode.AUTO
    module_loader: _ModuleLoader = importlib.import_module
    version_loader: _VersionLoader = _metadata_version
    _module: Any | None = field(default=None, init=False, repr=False)
    _status: AccelerationStatus | None = field(default=None, init=False, repr=False)

    def status(self, *, refresh: bool = False) -> AccelerationStatus:
        if self._status is not None and not refresh:
            return self._status

        mode = normalize_acceleration_mode(self.mode)
        if mode == AccelerationMode.OFF:
            self._status = AccelerationStatus(
                mode=mode,
                available=False,
                reason="acceleration disabled by policy",
            )
            self._module = None
            return self._status

        try:
            module = self.module_loader(_AGORA_RS_MODULE)
        except ImportError as exc:
            self._module = None
            self._status = AccelerationStatus(
                mode=mode,
                available=False,
                reason="agora-etl-rs is not installed",
                error=str(exc),
            )
            return self._status

        version = self._detect_version(module)
        capabilities = self._capabilities_for_mode(self._detect_capabilities(module), mode)
        compatible = _version_is_compatible(version)
        available = bool(capabilities) and compatible and self._module_reports_available(module)
        self._module = module if available else None
        self._status = AccelerationStatus(
            mode=mode,
            available=available,
            version=version,
            compatible=compatible,
            capabilities=frozenset(capabilities),
            reason=None
            if available
            else _unavailable_reason(
                version=version, compatible=compatible, capabilities=capabilities
            ),
        )
        return self._status

    def available(self) -> bool:
        return self.status().enabled

    def supports(self, capability: AccelerationCapability | str) -> bool:
        return self.status().supports(capability)

    def record_buffer_class(self) -> type[Any]:
        if self.supports(AccelerationCapability.RECORD_BUFFER):
            return cast("type[Any]", self._require_module().RecordBuffer)
        return _UnavailableRecordBuffer

    def metrics_accumulator_class(self) -> type[Any]:
        if self.supports(AccelerationCapability.METRICS_ACCUMULATOR):
            return cast("type[Any]", self._require_module().MetricsAccumulator)
        return _UnavailableMetricsAccumulator

    def linear_batch_buffer_class(self) -> type[Any]:
        if self.supports(AccelerationCapability.LINEAR_BATCH_BUFFER):
            return cast("type[Any]", self._require_module().LinearBatchBuffer)
        return _UnavailableLinearBatchBuffer

    def checkpoint_state_class(self) -> type[Any]:
        if self.supports(AccelerationCapability.CHECKPOINT_STATE):
            return cast("type[Any]", self._require_module().CheckpointState)
        return _UnavailableCheckpointState

    def sync_builtin_chain_executor_class(self) -> type[Any]:
        if self.supports(AccelerationCapability.SYNC_BUILTIN_CHAIN_EXECUTOR):
            return cast("type[Any]", self._require_module().SyncBuiltinChainExecutor)
        return _UnavailableSyncBuiltinChainExecutor

    def csv_arrow_writer_class(self) -> type[Any]:
        if self.supports(AccelerationCapability.CSV_ARROW_WRITER):
            return cast("type[Any]", self._require_module().CsvArrowWriter)
        return _UnavailableCsvArrowWriter

    def jsonl_arrow_writer_class(self) -> type[Any]:
        if self.supports(AccelerationCapability.JSONL_ARROW_WRITER):
            return cast("type[Any]", self._require_module().JsonlArrowWriter)
        return _UnavailableJsonlArrowWriter

    def make_record_buffer(self, capacity: int) -> Any:
        if not self.supports(AccelerationCapability.RECORD_BUFFER):
            self._raise_unavailable(AccelerationCapability.RECORD_BUFFER)
        return self.record_buffer_class()(capacity)

    def make_metrics_accumulator(self, flush_interval: int = 100) -> Any:
        if not self.supports(AccelerationCapability.METRICS_ACCUMULATOR):
            self._raise_unavailable(AccelerationCapability.METRICS_ACCUMULATOR)
        return self.metrics_accumulator_class()(flush_interval=flush_interval)

    def make_linear_batch_buffer(self, batch_size: int, metrics_flush_interval: int) -> Any:
        if not self.supports(AccelerationCapability.LINEAR_BATCH_BUFFER):
            self._raise_unavailable(AccelerationCapability.LINEAR_BATCH_BUFFER)
        return self.linear_batch_buffer_class()(batch_size, metrics_flush_interval)

    def make_checkpoint_state(self, fallback_factory: Callable[[], Any] | None = None) -> Any:
        if self.supports(AccelerationCapability.CHECKPOINT_STATE):
            return self.checkpoint_state_class()()
        if fallback_factory is not None:
            return fallback_factory()
        return self._raise_unavailable(AccelerationCapability.CHECKPOINT_STATE)

    def make_sync_builtin_chain_executor(self, callables: list[Any], names: list[str]) -> Any:
        if not self.supports(AccelerationCapability.SYNC_BUILTIN_CHAIN_EXECUTOR):
            self._raise_unavailable(AccelerationCapability.SYNC_BUILTIN_CHAIN_EXECUTOR)
        return self.sync_builtin_chain_executor_class()(callables, names)

    def make_csv_arrow_writer(self, path: str, append: bool = False) -> Any:
        if not self.supports(AccelerationCapability.CSV_ARROW_WRITER):
            self._raise_unavailable(AccelerationCapability.CSV_ARROW_WRITER)
        return self.csv_arrow_writer_class()(path, append)

    def make_jsonl_arrow_writer(self, path: str, append: bool = False) -> Any:
        if not self.supports(AccelerationCapability.JSONL_ARROW_WRITER):
            self._raise_unavailable(AccelerationCapability.JSONL_ARROW_WRITER)
        return self.jsonl_arrow_writer_class()(path, append)

    def read_jsonl_arrow_batches(self, path: str, batch_size: int) -> Any:
        if not self.supports(AccelerationCapability.JSONL_ARROW_READER):
            self._raise_unavailable(AccelerationCapability.JSONL_ARROW_READER)
        reader = getattr(self._require_module(), "read_jsonl_record_batches", None)
        if not callable(reader):
            return _unavailable_jsonl_arrow_reader(path, batch_size)
        return reader(path, batch_size)

    def _require_module(self) -> Any:
        status = self.status()
        if self._module is None or not status.enabled:
            raise AccelerationUnavailableError(status.reason or "acceleration is unavailable")
        return self._module

    def _raise_unavailable(self, capability: AccelerationCapability) -> NoReturn:
        status = self.status()
        detail = status.reason or "agora-etl-rs does not expose the requested capability"
        raise AccelerationUnavailableError(f"{capability.value} unavailable: {detail}")

    def _detect_version(self, module: Any) -> str | None:
        version_attr = getattr(module, "__version__", None)
        if isinstance(version_attr, str):
            return version_attr
        version_fn = getattr(module, "version", None)
        if callable(version_fn):
            try:
                version_value = version_fn()
            except Exception:
                version_value = None
            if isinstance(version_value, str):
                return version_value
        try:
            return self.version_loader(_AGORA_RS_PACKAGE)
        except importlib.metadata.PackageNotFoundError:
            return None

    @staticmethod
    def _module_reports_available(module: Any) -> bool:
        rust_available = getattr(module, "RUST_AVAILABLE", True)
        if not bool(rust_available):
            return False
        is_available = getattr(module, "is_available", None)
        if not callable(is_available):
            return True
        try:
            return bool(is_available())
        except Exception:
            return False

    def _detect_capabilities(self, module: Any) -> set[AccelerationCapability]:
        declared = self._declared_capabilities(module)
        if declared:
            return {
                capability
                for capability in declared
                if self._capability_is_constructible(module, capability)
            }

        capabilities: set[AccelerationCapability] = set()
        for capability in _CAPABILITY_VERIFIERS:
            if self._capability_is_constructible(module, capability):
                capabilities.add(capability)
        return capabilities

    @staticmethod
    def _capabilities_for_mode(
        capabilities: set[AccelerationCapability],
        mode: AccelerationMode,
    ) -> set[AccelerationCapability]:
        if mode == AccelerationMode.AUTO:
            return {
                capability
                for capability in capabilities
                if capability not in _AUTO_DISABLED_CAPABILITIES
            }
        return capabilities

    @staticmethod
    def _capability_is_constructible(module: Any, capability: AccelerationCapability) -> bool:
        verifier = _CAPABILITY_VERIFIERS[capability]
        try:
            verifier(module)
        except Exception:
            return False
        return True

    @staticmethod
    def _declared_capabilities(module: Any) -> set[AccelerationCapability]:
        capabilities_fn = getattr(module, "capabilities", None)
        if not callable(capabilities_fn):
            return set()
        try:
            raw_capabilities = capabilities_fn()
        except Exception:
            return set()

        result: set[AccelerationCapability] = set()
        for raw in raw_capabilities:
            try:
                result.add(AccelerationCapability(str(raw)))
            except ValueError:
                continue
        return result


def _verify_record_buffer(module: Any) -> None:
    module.RecordBuffer(1)


def _verify_metrics_accumulator(module: Any) -> None:
    module.MetricsAccumulator()


def _verify_linear_batch_buffer(module: Any) -> None:
    module.LinearBatchBuffer(1, 1)


def _verify_checkpoint_state(module: Any) -> None:
    module.CheckpointState()


def _verify_sync_builtin_chain_executor(module: Any) -> None:
    module.SyncBuiltinChainExecutor([], [])


def _verify_csv_arrow_writer(module: Any) -> None:
    module.CsvArrowWriter("__agora_rs_probe__.csv", False)


def _verify_jsonl_arrow_writer(module: Any) -> None:
    module.JsonlArrowWriter("__agora_rs_probe__.jsonl", False)


def _verify_jsonl_arrow_reader(module: Any) -> None:
    reader = getattr(module, "read_jsonl_record_batches", None)
    if not callable(reader):
        raise TypeError("agora-etl-rs does not expose read_jsonl_record_batches")


_CAPABILITY_VERIFIERS: dict[AccelerationCapability, Callable[[Any], None]] = {
    AccelerationCapability.RECORD_BUFFER: _verify_record_buffer,
    AccelerationCapability.METRICS_ACCUMULATOR: _verify_metrics_accumulator,
    AccelerationCapability.LINEAR_BATCH_BUFFER: _verify_linear_batch_buffer,
    AccelerationCapability.CHECKPOINT_STATE: _verify_checkpoint_state,
    AccelerationCapability.SYNC_BUILTIN_CHAIN_EXECUTOR: _verify_sync_builtin_chain_executor,
    AccelerationCapability.CSV_ARROW_WRITER: _verify_csv_arrow_writer,
    AccelerationCapability.JSONL_ARROW_WRITER: _verify_jsonl_arrow_writer,
    AccelerationCapability.JSONL_ARROW_READER: _verify_jsonl_arrow_reader,
}


def _version_is_compatible(version: str | None) -> bool:
    """Return whether the installed acceleration package matches core expectations."""
    if version is None:
        return False
    parsed: list[int] = []
    for part in version.split(".")[:3]:
        digits = ""
        for char in part:
            if not char.isdigit():
                break
            digits += char
        if not digits:
            break
        parsed.append(int(digits))
    if not parsed:
        return False
    while len(parsed) < 3:
        parsed.append(0)
    return tuple(parsed[:3]) >= _MIN_AGORA_RS_VERSION


def _unavailable_reason(
    *,
    version: str | None,
    compatible: bool,
    capabilities: set[AccelerationCapability],
) -> str:
    if version is None:
        return "agora-etl-rs loaded but version metadata is unavailable"
    if not compatible:
        minimum = ".".join(str(part) for part in _MIN_AGORA_RS_VERSION)
        return f"agora-etl-rs {version} is incompatible; requires >= {minimum}"
    if not capabilities:
        return "agora-etl-rs loaded but no usable capabilities were found"
    return "agora-etl-rs loaded but runtime availability check failed"


DEFAULT_ACCELERATION = AccelerationRuntime()


def _runtime_for_mode(mode: AccelerationMode | str) -> AccelerationRuntime:
    normalized = normalize_acceleration_mode(mode)
    if normalized == AccelerationMode.AUTO:
        return DEFAULT_ACCELERATION
    return AccelerationRuntime(mode=normalized)


def acceleration_status(
    mode: AccelerationMode | str = AccelerationMode.AUTO,
    *,
    refresh: bool = False,
) -> AccelerationStatus:
    return _runtime_for_mode(mode).status(refresh=refresh)


def acceleration_available(mode: AccelerationMode | str = AccelerationMode.AUTO) -> bool:
    return _runtime_for_mode(mode).available()


def acceleration_supports(
    capability: AccelerationCapability | str,
    *,
    mode: AccelerationMode | str = AccelerationMode.AUTO,
) -> bool:
    return _runtime_for_mode(mode).supports(capability)


def require_acceleration(
    mode: AccelerationMode | str = AccelerationMode.AUTO,
) -> AccelerationStatus:
    """Return status, raising when policy requires unavailable acceleration."""
    normalized = normalize_acceleration_mode(mode)
    status = acceleration_status(normalized, refresh=normalized == AccelerationMode.REQUIRED)
    if normalized == AccelerationMode.REQUIRED and not status.enabled:
        reason = status.reason or "acceleration is unavailable"
        raise AccelerationUnavailableError(f"required acceleration unavailable: {reason}")
    return status


def record_buffer_class() -> type[Any]:
    return DEFAULT_ACCELERATION.record_buffer_class()


def metrics_accumulator_class() -> type[Any]:
    return DEFAULT_ACCELERATION.metrics_accumulator_class()


def linear_batch_buffer_class() -> type[Any]:
    return DEFAULT_ACCELERATION.linear_batch_buffer_class()


def sync_builtin_chain_executor_class() -> type[Any]:
    return DEFAULT_ACCELERATION.sync_builtin_chain_executor_class()


def csv_arrow_writer_class() -> type[Any]:
    return DEFAULT_ACCELERATION.csv_arrow_writer_class()


def jsonl_arrow_writer_class() -> type[Any]:
    return DEFAULT_ACCELERATION.jsonl_arrow_writer_class()


def make_record_buffer(
    capacity: int,
    *,
    mode: AccelerationMode | str = AccelerationMode.AUTO,
) -> Any:
    return _runtime_for_mode(mode).make_record_buffer(capacity)


def make_metrics_accumulator(
    flush_interval: int = 100,
    *,
    mode: AccelerationMode | str = AccelerationMode.AUTO,
) -> Any:
    return _runtime_for_mode(mode).make_metrics_accumulator(flush_interval)


def make_linear_batch_buffer(
    batch_size: int,
    metrics_flush_interval: int,
    *,
    mode: AccelerationMode | str = AccelerationMode.AUTO,
) -> Any:
    return _runtime_for_mode(mode).make_linear_batch_buffer(batch_size, metrics_flush_interval)


def make_checkpoint_state(
    fallback_factory: Callable[[], Any] | None = None,
    *,
    mode: AccelerationMode | str = AccelerationMode.AUTO,
) -> Any:
    normalized = normalize_acceleration_mode(mode)
    if normalized == AccelerationMode.REQUIRED:
        require_acceleration(normalized)
    return _runtime_for_mode(normalized).make_checkpoint_state(fallback_factory)


def make_sync_builtin_chain_executor(
    callables: list[Any],
    names: list[str],
    *,
    mode: AccelerationMode | str = AccelerationMode.AUTO,
) -> Any:
    return _runtime_for_mode(mode).make_sync_builtin_chain_executor(callables, names)


def make_csv_arrow_writer(
    path: str,
    append: bool = False,
    *,
    mode: AccelerationMode | str = AccelerationMode.AUTO,
) -> Any:
    return _runtime_for_mode(mode).make_csv_arrow_writer(path, append)


def make_jsonl_arrow_writer(
    path: str,
    append: bool = False,
    *,
    mode: AccelerationMode | str = AccelerationMode.AUTO,
) -> Any:
    return _runtime_for_mode(mode).make_jsonl_arrow_writer(path, append)


def read_jsonl_arrow_batches(
    path: str,
    batch_size: int,
    *,
    mode: AccelerationMode | str = AccelerationMode.AUTO,
) -> Any:
    return _runtime_for_mode(mode).read_jsonl_arrow_batches(path, batch_size)


__all__ = [
    "AccelerationCapability",
    "AccelerationMode",
    "AccelerationRuntime",
    "AccelerationStatus",
    "AccelerationUnavailableError",
    "acceleration_available",
    "acceleration_status",
    "acceleration_supports",
    "csv_arrow_writer_class",
    "jsonl_arrow_writer_class",
    "linear_batch_buffer_class",
    "make_checkpoint_state",
    "make_csv_arrow_writer",
    "make_jsonl_arrow_writer",
    "make_linear_batch_buffer",
    "make_metrics_accumulator",
    "make_record_buffer",
    "make_sync_builtin_chain_executor",
    "metrics_accumulator_class",
    "normalize_acceleration_mode",
    "read_jsonl_arrow_batches",
    "record_buffer_class",
    "require_acceleration",
    "sync_builtin_chain_executor_class",
]
