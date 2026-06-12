from __future__ import annotations

import importlib.metadata
from types import SimpleNamespace
from typing import Any

import pytest

from agora.core.acceleration import (
    AccelerationCapability,
    AccelerationMode,
    AccelerationRuntime,
    AccelerationUnavailableError,
    make_checkpoint_state,
    make_metrics_accumulator,
    make_record_buffer,
    make_sync_builtin_chain_executor,
    normalize_acceleration_mode,
)
from agora.core.runtime import _source_adapter
from agora.core.runtime._hot_metrics import RustHotPathMetrics


class _FakeRecordBuffer:
    def __init__(self, capacity: int) -> None:
        self.capacity = capacity


class _FakeMetricsAccumulator:
    def __init__(self, flush_interval: int = 100) -> None:
        self.flush_interval = flush_interval


class _FakeLinearBatchBuffer:
    def __init__(self, batch_size: int, metrics_flush_interval: int) -> None:
        self.batch_size = batch_size
        self.metrics_flush_interval = metrics_flush_interval


class _FakeCheckpointState:
    pass


class _FakeSyncBuiltinChainExecutor:
    def __init__(self, callables: list[object], names: list[str]) -> None:
        self.callables = callables
        self.names = names


class _FakeCsvArrowWriter:
    def __init__(self, path: str, append: bool = False) -> None:
        self.path = path
        self.append = append


class _FakeJsonlArrowWriter:
    def __init__(self, path: str, append: bool = False) -> None:
        self.path = path
        self.append = append


def _fake_read_jsonl_record_batches(path: str, batch_size: int) -> object:
    return [path, batch_size]


def _fake_agora_rs_module(**overrides: Any) -> Any:
    payload: dict[str, Any] = {
        "RUST_AVAILABLE": True,
        "is_available": lambda: True,
        "RecordBuffer": _FakeRecordBuffer,
        "MetricsAccumulator": _FakeMetricsAccumulator,
        "LinearBatchBuffer": _FakeLinearBatchBuffer,
        "CheckpointState": _FakeCheckpointState,
        "SyncBuiltinChainExecutor": _FakeSyncBuiltinChainExecutor,
        "CsvArrowWriter": _FakeCsvArrowWriter,
        "JsonlArrowWriter": _FakeJsonlArrowWriter,
        "read_jsonl_record_batches": _fake_read_jsonl_record_batches,
    }
    payload.update(overrides)
    return SimpleNamespace(**payload)


def _missing_module_loader(name: str) -> Any:
    raise ImportError(name)


def _version_loader(name: str) -> str:
    if name != "agora-etl-rs":
        raise importlib.metadata.PackageNotFoundError(name)
    return "0.2.0"


def test_acceleration_status_reports_missing_package() -> None:
    runtime = AccelerationRuntime(module_loader=_missing_module_loader)

    status = runtime.status()

    assert status.mode == AccelerationMode.AUTO
    assert status.available is False
    assert status.enabled is False
    assert status.capabilities == frozenset()
    assert status.reason == "agora-etl-rs is not installed"


def test_acceleration_off_does_not_import_module() -> None:
    calls: list[str] = []

    def _loader(name: str) -> Any:
        calls.append(name)
        return _fake_agora_rs_module()

    runtime = AccelerationRuntime(mode=AccelerationMode.OFF, module_loader=_loader)

    status = runtime.status()

    assert status.mode == AccelerationMode.OFF
    assert status.available is False
    assert status.enabled is False
    assert status.reason == "acceleration disabled by policy"
    assert calls == []


def test_normalize_acceleration_mode_accepts_case_insensitive_strings() -> None:
    assert normalize_acceleration_mode(" OFF ") == AccelerationMode.OFF


def test_acceleration_detects_current_agora_rs_style_capabilities() -> None:
    runtime = AccelerationRuntime(
        module_loader=lambda name: _fake_agora_rs_module(),
        version_loader=_version_loader,
    )

    status = runtime.status()

    assert status.available is True
    assert status.enabled is True
    assert status.version == "0.2.0"
    assert status.supports(AccelerationCapability.RECORD_BUFFER)
    assert status.supports(AccelerationCapability.METRICS_ACCUMULATOR)
    assert status.supports(AccelerationCapability.LINEAR_BATCH_BUFFER)
    assert status.supports(AccelerationCapability.CHECKPOINT_STATE)
    assert status.supports(AccelerationCapability.SYNC_BUILTIN_CHAIN_EXECUTOR)
    assert status.supports(AccelerationCapability.CSV_ARROW_WRITER)
    assert status.supports(AccelerationCapability.JSONL_ARROW_WRITER)
    assert not status.supports(AccelerationCapability.JSONL_ARROW_READER)


def test_acceleration_required_exposes_csv_arrow_writer() -> None:
    runtime = AccelerationRuntime(
        mode=AccelerationMode.REQUIRED,
        module_loader=lambda name: _fake_agora_rs_module(),
        version_loader=_version_loader,
    )

    status = runtime.status()

    assert status.available is True
    assert status.supports(AccelerationCapability.CSV_ARROW_WRITER)
    assert status.supports(AccelerationCapability.JSONL_ARROW_WRITER)


def test_acceleration_required_preserves_jsonl_arrow_reader() -> None:
    runtime = AccelerationRuntime(
        mode=AccelerationMode.REQUIRED,
        module_loader=lambda name: _fake_agora_rs_module(),
        version_loader=_version_loader,
    )

    status = runtime.status()

    assert status.available is True
    assert status.supports(AccelerationCapability.JSONL_ARROW_READER)


def test_acceleration_uses_declared_capabilities_when_available() -> None:
    runtime = AccelerationRuntime(
        module_loader=lambda name: _fake_agora_rs_module(
            capabilities=lambda: [
                "record_buffer",
                "checkpoint_state",
                "csv_arrow_writer",
                "jsonl_arrow_writer",
                "jsonl_arrow_reader",
                "unknown",
            ]
        ),
        version_loader=_version_loader,
    )

    status = runtime.status()

    assert status.capabilities == frozenset(
        {
            AccelerationCapability.RECORD_BUFFER,
            AccelerationCapability.CHECKPOINT_STATE,
            AccelerationCapability.CSV_ARROW_WRITER,
            AccelerationCapability.JSONL_ARROW_WRITER,
        }
    )
    assert status.supports("record_buffer")
    assert status.supports("csv_arrow_writer")
    assert status.supports("jsonl_arrow_writer")
    assert not status.supports("jsonl_arrow_reader")
    assert not status.supports("linear_batch_buffer")
    assert not status.supports("unknown")


def test_acceleration_required_preserves_declared_csv_arrow_writer() -> None:
    runtime = AccelerationRuntime(
        mode=AccelerationMode.REQUIRED,
        module_loader=lambda name: _fake_agora_rs_module(
            capabilities=lambda: [
                "record_buffer",
                "checkpoint_state",
                "csv_arrow_writer",
                "jsonl_arrow_writer",
                "jsonl_arrow_reader",
                "unknown",
            ]
        ),
        version_loader=_version_loader,
    )

    status = runtime.status()

    assert status.capabilities == frozenset(
        {
            AccelerationCapability.RECORD_BUFFER,
            AccelerationCapability.CHECKPOINT_STATE,
            AccelerationCapability.CSV_ARROW_WRITER,
            AccelerationCapability.JSONL_ARROW_WRITER,
            AccelerationCapability.JSONL_ARROW_READER,
        }
    )
    assert status.supports("csv_arrow_writer")
    assert status.supports("jsonl_arrow_writer")
    assert status.supports("jsonl_arrow_reader")


def test_declared_capability_must_be_constructible() -> None:
    runtime = AccelerationRuntime(
        module_loader=lambda name: _fake_agora_rs_module(
            capabilities=lambda: ["record_buffer"],
            RecordBuffer=lambda capacity: (_ for _ in ()).throw(RuntimeError("broken")),
        ),
        version_loader=_version_loader,
    )

    status = runtime.status()

    assert status.available is False
    assert status.capabilities == frozenset()
    assert not status.supports("record_buffer")


def test_acceleration_reads_version_function_before_distribution_metadata() -> None:
    runtime = AccelerationRuntime(
        module_loader=lambda name: _fake_agora_rs_module(version=lambda: "0.2.0"),
        version_loader=lambda name: "0.1.0",
    )

    assert runtime.status().version == "0.2.0"


def test_acceleration_marks_old_agora_rs_incompatible() -> None:
    runtime = AccelerationRuntime(
        module_loader=lambda name: _fake_agora_rs_module(version=lambda: "0.1.0"),
        version_loader=_version_loader,
    )

    status = runtime.status()

    assert status.available is False
    assert status.enabled is False
    assert status.compatible is False
    assert "incompatible" in (status.reason or "")


def test_acceleration_constructors_return_rust_primitives() -> None:
    runtime = AccelerationRuntime(
        module_loader=lambda name: _fake_agora_rs_module(),
        version_loader=_version_loader,
    )

    record_buffer = runtime.make_record_buffer(9)
    metrics = runtime.make_metrics_accumulator(17)
    linear_batch = runtime.make_linear_batch_buffer(3, 11)
    checkpoint_state = runtime.make_checkpoint_state()
    chain_executor = runtime.make_sync_builtin_chain_executor([lambda record: record], ["map"])

    assert isinstance(record_buffer, _FakeRecordBuffer)
    assert record_buffer.capacity == 9
    assert isinstance(metrics, _FakeMetricsAccumulator)
    assert metrics.flush_interval == 17
    assert isinstance(linear_batch, _FakeLinearBatchBuffer)
    assert linear_batch.batch_size == 3
    assert linear_batch.metrics_flush_interval == 11
    assert isinstance(checkpoint_state, _FakeCheckpointState)
    assert isinstance(chain_executor, _FakeSyncBuiltinChainExecutor)
    assert len(chain_executor.callables) == 1
    assert chain_executor.names == ["map"]

    csv_writer = runtime.make_csv_arrow_writer("output.csv", append=True)
    jsonl_writer = runtime.make_jsonl_arrow_writer("output.jsonl", append=True)
    jsonl_batches = AccelerationRuntime(
        mode=AccelerationMode.REQUIRED,
        module_loader=lambda name: _fake_agora_rs_module(),
        version_loader=_version_loader,
    ).read_jsonl_arrow_batches("input.jsonl", 4096)

    assert isinstance(csv_writer, _FakeCsvArrowWriter)
    assert csv_writer.path == "output.csv"
    assert csv_writer.append is True
    assert isinstance(jsonl_writer, _FakeJsonlArrowWriter)
    assert jsonl_writer.path == "output.jsonl"
    assert jsonl_writer.append is True
    assert jsonl_batches == ["input.jsonl", 4096]


def test_required_csv_arrow_writer_constructor_returns_rust_primitive() -> None:
    runtime = AccelerationRuntime(
        mode=AccelerationMode.REQUIRED,
        module_loader=lambda name: _fake_agora_rs_module(),
        version_loader=_version_loader,
    )

    csv_writer = runtime.make_csv_arrow_writer("output.csv", append=True)

    assert isinstance(csv_writer, _FakeCsvArrowWriter)
    assert csv_writer.path == "output.csv"
    assert csv_writer.append is True


def test_required_jsonl_arrow_writer_constructor_returns_rust_primitive() -> None:
    runtime = AccelerationRuntime(
        mode=AccelerationMode.REQUIRED,
        module_loader=lambda name: _fake_agora_rs_module(),
        version_loader=_version_loader,
    )

    jsonl_writer = runtime.make_jsonl_arrow_writer("output.jsonl", append=True)

    assert isinstance(jsonl_writer, _FakeJsonlArrowWriter)
    assert jsonl_writer.path == "output.jsonl"
    assert jsonl_writer.append is True


def test_checkpoint_state_uses_fallback_when_rust_unavailable() -> None:
    fallback = object()
    runtime = AccelerationRuntime(module_loader=_missing_module_loader)

    assert runtime.make_checkpoint_state(lambda: fallback) is fallback


def test_checkpoint_state_fallback_preserves_python_semantics() -> None:
    from agora.core.runtime._delivery_state import CheckpointState

    runtime = AccelerationRuntime(module_loader=_missing_module_loader)
    state = runtime.make_checkpoint_state(CheckpointState)

    assert state.processed_count == 0
    assert state.last_saved_value is None
    state.increment()
    state.increment_by(2)
    assert state.processed_count == 3
    assert state.should_save("cp-1", every=3) is True
    state.mark_saved("cp-1")
    assert state.should_save("cp-1", every=3) is False


def test_required_checkpoint_state_does_not_fallback_when_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _missing_loader(name: str) -> Any:
        raise ImportError(name)

    monkeypatch.setattr(
        "agora.core.acceleration._runtime_for_mode",
        lambda mode: AccelerationRuntime(mode=mode, module_loader=_missing_loader),
    )

    with pytest.raises(AccelerationUnavailableError, match="required acceleration unavailable"):
        make_checkpoint_state(lambda: object(), mode=AccelerationMode.REQUIRED)


def test_facade_constructors_respect_off_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "agora.core.acceleration._runtime_for_mode",
        lambda mode: AccelerationRuntime(
            mode=mode,
            module_loader=lambda name: _fake_agora_rs_module(),
        ),
    )

    with pytest.raises(AccelerationUnavailableError, match="record_buffer unavailable"):
        make_record_buffer(1, mode=AccelerationMode.OFF)
    with pytest.raises(AccelerationUnavailableError, match="metrics_accumulator unavailable"):
        make_metrics_accumulator(mode=AccelerationMode.OFF)
    with pytest.raises(
        AccelerationUnavailableError,
        match="sync_builtin_chain_executor unavailable",
    ):
        make_sync_builtin_chain_executor([], [], mode=AccelerationMode.OFF)


def test_rust_hot_metrics_zero_count_checks_pending_flush(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeAccumulator:
        def __init__(self) -> None:
            self.consumed = 0
            self.written = 0
            self.since = 0

        def add_consumed(self, source_name: str, count: int) -> bool:
            del source_name
            self.consumed += count
            self.since += count
            return self.since >= 3

        def add_written(self, count: int) -> None:
            self.written += count

        def snapshot(self) -> tuple[int, int, int]:
            return self.consumed, self.written, self.since

        def flush(self, metrics: object) -> None:
            del metrics
            self.consumed = 0
            self.written = 0
            self.since = 0

        def flush_final(self, metrics: object) -> None:
            self.flush(metrics)

    monkeypatch.setattr(
        "agora.core.runtime._hot_metrics.make_metrics_accumulator",
        lambda **kwargs: _FakeAccumulator(),
    )

    hot = RustHotPathMetrics(
        "src",
        3,
        acceleration_mode=AccelerationMode.AUTO,
    )

    assert hot.inc_consumed(3) is True
    assert hot.inc_consumed(0) is True


def test_source_runtime_adapter_respects_acceleration_off_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        _source_adapter,
        "acceleration_status",
        lambda mode: SimpleNamespace(enabled=True, reason=None),
    )

    adapter = _source_adapter.SourceRuntimeAdapter(
        source=object(),  # type: ignore[arg-type]
        has_buffered_stages=True,
        acceleration_mode=AccelerationMode.OFF,
    )

    assert adapter.rust_available() is False


def test_requested_primitive_raises_when_unavailable() -> None:
    runtime = AccelerationRuntime(module_loader=_missing_module_loader)

    with pytest.raises(AccelerationUnavailableError, match="record_buffer unavailable"):
        runtime.make_record_buffer(1)
