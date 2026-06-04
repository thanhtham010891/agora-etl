"""
tests/core/test_batch_pipeline.py
==================================
Unit tests for the 0.2.0 batch-native execution lane.

Covers:
- BatchableSource protocol detection
- BatchMiddleware.process_batch() dispatch
- run_batch_pipeline() end-to-end
- Arrow fast path (ParquetSource → ParquetSink)
- Batch failure policy (Option A: entire batch → DLQ)
- Checkpoint advances once per batch
- Source order preserved across batches
- Cancellation does not commit in-flight batch
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from agora import (
    BatchMiddleware,
    DeliveryConfig,
    InMemoryCheckpointStore,
    IterableSource,
    Pipeline,
    is_batch_capable_source,
)
from agora.core.batch import is_arrow_native_sink
from agora.core.data_plane import DataPlane, SourceDataPlaneSpec
from agora.core.errors import PipelineError
from agora.core.source import BaseSource

if TYPE_CHECKING:
    from agora.core.context import PipelineContext

# ======================================================================
# Test fixtures
# ======================================================================


class _CollectSink:
    sink_name = "collect"

    def __init__(self) -> None:
        self.records: list[Any] = []
        self.batches: list[list[Any]] = []

    async def open(self) -> None:
        return None

    async def write(self, record: Any) -> None:
        self.records.append(record)

    async def write_batch(self, records: list[Any]) -> None:
        self.batches.append(list(records))
        self.records.extend(records)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _DLQCollectSink:
    sink_name = "dlq"

    def __init__(self) -> None:
        self.records: list[Any] = []

    async def open(self) -> None:
        return None

    async def write(self, record: Any) -> None:
        self.records.append(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _BatchSource(BaseSource[int]):
    """A minimal batch-capable source for testing."""

    source_name = "batch_test_source"
    supports_checkpoint: bool = True

    def __init__(self, batches: list[list[int]]) -> None:
        self._batches = batches
        self._last_batch_index = -1

    def current_checkpoint(self) -> dict[str, int] | None:
        if self._last_batch_index < 0:
            return None
        return {"batch_index": self._last_batch_index}

    async def prepare_resume(self, checkpoint: Any) -> None:
        return None

    def data_plane_spec(self) -> SourceDataPlaneSpec:
        return SourceDataPlaneSpec(
            source_name=self.source_name,
            emitted_plane=DataPlane.PYTHON_BATCHES,
            supports_batch_emit=True,
            emits_arrow_batches=False,
        )

    async def stream_batches(self):  # type: ignore[override]
        for i, batch in enumerate(self._batches):
            self._last_batch_index = i
            yield batch

    async def stream(self):
        for batch in self._batches:
            for record in batch:
                yield record


class _DoubleAllBatchMiddleware(BatchMiddleware[int, int]):
    """Doubles every record in the batch."""

    name = "double_all"

    async def process_batch(self, records: list[int], ctx: PipelineContext) -> list[int | None]:
        del ctx
        return [r * 2 for r in records]


class _DropEvenBatchMiddleware(BatchMiddleware[int, int]):
    """Drops even numbers from the batch."""

    name = "drop_even"

    async def process_batch(self, records: list[int], ctx: PipelineContext) -> list[int | None]:
        del ctx
        return [r if r % 2 != 0 else None for r in records]


class _RaisingBatchMiddleware(BatchMiddleware[int, int]):
    """Always raises — used to test Option A failure policy."""

    name = "raising_batch"

    async def process_batch(self, records: list[int], ctx: PipelineContext) -> list[int | None]:
        del ctx
        raise ValueError("batch boom")


class _ArrowNativeSink:
    """Minimal Arrow-native sink for testing fast path detection."""

    sink_name = "arrow_native"

    def __init__(self) -> None:
        self.batches: list[Any] = []

    async def open(self) -> None:
        return None

    async def write_arrow_batch(self, batch: Any) -> None:
        self.batches.append(batch)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


# ======================================================================
# [BATCH-01] is_batch_capable_source detection
# ======================================================================


def test_b01_is_batch_capable_source_detects_batch_source() -> None:
    """[BATCH-01] is_batch_capable_source() returns True for BatchableSource."""
    source = _BatchSource([[1, 2], [3, 4]])
    assert is_batch_capable_source(source) is True


def test_b01b_is_batch_capable_source_returns_false_for_iterable_source() -> None:
    """[BATCH-01b] IterableSource does not support batch emission."""
    source = IterableSource([1, 2, 3])
    assert is_batch_capable_source(source) is False


def test_b01c_is_batch_capable_source_requires_flag() -> None:
    """[BATCH-01c] supports_batch_emit=False prevents batch lane routing."""

    class _FakeSource:
        source_name = "fake"
        supports_batch_emit = False

        async def stream_batches(self):
            yield [1, 2]

        def current_checkpoint(self):
            return None

    assert is_batch_capable_source(_FakeSource()) is False


# ======================================================================
# [BATCH-02] is_arrow_native_sink detection
# ======================================================================


def test_b02_is_arrow_native_sink_detects_write_arrow_batch() -> None:
    """[BATCH-02] is_arrow_native_sink() returns True when write_arrow_batch exists."""
    sink = _ArrowNativeSink()
    assert is_arrow_native_sink(sink) is True


def test_b02b_is_arrow_native_sink_returns_false_for_regular_sink() -> None:
    """[BATCH-02b] Regular CollectSink is not Arrow-native."""
    sink = _CollectSink()
    assert is_arrow_native_sink(sink) is False


# ======================================================================
# [BATCH-03] BatchMiddleware.process_batch dispatch
# ======================================================================


@pytest.mark.asyncio
async def test_b03_batch_middleware_doubles_all_records() -> None:
    """[BATCH-03] BatchMiddleware.process_batch() is called with the full batch."""
    sink = _CollectSink()
    source = _BatchSource([[1, 2, 3], [4, 5, 6]])

    summary = await (
        Pipeline(source)
        .pipe(_DoubleAllBatchMiddleware())
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [2, 4, 6, 8, 10, 12]
    assert summary.records_written == 6
    assert summary.records_consumed == 6


# ======================================================================
# [BATCH-04] BatchMiddleware drops None results
# ======================================================================


@pytest.mark.asyncio
async def test_b04_batch_middleware_drops_none_results() -> None:
    """[BATCH-04] None entries in process_batch() result are dropped."""
    sink = _CollectSink()
    source = _BatchSource([[1, 2, 3, 4, 5]])

    summary = await (
        Pipeline(source)
        .pipe(_DropEvenBatchMiddleware())
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [1, 3, 5]
    assert summary.records_written == 3
    assert summary.records_dropped == 2


# ======================================================================
# [BATCH-05] Source order preserved across batches
# ======================================================================


@pytest.mark.asyncio
async def test_b05_source_order_preserved_across_batches() -> None:
    """[BATCH-05] Records are committed in source order across multiple batches."""
    sink = _CollectSink()
    source = _BatchSource([[10, 20], [30, 40], [50]])

    await (
        Pipeline(source)
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [10, 20, 30, 40, 50], (
        "[BATCH-05] batch pipeline must commit in source order"
    )


# ======================================================================
# [BATCH-06] Checkpoint advances once per batch
# ======================================================================


@pytest.mark.asyncio
async def test_b06_checkpoint_advances_once_per_batch() -> None:
    """[BATCH-06] Checkpoint is saved once per batch, not per record."""
    store = InMemoryCheckpointStore()
    sink = _CollectSink()
    source = _BatchSource([[1, 2, 3], [4, 5, 6]])

    summary = await (
        Pipeline(source)
        .build(sink, config=DeliveryConfig(checkpoint=store))  # type: ignore[arg-type]
        .run()
    )

    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"batch_index": 1}, (
        "[BATCH-06] checkpoint must reflect the last batch index"
    )
    assert summary.runtime.checkpoint_save_count == 2, (
        "[BATCH-06] checkpoint must be saved once per batch (2 batches = 2 saves)"
    )


@pytest.mark.asyncio
async def test_b06b_batch_checkpoint_every_honors_record_cadence() -> None:
    """[BATCH-06b] checkpoint_every still means records, not batches."""
    store = InMemoryCheckpointStore()
    sink = _CollectSink()
    source = _BatchSource([[1], [2], [3], [4]])

    summary = await (
        Pipeline(source)
        .build(sink, config=DeliveryConfig(checkpoint=store, checkpoint_every=3))  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [1, 2, 3, 4]
    assert summary.runtime.checkpoint_save_count == 1
    assert summary.last_checkpoint is not None
    assert summary.last_checkpoint.value == {"batch_index": 2}


# ======================================================================
# [BATCH-07] Batch failure (Option A) — entire batch → DLQ
# ======================================================================


@pytest.mark.asyncio
async def test_b07_batch_failure_routes_entire_batch_to_dlq() -> None:
    """[BATCH-07] When BatchMiddleware raises, the entire batch is routed to
    the DLQ (Option A). No per-record fallback.

    Validates: docs/guides/runtime-guarantees.md — DLQ routing on batch failure
    """
    sink = _CollectSink()
    dlq = _DLQCollectSink()
    source = _BatchSource([[1, 2, 3]])

    summary = await (
        Pipeline(source)
        .pipe(_RaisingBatchMiddleware())
        .build(sink, config=DeliveryConfig(dlq=dlq))  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [], "[BATCH-07] no records must reach the sink"
    assert len(dlq.records) == 3, "[BATCH-07] all 3 records must be in the DLQ"
    assert summary.records_errored == 3
    assert all(r.stage == "batch_middleware" for r in dlq.records)


# ======================================================================
# [BATCH-08] Batch failure without DLQ aborts run (FAIL_CLOSED)
# ======================================================================


@pytest.mark.asyncio
async def test_b08_batch_failure_without_dlq_aborts_run() -> None:
    """[BATCH-08] BatchMiddleware failure without DLQ aborts the run under
    FAIL_CLOSED (default).
    """
    from agora.core.runtime._delivery import RecordDeliveryError

    source = _BatchSource([[1, 2, 3]])

    with pytest.raises((ValueError, RecordDeliveryError)):
        await (
            Pipeline(source)
            .pipe(_RaisingBatchMiddleware())
            .build(_CollectSink())  # type: ignore[arg-type]
            .run()
        )


@pytest.mark.asyncio
async def test_b08b_batch_sink_failure_routes_only_non_dropped_records_to_dlq() -> None:
    """[BATCH-08b] sink failures after batch drops keep DLQ raw/processed alignment."""

    class _BoomBatchSink:
        sink_name = "boom_batch_sink"

        async def open(self) -> None:
            return None

        async def write(self, record: Any) -> None:
            raise AssertionError("single-record path should not be used")

        async def write_batch(self, records: list[Any]) -> None:
            raise RuntimeError("batch sink broke")

        async def flush(self) -> None:
            return None

        async def close(self) -> None:
            return None

    sink = _BoomBatchSink()
    dlq = _DLQCollectSink()
    source = _BatchSource([[1, 2, 3, 4]])

    summary = await (
        Pipeline(source)
        .pipe(_DropEvenBatchMiddleware())
        .build(sink, config=DeliveryConfig(dlq=dlq))  # type: ignore[arg-type]
        .run()
    )

    assert summary.records_dropped == 2
    assert summary.records_errored == 2
    assert [record.record for record in dlq.records] == [1, 3]
    assert [record.original_record for record in dlq.records] == [1, 3]
    assert [record.processed_record for record in dlq.records] == [1, 3]


# ======================================================================
# [BATCH-09] No-middleware batch pipeline writes all records
# ======================================================================


@pytest.mark.asyncio
async def test_b09_no_middleware_batch_pipeline_writes_all_records() -> None:
    """[BATCH-09] A batch pipeline with no middleware writes all records."""
    sink = _CollectSink()
    source = _BatchSource([[1, 2], [3, 4], [5]])

    summary = await (
        Pipeline(source)
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [1, 2, 3, 4, 5]
    assert summary.records_written == 5
    assert summary.records_consumed == 5


# ======================================================================
# [BATCH-10] max_records respected in batch mode
# ======================================================================


@pytest.mark.asyncio
async def test_b10_max_records_respected_in_batch_mode() -> None:
    """[BATCH-10] max_records trims the source before the final batch overshoots."""
    sink = _CollectSink()
    source = _BatchSource([[1, 2, 3], [4, 5, 6], [7, 8, 9]])

    summary = await (
        Pipeline(source)
        .build(sink)  # type: ignore[arg-type]
        .run(max_records=4)
    )

    assert summary.records_consumed == 4
    assert summary.records_written == 4
    assert sink.records == [1, 2, 3, 4]


# ======================================================================
# BatchMapMiddleware / BatchFilterMiddleware
# ======================================================================


async def test_batch_map_middleware_matches_map_semantics() -> None:
    """BatchMapMiddleware transforms each record like MapMiddleware."""
    from agora import BatchMapMiddleware, MapMiddleware

    batch_sink = _CollectSink()
    row_sink = _CollectSink()

    await (
        Pipeline(_BatchSource([[1, 2, 3], [4, 5]]))
        .pipe(BatchMapMiddleware(lambda r: r * 10))
        .build(batch_sink)  # type: ignore[arg-type]
        .run()
    )
    await (
        Pipeline(IterableSource([1, 2, 3, 4, 5]))
        .pipe(MapMiddleware(lambda r: r * 10))
        .build(row_sink)  # type: ignore[arg-type]
        .run()
    )

    assert batch_sink.records == [10, 20, 30, 40, 50]
    assert batch_sink.records == row_sink.records


async def test_batch_filter_middleware_drops_records() -> None:
    """BatchFilterMiddleware drops records failing the predicate (None slots)."""
    from agora import BatchFilterMiddleware, FilterMiddleware

    batch_sink = _CollectSink()
    row_sink = _CollectSink()

    await (
        Pipeline(_BatchSource([[1, 2, 3, 4], [5, 6]]))
        .pipe(BatchFilterMiddleware(lambda r: r % 2 == 0))
        .build(batch_sink)  # type: ignore[arg-type]
        .run()
    )
    await (
        Pipeline(IterableSource([1, 2, 3, 4, 5, 6]))
        .pipe(FilterMiddleware(lambda r: r % 2 == 0))
        .build(row_sink)  # type: ignore[arg-type]
        .run()
    )

    assert batch_sink.records == [2, 4, 6]
    assert batch_sink.records == row_sink.records


async def test_regular_map_middleware_on_batch_source_matches_row_path() -> None:
    """Regular MapMiddleware on a batch source keeps semantics while using the batch lane."""
    from agora import MapMiddleware

    batch_sink = _CollectSink()
    row_sink = _CollectSink()

    await (
        Pipeline(_BatchSource([[1, 2, 3], [4, 5]]))
        .pipe(MapMiddleware(lambda r: r * 10))
        .build(batch_sink)  # type: ignore[arg-type]
        .run()
    )
    await (
        Pipeline(IterableSource([1, 2, 3, 4, 5]))
        .pipe(MapMiddleware(lambda r: r * 10))
        .build(row_sink)  # type: ignore[arg-type]
        .run()
    )

    assert batch_sink.records == [10, 20, 30, 40, 50]
    assert batch_sink.records == row_sink.records


async def test_regular_filter_middleware_on_batch_source_matches_row_path() -> None:
    """Regular FilterMiddleware on a batch source keeps drop semantics."""
    from agora import FilterMiddleware

    batch_sink = _CollectSink()
    row_sink = _CollectSink()

    await (
        Pipeline(_BatchSource([[1, 2, 3, 4], [5, 6]]))
        .pipe(FilterMiddleware(lambda r: r % 2 == 0))
        .build(batch_sink)  # type: ignore[arg-type]
        .run()
    )
    await (
        Pipeline(IterableSource([1, 2, 3, 4, 5, 6]))
        .pipe(FilterMiddleware(lambda r: r % 2 == 0))
        .build(row_sink)  # type: ignore[arg-type]
        .run()
    )

    assert batch_sink.records == [2, 4, 6]
    assert batch_sink.records == row_sink.records


async def test_batch_map_then_filter_chained() -> None:
    """Chained batch middleware compose correctly in the batch lane."""
    from agora import BatchFilterMiddleware, BatchMapMiddleware

    sink = _CollectSink()
    await (
        Pipeline(_BatchSource([[1, 2, 3], [4, 5, 6]]))
        .pipe(BatchMapMiddleware(lambda r: r + 1))
        .pipe(BatchFilterMiddleware(lambda r: r > 3))
        .build(sink)  # type: ignore[arg-type]
        .run()
    )
    # +1 → [2,3,4,5,6,7]; keep >3 → [4,5,6,7]
    assert sink.records == [4, 5, 6, 7]


async def test_batch_map_async_fn() -> None:
    """BatchMapMiddleware supports async transform functions."""
    from agora import BatchMapMiddleware

    async def double(r: int) -> int:
        return r * 2

    sink = _CollectSink()
    await (
        Pipeline(_BatchSource([[1, 2], [3]]))
        .pipe(BatchMapMiddleware(double))
        .build(sink)  # type: ignore[arg-type]
        .run()
    )
    assert sink.records == [2, 4, 6]


# ======================================================================
# Arrow-native middleware chain tests
# ======================================================================


class _ArrowBatchSource(BaseSource[Any]):
    """Arrow-emitting batch source for testing the arrow chain path."""

    source_name = "arrow_batch_source"

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self._checkpoint: int = 0

    def current_checkpoint(self) -> dict[str, int] | None:
        return {"rows": self._checkpoint}

    async def prepare_resume(self, checkpoint: Any) -> None:
        return None

    def data_plane_spec(self) -> SourceDataPlaneSpec:
        return SourceDataPlaneSpec(
            source_name=self.source_name,
            emitted_plane=DataPlane.ARROW_BATCHES,
            supports_batch_emit=True,
            emits_arrow_batches=True,
        )

    async def stream_batches(self):
        pa = pytest.importorskip("pyarrow")
        batch = pa.RecordBatch.from_pylist(self._rows)
        self._checkpoint = len(self._rows)
        yield batch

    async def stream(self):
        for row in self._rows:
            yield row


async def test_arrow_chain_stays_columnar() -> None:
    """Arrow chain: sink receives pa.RecordBatch, not list."""
    pa = pytest.importorskip("pyarrow")

    from agora import ArrowMapMiddleware

    src = _ArrowBatchSource([{"id": 1, "v": 10}, {"id": 2, "v": 20}])
    sink = _ArrowNativeSink()

    summary = await (
        Pipeline(src)
        .pipe(ArrowMapMiddleware(lambda b: b))  # identity — stays columnar
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert len(sink.batches) == 1
    assert isinstance(sink.batches[0], pa.RecordBatch)
    assert sink.batches[0].num_rows == 2
    assert summary.runtime.execution_lane == "batch"
    assert summary.runtime.arrow_fast_path_active is True
    assert summary.runtime.arrow_chain_active is True


async def test_arrow_filter_shrinks_batch() -> None:
    """ArrowFilterMiddleware: rows failing predicate are dropped; consumed != written."""
    pytest.importorskip("pyarrow")
    import pyarrow.compute as pc

    from agora import ArrowFilterMiddleware

    rows = [{"id": i, "v": i} for i in range(10)]
    src = _ArrowBatchSource(rows)
    sink = _ArrowNativeSink()

    summary = await (
        Pipeline(src)
        .pipe(ArrowFilterMiddleware(lambda b: pc.greater(b.column("v"), 4)))
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert len(sink.batches) == 1
    assert sink.batches[0].num_rows == 5  # v in [5,6,7,8,9]
    assert summary.records_consumed == 10
    assert summary.records_written == 5
    assert summary.runtime.execution_lane == "batch"
    assert summary.runtime.arrow_fast_path_active is True
    assert summary.runtime.arrow_chain_active is True


async def test_arrow_filter_all_rows_dropped_skips_write() -> None:
    """ArrowFilterMiddleware: zero-row result skips write, consumed still counted."""
    pytest.importorskip("pyarrow")
    import pyarrow.compute as pc

    from agora import ArrowFilterMiddleware

    src = _ArrowBatchSource([{"id": 1, "v": 1}, {"id": 2, "v": 2}])
    sink = _ArrowNativeSink()

    summary = await (
        Pipeline(src)
        .pipe(ArrowFilterMiddleware(lambda b: pc.greater(b.column("v"), 100)))
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert len(sink.batches) == 0
    assert summary.records_consumed == 2
    assert summary.records_written == 0
    assert summary.runtime.execution_lane == "batch"
    assert summary.runtime.arrow_fast_path_active is True
    assert summary.runtime.arrow_chain_active is True


async def test_arrow_fast_path_active_with_fan_out_arrow_sinks() -> None:
    """Arrow-native fan-out keeps the batch columnar when every sink supports Arrow."""
    pytest.importorskip("pyarrow")
    from agora import ArrowMapMiddleware

    src = _ArrowBatchSource([{"id": 1, "v": 10}, {"id": 2, "v": 20}])
    sink_one = _ArrowNativeSink()
    sink_two = _ArrowNativeSink()

    summary = await (
        Pipeline(src)
        .pipe(ArrowMapMiddleware(lambda b: b))
        .fan_out([sink_one, sink_two])  # type: ignore[list-item]
        .run()
    )

    assert len(sink_one.batches) == 1
    assert len(sink_two.batches) == 1
    assert sink_one.batches[0].num_rows == 2
    assert sink_two.batches[0].num_rows == 2
    assert summary.runtime.execution_lane == "batch"
    assert summary.runtime.arrow_fast_path_active is True
    assert summary.runtime.arrow_chain_active is True


async def test_arrow_chain_fan_out_mixes_arrow_and_list_sinks() -> None:
    """Arrow middleware stays columnar, but non-Arrow sinks still receive Python rows."""
    pytest.importorskip("pyarrow")
    from agora import ArrowMapMiddleware

    src = _ArrowBatchSource([{"id": 1, "v": 10}, {"id": 2, "v": 20}])
    arrow_sink = _ArrowNativeSink()
    list_sink = _CollectSink()

    summary = await (
        Pipeline(src)
        .pipe(ArrowMapMiddleware(lambda b: b))
        .fan_out([arrow_sink, list_sink])  # type: ignore[list-item]
        .run()
    )

    assert len(arrow_sink.batches) == 1
    assert arrow_sink.batches[0].num_rows == 2
    assert list_sink.records == [{"id": 1, "v": 10}, {"id": 2, "v": 20}]
    assert summary.runtime.execution_lane == "batch"
    assert summary.runtime.arrow_fast_path_active is True
    assert summary.runtime.arrow_chain_active is True


async def test_arrow_source_can_materialize_into_python_row_chain() -> None:
    """Arrow source may materialize once into a pure Python row/list-dict chain."""
    pytest.importorskip("pyarrow")

    from agora import MapMiddleware

    src = _ArrowBatchSource([{"id": 1}, {"id": 2}])
    sink = _CollectSink()

    summary = await (
        Pipeline(src)
        .pipe(MapMiddleware(lambda row: {"id": row["id"], "seen": True}))
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    assert sink.records == [{"id": 1, "seen": True}, {"id": 2, "seen": True}]
    assert summary.runtime.execution_lane == "batch"
    assert summary.runtime.arrow_fast_path_active is False
    assert summary.runtime.arrow_chain_active is False


async def test_arrow_map_then_filter_chained() -> None:
    """Chained ArrowMap + ArrowFilter compose correctly."""
    pytest.importorskip("pyarrow")
    import pyarrow.compute as pc

    from agora import ArrowFilterMiddleware, ArrowMapMiddleware

    rows = [{"id": i, "v": i} for i in range(6)]
    src = _ArrowBatchSource(rows)
    sink = _ArrowNativeSink()

    def double_v(b):
        idx = b.schema.get_field_index("v")
        return b.set_column(idx, "v", pc.multiply(b.column("v"), 2))

    await (
        Pipeline(src)
        .pipe(ArrowMapMiddleware(double_v))
        .pipe(ArrowFilterMiddleware(lambda b: pc.greater(b.column("v"), 5)))
        .build(sink)  # type: ignore[arg-type]
        .run()
    )

    # v*2 in [0,2,4,6,8,10]; keep >5 → [6,8,10] = 3 rows
    assert len(sink.batches) == 1
    assert sink.batches[0].num_rows == 3


async def test_mixed_chain_raises_pipeline_error() -> None:
    """Arrow and Python row/list-dict stages cannot coexist in one chain."""
    pytest.importorskip("pyarrow")

    from agora import ArrowMapMiddleware, MapMiddleware

    src = _ArrowBatchSource([{"id": 1}, {"id": 2}])
    sink = _CollectSink()

    with pytest.raises(PipelineError, match="mixes incompatible data planes"):
        await (
            Pipeline(src)
            .pipe(ArrowMapMiddleware(lambda b: b))
            .pipe(MapMiddleware(lambda r: r))
            .build(sink)  # type: ignore[arg-type]
            .run()
        )


async def test_arrow_chain_middleware_failure_routes_to_dlq() -> None:
    """A raising ArrowBatchMiddleware routes the batch to DLQ (Option A)."""
    pytest.importorskip("pyarrow")

    from agora import ArrowBatchMiddleware, DeliveryConfig

    class _FailingArrowMW(ArrowBatchMiddleware):
        name = "failing_arrow"

        async def process_arrow_batch(self, batch: Any, ctx: Any) -> Any:
            raise ValueError("arrow transform failed")

    src = _ArrowBatchSource([{"id": 1}, {"id": 2}])
    sink = _ArrowNativeSink()
    dlq = _DLQCollectSink()

    summary = await (
        Pipeline(src)
        .pipe(_FailingArrowMW())
        .build(sink, config=DeliveryConfig(dlq=dlq, sink_failure_policy="log_and_continue"))  # type: ignore[arg-type]
        .run()
    )

    assert len(sink.batches) == 0
    assert summary.records_consumed == 2
