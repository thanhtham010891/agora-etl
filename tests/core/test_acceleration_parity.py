"""End-to-end parity tests between pure-Python and agora-etl-rs paths.

These run the *same* pipeline under ``acceleration_mode="off"`` and
``acceleration_mode="auto"`` and assert identical observable results
(record counts, checkpoint advancement, sink output). They require the
native ``agora_rs`` extension; otherwise they skip.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from agora import BatchMiddleware, DeliveryConfig, IterableSource, MapMiddleware, Pipeline
from agora.core.checkpoint import InMemoryCheckpointStore
from agora.core.data_plane import DataPlane, SourceDataPlaneSpec
from agora.core.middleware import Middleware
from agora.core.source import BaseSource
from agora.core.types import Backpressure

pytest.importorskip("agora_rs")

from agora.core.acceleration import acceleration_status


def _rust_enabled() -> bool:
    return acceleration_status("auto").enabled


pytestmark = pytest.mark.skipif(
    not _rust_enabled(),
    reason="agora-etl-rs not available or incompatible",
)


class _CollectSink:
    sink_name = "collect"

    def __init__(self) -> None:
        self.records: list[int] = []

    async def open(self) -> None:
        return None

    async def write(self, record: int) -> None:
        self.records.append(record)

    async def write_batch(self, records: list[int]) -> None:
        self.records.extend(records)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _CheckpointedSource(BaseSource[int]):
    source_name = "checkpointed"
    supports_checkpoint = True

    def __init__(self, records: list[int]) -> None:
        self._records = records
        self._resume_index = -1
        self._last_index = -1

    async def prepare_resume(self, checkpoint) -> None:
        if checkpoint is None:
            self._resume_index = -1
            return
        self._resume_index = int(checkpoint.value["index"])

    def current_checkpoint(self) -> dict[str, int] | None:
        if self._last_index < 0:
            return None
        return {"index": self._last_index}

    async def stream(self):
        for index, record in enumerate(self._records):
            if index <= self._resume_index:
                continue
            self._last_index = index
            yield record


class _BlockingCheckpointStore(InMemoryCheckpointStore):
    """Hold the acknowledged-write/checkpoint-save crash window open."""

    def __init__(self) -> None:
        super().__init__()
        self.save_started = asyncio.Event()
        self.release_save = asyncio.Event()

    async def save(self, key: str, checkpoint) -> None:
        self.save_started.set()
        await self.release_save.wait()
        await super().save(key, checkpoint)


class _BufferedPassThroughMiddleware(Middleware[int, int]):
    """Small deterministic buffered stage for lane-level parity checks."""

    name = "buffered_passthrough"

    def __init__(self, batch_size: int = 3) -> None:
        self.min_concurrency = batch_size
        self._batch_size = batch_size
        self._pending: list[tuple[int, asyncio.Future[int | None]]] = []

    async def process(self, record: int, ctx) -> int | None:
        del ctx
        return record

    async def submit(self, record: int, ctx) -> asyncio.Future[int | None]:
        del ctx
        future: asyncio.Future[int | None] = asyncio.get_running_loop().create_future()
        self._pending.append((record, future))
        if len(self._pending) >= self._batch_size:
            await self._flush_pending()
        return future

    async def drain_pending(self, ctx) -> None:
        del ctx
        await self._flush_pending()

    async def _flush_pending(self) -> None:
        pending, self._pending = self._pending, []
        for record, future in pending:
            if not future.done():
                future.set_result(record)


class _CheckpointedBatchSource(BaseSource[int]):
    """Batch source whose cursor advances only after a committed batch."""

    source_name = "checkpointed_batch"
    supports_checkpoint = True

    def __init__(self, batches: list[list[int]]) -> None:
        self._batches = batches
        self._resume_batch_index = -1
        self._last_batch_index = -1

    def data_plane_spec(self) -> SourceDataPlaneSpec:
        return SourceDataPlaneSpec(
            source_name=self.source_name,
            emitted_plane=DataPlane.PYTHON_BATCHES,
            supports_batch_emit=True,
            emits_arrow_batches=False,
        )

    async def prepare_resume(self, checkpoint) -> None:
        self._resume_batch_index = (
            -1 if checkpoint is None else int(checkpoint.value["batch_index"])
        )

    def current_checkpoint(self) -> dict[str, int] | None:
        if self._last_batch_index < 0:
            return None
        return {"batch_index": self._last_batch_index}

    async def stream_batches(self):  # type: ignore[override]
        for index, batch in enumerate(self._batches):
            if index <= self._resume_batch_index:
                continue
            self._last_batch_index = index
            yield batch

    async def stream(self):
        async for batch in self.stream_batches():
            for record in batch:
                yield record


class _BatchPassThroughMiddleware(BatchMiddleware[int, int]):
    name = "batch_passthrough"

    async def process_batch(self, records: list[int], ctx) -> list[int | None]:
        del ctx
        return records


async def _run_linear_map(mode: str) -> tuple[list[int], int, int]:
    sink = _CollectSink()
    summary = await (
        Pipeline(IterableSource(list(range(50))))
        .pipe(MapMiddleware(lambda x: x * 2, name="double"))
        .build(
            sink,  # type: ignore[arg-type]
            config=DeliveryConfig(acceleration_mode=mode, batch_size=8),
        )
        .run()
    )
    return sink.records, summary.records_consumed, summary.records_written


@pytest.mark.asyncio
async def test_metrics_and_output_parity_off_vs_rs() -> None:
    off_records, off_consumed, off_written = await _run_linear_map("off")
    rs_records, rs_consumed, rs_written = await _run_linear_map("auto")

    assert off_records == rs_records
    assert off_consumed == rs_consumed == 50
    assert off_written == rs_written == 50


async def _run_checkpointed(mode: str) -> tuple[list[int], dict | None, int]:
    store = InMemoryCheckpointStore()
    sink = _CollectSink()
    summary = await (
        Pipeline(_CheckpointedSource(list(range(20))))
        .build(
            sink,  # type: ignore[arg-type]
            config=DeliveryConfig(
                acceleration_mode=mode,
                checkpoint=store,
                checkpoint_every=4,
            ),
        )
        .run()
    )
    last = summary.last_checkpoint.value if summary.last_checkpoint else None
    return sink.records, last, summary.records_written


@pytest.mark.asyncio
async def test_checkpoint_parity_off_vs_rs() -> None:
    off_records, off_cp, off_written = await _run_checkpointed("off")
    rs_records, rs_cp, rs_written = await _run_checkpointed("auto")

    assert off_records == rs_records == list(range(20))
    assert off_written == rs_written == 20
    assert off_cp == rs_cp == {"index": 19}


async def _run_writer_batch_checkpoint_cancellation(mode: str) -> tuple[list[int], dict | None]:
    store = _BlockingCheckpointStore()
    first_sink = _CollectSink()
    pipeline_id = f"acceleration_writer_batch_cancellation_{mode}"
    run = asyncio.create_task(
        Pipeline(_CheckpointedSource([10, 20, 30]), id=pipeline_id)
        .build(
            first_sink,  # type: ignore[arg-type]
            config=DeliveryConfig(
                acceleration_mode=mode,
                checkpoint=store,
                batch_size=2,
            ),
        )
        .run()
    )

    await asyncio.wait_for(store.save_started.wait(), timeout=1.0)
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run

    assert first_sink.records == [10, 20]
    assert await store.load(pipeline_id) is None

    store.release_save.set()
    resumed_sink = _CollectSink()
    summary = await (
        Pipeline(_CheckpointedSource([10, 20, 30]), id=pipeline_id)
        .build(
            resumed_sink,  # type: ignore[arg-type]
            config=DeliveryConfig(
                acceleration_mode=mode,
                checkpoint=store,
                batch_size=2,
            ),
        )
        .run()
    )
    return [*first_sink.records, *resumed_sink.records], (
        None if summary.last_checkpoint is None else summary.last_checkpoint.value
    )


@pytest.mark.asyncio
async def test_writer_batch_checkpoint_cancellation_parity_off_vs_rs() -> None:
    """Rust bookkeeping must preserve the same replay window as Python."""
    off_records, off_checkpoint = await _run_writer_batch_checkpoint_cancellation("off")
    rs_records, rs_checkpoint = await _run_writer_batch_checkpoint_cancellation("auto")

    assert off_records == rs_records == [10, 20, 10, 20, 30]
    assert off_checkpoint == rs_checkpoint == {"index": 2}


async def _run_buffered_checkpoint_cancellation(mode: str) -> tuple[list[int], dict | None]:
    store = _BlockingCheckpointStore()
    first_sink = _CollectSink()
    pipeline_id = f"acceleration_buffered_cancellation_{mode}"
    run = asyncio.create_task(
        Pipeline(_CheckpointedSource([10, 20, 30]), id=pipeline_id)
        .pipe(_BufferedPassThroughMiddleware())
        .build(
            first_sink,  # type: ignore[arg-type]
            config=DeliveryConfig(acceleration_mode=mode, checkpoint=store),
        )
        .run()
    )

    await asyncio.wait_for(store.save_started.wait(), timeout=1.0)
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run

    assert first_sink.records == [10]
    assert await store.load(pipeline_id) is None

    store.release_save.set()
    resumed_sink = _CollectSink()
    summary = await (
        Pipeline(_CheckpointedSource([10, 20, 30]), id=pipeline_id)
        .pipe(_BufferedPassThroughMiddleware())
        .build(
            resumed_sink,  # type: ignore[arg-type]
            config=DeliveryConfig(acceleration_mode=mode, checkpoint=store),
        )
        .run()
    )
    return [*first_sink.records, *resumed_sink.records], (
        None if summary.last_checkpoint is None else summary.last_checkpoint.value
    )


@pytest.mark.asyncio
async def test_buffered_checkpoint_cancellation_parity_off_vs_rs() -> None:
    """Buffered lane recovery cannot change when Rust hot metrics are active."""
    off_records, off_checkpoint = await _run_buffered_checkpoint_cancellation("off")
    rs_records, rs_checkpoint = await _run_buffered_checkpoint_cancellation("auto")

    assert off_records == rs_records == [10, 10, 20, 30]
    assert off_checkpoint == rs_checkpoint == {"index": 2}


async def _run_batch_checkpoint_cancellation(mode: str) -> tuple[list[int], dict | None, str]:
    store = _BlockingCheckpointStore()
    first_sink = _CollectSink()
    pipeline_id = f"acceleration_batch_cancellation_{mode}"
    run = asyncio.create_task(
        Pipeline(_CheckpointedBatchSource([[10, 20], [30, 40]]), id=pipeline_id)
        .pipe(_BatchPassThroughMiddleware())
        .build(
            first_sink,  # type: ignore[arg-type]
            config=DeliveryConfig(acceleration_mode=mode, checkpoint=store),
        )
        .run()
    )

    await asyncio.wait_for(store.save_started.wait(), timeout=1.0)
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run

    assert first_sink.records == [10, 20]
    assert await store.load(pipeline_id) is None

    store.release_save.set()
    resumed_sink = _CollectSink()
    summary = await (
        Pipeline(_CheckpointedBatchSource([[10, 20], [30, 40]]), id=pipeline_id)
        .pipe(_BatchPassThroughMiddleware())
        .build(
            resumed_sink,  # type: ignore[arg-type]
            config=DeliveryConfig(acceleration_mode=mode, checkpoint=store),
        )
        .run()
    )
    return (
        [*first_sink.records, *resumed_sink.records],
        (None if summary.last_checkpoint is None else summary.last_checkpoint.value),
        summary.runtime.execution_lane,
    )


@pytest.mark.asyncio
async def test_batch_checkpoint_cancellation_parity_off_vs_rs() -> None:
    """Native batch execution must preserve Python checkpoint/replay semantics."""
    off_records, off_checkpoint, off_lane = await _run_batch_checkpoint_cancellation("off")
    rs_records, rs_checkpoint, rs_lane = await _run_batch_checkpoint_cancellation("auto")

    assert off_records == rs_records == [10, 20, 10, 20, 30, 40]
    assert off_checkpoint == rs_checkpoint == {"batch_index": 1}
    assert off_lane == rs_lane == "batch"


async def _run_buffered_backpressure(mode: str) -> tuple[list[int], Any]:
    sink = _CollectSink()
    summary = await (
        Pipeline(IterableSource(list(range(18))))
        .pipe(_BufferedPassThroughMiddleware(batch_size=2))
        .build(
            sink,  # type: ignore[arg-type]
            config=DeliveryConfig(
                acceleration_mode=mode,
                batch_size=2,
                backpressure=Backpressure.adaptive(
                    max_buffer_size=5,
                    writer_slow_ms=100.0,
                    checkpoint_slow_ms=100.0,
                ),
            ),
        )
        .run()
    )
    return sink.records, summary.runtime


@pytest.mark.asyncio
async def test_buffered_backpressure_bounds_hold_off_vs_rs() -> None:
    """Optional Rust hot paths cannot bypass configured buffered-lane bounds."""
    off_records, off_runtime = await _run_buffered_backpressure("off")
    rs_records, rs_runtime = await _run_buffered_backpressure("auto")

    assert off_records == rs_records == list(range(18))
    for runtime in (off_runtime, rs_runtime):
        assert runtime.execution_lane == "buffered"
        assert runtime.adaptive_backpressure_enabled is True
        assert 1 <= runtime.buffered_stage_limit <= 5
        assert runtime.buffered_stage_max_in_flight <= 5


@pytest.mark.asyncio
async def test_explain_reports_acceleration_active_when_rust_available() -> None:
    pipeline = (
        Pipeline(IterableSource(list(range(10))))
        .pipe(MapMiddleware(lambda x: x + 1, name="inc"))
        .build(
            _CollectSink(),  # type: ignore[arg-type]
            config=DeliveryConfig(acceleration_mode="auto", batch_size=4),
        )
    )

    explain = pipeline.explain()

    assert explain.acceleration.mode == "auto"
    assert explain.acceleration.available is True
    assert explain.acceleration.direct_flush_eligible is True
    assert "checkpoint_state" in explain.acceleration.active_capabilities
    assert explain.to_dict()["acceleration"]["available"] is True
    assert "acceleration=auto" in str(explain)
