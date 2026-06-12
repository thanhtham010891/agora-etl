"""End-to-end parity tests between pure-Python and agora-etl-rs paths.

These run the *same* pipeline under ``acceleration_mode="off"`` and
``acceleration_mode="auto"`` and assert identical observable results
(record counts, checkpoint advancement, sink output). They require the
native ``agora_rs`` extension; otherwise they skip.
"""

from __future__ import annotations

import pytest

from agora import DeliveryConfig, InMemoryCheckpointStore, IterableSource, MapMiddleware, Pipeline
from agora.core.source import BaseSource

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
