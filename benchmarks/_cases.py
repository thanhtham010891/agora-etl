"""Pipeline case definitions — one entry per benchmark case.

Each case runs in its own subprocess. This module is the subprocess entry point.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# NullSink — extends BaseSink properly, zero I/O overhead
# ---------------------------------------------------------------------------


class _NullSink:
    """Discard all records — zero I/O overhead in sink."""

    sink_name = "null"

    async def open(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def flush(self) -> None:
        pass

    async def write(self, record: Any) -> Any:
        from agora.core.writer import WriteResult

        return WriteResult(written=True)

    async def write_batch(self, records: list[Any]) -> tuple[list[Any], Any]:
        from agora.core.writer import WriteResult

        return [WriteResult(written=True) for _ in records], None

    async def write_arrow_batch(self, batch: Any) -> None:
        pass


# ---------------------------------------------------------------------------
# Row mappers — minimal, no extra allocation
# ---------------------------------------------------------------------------


def _csv_mapper(row: dict[str, str]) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "name": row["name"],
        "value": float(row["value"]),
        "score": int(row["score"]),
        "active": row["active"] == "True",
        "category": row["category"],
        "tag": row["tag"],
    }


def _passthrough_mapper(row: dict[str, Any]) -> dict[str, Any]:
    return row


# ---------------------------------------------------------------------------
# Case builder — returns (source, pipeline_builder, config)
# ---------------------------------------------------------------------------


def _build_case(data_dir: Path, lane: str, case: str) -> Any:
    from agora import DeliveryConfig, Pipeline

    # ---- source ----
    if lane == "csv":
        from agora.sources.file import ArrowCsvSource, CsvSource

        if case == "arrow":
            source = ArrowCsvSource(data_dir / "input.csv")
        elif case in ("batch_map", "batch_filter"):
            source = CsvSource(
                data_dir / "input.csv",
                row_mapper=_csv_mapper,
                emit_batches=True,
                emit_batch_size=5000,
            )
        else:
            source = CsvSource(data_dir / "input.csv", row_mapper=_csv_mapper)

    elif lane == "jsonl":
        from agora.sources.file import ArrowJsonLinesSource, JsonLinesSource

        if case == "arrow":
            source = ArrowJsonLinesSource(data_dir / "input.jsonl")
        elif case in ("batch_map", "batch_filter"):
            source = JsonLinesSource(
                data_dir / "input.jsonl",
                row_mapper=_passthrough_mapper,
                emit_batches=True,
                emit_batch_size=5000,
            )
        else:
            source = JsonLinesSource(data_dir / "input.jsonl", row_mapper=_passthrough_mapper)

    elif lane == "parquet":
        from agora.sources.file import ParquetSource

        if case == "arrow":
            # use_arrow_batches=True: yields pa.RecordBatch, no row_mapper
            source = ParquetSource(
                data_dir / "input.parquet",
                row_mapper=_passthrough_mapper,
                use_arrow_batches=True,
            )
        else:
            source = ParquetSource(data_dir / "input.parquet", row_mapper=_passthrough_mapper)
    else:
        raise ValueError(f"Unknown lane: {lane!r}")

    # ---- middleware + config ----
    pipeline = Pipeline(source)
    # batch_size=100 is the recommended default for per-record pipelines —
    # reduces delivery overhead ~2x vs batch_size=1 (the framework default).
    _per_record_config = DeliveryConfig(batch_size=100)

    if case == "direct":
        config = _per_record_config

    elif case == "map":
        from agora import MapMiddleware

        pipeline = pipeline.pipe(MapMiddleware(lambda r: r, name="map"))
        config = _per_record_config

    elif case == "filter":
        from agora import FilterMiddleware

        pipeline = pipeline.pipe(FilterMiddleware(lambda r: True, name="filter"))
        config = _per_record_config

    elif case == "map_filter":
        from agora import FilterMiddleware, MapMiddleware

        pipeline = pipeline.pipe(MapMiddleware(lambda r: r, name="map")).pipe(
            FilterMiddleware(lambda r: True, name="filter")
        )
        config = _per_record_config

    elif case == "batch_map":
        from agora import BatchMapMiddleware

        pipeline = pipeline.pipe(BatchMapMiddleware(lambda r: r, name="batch_map"))
        config = DeliveryConfig(batch_size=1000)

    elif case == "batch_filter":
        from agora import BatchFilterMiddleware

        pipeline = pipeline.pipe(BatchFilterMiddleware(lambda r: True, name="batch_filter"))
        config = DeliveryConfig(batch_size=1000)

    elif case == "arrow":
        config = DeliveryConfig()

    elif case == "arrow_map":
        from agora import ArrowMapMiddleware

        pipeline = pipeline.pipe(ArrowMapMiddleware(lambda b: b, name="arrow_map"))
        config = DeliveryConfig()

    elif case == "arrow_filter":
        import pyarrow.compute as pc

        from agora import ArrowFilterMiddleware

        pipeline = pipeline.pipe(
            ArrowFilterMiddleware(
                lambda b: pc.greater_equal(b.column("score"), 0),
                name="arrow_filter",
            )
        )
        config = DeliveryConfig()

    elif case in ("arrow_to_csv", "arrow_to_jsonl"):
        config = DeliveryConfig()

    else:
        raise ValueError(f"Unknown case: {case!r}")

    return pipeline, config


# ---------------------------------------------------------------------------
# Runner — called inside subprocess
# ---------------------------------------------------------------------------


async def _run(data_dir: Path, lane: str, case: str) -> dict[str, Any]:
    import contextlib
    import os
    import tempfile

    pipeline, config = _build_case(data_dir, lane, case)

    # Cases that write to a real sink use a temp file — cleaned up after run
    tmp_name: str | None = None
    if case == "arrow_to_csv":
        from agora.sinks.file.csv import CsvSink

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp_name = tmp.name
        sink: Any = CsvSink(tmp_name, row_mapper=lambda r: r)
    elif case == "arrow_to_jsonl":
        from agora.sinks.file.jsonlines import JsonLinesSink

        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as tmp:
            tmp_name = tmp.name
        sink = JsonLinesSink(tmp_name)
    else:
        sink = _NullSink()

    t0 = time.perf_counter()
    summary = await pipeline.build(sink, config=config).run()
    elapsed = time.perf_counter() - t0

    # Clean up temp file
    if tmp_name is not None:
        with contextlib.suppress(OSError):
            os.unlink(tmp_name)

    consumed = summary.records_consumed
    return {
        "lane": lane,
        "case": case,
        "rows": consumed,
        "elapsed_s": round(elapsed, 6),
        "throughput_rps": round(consumed / elapsed) if elapsed > 0 else 0,
    }


def run_case(data_dir: str, lane: str, case: str) -> None:
    """Subprocess entry point — prints a single JSON line to stdout."""
    result = asyncio.run(_run(Path(data_dir), lane, case))
    print(json.dumps(result), flush=True)


# ---------------------------------------------------------------------------
# Case registry
# ---------------------------------------------------------------------------

CSV_CASES: list[tuple[str, str]] = [
    ("csv", "direct"),
    ("csv", "map"),
    ("csv", "filter"),
    ("csv", "map_filter"),
    ("csv", "batch_map"),
    ("csv", "batch_filter"),
    ("csv", "arrow"),
    ("csv", "arrow_map"),
    ("csv", "arrow_filter"),
    ("csv", "arrow_to_csv"),
]

JSONL_CASES: list[tuple[str, str]] = [
    ("jsonl", "direct"),
    ("jsonl", "map"),
    ("jsonl", "filter"),
    ("jsonl", "map_filter"),
    ("jsonl", "batch_map"),
    ("jsonl", "batch_filter"),
    ("jsonl", "arrow"),
    ("jsonl", "arrow_map"),
    ("jsonl", "arrow_filter"),
    ("jsonl", "arrow_to_jsonl"),
]

# Parquet has no list-batch emit mode — batch_map/batch_filter not applicable
PARQUET_CASES: list[tuple[str, str]] = [
    ("parquet", "direct"),
    ("parquet", "map"),
    ("parquet", "filter"),
    ("parquet", "map_filter"),
    ("parquet", "arrow"),
    ("parquet", "arrow_map"),
    ("parquet", "arrow_filter"),
]

ALL_CASES: list[tuple[str, str]] = CSV_CASES + JSONL_CASES + PARQUET_CASES
