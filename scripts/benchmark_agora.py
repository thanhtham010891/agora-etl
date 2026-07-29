#!/usr/bin/env python3
"""Run a repeatable synthetic core-runtime benchmark.

This measures Python-row dispatch through a real Agora pipeline. It is useful
for comparing runtime profiles and optional acceleration on the same machine;
it is not a backend benchmark and does not certify delivery semantics.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import statistics
import time
import tracemalloc
from typing import TYPE_CHECKING, Any

from agora import DeliveryConfig, IterableSource, MapMiddleware, Pipeline

if TYPE_CHECKING:
    from collections.abc import Iterable


class CountingSink:
    """A batch-capable sink that prevents synthetic work from being discarded."""

    sink_name = "benchmark_counting"

    def __init__(self) -> None:
        self.records_written = 0
        self.checksum = 0

    async def open(self) -> None:
        return None

    async def write(self, record: dict[str, int]) -> None:
        self.records_written += 1
        self.checksum ^= record["checksum"]

    async def write_batch(self, records: list[dict[str, int]]) -> None:
        for record in records:
            await self.write(record)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None


def _records(count: int, payload_bytes: int) -> Iterable[dict[str, object]]:
    payload = "x" * payload_bytes
    for index in range(count):
        yield {"id": index, "payload": payload}


def _transform(record: dict[str, object]) -> dict[str, int]:
    record_id = int(record["id"])
    payload_size = len(str(record["payload"]))
    return {
        "id": record_id,
        "payload_bytes": payload_size,
        "checksum": record_id ^ payload_size,
    }


async def run_once(args: argparse.Namespace, *, measure_memory: bool) -> dict[str, Any]:
    sink = CountingSink()
    pipeline = (
        Pipeline(IterableSource(_records(args.records, args.payload_bytes)))
        .pipe(MapMiddleware(_transform, name="benchmark_payload_projection"))
        .build(
            sink,  # type: ignore[arg-type]
            config=DeliveryConfig(
                acceleration_mode=args.acceleration_mode,
                performance_profile=args.performance_profile,
                batch_size=args.batch_size,
            ),
        )
    )

    if measure_memory:
        tracemalloc.start()
    started = time.perf_counter()
    try:
        summary = await pipeline.run()
    finally:
        elapsed_s = time.perf_counter() - started

    peak_memory_bytes = 0
    if measure_memory:
        _, peak_memory_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

    if summary.records_consumed != args.records or sink.records_written != args.records:
        raise RuntimeError(
            "Benchmark pipeline did not deliver every generated record: "
            f"consumed={summary.records_consumed}, written={sink.records_written}, "
            f"expected={args.records}."
        )

    return {
        "elapsed_s": elapsed_s,
        "peak_traced_memory_bytes": peak_memory_bytes,
        "records_consumed": summary.records_consumed,
        "records_written": sink.records_written,
        "checksum": sink.checksum,
    }


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * percentile) - 1)]


async def benchmark(args: argparse.Namespace) -> dict[str, Any]:
    for _ in range(args.warmup_runs):
        await run_once(args, measure_memory=False)

    results = [await run_once(args, measure_memory=True) for _ in range(args.runs)]
    elapsed_values = [float(result["elapsed_s"]) for result in results]
    median_elapsed_s = statistics.median(elapsed_values)
    logical_payload_bytes = args.records * args.payload_bytes

    return {
        "schema_version": 1,
        "workload": "synthetic_python_row_dispatch",
        "semantics": {
            "scope": "core runtime only; no external source or sink",
            "delivery_claim": "not a delivery-guarantee or backend certification",
        },
        "configuration": {
            "records": args.records,
            "payload_bytes": args.payload_bytes,
            "batch_size": args.batch_size,
            "acceleration_mode": args.acceleration_mode,
            "performance_profile": args.performance_profile,
            "warmup_runs": args.warmup_runs,
            "measured_runs": args.runs,
        },
        "latency_s": {
            "min": min(elapsed_values),
            "p50": median_elapsed_s,
            "p95": _percentile(elapsed_values, 0.95),
            "max": max(elapsed_values),
        },
        "throughput": {
            "records_per_s": args.records / median_elapsed_s,
            "logical_payload_mib_per_s": logical_payload_bytes / median_elapsed_s / (1024**2),
        },
        "peak_traced_memory_bytes": max(
            int(result["peak_traced_memory_bytes"]) for result in results
        ),
        "checksums": [int(result["checksum"]) for result in results],
        "runs": results,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=20_000)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmup-runs", type=int, default=1)
    parser.add_argument("--acceleration-mode", choices=("auto", "off", "required"), default="auto")
    parser.add_argument(
        "--performance-profile",
        choices=("balanced", "throughput", "low_latency"),
        default="balanced",
    )
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    args = parser.parse_args()
    for name in ("records", "payload_bytes", "batch_size", "runs"):
        if getattr(args, name) < 1:
            parser.error(f"--{name.replace('_', '-')} must be >= 1")
    if args.warmup_runs < 0:
        parser.error("--warmup-runs must be >= 0")
    return args


def main() -> None:
    args = parse_args()
    result = asyncio.run(benchmark(args))
    print(json.dumps(result, indent=2 if args.pretty else None, sort_keys=True))


if __name__ == "__main__":
    main()
