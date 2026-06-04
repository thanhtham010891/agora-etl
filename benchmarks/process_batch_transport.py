"""Benchmark Arrow process-batch transport choices.

This script compares three candidate worker payload shapes for a future
Arrow-native process batch path:

- direct ``pyarrow.RecordBatch``
- ``pyarrow.Table``
- Arrow IPC bytes

Usage::

    PYTHONPATH=src ./.venv/bin/python benchmarks/process_batch_transport.py
    PYTHONPATH=src ./.venv/bin/python benchmarks/process_batch_transport.py --rows 100000 --repeats 7
    PYTHONPATH=src ./.venv/bin/python benchmarks/process_batch_transport.py --save-json benchmarks/_results/arrow_transport.json

The benchmark uses one long-lived ``ProcessPoolExecutor`` by default so the
timings focus on per-batch transport + worker execution rather than pool startup
cost.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc

if TYPE_CHECKING:
    from collections.abc import Callable


def _build_batch(rows: int) -> pa.RecordBatch:
    ids = pa.array(range(rows), type=pa.int64())
    score = pa.array([(idx % 100) for idx in range(rows)], type=pa.int64())
    value = pa.array([float(idx % 1000) / 3.0 for idx in range(rows)], type=pa.float64())
    category = pa.array([f"cat-{idx % 8}" for idx in range(rows)])
    return pa.record_batch(
        [ids, score, value, category],
        names=["id", "score", "value", "category"],
    )


def _record_batch_to_ipc_bytes(batch: pa.RecordBatch) -> bytes:
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _ipc_bytes_to_record_batch(payload: bytes) -> pa.RecordBatch:
    with ipc.open_stream(payload) as reader:
        return reader.read_next_batch()


def _vectorized_transform(batch: pa.RecordBatch, *, rounds: int) -> pa.RecordBatch:
    score_idx = batch.schema.get_field_index("score")
    value_idx = batch.schema.get_field_index("value")

    score = pc.cast(batch.column(score_idx), pa.float64())
    value = pc.cast(batch.column(value_idx), pa.float64())

    for _ in range(rounds):
        score = pc.multiply(score, 1.01)
        score = pc.add(score, 3.0)
        value = pc.multiply(value, 0.99)
        value = pc.add(value, 1.25)

    out = batch.set_column(score_idx, "score", score)
    return out.set_column(value_idx, "value", value)


def _rb_identity(batch: pa.RecordBatch) -> pa.RecordBatch:
    return batch


def _rb_vectorized(batch: pa.RecordBatch) -> pa.RecordBatch:
    return _vectorized_transform(batch, rounds=1)


def _rb_heavy(batch: pa.RecordBatch) -> pa.RecordBatch:
    return _vectorized_transform(batch, rounds=8)


def _table_identity(table: pa.Table) -> pa.Table:
    return table


def _table_vectorized(table: pa.Table) -> pa.Table:
    return pa.Table.from_batches([_vectorized_transform(table.to_batches()[0], rounds=1)])


def _table_heavy(table: pa.Table) -> pa.Table:
    return pa.Table.from_batches([_vectorized_transform(table.to_batches()[0], rounds=8)])


def _ipc_identity(payload: bytes) -> bytes:
    return _record_batch_to_ipc_bytes(_ipc_bytes_to_record_batch(payload))


def _ipc_vectorized(payload: bytes) -> bytes:
    batch = _ipc_bytes_to_record_batch(payload)
    return _record_batch_to_ipc_bytes(_vectorized_transform(batch, rounds=1))


def _ipc_heavy(payload: bytes) -> bytes:
    batch = _ipc_bytes_to_record_batch(payload)
    return _record_batch_to_ipc_bytes(_vectorized_transform(batch, rounds=8))


def _payload_bytes(payload: Any) -> int:
    if isinstance(payload, bytes):
        return len(payload)
    if isinstance(payload, pa.RecordBatch):
        return payload.nbytes
    if isinstance(payload, pa.Table):
        return payload.nbytes
    raise TypeError(f"Unsupported payload type: {type(payload).__name__}")


def _transport_cases(batch: pa.RecordBatch) -> list[tuple[str, str, Any, Callable[[Any], Any]]]:
    table = pa.Table.from_batches([batch])
    ipc_payload = _record_batch_to_ipc_bytes(batch)
    return [
        ("record_batch", "identity", batch, _rb_identity),
        ("record_batch", "vectorized", batch, _rb_vectorized),
        ("record_batch", "heavy", batch, _rb_heavy),
        ("table", "identity", table, _table_identity),
        ("table", "vectorized", table, _table_vectorized),
        ("table", "heavy", table, _table_heavy),
        ("ipc_bytes", "identity", ipc_payload, _ipc_identity),
        ("ipc_bytes", "vectorized", ipc_payload, _ipc_vectorized),
        ("ipc_bytes", "heavy", ipc_payload, _ipc_heavy),
    ]


def _measure_case(
    executor: ProcessPoolExecutor,
    *,
    transport: str,
    workload: str,
    payload: Any,
    fn: Callable[[Any], Any],
    rows: int,
    repeats: int,
) -> dict[str, Any]:
    warmup = executor.submit(fn, payload)
    warmup.result()

    elapsed_runs: list[float] = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        future = executor.submit(fn, payload)
        future.result()
        elapsed_runs.append(time.perf_counter() - t0)

    median_s = statistics.median(elapsed_runs)
    return {
        "transport": transport,
        "workload": workload,
        "rows": rows,
        "payload_bytes": _payload_bytes(payload),
        "median_elapsed_s": round(median_s, 6),
        "throughput_rows_s": round(rows / median_s) if median_s > 0 else 0,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark Arrow process-batch transport choices")
    parser.add_argument("--rows", type=int, default=65_536, help="Rows per batch (default: 65536)")
    parser.add_argument(
        "--repeats",
        type=int,
        default=5,
        help="Measured repetitions per transport/workload after one warmup run (default: 5)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="ProcessPoolExecutor workers (default: 1)",
    )
    parser.add_argument(
        "--save-json",
        type=Path,
        default=None,
        help="Optional output path for JSON results",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    batch = _build_batch(args.rows)
    results: list[dict[str, Any]] = []

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        for transport, workload, payload, fn in _transport_cases(batch):
            result = _measure_case(
                executor,
                transport=transport,
                workload=workload,
                payload=payload,
                fn=fn,
                rows=args.rows,
                repeats=args.repeats,
            )
            results.append(result)

    print()
    print(f"{'transport':<14} {'workload':<12} {'payload':>10} {'elapsed':>10} {'throughput':>14}")
    print("-" * 66)
    for result in results:
        payload_mib = result["payload_bytes"] / (1024 * 1024)
        elapsed_ms = result["median_elapsed_s"] * 1000
        throughput = result["throughput_rows_s"]
        print(
            f"{result['transport']:<14} "
            f"{result['workload']:<12} "
            f"{payload_mib:>8.2f}MiB "
            f"{elapsed_ms:>8.2f}ms "
            f"{throughput:>12,} rows/s"
        )

    if args.save_json is not None:
        args.save_json.parent.mkdir(parents=True, exist_ok=True)
        args.save_json.write_text(json.dumps(results, indent=2))
        print()
        print(f"Saved results to {args.save_json}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
