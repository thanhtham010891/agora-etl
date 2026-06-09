"""Benchmark canonical runtime execution shapes beyond file-format lanes.

This complements ``benchmarks/run.py``:

- ``run.py`` measures file-backed lane throughput (linear, batch, Arrow)
- ``runtime_shapes.py`` measures orchestration-heavy runtime shapes
  (buffered lane, sink fan-out, observability overhead)

Usage::

    PYTHONPATH=src ./.venv/bin/python benchmarks/runtime_shapes.py
    PYTHONPATH=src ./.venv/bin/python benchmarks/runtime_shapes.py --rows 200000 --median 5
    PYTHONPATH=src ./.venv/bin/python benchmarks/runtime_shapes.py --only linear_direct fanout_dual
    PYTHONPATH=src ./.venv/bin/python benchmarks/runtime_shapes.py --save-json benchmarks/_results/runtime_shapes.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Any

import uvloop

_REPO_ROOT = Path(__file__).parent.parent
_RESULTS_DIR = _REPO_ROOT / "benchmarks" / "_results"
_SRC_DIR = _REPO_ROOT / "src"

SCENARIOS = (
    "linear_direct",
    "linear_map",
    "buffered_submit",
    "fanout_dual",
    "observability_full",
)


class _NullSink:
    sink_name = "null"

    async def open(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def flush(self) -> None:
        pass

    async def write(self, record: Any) -> Any:
        from agora.core.writer import WriteResult

        del record
        return WriteResult(written=True)

    async def write_batch(self, records: list[Any]) -> None:
        del records

    async def write_arrow_batch(self, batch: Any) -> None:
        del batch


class _BufferedPassThroughMiddleware:
    name = "buffered_passthrough"

    def __init__(self, batch_size: int = 64) -> None:
        self.min_concurrency = batch_size
        self._batch_size = batch_size
        self._pending: list[tuple[int, asyncio.Future[int | None]]] = []

    async def process(self, record: int, ctx: Any) -> int | None:
        del ctx
        return record

    async def submit(self, record: int, ctx: Any) -> asyncio.Future[int | None]:
        del ctx
        future: asyncio.Future[int | None] = asyncio.get_running_loop().create_future()
        self._pending.append((record, future))
        if len(self._pending) >= self._batch_size:
            await self._flush_pending()
        return future

    async def drain_pending(self, ctx: Any) -> None:
        del ctx
        await self._flush_pending()

    async def _flush_pending(self) -> None:
        batch, self._pending = self._pending, []
        for record, future in batch:
            if not future.done():
                future.set_result(record)


def _delivery_config(*, tracer: Any | None = None) -> Any:
    from agora import DeliveryConfig

    return DeliveryConfig(
        batch_size=100,
        max_buffer_size=64,
        tracer=tracer,
    )


def _build_pipeline(rows: int, scenario: str) -> Any:
    from agora import InMemoryTracer, IterableSource, MapMiddleware, Pipeline
    from agora.core.middleware import Middleware

    class _BufferedPassThroughMiddlewareImpl(_BufferedPassThroughMiddleware, Middleware[int, int]):
        pass

    source = IterableSource(range(rows))

    if scenario == "linear_direct":
        return Pipeline(source).build(_NullSink(), config=_delivery_config())

    if scenario == "linear_map":
        return (
            Pipeline(source)
            .pipe(MapMiddleware(lambda record: record, name="noop_map"))
            .build(_NullSink(), config=_delivery_config())
        )

    if scenario == "buffered_submit":
        return (
            Pipeline(source)
            .pipe(_BufferedPassThroughMiddlewareImpl())
            .build(
                _NullSink(),
                config=_delivery_config(),
            )
        )

    if scenario == "fanout_dual":
        return Pipeline(source).fan_out(
            [_NullSink(), _NullSink()],
            config=_delivery_config(),
        )

    if scenario == "observability_full":
        bound = (
            Pipeline(source)
            .pipe(MapMiddleware(lambda record: record, name="noop_map"))
            .build(_NullSink(), config=_delivery_config(tracer=InMemoryTracer()))
        )

        async def _live_metrics_callback(ctx: Any) -> None:
            _ = (
                ctx.metrics.records_processed,
                ctx.metrics.runtime.execution_lane,
                ctx.metrics.runtime.arrow_fast_path_active,
            )

        bound.set_live_metrics_callback(_live_metrics_callback)
        return bound

    raise ValueError(f"Unknown scenario: {scenario!r}")


async def _run_scenario(rows: int, scenario: str) -> dict[str, Any]:
    pipeline = _build_pipeline(rows, scenario)
    t0 = time.perf_counter()
    summary = await pipeline.run()
    elapsed = time.perf_counter() - t0
    return {
        "scenario": scenario,
        "rows": rows,
        "elapsed_s": round(elapsed, 6),
        "throughput_rps": round(summary.records_consumed / elapsed) if elapsed > 0 else 0,
    }


def run_case(rows: int, scenario: str) -> None:
    result = asyncio.run(_run_scenario(rows, scenario), loop_factory=uvloop.new_event_loop)
    print(json.dumps(result), flush=True)


def _run_case_isolated(rows: int, scenario: str, *, timeout: int) -> dict[str, Any]:
    script = textwrap.dedent(f"""\
        import sys
        sys.path.insert(0, {str(_SRC_DIR)!r})
        sys.path.insert(0, {str(_REPO_ROOT)!r})
        from benchmarks.runtime_shapes import run_case
        run_case({rows!r}, {scenario!r})
    """)
    t0 = time.perf_counter()
    try:
        proc = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(_REPO_ROOT),
        )
    except subprocess.TimeoutExpired:
        return {
            "scenario": scenario,
            "rows": rows,
            "error": f"timeout after {timeout}s",
            "elapsed_s": timeout,
            "throughput_rps": 0,
        }

    wall = time.perf_counter() - t0
    if proc.returncode != 0:
        stderr = proc.stderr.strip()
        return {
            "scenario": scenario,
            "rows": rows,
            "error": stderr[-500:] if stderr else f"exit code {proc.returncode}",
            "elapsed_s": round(wall, 6),
            "throughput_rps": 0,
        }

    stdout = proc.stdout.strip()
    if not stdout:
        return {
            "scenario": scenario,
            "rows": rows,
            "error": "no output from subprocess",
            "elapsed_s": round(wall, 6),
            "throughput_rps": 0,
        }

    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return {
            "scenario": scenario,
            "rows": rows,
            "error": f"bad JSON: {stdout[:200]}",
            "elapsed_s": round(wall, 6),
            "throughput_rps": 0,
        }


def _median_result(runs: list[dict[str, Any]]) -> dict[str, Any]:
    ok_runs = [run for run in runs if "error" not in run]
    if not ok_runs:
        return runs[-1]
    elapsed = [run["elapsed_s"] for run in ok_runs]
    target = statistics.median(elapsed)
    return min(ok_runs, key=lambda run: abs(run["elapsed_s"] - target))


def _fmt_rps(rps: int) -> str:
    if rps >= 1_000_000:
        return f"{rps / 1_000_000:.2f}M rec/s"
    if rps >= 1_000:
        return f"{rps / 1_000:.1f}K rec/s"
    return f"{rps} rec/s"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark Agora runtime execution shapes")
    parser.add_argument(
        "--rows",
        type=int,
        default=200_000,
        help="Records per scenario (default: 200000)",
    )
    parser.add_argument(
        "--median",
        type=int,
        default=3,
        help="Number of runs per scenario; report median elapsed/throughput (default: 3)",
    )
    parser.add_argument(
        "--only",
        nargs="*",
        choices=SCENARIOS,
        metavar="SCENARIO",
        help="Run only these scenarios. Omit to run the full runtime-shape set.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=120,
        help="Per-scenario timeout in seconds (default: 120)",
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
    rows = args.rows
    median = max(1, args.median)
    timeout = args.timeout
    scenarios = list(args.only or SCENARIOS)
    results: list[dict[str, Any]] = []

    print()
    print("  Agora Runtime Shapes Benchmark")
    print(f"  rows={rows:,}  scenarios={', '.join(scenarios)}  median={median}")
    print()
    print(f"  {'scenario':<20} {'elapsed':>10} {'throughput':>14}  status")
    print("  " + "-" * 58)

    for scenario in scenarios:
        runs = [_run_case_isolated(rows, scenario, timeout=timeout) for _ in range(median)]
        result = _median_result(runs)
        results.append(result)
        if "error" in result:
            print(f"  {scenario:<20} {'':>10} {'':>14}  FAIL {result['error'][:60]}")
            continue
        print(
            f"  {scenario:<20} {result['elapsed_s']:>9.3f}s "
            f"{_fmt_rps(result['throughput_rps']):>14}  ok"
        )

    print("  " + "-" * 58)
    ok = sum(1 for result in results if "error" not in result)
    fail = len(results) - ok
    print(f"\n  {ok} passed  {fail} failed\n")

    if args.save_json is not None:
        args.save_json.parent.mkdir(parents=True, exist_ok=True)
        args.save_json.write_text(json.dumps(results, indent=2))
        print(f"Saved results to {args.save_json}")
    elif results:
        _RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        out = _RESULTS_DIR / "runtime_shapes_latest.json"
        out.write_text(json.dumps(results, indent=2))
        print(f"Saved results to {out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
