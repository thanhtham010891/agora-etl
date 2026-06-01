"""Agora pipeline benchmark runner.

Usage
-----
    # Run all lanes (generates fixture data automatically)
    python benchmarks/run.py --rows 100000

    # Run specific lanes only
    python benchmarks/run.py --rows 100000 --only csv jsonl

    # Run each case 5 times and take the median
    python benchmarks/run.py --rows 100000 --median 5

    # Save results to JSON
    python benchmarks/run.py --rows 100000 --save

Each case runs in an isolated subprocess to prevent GC noise and
event-loop state from leaking between measurements.
Fixture data is always regenerated before each run.
Each case is run --median times; elapsed and throughput show the median run.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).parent.parent
_DATA_DIR = _REPO_ROOT / "benchmarks" / "_data"
_RESULTS_DIR = _REPO_ROOT / "benchmarks" / "_results"

# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

_LANE_COLORS = {
    "csv": "\033[36m",  # cyan
    "jsonl": "\033[33m",  # yellow
    "parquet": "\033[35m",  # magenta
}
_RESET = "\033[0m"
_BOLD = "\033[1m"
_RED = "\033[31m"
_GREEN = "\033[32m"
_DIM = "\033[2m"


def _color(text: str, code: str) -> str:
    if not sys.stdout.isatty():
        return text
    return f"{code}{text}{_RESET}"


def _fmt_rps(rps: int) -> str:
    if rps >= 1_000_000:
        return f"{rps / 1_000_000:.2f}M rec/s"
    if rps >= 1_000:
        return f"{rps / 1_000:.1f}K rec/s"
    return f"{rps} rec/s"


def _print_header(rows: int, lanes: list[str], median: int) -> None:
    print()
    print(_color("  Agora Pipeline Benchmark", _BOLD))
    print(_color(f"  rows={rows:,}  lanes={', '.join(lanes)}  median={median}", _DIM))
    print()
    print(f"  {'lane':<8} {'case':<16} {'elapsed':>10} {'throughput':>14}  status")
    print("  " + "-" * 58)


def _print_result(result: dict) -> None:
    lane = result.get("lane", "?")
    case = result.get("case", "?")
    color = _LANE_COLORS.get(lane, "")

    if "error" in result:
        status = _color("FAIL", _RED)
        row = (
            f"  {_color(lane, color):<8} {case:<16} {'':>10} {'':>14}  "
            f"{status} {_color(result['error'][:60], _DIM)}"
        )
    else:
        elapsed = result.get("elapsed_s", 0)
        rps = result.get("throughput_rps", 0)
        status = _color("ok", _GREEN)
        row = (
            f"  {_color(lane, color):<8} {case:<16} {elapsed:>9.3f}s {_fmt_rps(rps):>14}  {status}"
        )
    print(row)


def _print_summary(results: list[dict], total_elapsed: float) -> None:
    print("  " + "-" * 58)
    ok = sum(1 for r in results if "error" not in r)
    fail = len(results) - ok
    print(
        f"\n  {_color(str(ok), _GREEN)} passed  "
        f"{_color(str(fail), _RED) if fail else str(fail)} failed  "
        f"total {total_elapsed:.1f}s\n"
    )


def _print_lane_summary(results: list[dict]) -> None:
    """Print per-lane throughput comparison table."""
    from collections import defaultdict

    by_lane: dict[str, list[dict]] = defaultdict(list)
    for r in results:
        if "error" not in r:
            by_lane[r["lane"]].append(r)

    if not by_lane:
        return

    print(_color("  Throughput by lane (median rec/s)", _BOLD))
    print()

    all_cases = sorted({r["case"] for r in results if "error" not in r})
    col_w = 16

    header = f"  {'case':<16}" + "".join(
        f"{_color(lane, _LANE_COLORS.get(lane, '')):<{col_w}}" for lane in sorted(by_lane)
    )
    print(header)
    print("  " + "-" * (16 + col_w * len(by_lane)))

    for case in all_cases:
        row = f"  {case:<16}"
        for lane in sorted(by_lane):
            match = next((r for r in by_lane[lane] if r["case"] == case), None)
            if match:
                row += f"{_fmt_rps(match['throughput_rps']):<{col_w}}"
            else:
                row += f"{'—':<{col_w}}"
        print(row)
    print()


# ---------------------------------------------------------------------------
# Median aggregation
# ---------------------------------------------------------------------------


def _median_result(runs: list[dict]) -> dict:
    """Pick the run whose elapsed_s is closest to the median elapsed_s."""
    ok_runs = [r for r in runs if "error" not in r]
    if not ok_runs:
        # All failed — return the last error
        return runs[-1]

    sorted_by_elapsed = sorted(ok_runs, key=lambda r: r["elapsed_s"])
    mid = len(sorted_by_elapsed) // 2
    return sorted_by_elapsed[mid]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Agora pipeline benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=100_000,
        help="Number of rows to generate / benchmark (default: 100000)",
    )
    parser.add_argument(
        "--median",
        type=int,
        default=3,
        help="Number of runs per case; report median elapsed/throughput (default: 3)",
    )
    parser.add_argument(
        "--only",
        nargs="*",
        choices=["csv", "jsonl", "parquet"],
        metavar="LANE",
        help="Run only these lanes (csv, jsonl, parquet). Omit to run all.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=120,
        help="Per-case timeout in seconds (default: 120)",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Save results as JSON to benchmarks/_results/",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=_DATA_DIR,
        help=f"Directory for fixture data (default: {_DATA_DIR})",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    data_dir: Path = args.data_dir
    rows: int = args.rows
    median: int = max(1, args.median)
    lanes: list[str] = args.only or ["csv", "jsonl", "parquet"]
    timeout: int = args.timeout

    # ---- always generate fresh fixture data ----
    sys.path.insert(0, str(_REPO_ROOT / "src"))
    sys.path.insert(0, str(_REPO_ROOT))
    print(_color(f"\n  Generating {rows:,} rows → {data_dir}", _BOLD))
    from benchmarks._generate import generate

    generate(data_dir, rows)
    print()

    # ---- select cases ----
    from benchmarks._cases import ALL_CASES
    from benchmarks._runner import run_case_isolated

    cases = [(lane, case) for lane, case in ALL_CASES if lane in lanes]

    # ---- run with median ----
    _print_header(rows, lanes, median)
    t_start = time.perf_counter()
    final_results: list[dict] = []

    for lane, case in cases:
        runs: list[dict] = []
        for _ in range(median):
            r = run_case_isolated(data_dir, lane, case, timeout=timeout)
            runs.append(r)
        result = _median_result(runs)
        _print_result(result)
        final_results.append(result)

    total_elapsed = time.perf_counter() - t_start
    _print_summary(final_results, total_elapsed)
    _print_lane_summary(final_results)

    # ---- save ----
    if args.save:
        _RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y%m%d_%H%M%S")
        out = _RESULTS_DIR / f"benchmark_{ts}_rows{rows}.json"
        out.write_text(
            json.dumps(
                {"rows": rows, "lanes": lanes, "median": median, "results": final_results},
                indent=2,
            )
        )
        print(_color(f"  Results saved → {out}\n", _DIM))

    return 1 if any("error" in r for r in final_results) else 0


if __name__ == "__main__":
    sys.exit(main())
