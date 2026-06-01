"""Subprocess-isolated benchmark runner.

Each case runs in a fresh Python process to avoid GC noise, module-level
caches, and event-loop state leaking between cases.
"""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Any


def run_case_isolated(
    data_dir: Path,
    lane: str,
    case: str,
    *,
    timeout: int = 120,
) -> dict[str, Any]:
    """Run one benchmark case in a subprocess. Returns the result dict."""
    # Bootstrap script: adds src/ and repo root to sys.path then calls run_case.
    src_dir = Path(__file__).parent.parent / "src"
    repo_root = Path(__file__).parent.parent
    script = textwrap.dedent(f"""\
        import sys
        sys.path.insert(0, {str(src_dir)!r})
        sys.path.insert(0, {str(repo_root)!r})
        from benchmarks._cases import run_case
        run_case({str(data_dir)!r}, {lane!r}, {case!r})
    """)

    t0 = time.perf_counter()
    try:
        proc = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(Path(__file__).parent.parent),
        )
    except subprocess.TimeoutExpired:
        return {
            "lane": lane,
            "case": case,
            "error": f"timeout after {timeout}s",
            "elapsed_s": timeout,
            "throughput_rps": 0,
        }

    wall = time.perf_counter() - t0

    if proc.returncode != 0:
        stderr = proc.stderr.strip()
        return {
            "lane": lane,
            "case": case,
            "error": stderr[-500:] if stderr else f"exit code {proc.returncode}",
            "elapsed_s": round(wall, 4),
            "throughput_rps": 0,
        }

    stdout = proc.stdout.strip()
    if not stdout:
        return {
            "lane": lane,
            "case": case,
            "error": "no output from subprocess",
            "elapsed_s": round(wall, 4),
            "throughput_rps": 0,
        }

    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return {
            "lane": lane,
            "case": case,
            "error": f"bad JSON: {stdout[:200]}",
            "elapsed_s": round(wall, 4),
            "throughput_rps": 0,
        }


def run_all(
    data_dir: Path,
    cases: list[tuple[str, str]],
    *,
    timeout: int = 120,
    on_result: Any = None,
) -> list[dict[str, Any]]:
    """Run all cases sequentially, each in its own subprocess.

    Args:
        data_dir: Directory containing input.csv / input.jsonl / input.parquet.
        cases: List of (lane, case) tuples.
        timeout: Per-case timeout in seconds.
        on_result: Optional callback(result) called after each case completes.

    Returns:
        List of result dicts in the same order as *cases*.
    """
    results = []
    for lane, case in cases:
        result = run_case_isolated(data_dir, lane, case, timeout=timeout)
        results.append(result)
        if on_result is not None:
            on_result(result)
    return results
