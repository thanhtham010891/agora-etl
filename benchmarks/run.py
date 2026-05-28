# ruff: noqa: E402
"""Unified benchmark CLI for Agora ETL core and first-party plugins."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[0]
SRC_DIR = PROJECT_ROOT / "src"

if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from _core import run_core_benchmarks, run_single_core
from _plugins import run_plugin_benchmarks, run_single_plugin
from _shared import prepare_runtime


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Agora benchmark matrix")
    parser.add_argument("--rows", type=int, default=1_000, help="Rows to generate or run")
    parser.add_argument(
        "--generate",
        action="store_true",
        help="Generate core benchmark input data before running the matrix.",
    )
    parser.add_argument(
        "--plugins",
        action="store_true",
        help="Run Kafka and Redis plugin benchmarks instead of the core matrix.",
    )
    parser.add_argument(
        "--markdown",
        action="store_true",
        help="Export markdown reports into docs/benchmark/.",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=3,
        help="Repeat each scenario N times and report the median.",
    )
    parser.add_argument(
        "--only",
        choices=(
            "kafka",
            "redis",
            "csv",
            "csv_batch",
            "arrow_csv",
            "jsonl",
            "jsonl_batch",
            "arrow_jsonl",
            "parquet",
            "parquet_arrow",
        ),
        help="Run only the specified scenario.",
    )
    parser.add_argument("--no-progress", action="store_true", help="Disable progress spinners")
    return parser


def build_hidden_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--_run-single", action="store_true")
    parser.add_argument("--lane", choices=("core", "plugins"), required=True)
    parser.add_argument("--rows", type=int, default=1_000)
    parser.add_argument("--source", type=str)
    parser.add_argument("--middleware", type=str)
    parser.add_argument("--sink", type=str)
    parser.add_argument("--scenario", type=str)
    return parser


async def main(args: argparse.Namespace) -> None:
    only = getattr(args, "only", None)
    if args.plugins or only in ("kafka", "redis"):
        await run_plugin_benchmarks(args)
        return
    await run_core_benchmarks(args)


if __name__ == "__main__":
    if "--_run-single" in sys.argv:
        hidden_args = build_hidden_parser().parse_args()
        prepare_runtime(plugins=hidden_args.lane == "plugins")
        if hidden_args.lane == "core":
            run_single_core(hidden_args)
        else:
            run_single_plugin(hidden_args)
        sys.exit(0)

    cli_args = build_parser().parse_args()
    prepare_runtime(plugins=cli_args.plugins)
    asyncio.run(main(cli_args))
