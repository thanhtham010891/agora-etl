# ruff: noqa: E402
"""Standalone benchmark matrix CLI for Agora ETL.

Usage:
    ./.venv/bin/python benchmarks/run.py --generate
    ./.venv/bin/python benchmarks/run.py --rows 100000
    ./.venv/bin/python benchmarks/run.py --generate --markdown
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import importlib.util
import json
import os
import random
import statistics
import string
import sys
import time
import tracemalloc
from contextlib import contextmanager, nullcontext, redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

REQUIRED_BENCHMARK_MODULES = {
    "pyarrow": "agora-etl[file]",
    "uvloop": "uvloop>=0.21,<1",
    "pyinstrument": "pyinstrument>=5.0,<6",
}


def _validate_benchmark_environment() -> None:
    missing = [
        f"{module} ({requirement})"
        for module, requirement in REQUIRED_BENCHMARK_MODULES.items()
        if importlib.util.find_spec(module) is None
    ]
    if not missing:
        return

    missing_text = "\n".join(f"- {entry}" for entry in missing)
    raise SystemExit(
        "Missing benchmark dependencies.\n"
        "Install the benchmark extra before running this script:\n"
        "  pip install 'agora-etl[benchmark]'\n"
        "Or from this repository:\n"
        "  ./.venv/bin/pip install -e '.[benchmark]'\n"
        f"Required modules:\n{missing_text}"
    )


_validate_benchmark_environment()

import uvloop

from agora import MapMiddleware
from agora.core.middleware import Middleware
from agora.core.pipeline import Pipeline
from agora.core.sink import BaseSink
from agora.sinks.file.csv import CsvSink
from agora.sinks.file.jsonlines import JsonLinesSink
from agora.sinks.file.parquet import ParquetSink
from agora.sinks.io.stdout import StdoutSink
from agora.sources.file.csv import CsvSource
from agora.sources.file.jsonlines import JsonLinesSource

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager

DATA_DIR = Path(__file__).parent / "data"
MANIFEST_PATH = DATA_DIR / "manifest.json"
MARKDOWN_REPORT_PATH = PROJECT_ROOT / "docs" / "benchmark" / "matrix.md"
MB = 1024 * 1024

CATEGORIES = ["electronics", "clothing", "food", "sports", "books", "home", "toys"]
STATUSES = ["active", "inactive", "pending", "archived"]


@dataclass(slots=True)
class Profile:
    name: str
    label: str
    description: str
    factory: Callable[[int], Any | None]
    run_context_factory: Callable[[], AbstractContextManager[object]] | None = None
    cleanup: Callable[[], None] | None = None


@dataclass(slots=True)
class BenchmarkResult:
    source: str
    middleware: str
    sink: str
    status: str
    rows: int = 0
    records_written: int = 0
    elapsed_seconds: float | None = None
    peak_py_heap_mb: float | None = None
    source_input_mb: float | None = None
    writer_flush_count: int = 0
    checkpoint_save_count: int = 0
    buffered_stage_limit: int = 0
    buffered_stage_max_in_flight: int = 0
    repeat_count: int = 1
    detail: str | None = None

    @property
    def throughput_rps(self) -> float | None:
        if self.elapsed_seconds is None or self.elapsed_seconds <= 0 or self.records_written <= 0:
            return None
        return self.records_written / self.elapsed_seconds

    @property
    def throughput_mbps(self) -> float | None:
        if (
            self.elapsed_seconds is None
            or self.elapsed_seconds <= 0
            or self.source_input_mb is None
        ):
            return None
        return self.source_input_mb / self.elapsed_seconds


class NullSink(BaseSink):
    batch_writable_native = True

    async def write(self, record) -> None:
        del record

    async def write_batch(self, records) -> None:
        del records


class BufferedPassThroughMiddleware(Middleware[Any, Any]):
    name = "buffered_passthrough"

    def __init__(self, batch_size: int = 4) -> None:
        self.min_concurrency = batch_size
        self._batch_size = batch_size
        self._pending: list[tuple[Any, asyncio.Future[Any]]] = []

    async def process(self, record: Any, ctx) -> Any | None:
        del ctx
        return record

    async def submit(self, record: Any, ctx) -> asyncio.Future[Any]:
        del ctx
        future = asyncio.get_running_loop().create_future()
        self._pending.append((record, future))
        if len(self._pending) >= self._batch_size:
            await self._flush_pending()
        return future

    async def drain_pending(self, ctx) -> None:
        del ctx
        await self._flush_pending()

    async def _flush_pending(self) -> None:
        batch, self._pending = self._pending, []
        for record, future in batch:
            if not future.done():
                future.set_result(record)


def _random_string(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))


def _make_row(index: int) -> dict[str, object]:
    return {
        "id": index,
        "name": f"product_{_random_string(6)}",
        "category": random.choice(CATEGORIES),
        "price": round(random.uniform(1.0, 999.99), 2),
        "quantity": random.randint(0, 1000),
        "status": random.choice(STATUSES),
        "score": round(random.uniform(0.0, 1.0), 4),
        "tags": f"{_random_string(4)},{_random_string(4)}",
        "description": f"A {random.choice(CATEGORIES)} item with id {index}",
        "created_at": f"2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
    }


def generate_csv(rows: int) -> Path:
    path = DATA_DIR / "sample.csv"
    t0 = time.monotonic()
    with path.open("w", newline="", encoding="utf-8") as handle:
        fieldnames = list(_make_row(0).keys())
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for index in range(rows):
            writer.writerow(_make_row(index))
    _print_generation_result("CSV", path, rows, time.monotonic() - t0)
    return path


def generate_jsonl(rows: int) -> Path:
    path = DATA_DIR / "sample.jsonl"
    t0 = time.monotonic()
    with path.open("w", encoding="utf-8") as handle:
        for index in range(rows):
            handle.write(json.dumps(_make_row(index)) + "\n")
    _print_generation_result("JSONL", path, rows, time.monotonic() - t0)
    return path


def generate_parquet(rows: int) -> Path | None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print("  Parquet  SKIPPED (pip install pyarrow)")
        return None

    path = DATA_DIR / "sample.parquet"
    t0 = time.monotonic()
    batch_size = 10_000
    writer = None
    for start in range(0, rows, batch_size):
        batch = [_make_row(index) for index in range(start, min(start + batch_size, rows))]
        table = pa.Table.from_pylist(batch)
        if writer is None:
            writer = pq.ParquetWriter(path, table.schema)
        writer.write_table(table)
    if writer is not None:
        writer.close()
    _print_generation_result("Parquet", path, rows, time.monotonic() - t0)
    return path


def _print_generation_result(kind: str, path: Path, rows: int, elapsed_seconds: float) -> None:
    size_mb = path.stat().st_size / MB
    print(f"  {kind:<8} {rows:>10,} rows  {size_mb:6.1f} MB  {elapsed_seconds:.1f}s")


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _write_dataset_manifest(rows: int) -> None:
    payload = {
        "rows": rows,
        "sources": {
            "csv": {"size_bytes": (DATA_DIR / "sample.csv").stat().st_size},
            "jsonl": {"size_bytes": (DATA_DIR / "sample.jsonl").stat().st_size},
            "parquet": {"size_bytes": (DATA_DIR / "sample.parquet").stat().st_size},
        },
    }
    MANIFEST_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_dataset_manifest() -> dict[str, Any] | None:
    if not MANIFEST_PATH.exists():
        return None
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def _estimate_source_input_mb(source_name: str, rows_consumed: int) -> float | None:
    manifest = _load_dataset_manifest()
    if manifest is None:
        return None

    total_rows = int(manifest.get("rows", 0))
    source_meta = manifest.get("sources", {}).get(source_name)
    if total_rows <= 0 or not isinstance(source_meta, dict):
        return None

    size_bytes = int(source_meta.get("size_bytes", 0))
    if size_bytes <= 0:
        return None

    consumed_fraction = min(max(rows_consumed, 0), total_rows) / total_rows
    return (size_bytes * consumed_fraction) / MB


@contextmanager
def _redirect_stdout_to_devnull():
    devnull = open(os.devnull, "w")  # noqa: SIM115
    try:
        with redirect_stdout(devnull):
            yield
    finally:
        devnull.close()


def _remove_output_file() -> None:
    path = DATA_DIR / "out.jsonl"
    if path.exists():
        path.unlink()


def _remove_csv_output_file() -> None:
    path = DATA_DIR / "out.csv"
    if path.exists():
        path.unlink()


def _remove_parquet_output_file() -> None:
    path = DATA_DIR / "out.parquet"
    if path.exists():
        path.unlink()


def _build_source_profiles() -> dict[str, Profile]:
    def _csv_source(rows: int):
        del rows
        path = DATA_DIR / "sample.csv"
        if not path.exists():
            return None
        return CsvSource(path=path, row_mapper=lambda row: row, batch_size=1000, queue_maxsize=10)

    def _jsonl_source(rows: int):
        del rows
        path = DATA_DIR / "sample.jsonl"
        if not path.exists():
            return None
        return JsonLinesSource(
            path=path,
            row_mapper=lambda row: row,
            batch_size=1000,
            queue_maxsize=10,
        )

    def _parquet_source(rows: int):
        del rows
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            return None

        from agora.sources.file.parquet import ParquetSource

        path = DATA_DIR / "sample.parquet"
        if not path.exists():
            return None
        source = ParquetSource(path=path, row_mapper=lambda row: row, batch_size=1000)
        source.prefetch_limit = 10
        return source

    return {
        "csv": Profile(
            name="csv",
            label="CSV",
            description="CsvSource over benchmark sample.csv.",
            factory=_csv_source,
        ),
        "jsonl": Profile(
            name="jsonl",
            label="JSONL",
            description="JsonLinesSource over benchmark sample.jsonl.",
            factory=_jsonl_source,
        ),
        "parquet": Profile(
            name="parquet",
            label="Parquet",
            description="ParquetSource over benchmark sample.parquet.",
            factory=_parquet_source,
        ),
    }


def _build_sink_profiles() -> dict[str, Profile]:
    return {
        "null": Profile(
            name="null",
            label="Null",
            description="Discards all output records.",
            factory=lambda rows: NullSink(),
        ),
        "jsonl": Profile(
            name="jsonl",
            label="JSONL",
            description="Writes JSONL output to benchmarks/data/out.jsonl.",
            factory=lambda rows: JsonLinesSink(
                path=DATA_DIR / "out.jsonl",
                append=False,
                flush_every=5000,
            ),
            cleanup=_remove_output_file,
        ),
        "csv": Profile(
            name="csv",
            label="CSV",
            description="Writes CSV output to benchmarks/data/out.csv.",
            factory=lambda rows: CsvSink(
                path=DATA_DIR / "out.csv",
                row_mapper=lambda record: record,
                flush_every=5000,
            ),
            cleanup=_remove_csv_output_file,
        ),
        "parquet": Profile(
            name="parquet",
            label="Parquet",
            description="Writes Parquet output to benchmarks/data/out.parquet.",
            factory=lambda rows: ParquetSink(
                path=DATA_DIR / "out.parquet",
                row_mapper=lambda record: record,
                batch_size=5000,
            ),
            cleanup=_remove_parquet_output_file,
        ),
        "stdout": Profile(
            name="stdout",
            label="Stdout",
            description="StdoutSink redirected to /dev/null.",
            factory=lambda rows: StdoutSink(formatter=lambda record: ""),
            run_context_factory=_redirect_stdout_to_devnull,
        ),
    }


def _build_middleware_profiles() -> dict[str, Profile]:
    return {
        "direct": Profile(
            name="direct",
            label="Direct",
            description="No middleware.",
            factory=lambda rows: None,
        ),
        "map": Profile(
            name="map",
            label="Map",
            description="Identity MapMiddleware on the linear path.",
            factory=lambda rows: MapMiddleware(lambda record: record, name="identity_map"),
        ),
        "buffered": Profile(
            name="buffered",
            label="Buffered",
            description="Buffered pass-through middleware for concurrent execution.",
            factory=lambda rows: BufferedPassThroughMiddleware(batch_size=4),
        ),
    }


async def _run_case(
    rows: int, source_profile: Profile, middleware_profile: Profile, sink_profile: Profile
):
    source = source_profile.factory(rows)
    sink = sink_profile.factory(rows)
    middleware = middleware_profile.factory(rows)

    if source is None:
        return BenchmarkResult(
            source=source_profile.label,
            middleware=middleware_profile.label,
            sink=sink_profile.label,
            status="skipped",
            detail=f"{source_profile.name} source is unavailable or data is missing",
        )

    pipeline = Pipeline(source)
    if middleware is not None:
        pipeline = pipeline.pipe(middleware)

    run_context = (
        sink_profile.run_context_factory() if sink_profile.run_context_factory else nullcontext()
    )

    tracemalloc.start()
    t0 = time.monotonic()
    try:
        with run_context:
            summary = await pipeline.build(sink, batch_size=5000, checkpoint_every=10000).run(
                max_records=rows
            )
    except Exception as exc:
        return BenchmarkResult(
            source=source_profile.label,
            middleware=middleware_profile.label,
            sink=sink_profile.label,
            status="failed",
            detail=f"{type(exc).__name__}: {exc}",
        )
    finally:
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        if sink_profile.cleanup is not None:
            sink_profile.cleanup()

    runtime = summary.runtime
    source_input_mb = _estimate_source_input_mb(source_profile.name, int(summary.records_consumed))
    return BenchmarkResult(
        source=source_profile.label,
        middleware=middleware_profile.label,
        sink=sink_profile.label,
        status="ok",
        rows=int(summary.records_consumed),
        records_written=int(summary.records_written),
        elapsed_seconds=time.monotonic() - t0,
        peak_py_heap_mb=peak / 1024 / 1024,
        source_input_mb=source_input_mb,
        writer_flush_count=runtime.writer_flush_count,
        checkpoint_save_count=runtime.checkpoint_save_count,
        buffered_stage_limit=runtime.buffered_stage_limit,
        buffered_stage_max_in_flight=runtime.buffered_stage_max_in_flight,
    )


def _median_or_none(values: list[float | int | None]) -> float | None:
    samples = [float(value) for value in values if value is not None]
    if not samples:
        return None
    return float(statistics.median(samples))


def _aggregate_repeats(results: list[BenchmarkResult]) -> BenchmarkResult:
    first = results[0]
    if all(result.status == "skipped" for result in results):
        return BenchmarkResult(
            source=first.source,
            middleware=first.middleware,
            sink=first.sink,
            status="skipped",
            repeat_count=len(results),
            detail=first.detail,
        )

    failed = [result for result in results if result.status != "ok"]
    if failed:
        detail = failed[0].detail or "repeat failed"
        if len(results) > 1:
            detail = f"{len(failed)}/{len(results)} repeats failed; {detail}"
        return BenchmarkResult(
            source=first.source,
            middleware=first.middleware,
            sink=first.sink,
            status="failed",
            repeat_count=len(results),
            detail=detail,
        )

    return BenchmarkResult(
        source=first.source,
        middleware=first.middleware,
        sink=first.sink,
        status="ok",
        rows=int(statistics.median(result.rows for result in results)),
        records_written=int(statistics.median(result.records_written for result in results)),
        elapsed_seconds=_median_or_none([result.elapsed_seconds for result in results]),
        peak_py_heap_mb=_median_or_none([result.peak_py_heap_mb for result in results]),
        source_input_mb=_median_or_none([result.source_input_mb for result in results]),
        writer_flush_count=int(statistics.median(result.writer_flush_count for result in results)),
        checkpoint_save_count=int(
            statistics.median(result.checkpoint_save_count for result in results)
        ),
        buffered_stage_limit=int(
            statistics.median(result.buffered_stage_limit for result in results)
        ),
        buffered_stage_max_in_flight=int(
            statistics.median(result.buffered_stage_max_in_flight for result in results)
        ),
        repeat_count=len(results),
    )


def _build_rich_table(results: list[BenchmarkResult], rows_requested: int):
    from rich import box
    from rich.table import Table

    table = Table(
        title=f"Agora ETL — Benchmark Matrix ({rows_requested:,} rows)",
        box=box.ROUNDED,
        header_style="bold cyan",
        title_style="bold white",
    )
    table.add_column("Source", style="bold")
    table.add_column("Middleware")
    table.add_column("Sink")
    table.add_column("Repeat", justify="right")
    table.add_column("Time", justify="right")
    table.add_column("Rows/s", justify="right", style="bold green")
    table.add_column("MB/s", justify="right", style="bold cyan")
    table.add_column("Peak Py Heap", justify="right", style="dim")
    table.add_column("Buffered", justify="right", style="dim")

    previous_source: str | None = None
    for result in results:
        if previous_source is not None and previous_source != result.source:
            table.add_section()
        previous_source = result.source

        if result.status == "skipped":
            table.add_row(
                result.source,
                result.middleware,
                result.sink,
                str(result.repeat_count),
                "—",
                "[yellow]SKIPPED[/]",
                "—",
                "—",
                "—",
            )
            continue
        if result.status == "failed":
            table.add_row(
                result.source,
                result.middleware,
                result.sink,
                str(result.repeat_count),
                "—",
                "[red]FAILED[/]",
                "—",
                "—",
                "—",
            )
            if result.detail:
                table.add_row("", "", "", "", "", result.detail, "", "", "")
            continue

        buffered = (
            f"{result.buffered_stage_max_in_flight}/{result.buffered_stage_limit}"
            if result.buffered_stage_limit > 0
            else "—"
        )
        table.add_row(
            result.source,
            result.middleware,
            result.sink,
            str(result.repeat_count),
            "—" if result.elapsed_seconds is None else f"{result.elapsed_seconds:.2f}s",
            "—" if result.throughput_rps is None else f"{result.throughput_rps:,.0f} r/s",
            "—" if result.throughput_mbps is None else f"{result.throughput_mbps:,.1f} MB/s",
            "—" if result.peak_py_heap_mb is None else f"{result.peak_py_heap_mb:.1f} MB",
            buffered,
        )

    return table


def _result_lookup(results: list[BenchmarkResult]) -> dict[tuple[str, str, str], BenchmarkResult]:
    return {(result.source, result.middleware, result.sink): result for result in results}


def _format_rate(value: float | None, unit: str) -> str:
    if value is None:
        return "—"
    return f"{value:,.1f} {unit}"


def _format_percent(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value * 100:.1f}%"


def _build_source_summary(results: list[BenchmarkResult]) -> list[dict[str, str]]:
    lookup = _result_lookup(results)
    rows: list[dict[str, str]] = []
    for source in ("CSV", "JSONL", "Parquet"):
        result = lookup.get((source, "Direct", "Null"))
        if result is None:
            continue
        rows.append(
            {
                "source": source,
                "time": "—" if result.elapsed_seconds is None else f"{result.elapsed_seconds:.2f}s",
                "rows_s": _format_rate(result.throughput_rps, "r/s"),
                "mb_s": _format_rate(result.throughput_mbps, "MB/s"),
                "peak": "—"
                if result.peak_py_heap_mb is None
                else f"{result.peak_py_heap_mb:.1f} MB",
            }
        )
    return rows


def _build_sink_summary(results: list[BenchmarkResult]) -> list[dict[str, str]]:
    lookup = _result_lookup(results)
    rows: list[dict[str, str]] = []
    for sink in ("Null", "JSONL", "CSV", "Parquet", "Stdout"):
        direct_results = [
            lookup[(source, "Direct", sink)]
            for source in ("CSV", "JSONL", "Parquet")
            if (source, "Direct", sink) in lookup
        ]
        if not direct_results:
            continue

        retention_samples: list[float] = []
        for source in ("CSV", "JSONL", "Parquet"):
            sink_result = lookup.get((source, "Direct", sink))
            null_result = lookup.get((source, "Direct", "Null"))
            if (
                sink_result is None
                or null_result is None
                or sink_result.throughput_rps is None
                or null_result.throughput_rps in {None, 0}
            ):
                continue
            retention_samples.append(sink_result.throughput_rps / null_result.throughput_rps)

        rows.append(
            {
                "sink": sink,
                "rows_s": _format_rate(
                    _median_or_none([result.throughput_rps for result in direct_results]), "r/s"
                ),
                "mb_s": _format_rate(
                    _median_or_none([result.throughput_mbps for result in direct_results]), "MB/s"
                ),
                "retention": _format_percent(_median_or_none(retention_samples)),
                "peak": _format_rate(
                    _median_or_none([result.peak_py_heap_mb for result in direct_results]), "MB"
                ),
            }
        )
    return rows


def _build_buffered_summary(results: list[BenchmarkResult]) -> list[dict[str, str]]:
    lookup = _result_lookup(results)
    rows: list[dict[str, str]] = []
    for source in ("CSV", "JSONL", "Parquet"):
        direct = lookup.get((source, "Direct", "Null"))
        buffered = lookup.get((source, "Buffered", "Null"))
        if direct is None or buffered is None:
            continue

        retention = None
        if direct.throughput_rps not in {None, 0} and buffered.throughput_rps is not None:
            retention = buffered.throughput_rps / direct.throughput_rps

        rows.append(
            {
                "source": source,
                "direct_rows_s": _format_rate(direct.throughput_rps, "r/s"),
                "buffered_rows_s": _format_rate(buffered.throughput_rps, "r/s"),
                "retention": _format_percent(retention),
                "buffered_limit": (
                    f"{buffered.buffered_stage_max_in_flight}/{buffered.buffered_stage_limit}"
                    if buffered.buffered_stage_limit > 0
                    else "—"
                ),
            }
        )
    return rows


def _collect_env_info() -> dict[str, str]:
    import os
    import platform
    import subprocess

    def _run_text_command(*args: str) -> str | None:
        try:
            value = subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL).strip()
        except Exception:
            return None
        return value or None

    def _cpu_label() -> str:
        system = platform.system()
        arch = (
            _run_text_command("uname", "-m")
            or platform.machine()
            or platform.uname().machine
            or platform.processor()
            or "unknown"
        )
        brand: str | None = None

        if system == "Darwin":
            brand = _run_text_command("sysctl", "-n", "machdep.cpu.brand_string")
        elif system == "Linux":
            cpuinfo = _run_text_command("grep", "-m1", "model name", "/proc/cpuinfo")
            if cpuinfo and ":" in cpuinfo:
                brand = cpuinfo.split(":", 1)[1].strip()

        if brand:
            return f"{brand} ({arch})"
        return arch

    def _ram_bytes() -> int | None:
        system = platform.system()

        if system == "Darwin":
            memsize = _run_text_command("sysctl", "-n", "hw.memsize")
            if memsize and memsize.isdigit():
                return int(memsize)
        elif system == "Linux":
            try:
                with open("/proc/meminfo") as f:
                    line = next(ln for ln in f if ln.startswith("MemTotal"))
                return int(line.split()[1]) * 1024
            except Exception:
                pass

        try:
            page_size = os.sysconf("SC_PAGE_SIZE")
            phys_pages = os.sysconf("SC_PHYS_PAGES")
        except (AttributeError, OSError, ValueError):
            return None
        if page_size <= 0 or phys_pages <= 0:
            return None
        return int(page_size * phys_pages)

    cpu = _cpu_label()
    ram_bytes = _ram_bytes()
    ram = f"{ram_bytes // (1024**3)} GB" if ram_bytes else "unknown"

    return {
        "cpu": cpu,
        "ram": ram,
        "python": sys.version.split()[0],
        "os": f"{platform.system()} {platform.release()}",
        "date": __import__("datetime").date.today().isoformat(),
    }


def _markdown_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def _render_markdown(
    results: list[BenchmarkResult], rows_requested: int, env: dict[str, str]
) -> str:
    repeat_count = results[0].repeat_count if results else 1
    source_summary = _build_source_summary(results)
    sink_summary = _build_sink_summary(results)
    buffered_summary = _build_buffered_summary(results)

    lines = [
        "# Agora ETL — Benchmark Matrix",
        "",
        "## Environment",
        "",
        "| | |",
        "| --- | --- |",
        f"| **Date** | {env['date']} |",
        f"| **OS** | {env['os']} |",
        f"| **CPU** | {env['cpu']} |",
        f"| **RAM** | {env['ram']} |",
        f"| **Python** | {env['python']} |",
        f"| **Repeat** | median of {repeat_count} isolated runs per scenario |",
        "",
        "## Source Summary",
        "",
        "This section isolates source read cost using `Direct + Null`.",
        "",
    ]
    lines.extend(
        _markdown_table(
            ["Source", "Median Time", "Median Rows/s", "Median MB/s", "Median Peak Py Heap"],
            [
                [row["source"], row["time"], row["rows_s"], row["mb_s"], row["peak"]]
                for row in source_summary
            ],
        )
    )
    lines.extend(
        [
            "",
            "## Sink Summary",
            "",
            "This section isolates sink cost using `Direct` scenarios. `Median vs Null` shows how much throughput each sink retains compared with the same-source `Null` baseline.",
            "",
        ]
    )
    lines.extend(
        _markdown_table(
            [
                "Sink",
                "Median Direct Rows/s",
                "Median Direct MB/s",
                "Median vs Null",
                "Median Peak Py Heap",
            ],
            [
                [row["sink"], row["rows_s"], row["mb_s"], row["retention"], row["peak"]]
                for row in sink_summary
            ],
        )
    )
    lines.extend(
        [
            "",
            "## Buffered Overhead",
            "",
            "This section isolates buffered runtime overhead using the `Null` sink.",
            "",
        ]
    )
    lines.extend(
        _markdown_table(
            [
                "Source",
                "Direct Null Rows/s",
                "Buffered Null Rows/s",
                "Buffered Retention",
                "Buffered In-Flight",
            ],
            [
                [
                    row["source"],
                    row["direct_rows_s"],
                    row["buffered_rows_s"],
                    row["retention"],
                    row["buffered_limit"],
                ]
                for row in buffered_summary
            ],
        )
    )
    lines.extend(
        [
            "",
            "## Full Matrix",
            "",
            f"Rows per scenario: `{rows_requested:,}`",
            "",
            "| Source | Middleware | Sink | Repeat | Median Time | Median Rows/s | Median MB/s | Median Peak Py Heap | Buffered |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for result in results:
        if result.status == "skipped":
            lines.append(
                f"| {result.source} | {result.middleware} | {result.sink} | {result.repeat_count} | — | SKIPPED | — | — | — |"
            )
            continue
        if result.status == "failed":
            detail = result.detail or ""
            lines.append(
                f"| {result.source} | {result.middleware} | {result.sink} | {result.repeat_count} | — | FAILED: {detail} | — | — | — |"
            )
            continue

        buffered = (
            f"{result.buffered_stage_max_in_flight}/{result.buffered_stage_limit}"
            if result.buffered_stage_limit > 0
            else "—"
        )
        lines.append(
            "| "
            + " | ".join(
                [
                    result.source,
                    result.middleware,
                    result.sink,
                    str(result.repeat_count),
                    "—" if result.elapsed_seconds is None else f"{result.elapsed_seconds:.2f}s",
                    "—" if result.throughput_rps is None else f"{result.throughput_rps:,.0f} r/s",
                    "—"
                    if result.throughput_mbps is None
                    else f"{result.throughput_mbps:,.1f} MB/s",
                    "—" if result.peak_py_heap_mb is None else f"{result.peak_py_heap_mb:.1f} MB",
                    buffered,
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Notes",
            "",
            f"- Each scenario reports the median of {repeat_count} isolated subprocess runs.",
            "- `MB/s` uses the generated input file size for each source, scaled by consumed rows.",
            "- Source Summary uses `Direct + Null` to isolate source read cost.",
            "- Sink Summary uses `Direct` scenarios and compares each sink to the same-source `Null` baseline.",
            "- Buffered Overhead uses the `Null` sink to isolate runtime coordination cost.",
            "- Peak Py Heap uses tracemalloc (Python heap only — excludes native memory from pyarrow, uvloop).",
            "- Use `--generate` to regenerate benchmark input data.",
        ]
    )
    return "\n".join(lines) + "\n"


def _generate_requested_data(rows: int) -> None:
    _ensure_data_dir()
    print(f"Generating benchmark data into {DATA_DIR}/\n")
    generate_csv(rows)
    generate_jsonl(rows)
    generate_parquet(rows)
    _write_dataset_manifest(rows)
    print()


async def _run_case_subprocess(
    rows: int,
    source_profile: Profile,
    middleware_profile: Profile,
    sink_profile: Profile,
) -> BenchmarkResult:
    """Run a single benchmark scenario in an isolated subprocess."""

    cmd = [
        sys.executable,
        __file__,
        "--_run-single",
        f"--rows={rows}",
        f"--source={source_profile.name}",
        f"--middleware={middleware_profile.name}",
        f"--sink={sink_profile.name}",
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        output = stdout.decode().strip()
        if not output:
            err = stderr.decode().strip()
            return BenchmarkResult(
                source=source_profile.label,
                middleware=middleware_profile.label,
                sink=sink_profile.label,
                status="failed",
                detail=err.splitlines()[-1] if err else "subprocess produced no output",
            )
        last_line = output.splitlines()[-1]
        data = json.loads(last_line)
        return BenchmarkResult(**data)
    except Exception as exc:
        return BenchmarkResult(
            source=source_profile.label,
            middleware=middleware_profile.label,
            sink=sink_profile.label,
            status="failed",
            detail=f"{type(exc).__name__}: {exc}",
        )


def _run_single(args: argparse.Namespace) -> None:
    """Entry point for subprocess isolation — runs one scenario and prints JSON result."""
    source_profiles = _build_source_profiles()
    sink_profiles = _build_sink_profiles()
    middleware_profiles = _build_middleware_profiles()

    source_profile = source_profiles[args.source]
    sink_profile = sink_profiles[args.sink]
    middleware_profile = middleware_profiles[args.middleware]

    async def _run():
        result = await _run_case(args.rows, source_profile, middleware_profile, sink_profile)
        from dataclasses import asdict

        print(json.dumps(asdict(result)))

    uvloop.install()
    asyncio.run(_run())


async def main(args: argparse.Namespace) -> None:
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, TextColumn

    if args.generate:
        _generate_requested_data(args.rows)

    env = _collect_env_info()

    source_profiles = list(_build_source_profiles().values())
    sink_profiles = list(_build_sink_profiles().values())
    middleware_profiles = list(_build_middleware_profiles().values())

    console = Console()
    results: list[BenchmarkResult] = []

    console.print(
        f"\n[bold]Agora ETL Benchmark Matrix[/bold] — [dim]{args.rows:,} rows per scenario[/dim]"
    )
    console.print(
        f"[dim]  {env['cpu']}  ·  {env['ram']}  ·  Python {env['python']}  ·  {env['os']}  ·  median of {args.repeat} runs[/dim]\n"
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
        disable=args.no_progress,
    ) as progress:
        for source_profile in source_profiles:
            for middleware_profile in middleware_profiles:
                for sink_profile in sink_profiles:
                    label = (
                        f"{source_profile.label}/{middleware_profile.label}/{sink_profile.label}"
                    )
                    task = progress.add_task(f"[cyan]{label}[/cyan]...", total=None)
                    repeat_results: list[BenchmarkResult] = []
                    for _ in range(args.repeat):
                        repeat_results.append(
                            await _run_case_subprocess(
                                args.rows,
                                source_profile,
                                middleware_profile,
                                sink_profile,
                            )
                        )
                    result = _aggregate_repeats(repeat_results)
                    results.append(result)
                    progress.remove_task(task)
                    if result.status == "ok":
                        console.print(
                            f"  [cyan]{label:<28}[/cyan] "
                            f"[green]{(result.throughput_rps or 0.0):>12,.0f} r/s[/green]  "
                            f"[cyan]{(result.throughput_mbps or 0.0):>8.1f} MB/s[/cyan]  "
                            f"[dim]{(result.elapsed_seconds or 0.0):.2f}s[/dim]"
                        )
                    else:
                        detail = f" ({result.detail})" if result.detail else ""
                        console.print(
                            f"  [cyan]{label:<28}[/cyan] "
                            f"[yellow]{result.status.upper()}[/yellow]{detail}"
                        )

    markdown = _render_markdown(results, args.rows, env)

    console.print()
    console.print(_build_rich_table(results, args.rows))
    console.print()
    console.print(
        f"[dim]- Each scenario reports the median of {args.repeat} isolated subprocess runs.[/dim]"
    )
    console.print(
        "[dim]- Sinks: Null=discard, JSONL/CSV/Parquet=write to disk, Stdout=redirected to /dev/null.[/dim]"
    )
    console.print("[dim]- MB/s uses generated input file size, scaled by consumed rows.[/dim]")
    console.print(
        "[dim]- Peak Py Heap: Python heap only (tracemalloc), excludes pyarrow/uvloop native memory.[/dim]"
    )
    console.print("[dim]- Buffered: max_in_flight/limit; — for non-buffered middleware.[/dim]")

    if args.markdown:
        MARKDOWN_REPORT_PATH.write_text(markdown, encoding="utf-8")
        console.print(f"\n[green]Saved markdown report:[/] {MARKDOWN_REPORT_PATH}")


if __name__ == "__main__":
    # Hidden flag for subprocess isolation — runs a single scenario and exits
    if "--_run-single" in sys.argv:
        uvloop.install()
        _parser = argparse.ArgumentParser(add_help=False)
        _parser.add_argument("--_run-single", action="store_true")
        _parser.add_argument("--rows", type=int, default=1_000_000)
        _parser.add_argument("--source", type=str)
        _parser.add_argument("--middleware", type=str)
        _parser.add_argument("--sink", type=str)
        _run_single(_parser.parse_args())
        sys.exit(0)

    uvloop.install()
    parser = argparse.ArgumentParser(description="Run Agora standalone benchmark matrix")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Rows to generate or run")
    parser.add_argument(
        "--generate",
        action="store_true",
        help="Generate benchmark input data before running the matrix.",
    )
    parser.add_argument(
        "--markdown",
        action="store_true",
        help="Export the result table to docs/benchmark/matrix.md.",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=3,
        help="Repeat each scenario N times and report the median.",
    )
    parser.add_argument("--no-progress", action="store_true", help="Disable progress spinners")
    cli_args = parser.parse_args()
    asyncio.run(main(cli_args))
