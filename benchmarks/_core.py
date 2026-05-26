from __future__ import annotations

import json
import sys
import tracemalloc
from contextlib import nullcontext
from dataclasses import asdict
from pathlib import Path

from _shared import (
    CORE_MARKDOWN_REPORT_PATH,
    DATA_DIR,
    BenchmarkResult,
    BufferedPassThroughMiddleware,
    NullSink,
    Profile,
    collect_env_info,
    ensure_data_dir,
    estimate_source_input_mb,
    format_percent,
    format_rate,
    generate_csv,
    generate_jsonl,
    generate_parquet,
    markdown_table,
    median_or_none,
    redirect_stdout_to_devnull,
    remove_csv_output_file,
    remove_jsonl_output_file,
    remove_parquet_output_file,
    run_subprocess_json,
    write_dataset_manifest,
)

from agora import MapMiddleware
from agora.core.pipeline import Pipeline
from agora.sinks.file.csv import CsvSink
from agora.sinks.file.jsonlines import JsonLinesSink
from agora.sinks.file.parquet import ParquetSink
from agora.sinks.io.stdout import StdoutSink
from agora.sources.file.csv import CsvSource
from agora.sources.file.jsonlines import JsonLinesSource


def build_source_profiles() -> dict[str, Profile]:
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
            path=path, row_mapper=lambda row: row, batch_size=1000, queue_maxsize=10
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
        "csv": Profile("csv", "CSV", "CsvSource over benchmark sample.csv.", _csv_source),
        "jsonl": Profile(
            "jsonl", "JSONL", "JsonLinesSource over benchmark sample.jsonl.", _jsonl_source
        ),
        "parquet": Profile(
            "parquet", "Parquet", "ParquetSource over benchmark sample.parquet.", _parquet_source
        ),
    }


def build_sink_profiles() -> dict[str, Profile]:
    return {
        "null": Profile("null", "Null", "Discards all output records.", lambda rows: NullSink()),
        "jsonl": Profile(
            "jsonl",
            "JSONL",
            "Writes JSONL output to benchmarks/data/out.jsonl.",
            lambda rows: JsonLinesSink(path=DATA_DIR / "out.jsonl", append=False, flush_every=5000),
            cleanup=remove_jsonl_output_file,
        ),
        "csv": Profile(
            "csv",
            "CSV",
            "Writes CSV output to benchmarks/data/out.csv.",
            lambda rows: CsvSink(
                path=DATA_DIR / "out.csv", row_mapper=lambda record: record, flush_every=5000
            ),
            cleanup=remove_csv_output_file,
        ),
        "parquet": Profile(
            "parquet",
            "Parquet",
            "Writes Parquet output to benchmarks/data/out.parquet.",
            lambda rows: ParquetSink(
                path=DATA_DIR / "out.parquet", row_mapper=lambda record: record, batch_size=5000
            ),
            cleanup=remove_parquet_output_file,
        ),
        "stdout": Profile(
            "stdout",
            "Stdout",
            "StdoutSink redirected to /dev/null.",
            lambda rows: StdoutSink(formatter=lambda record: ""),
            run_context_factory=redirect_stdout_to_devnull,
        ),
    }


def build_middleware_profiles() -> dict[str, Profile]:
    return {
        "direct": Profile("direct", "Direct", "No middleware.", lambda rows: None),
        "map": Profile(
            "map",
            "Map",
            "Identity MapMiddleware on the linear path.",
            lambda rows: MapMiddleware(lambda record: record, name="identity_map"),
        ),
        "buffered": Profile(
            "buffered",
            "Buffered",
            "Buffered pass-through middleware for concurrent execution.",
            lambda rows: BufferedPassThroughMiddleware(batch_size=4),
        ),
    }


async def run_core_case(
    rows: int,
    source_profile: Profile,
    middleware_profile: Profile,
    sink_profile: Profile,
) -> BenchmarkResult:
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
    t0 = __import__("time").monotonic()
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
    source_input_mb = estimate_source_input_mb(source_profile.name, int(summary.records_consumed))
    return BenchmarkResult(
        source=source_profile.label,
        middleware=middleware_profile.label,
        sink=sink_profile.label,
        status="ok",
        rows=int(summary.records_consumed),
        records_written=int(summary.records_written),
        elapsed_seconds=__import__("time").monotonic() - t0,
        peak_py_heap_mb=peak / 1024 / 1024,
        source_input_mb=source_input_mb,
        writer_flush_count=runtime.writer_flush_count,
        checkpoint_save_count=runtime.checkpoint_save_count,
        buffered_stage_limit=runtime.buffered_stage_limit,
        buffered_stage_max_in_flight=runtime.buffered_stage_max_in_flight,
    )


def aggregate_core_repeats(results: list[BenchmarkResult]) -> BenchmarkResult:
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
        rows=int(__import__("statistics").median(result.rows for result in results)),
        records_written=int(
            __import__("statistics").median(result.records_written for result in results)
        ),
        elapsed_seconds=median_or_none([result.elapsed_seconds for result in results]),
        peak_py_heap_mb=median_or_none([result.peak_py_heap_mb for result in results]),
        source_input_mb=median_or_none([result.source_input_mb for result in results]),
        writer_flush_count=int(
            __import__("statistics").median(result.writer_flush_count for result in results)
        ),
        checkpoint_save_count=int(
            __import__("statistics").median(result.checkpoint_save_count for result in results)
        ),
        buffered_stage_limit=int(
            __import__("statistics").median(result.buffered_stage_limit for result in results)
        ),
        buffered_stage_max_in_flight=int(
            __import__("statistics").median(
                result.buffered_stage_max_in_flight for result in results
            )
        ),
        repeat_count=len(results),
    )


def result_lookup(results: list[BenchmarkResult]) -> dict[tuple[str, str, str], BenchmarkResult]:
    return {(result.source, result.middleware, result.sink): result for result in results}


def build_source_summary(results: list[BenchmarkResult]) -> list[dict[str, str]]:
    lookup = result_lookup(results)
    rows: list[dict[str, str]] = []
    for source in ("CSV", "JSONL", "Parquet"):
        result = lookup.get((source, "Direct", "Null"))
        if result is None:
            continue
        rows.append(
            {
                "source": source,
                "time": "—" if result.elapsed_seconds is None else f"{result.elapsed_seconds:.2f}s",
                "rows_s": format_rate(result.throughput_rps, "r/s"),
                "mb_s": format_rate(result.throughput_mbps, "MB/s"),
                "peak": "—"
                if result.peak_py_heap_mb is None
                else f"{result.peak_py_heap_mb:.1f} MB",
            }
        )
    return rows


def build_sink_summary(results: list[BenchmarkResult]) -> list[dict[str, str]]:
    lookup = result_lookup(results)
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
                "rows_s": format_rate(
                    median_or_none([result.throughput_rps for result in direct_results]), "r/s"
                ),
                "mb_s": format_rate(
                    median_or_none([result.throughput_mbps for result in direct_results]), "MB/s"
                ),
                "retention": format_percent(median_or_none(retention_samples)),
                "peak": format_rate(
                    median_or_none([result.peak_py_heap_mb for result in direct_results]), "MB"
                ),
            }
        )
    return rows


def build_buffered_summary(results: list[BenchmarkResult]) -> list[dict[str, str]]:
    lookup = result_lookup(results)
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
                "direct_rows_s": format_rate(direct.throughput_rps, "r/s"),
                "buffered_rows_s": format_rate(buffered.throughput_rps, "r/s"),
                "retention": format_percent(retention),
                "buffered_limit": (
                    f"{buffered.buffered_stage_max_in_flight}/{buffered.buffered_stage_limit}"
                    if buffered.buffered_stage_limit > 0
                    else "—"
                ),
            }
        )
    return rows


def build_rich_table(results: list[BenchmarkResult], rows_requested: int):
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


def render_core_markdown(
    results: list[BenchmarkResult], rows_requested: int, env: dict[str, str]
) -> str:
    repeat_count = results[0].repeat_count if results else 1
    source_summary = build_source_summary(results)
    sink_summary = build_sink_summary(results)
    buffered_summary = build_buffered_summary(results)

    lines = [
        "# Agora ETL — Benchmark Matrix",
        "",
        "[Kafka Benchmark](kafka.md) | [Redis Benchmark](redis.md)",
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
        markdown_table(
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
        markdown_table(
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
        markdown_table(
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
            "`Peak Py Heap` reflects Python heap only. It does not include native memory from components such as `pyarrow` or `uvloop`.",
        ]
    )
    return "\n".join(lines) + "\n"


def generate_requested_data(rows: int) -> None:
    ensure_data_dir()
    print(f"Generating benchmark data into {DATA_DIR}/\n")
    generate_csv(rows)
    generate_jsonl(rows)
    generate_parquet(rows)
    write_dataset_manifest(rows)
    print()


async def run_core_case_subprocess(
    rows: int, source_profile: Profile, middleware_profile: Profile, sink_profile: Profile
) -> BenchmarkResult:
    data, detail = await run_subprocess_json(
        [
            sys.executable,
            str(Path(__file__).with_name("run.py")),
            "--_run-single",
            "--lane=core",
            f"--rows={rows}",
            f"--source={source_profile.name}",
            f"--middleware={middleware_profile.name}",
            f"--sink={sink_profile.name}",
        ]
    )
    if data is not None:
        return BenchmarkResult(**data)
    return BenchmarkResult(
        source=source_profile.label,
        middleware=middleware_profile.label,
        sink=sink_profile.label,
        status="failed",
        detail=detail,
    )


def run_single_core(args) -> None:
    source_profile = build_source_profiles()[args.source]
    sink_profile = build_sink_profiles()[args.sink]
    middleware_profile = build_middleware_profiles()[args.middleware]

    async def _run() -> None:
        result = await run_core_case(args.rows, source_profile, middleware_profile, sink_profile)
        print(json.dumps(asdict(result)))

    __import__("asyncio").run(_run())


async def run_core_benchmarks(args) -> None:
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, TextColumn

    if args.generate:
        generate_requested_data(args.rows)

    env = collect_env_info()
    source_profiles = list(build_source_profiles().values())
    sink_profiles = list(build_sink_profiles().values())
    middleware_profiles = list(build_middleware_profiles().values())

    only = getattr(args, "only", None)
    if only and only in ("csv", "jsonl", "parquet"):
        source_profiles = [p for p in source_profiles if p.name == only]

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
                            await run_core_case_subprocess(
                                args.rows, source_profile, middleware_profile, sink_profile
                            )
                        )
                    result = aggregate_core_repeats(repeat_results)
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
                            f"  [cyan]{label:<28}[/cyan] [yellow]{result.status.upper()}[/yellow]{detail}"
                        )

    console.print()
    console.print(build_rich_table(results, args.rows))

    if args.markdown:
        CORE_MARKDOWN_REPORT_PATH.write_text(
            render_core_markdown(results, args.rows, env), encoding="utf-8"
        )
        console.print(f"\n[green]Saved markdown report:[/] {CORE_MARKDOWN_REPORT_PATH}")
