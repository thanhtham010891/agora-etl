from __future__ import annotations

import asyncio
import json
import sys
import time
import uuid
from contextlib import suppress
from dataclasses import asdict
from pathlib import Path

import orjson
from _shared import (
    KAFKA_MARKDOWN_REPORT_PATH,
    REDIS_MARKDOWN_REPORT_PATH,
    CountSink,
    PluginBenchmarkResult,
    PluginScenarioProfile,
    SkipScenarioError,
    collect_plugin_env_info,
    kafka_bootstrap,
    make_plugin_records,
    median_or_none,
    redis_url,
    run_plugin_with_measurement,
    run_subprocess_json,
)

from agora import IterableSource, Pipeline


class _RoundRobinPartitioner:
    """Simple round-robin partitioner — avoids Random.choice overhead per record."""

    def __init__(self) -> None:
        self._counter = 0

    def __call__(self, key, all_partitions, available):
        partitions = available if available else all_partitions
        idx = self._counter % len(partitions)
        self._counter += 1
        return partitions[idx]


SCENARIO_TIMEOUT_SECONDS = 60.0
KAFKA_READY_TIMEOUT_SECONDS = 60.0
PLUGIN_BATCH_SIZE = 500
KAFKA_MAX_PENDING_ACKS = 500
KAFKA_COMMIT_EVERY = 500
REDIS_BATCH_SIZE = 5000
REDIS_STREAM_BATCH_SIZE = 2000
REDIS_STREAM_ACK_BATCH_SIZE = 2000
REDIS_STREAM_BLOCK_MS = 10


async def ensure_kafka_topic_exists(bootstrap_servers: str, topic: str) -> None:
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    deadline = time.monotonic() + KAFKA_READY_TIMEOUT_SECONDS
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
        try:
            await asyncio.wait_for(admin.start(), timeout=10.0)
            try:
                await asyncio.wait_for(
                    admin.create_topics(
                        [NewTopic(name=topic, num_partitions=3, replication_factor=1)]
                    ),
                    timeout=10.0,
                )
            except TopicAlreadyExistsError:
                return
            return
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(1.0)
        finally:
            with suppress(Exception):
                await admin.close()

    detail = f"{type(last_error).__name__}: {last_error}" if last_error else "broker not ready"
    raise SkipScenarioError(f"Kafka broker is reachable but not ready yet ({detail}).")


async def run_kafka_produce(records_count: int) -> PluginBenchmarkResult:
    from agora_plugins.kafka import KafkaSink

    bootstrap = kafka_bootstrap()
    topic = f"agora-bench-kafka-produce-{uuid.uuid4().hex[:8]}"
    records = make_plugin_records(records_count, prefix="kafka-produce")

    await ensure_kafka_topic_exists(bootstrap, topic)

    async def _runner():
        summary = (
            await Pipeline(IterableSource(records))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=bootstrap,
                    serializer=lambda record: orjson.dumps(record),
                    max_pending_acks=KAFKA_MAX_PENDING_ACKS,
                    compression_type=None,
                    linger_ms=0,
                    acks=1,
                    enable_idempotence=False,
                    max_batch_size=1048576,
                    partitioner=_RoundRobinPartitioner(),
                ),
                batch_size=PLUGIN_BATCH_SIZE,
            )
            .run()
        )
        return int(summary.records_consumed), int(summary.records_written)

    return await run_plugin_with_measurement("Kafka", "Produce", records, _runner)


async def run_kafka_roundtrip(records_count: int) -> PluginBenchmarkResult:
    from agora_plugins.kafka import KafkaSink, KafkaSource

    bootstrap = kafka_bootstrap()
    suffix = uuid.uuid4().hex[:8]
    topic = f"agora-bench-kafka-roundtrip-{suffix}"
    group_id = f"agora-bench-kafka-group-{suffix}"
    records = make_plugin_records(records_count, prefix="kafka-rt")

    await ensure_kafka_topic_exists(bootstrap, topic)

    async def _runner():
        produce_summary = (
            await Pipeline(IterableSource(records))
            .build(
                KafkaSink(
                    topic=topic,
                    bootstrap_servers=bootstrap,
                    serializer=lambda record: orjson.dumps(record),
                    max_pending_acks=KAFKA_MAX_PENDING_ACKS,
                    compression_type=None,
                    linger_ms=0,
                    acks=1,
                    enable_idempotence=False,
                    max_batch_size=1048576,
                    partitioner=_RoundRobinPartitioner(),
                ),
                batch_size=PLUGIN_BATCH_SIZE,
            )
            .run()
        )
        await asyncio.sleep(0.25)
        sink = CountSink()
        consume_summary = (
            await Pipeline(
                KafkaSource(
                    topics=[topic],
                    bootstrap_servers=bootstrap,
                    group_id=group_id,
                    deserializer=lambda value: json.loads(value.decode("utf-8")),
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    commit_every=max(1, min(records_count, KAFKA_COMMIT_EVERY)),
                    max_poll_records=PLUGIN_BATCH_SIZE,
                    fetch_max_wait_ms=10,
                    poll_timeout_ms=50,
                )
            )
            .build(sink, batch_size=PLUGIN_BATCH_SIZE)
            .run(max_records=records_count)
        )
        return int(consume_summary.records_consumed), int(produce_summary.records_written)

    return await run_plugin_with_measurement("Kafka", "Roundtrip", records, _runner)


async def run_redis_set(records_count: int) -> PluginBenchmarkResult:
    import redis.asyncio as aioredis
    from agora_plugins.redis import RedisSink

    url = redis_url()
    prefix = f"agora:bench:redis:set:{uuid.uuid4().hex[:8]}"
    records = make_plugin_records(records_count, prefix="redis-set")
    keys = [f"{prefix}:{record['id']}" for record in records]

    async def _runner():
        summary = (
            await Pipeline(IterableSource(records))
            .build(
                RedisSink(
                    url=url,
                    mode="set",
                    key_fn=lambda record: f"{prefix}:{record['id']}",
                    serializer=lambda record: orjson.dumps(record),
                ),
                batch_size=REDIS_BATCH_SIZE,
            )
            .run()
        )
        return int(summary.records_consumed), int(summary.records_written)

    try:
        return await run_plugin_with_measurement("Redis", "SET", records, _runner)
    finally:
        client = aioredis.from_url(url, decode_responses=True)
        try:
            if keys:
                await client.delete(*keys)
        finally:
            await client.aclose()


async def _seed_redis_stream(url: str, stream: str, records: list[dict[str, object]]) -> None:
    import redis.asyncio as aioredis

    client = aioredis.from_url(url, decode_responses=False)
    try:
        for offset in range(0, len(records), REDIS_BATCH_SIZE):
            chunk = records[offset : offset + REDIS_BATCH_SIZE]
            async with client.pipeline(transaction=False) as pipe:
                for record in chunk:
                    pipe.xadd(stream, {"payload": orjson.dumps(record)})
                await pipe.execute()
    finally:
        await client.aclose()


async def run_redis_xadd(records_count: int) -> PluginBenchmarkResult:
    import redis.asyncio as aioredis
    from agora_plugins.redis import RedisSink

    url = redis_url()
    stream = f"agora:bench:redis:xadd:{uuid.uuid4().hex[:8]}"
    records = make_plugin_records(records_count, prefix="redis-xadd")

    async def _runner():
        summary = (
            await Pipeline(IterableSource(records))
            .build(
                RedisSink(
                    url=url,
                    mode="xadd",
                    key_fn=lambda record: stream,
                    serializer=lambda record: {"payload": orjson.dumps(record)},
                ),
                batch_size=REDIS_BATCH_SIZE,
            )
            .run()
        )
        return int(summary.records_consumed), int(summary.records_written)

    try:
        return await run_plugin_with_measurement("Redis", "XADD", records, _runner)
    finally:
        client = aioredis.from_url(url, decode_responses=True)
        try:
            await client.delete(stream)
        finally:
            await client.aclose()


async def run_redis_xreadgroup(records_count: int) -> PluginBenchmarkResult:
    import redis.asyncio as aioredis
    from agora_plugins.redis import RedisStreamSource

    url = redis_url()
    suffix = uuid.uuid4().hex[:8]
    stream = f"agora:bench:redis:xread:{suffix}"
    group = f"agora-bench-group-{suffix}"
    consumer = f"agora-bench-consumer-{suffix}"
    records = make_plugin_records(records_count, prefix="redis-xread")

    await _seed_redis_stream(url, stream, records)

    async def _runner():
        sink = CountSink()
        consume_summary = (
            await Pipeline(
                RedisStreamSource(
                    url=url,
                    stream=stream,
                    group=group,
                    consumer=consumer,
                    deserializer=lambda fields: orjson.loads(fields[b"payload"]),
                    block_ms=REDIS_STREAM_BLOCK_MS,
                    batch_size=REDIS_STREAM_BATCH_SIZE,
                    ack_on_success=True,
                    ack_batch_size=REDIS_STREAM_ACK_BATCH_SIZE,
                    decode_responses=False,
                )
            )
            .build(sink, batch_size=PLUGIN_BATCH_SIZE)
            .run(max_records=records_count)
        )
        return int(consume_summary.records_consumed), int(consume_summary.records_consumed)

    try:
        return await run_plugin_with_measurement("Redis", "XREADGROUP", records, _runner)
    finally:
        client = aioredis.from_url(url, decode_responses=True)
        try:
            await client.delete(stream)
        finally:
            await client.aclose()


async def run_redis_stream_roundtrip(records_count: int) -> PluginBenchmarkResult:
    import redis.asyncio as aioredis
    from agora_plugins.redis import RedisSink, RedisStreamSource

    url = redis_url()
    suffix = uuid.uuid4().hex[:8]
    stream = f"agora:bench:redis:stream:{suffix}"
    group = f"agora-bench-group-{suffix}"
    consumer = f"agora-bench-consumer-{suffix}"
    records = make_plugin_records(records_count, prefix="redis-stream")

    async def _runner():
        produce_summary = (
            await Pipeline(IterableSource(records))
            .build(
                RedisSink(
                    url=url,
                    mode="xadd",
                    key_fn=lambda record: stream,
                    serializer=lambda record: {"payload": orjson.dumps(record)},
                ),
                batch_size=REDIS_BATCH_SIZE,
            )
            .run()
        )

        sink = CountSink()
        consume_summary = (
            await Pipeline(
                RedisStreamSource(
                    url=url,
                    stream=stream,
                    group=group,
                    consumer=consumer,
                    deserializer=lambda fields: orjson.loads(fields[b"payload"]),
                    block_ms=REDIS_STREAM_BLOCK_MS,
                    batch_size=REDIS_STREAM_BATCH_SIZE,
                    ack_on_success=True,
                    ack_batch_size=REDIS_STREAM_ACK_BATCH_SIZE,
                    decode_responses=False,
                )
            )
            .build(sink, batch_size=PLUGIN_BATCH_SIZE)
            .run(max_records=records_count)
        )

        return int(consume_summary.records_consumed), int(produce_summary.records_written)

    try:
        return await run_plugin_with_measurement("Redis", "Stream Roundtrip", records, _runner)
    finally:
        client = aioredis.from_url(url, decode_responses=True)
        try:
            await client.delete(stream)
        finally:
            await client.aclose()


def build_plugin_scenarios() -> dict[str, PluginScenarioProfile]:
    return {
        "kafka_produce": PluginScenarioProfile(
            "kafka_produce",
            "Kafka / Produce",
            "Kafka",
            "IterableSource -> KafkaSink producer path.",
            run_kafka_produce,
        ),
        "kafka_roundtrip": PluginScenarioProfile(
            "kafka_roundtrip",
            "Kafka / Roundtrip",
            "Kafka",
            "KafkaSink produce + KafkaSource consume on one topic.",
            run_kafka_roundtrip,
        ),
        "redis_set": PluginScenarioProfile(
            "redis_set",
            "Redis / SET",
            "Redis",
            "IterableSource -> RedisSink(mode='set').",
            run_redis_set,
        ),
        "redis_xadd": PluginScenarioProfile(
            "redis_xadd",
            "Redis / XADD",
            "Redis",
            "IterableSource -> RedisSink(mode='xadd').",
            run_redis_xadd,
        ),
        "redis_xreadgroup": PluginScenarioProfile(
            "redis_xreadgroup",
            "Redis / XREADGROUP",
            "Redis",
            "Pre-seeded stream -> RedisStreamSource consumer path.",
            run_redis_xreadgroup,
        ),
        "redis_stream_roundtrip": PluginScenarioProfile(
            "redis_stream_roundtrip",
            "Redis / Stream RT",
            "Redis",
            "RedisSink(mode='xadd') produce + RedisStreamSource consume.",
            run_redis_stream_roundtrip,
        ),
    }


def aggregate_plugin_repeats(results: list[PluginBenchmarkResult]) -> PluginBenchmarkResult:
    first = results[0]
    if all(result.status == "skipped" for result in results):
        return PluginBenchmarkResult(
            first.family, first.scenario, "skipped", repeat_count=len(results), detail=first.detail
        )

    failed = [result for result in results if result.status != "ok"]
    if failed:
        detail = failed[0].detail or "repeat failed"
        if len(results) > 1:
            detail = f"{len(failed)}/{len(results)} repeats failed; {detail}"
        return PluginBenchmarkResult(
            first.family, first.scenario, "failed", repeat_count=len(results), detail=detail
        )

    return PluginBenchmarkResult(
        family=first.family,
        scenario=first.scenario,
        status="ok",
        rows=int(median_or_none([result.rows for result in results]) or 0),
        records_written=int(median_or_none([result.records_written for result in results]) or 0),
        elapsed_seconds=median_or_none([result.elapsed_seconds for result in results]),
        payload_mb=median_or_none([result.payload_mb for result in results]),
        peak_py_heap_mb=median_or_none([result.peak_py_heap_mb for result in results]),
        repeat_count=len(results),
    )


def render_plugin_markdown(
    family: str,
    results: list[PluginBenchmarkResult],
    rows_requested: int,
    env: dict[str, str],
) -> str:
    repeat_count = results[0].repeat_count if results else 1
    counterpart_link = "redis.md" if family == "Kafka" else "kafka.md"
    service_label = env["kafka_bootstrap"] if family == "Kafka" else env["redis_url"]
    service_name = "Kafka" if family == "Kafka" else "Redis"
    settings_rows = [
        f"| **Pipeline batch size** | {PLUGIN_BATCH_SIZE} |",
    ]
    if family == "Kafka":
        settings_rows.extend(
            [
                f"| **Kafka max pending acks** | {KAFKA_MAX_PENDING_ACKS} |",
                f"| **Kafka commit every** | {KAFKA_COMMIT_EVERY} |",
            ]
        )
        scenario_rows = [
            "| `Kafka / Produce` | Measures producer throughput only. |",
            "| `Kafka / Roundtrip` | Measures produce and consume together on one topic. |",
        ]
        notes = [
            "- `Produce` isolates write-side throughput.",
            "- `Roundtrip` includes producer, consumer, commit, and broker coordination cost.",
        ]
    else:
        settings_rows.extend(
            [
                f"| **Redis sink batch size** | {REDIS_BATCH_SIZE} |",
                f"| **Redis stream batch size** | {REDIS_STREAM_BATCH_SIZE} |",
                f"| **Redis stream ack batch size** | {REDIS_STREAM_ACK_BATCH_SIZE} |",
                f"| **Redis stream block ms** | {REDIS_STREAM_BLOCK_MS} |",
            ]
        )
        scenario_rows = [
            "| `Redis / SET` | Measures key-value writes through `SET`/`MSET`. |",
            "| `Redis / XADD` | Measures stream write throughput only. |",
            "| `Redis / XREADGROUP` | Measures consumer-group read throughput on a pre-seeded stream. |",
            "| `Redis / Stream RT` | Measures `XADD` and `XREADGROUP` together in one end-to-end pass. |",
        ]
        notes = [
            "- `SET` and `XADD` isolate write-side cost.",
            "- `XREADGROUP` isolates consumer-group read cost.",
            "- `Stream RT` captures the combined cost of stream writes and consumer-group reads.",
        ]
    lines = [
        f"# Agora ETL — {family} Benchmark",
        "",
        f"[Core Benchmark](core.md) | [{'Redis' if family == 'Kafka' else 'Kafka'} Benchmark]({counterpart_link})",
        "",
        f"This page records the current {family.lower()} benchmark snapshot for Agora ETL.",
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
        f"| **{service_name}** | {service_label} |",
        f"| **Repeat** | median of {repeat_count} isolated runs per scenario |",
        *settings_rows,
        "",
        "## Scenarios",
        "",
        "| Scenario | Purpose |",
        "| --- | --- |",
        *scenario_rows,
        "",
        "## Results",
        "",
        f"Rows per scenario: `{rows_requested:,}`",
        "",
    ]
    if results:
        lines.extend(
            [
                "| Scenario | Repeat | Median Time | Median Rows/s | Median MB/s | Median Peak Py Heap |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for result in results:
            if result.status == "skipped":
                lines.append(f"| {result.scenario} | {result.repeat_count} | — | SKIPPED | — | — |")
                continue
            if result.status == "failed":
                detail = result.detail or ""
                lines.append(
                    f"| {result.scenario} | {result.repeat_count} | — | FAILED: {detail} | — | — |"
                )
                continue
            lines.append(
                "| "
                + " | ".join(
                    [
                        result.scenario,
                        str(result.repeat_count),
                        "—" if result.elapsed_seconds is None else f"{result.elapsed_seconds:.2f}s",
                        "—"
                        if result.throughput_rps is None
                        else f"{result.throughput_rps:,.0f} r/s",
                        "—"
                        if result.throughput_mbps is None
                        else f"{result.throughput_mbps:,.1f} MB/s",
                        "—"
                        if result.peak_py_heap_mb is None
                        else f"{result.peak_py_heap_mb:.1f} MB",
                    ]
                )
                + " |"
            )
    else:
        lines.append("No benchmark results are included in this snapshot.")
    lines.extend(
        [
            "",
            "## Reading the results",
            "",
            *notes,
            "",
            "`Peak Py Heap` reflects Python heap only. It does not include broker/server memory or native allocations.",
        ]
    )
    return "\n".join(lines) + "\n"


async def run_plugin_scenario_subprocess(
    rows: int, scenario: PluginScenarioProfile
) -> PluginBenchmarkResult:
    try:
        data, detail = await asyncio.wait_for(
            run_subprocess_json(
                [
                    sys.executable,
                    str(Path(__file__).with_name("run.py")),
                    "--_run-single",
                    "--lane=plugins",
                    f"--rows={rows}",
                    f"--scenario={scenario.name}",
                ]
            ),
            timeout=SCENARIO_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        return PluginBenchmarkResult(
            family=scenario.family,
            scenario=scenario.label,
            status="failed",
            detail=f"timed out after {SCENARIO_TIMEOUT_SECONDS:.0f}s",
        )
    if data is not None:
        return PluginBenchmarkResult(**data)
    return PluginBenchmarkResult(
        family=scenario.family, scenario=scenario.label, status="failed", detail=detail
    )


def run_single_plugin(args) -> None:
    scenario = build_plugin_scenarios()[args.scenario]

    async def _run() -> None:
        try:
            result = await scenario.runner(args.rows)
        except SkipScenarioError as exc:
            result = PluginBenchmarkResult(
                family=scenario.family, scenario=scenario.label, status="skipped", detail=str(exc)
            )
        except Exception as exc:
            result = PluginBenchmarkResult(
                family=scenario.family,
                scenario=scenario.label,
                status="failed",
                detail=f"{type(exc).__name__}: {exc}",
            )
        print(json.dumps(asdict(result)))

    asyncio.run(_run())


async def run_plugin_benchmarks(args) -> None:
    from rich.console import Console
    from rich.table import Table

    env = collect_plugin_env_info()
    scenarios = list(build_plugin_scenarios().values())
    only = getattr(args, "only", None)
    if only:
        scenarios = [s for s in scenarios if s.family.lower() == only.lower()]
    console = Console()
    results: list[PluginBenchmarkResult] = []

    if args.generate:
        console.print("[dim]Ignoring --generate for plugin benchmarks.[/dim]\n")

    console.print(
        f"\n[bold]Agora ETL Plugin Benchmarks[/bold] — [dim]{args.rows:,} rows per scenario[/dim]"
    )
    console.print(
        f"[dim]  Kafka {env['kafka_bootstrap']}  ·  Redis {env['redis_url']}  ·  Python {env['python']}  ·  {env['os']}  ·  median of {args.repeat} runs[/dim]\n"
    )
    console.print(
        f"[dim]  batch_size={PLUGIN_BATCH_SIZE}  ·  kafka_max_pending_acks={KAFKA_MAX_PENDING_ACKS}  ·  kafka_commit_every={KAFKA_COMMIT_EVERY}  ·  redis_batch_size={REDIS_BATCH_SIZE}  ·  redis_stream_batch_size={REDIS_STREAM_BATCH_SIZE}  ·  redis_stream_ack_batch_size={REDIS_STREAM_ACK_BATCH_SIZE}  ·  redis_stream_block_ms={REDIS_STREAM_BLOCK_MS}[/dim]\n"
    )

    for scenario in scenarios:
        console.print(f"[dim]Running {scenario.label}...[/dim]")
        repeat_results: list[PluginBenchmarkResult] = []
        for _ in range(args.repeat):
            repeat_results.append(await run_plugin_scenario_subprocess(args.rows, scenario))
        result = aggregate_plugin_repeats(repeat_results)
        result.scenario = scenario.label
        results.append(result)
        if result.status == "ok":
            console.print(
                f"  [cyan]{scenario.label:<24}[/cyan] "
                f"[green]{(result.throughput_rps or 0.0):>12,.0f} r/s[/green]  "
                f"[cyan]{(result.throughput_mbps or 0.0):>8.1f} MB/s[/cyan]  "
                f"[dim]{(result.elapsed_seconds or 0.0):.2f}s[/dim]"
            )
        else:
            detail = f" ({result.detail})" if result.detail else ""
            console.print(
                f"  [cyan]{scenario.label:<24}[/cyan] [yellow]{result.status.upper()}[/yellow]{detail}"
            )

    table = Table(title=f"Agora ETL Plugin Benchmarks ({args.rows:,} rows)")
    table.add_column("Family", style="bold")
    table.add_column("Scenario")
    table.add_column("Repeat", justify="right")
    table.add_column("Time", justify="right")
    table.add_column("Rows/s", justify="right", style="bold green")
    table.add_column("MB/s", justify="right", style="bold cyan")
    table.add_column("Peak Py Heap", justify="right", style="dim")
    for result in results:
        if result.status == "ok":
            table.add_row(
                result.family,
                result.scenario,
                str(result.repeat_count),
                "—" if result.elapsed_seconds is None else f"{result.elapsed_seconds:.2f}s",
                "—" if result.throughput_rps is None else f"{result.throughput_rps:,.0f} r/s",
                "—" if result.throughput_mbps is None else f"{result.throughput_mbps:,.1f} MB/s",
                "—" if result.peak_py_heap_mb is None else f"{result.peak_py_heap_mb:.1f} MB",
            )
        elif result.status == "skipped":
            table.add_row(
                result.family, result.scenario, str(result.repeat_count), "—", "SKIPPED", "—", "—"
            )
        else:
            table.add_row(
                result.family, result.scenario, str(result.repeat_count), "—", "FAILED", "—", "—"
            )
    console.print()
    console.print(table)

    if args.markdown:
        kafka_results = [result for result in results if result.family == "Kafka"]
        redis_results = [result for result in results if result.family == "Redis"]
        KAFKA_MARKDOWN_REPORT_PATH.write_text(
            render_plugin_markdown("Kafka", kafka_results, args.rows, env), encoding="utf-8"
        )
        REDIS_MARKDOWN_REPORT_PATH.write_text(
            render_plugin_markdown("Redis", redis_results, args.rows, env), encoding="utf-8"
        )
        console.print(f"\n[green]Saved markdown report:[/] {KAFKA_MARKDOWN_REPORT_PATH}")
        console.print(f"[green]Saved markdown report:[/] {REDIS_MARKDOWN_REPORT_PATH}")
