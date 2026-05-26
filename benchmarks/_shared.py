from __future__ import annotations

import csv
import importlib.util
import json
import os
import random
import socket
import statistics
import string
import subprocess
import sys
import time
import tracemalloc
import uuid
from contextlib import contextmanager, redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from contextlib import AbstractContextManager

from agora.core.middleware import Middleware
from agora.core.sink import BaseSink

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
DATA_DIR = Path(__file__).resolve().parent / "data"
MANIFEST_PATH = DATA_DIR / "manifest.json"
CORE_MARKDOWN_REPORT_PATH = PROJECT_ROOT / "docs" / "benchmark" / "core.md"
KAFKA_MARKDOWN_REPORT_PATH = PROJECT_ROOT / "docs" / "benchmark" / "kafka.md"
REDIS_MARKDOWN_REPORT_PATH = PROJECT_ROOT / "docs" / "benchmark" / "redis.md"
MB = 1024 * 1024

COMMON_REQUIRED_MODULES = {
    "uvloop": "uvloop>=0.21,<1",
}
CORE_REQUIRED_MODULES = {
    "pyarrow": "agora-etl[file]",
    "pyinstrument": "pyinstrument>=5.0,<6",
}
PLUGIN_REQUIRED_MODULES = {
    "agora_plugins.kafka": "agora-etl-plugins[redis,kafka]",
    "agora_plugins.redis": "agora-etl-plugins[redis,kafka]",
    "aiokafka": "agora-etl-plugins[redis,kafka]",
    "redis.asyncio": "agora-etl-plugins[redis,kafka]",
}

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


@dataclass(slots=True)
class PluginScenarioProfile:
    name: str
    label: str
    family: str
    description: str
    runner: Callable[[int], Awaitable[PluginBenchmarkResult]]


@dataclass(slots=True)
class PluginBenchmarkResult:
    family: str
    scenario: str
    status: str
    rows: int = 0
    records_written: int = 0
    elapsed_seconds: float | None = None
    payload_mb: float | None = None
    peak_py_heap_mb: float | None = None
    repeat_count: int = 1
    detail: str | None = None

    @property
    def throughput_rps(self) -> float | None:
        if self.elapsed_seconds is None or self.elapsed_seconds <= 0 or self.records_written <= 0:
            return None
        return self.records_written / self.elapsed_seconds

    @property
    def throughput_mbps(self) -> float | None:
        if self.elapsed_seconds is None or self.elapsed_seconds <= 0 or self.payload_mb is None:
            return None
        return self.payload_mb / self.elapsed_seconds


class SkipScenarioError(RuntimeError):
    """Signal that a plugin benchmark scenario should be skipped cleanly."""


class NullSink(BaseSink):
    batch_writable_native = True

    async def write(self, record) -> None:
        del record

    async def write_batch(self, records) -> None:
        del records


class CountSink(BaseSink[dict[str, Any]]):
    sink_name = "count"

    def __init__(self) -> None:
        self.records = 0

    async def write(self, record: dict[str, Any]) -> None:
        del record
        self.records += 1


class BufferedPassThroughMiddleware(Middleware[Any, Any]):
    name = "buffered_passthrough"

    def __init__(self, batch_size: int = 4) -> None:
        self.min_concurrency = batch_size
        self._batch_size = batch_size
        self._pending: list[tuple[Any, Any]] = []

    async def process(self, record: Any, ctx) -> Any | None:
        del ctx
        return record

    async def submit(self, record: Any, ctx):
        del ctx
        future = __import__("asyncio").get_running_loop().create_future()
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


def ensure_src_on_path() -> None:
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))


def validate_required_modules(
    required_modules: dict[str, str],
    *,
    install_message: str,
) -> None:
    missing = [
        f"{module} ({requirement})"
        for module, requirement in required_modules.items()
        if importlib.util.find_spec(module) is None
    ]
    if not missing:
        return

    missing_text = "\n".join(f"- {entry}" for entry in missing)
    raise SystemExit(
        f"Missing benchmark dependencies.\n{install_message}\nRequired modules:\n{missing_text}"
    )


def prepare_runtime(*, plugins: bool) -> None:
    validate_required_modules(
        COMMON_REQUIRED_MODULES,
        install_message=(
            "Install the benchmark runtime before running this script:\n"
            "  pip install 'agora-etl[benchmark]'"
        ),
    )
    if plugins:
        validate_required_modules(
            PLUGIN_REQUIRED_MODULES,
            install_message=(
                "Install the plugin extras before running plugin benchmarks:\n"
                "  pip install 'agora-etl-plugins[redis,kafka]'"
            ),
        )
    else:
        validate_required_modules(
            CORE_REQUIRED_MODULES,
            install_message=(
                "Install the core benchmark extra before running this script:\n"
                "  pip install 'agora-etl[benchmark]'\n"
                "Or from this repository:\n"
                "  ./.venv/bin/pip install -e '.[benchmark]'"
            ),
        )

    import uvloop

    uvloop.install()


def random_string(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))


def make_row(index: int) -> dict[str, object]:
    return {
        "id": index,
        "name": f"product_{random_string(6)}",
        "category": random.choice(CATEGORIES),
        "price": round(random.uniform(1.0, 999.99), 2),
        "quantity": random.randint(0, 1000),
        "status": random.choice(STATUSES),
        "score": round(random.uniform(0.0, 1.0), 4),
        "tags": f"{random_string(4)},{random_string(4)}",
        "description": f"A {random.choice(CATEGORIES)} item with id {index}",
        "created_at": f"2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
    }


def make_plugin_records(count: int, *, prefix: str) -> list[dict[str, Any]]:
    token = uuid.uuid4().hex[:8]
    return [
        {
            "id": index,
            "key": f"{prefix}-{token}-{index}",
            "status": "active" if index % 2 == 0 else "pending",
            "amount": round((index % 1000) * 1.17, 2),
            "payload": f"{prefix}-payload-{token}-{index}",
        }
        for index in range(count)
    ]


def payload_mb(records: list[dict[str, Any]]) -> float:
    total_bytes = sum(
        len(json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8"))
        for record in records
    )
    return total_bytes / MB


def generate_csv(rows: int) -> Path:
    path = DATA_DIR / "sample.csv"
    t0 = time.monotonic()
    with path.open("w", newline="", encoding="utf-8") as handle:
        fieldnames = list(make_row(0).keys())
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for index in range(rows):
            writer.writerow(make_row(index))
    print_generation_result("CSV", path, rows, time.monotonic() - t0)
    return path


def generate_jsonl(rows: int) -> Path:
    path = DATA_DIR / "sample.jsonl"
    t0 = time.monotonic()
    with path.open("w", encoding="utf-8") as handle:
        for index in range(rows):
            handle.write(json.dumps(make_row(index)) + "\n")
    print_generation_result("JSONL", path, rows, time.monotonic() - t0)
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
        batch = [make_row(index) for index in range(start, min(start + batch_size, rows))]
        table = pa.Table.from_pylist(batch)
        if writer is None:
            writer = pq.ParquetWriter(path, table.schema)
        writer.write_table(table)
    if writer is not None:
        writer.close()
    print_generation_result("Parquet", path, rows, time.monotonic() - t0)
    return path


def print_generation_result(kind: str, path: Path, rows: int, elapsed_seconds: float) -> None:
    size_mb = path.stat().st_size / MB
    print(f"  {kind:<8} {rows:>10,} rows  {size_mb:6.1f} MB  {elapsed_seconds:.1f}s")


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def write_dataset_manifest(rows: int) -> None:
    payload = {
        "rows": rows,
        "sources": {
            "csv": {"size_bytes": (DATA_DIR / "sample.csv").stat().st_size},
            "jsonl": {"size_bytes": (DATA_DIR / "sample.jsonl").stat().st_size},
            "parquet": {"size_bytes": (DATA_DIR / "sample.parquet").stat().st_size},
        },
    }
    MANIFEST_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_dataset_manifest() -> dict[str, Any] | None:
    if not MANIFEST_PATH.exists():
        return None
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def estimate_source_input_mb(source_name: str, rows_consumed: int) -> float | None:
    manifest = load_dataset_manifest()
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
def redirect_stdout_to_devnull():
    devnull = open(os.devnull, "w")  # noqa: SIM115
    try:
        with redirect_stdout(devnull):
            yield
    finally:
        devnull.close()


def remove_jsonl_output_file() -> None:
    path = DATA_DIR / "out.jsonl"
    if path.exists():
        path.unlink()


def remove_csv_output_file() -> None:
    path = DATA_DIR / "out.csv"
    if path.exists():
        path.unlink()


def remove_parquet_output_file() -> None:
    path = DATA_DIR / "out.parquet"
    if path.exists():
        path.unlink()


def median_or_none(values: list[float | int | None]) -> float | None:
    samples = [float(value) for value in values if value is not None]
    if not samples:
        return None
    return float(statistics.median(samples))


def format_rate(value: float | None, unit: str) -> str:
    if value is None:
        return "—"
    return f"{value:,.1f} {unit}"


def format_percent(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value * 100:.1f}%"


def markdown_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def run_text_command(*args: str) -> str | None:
    try:
        value = subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return None
    return value or None


def cpu_label() -> str:
    import platform

    system = platform.system()
    arch = (
        run_text_command("uname", "-m")
        or platform.machine()
        or platform.uname().machine
        or platform.processor()
        or "unknown"
    )
    brand: str | None = None

    if system == "Darwin":
        brand = run_text_command("sysctl", "-n", "machdep.cpu.brand_string")
    elif system == "Linux":
        cpuinfo = run_text_command("grep", "-m1", "model name", "/proc/cpuinfo")
        if cpuinfo and ":" in cpuinfo:
            brand = cpuinfo.split(":", 1)[1].strip()

    if brand:
        return f"{brand} ({arch})"
    return arch


def ram_label() -> str:
    import platform

    system = platform.system()
    ram_bytes: int | None = None

    if system == "Darwin":
        memsize = run_text_command("sysctl", "-n", "hw.memsize")
        if memsize and memsize.isdigit():
            ram_bytes = int(memsize)
    elif system == "Linux":
        try:
            with open("/proc/meminfo") as handle:
                line = next(ln for ln in handle if ln.startswith("MemTotal"))
            ram_bytes = int(line.split()[1]) * 1024
        except Exception:
            ram_bytes = None

    if ram_bytes is None:
        try:
            page_size = os.sysconf("SC_PAGE_SIZE")
            phys_pages = os.sysconf("SC_PHYS_PAGES")
            if page_size > 0 and phys_pages > 0:
                ram_bytes = int(page_size * phys_pages)
        except (AttributeError, OSError, ValueError):
            ram_bytes = None

    return f"{ram_bytes // (1024**3)} GB" if ram_bytes else "unknown"


def collect_env_info() -> dict[str, str]:
    import platform

    return {
        "cpu": cpu_label(),
        "ram": ram_label(),
        "python": sys.version.split()[0],
        "os": f"{platform.system()} {platform.release()}",
        "date": __import__("datetime").date.today().isoformat(),
    }


def collect_plugin_env_info() -> dict[str, str]:
    env = collect_env_info()
    env["kafka_bootstrap"] = os.getenv("AGORA_TEST_KAFKA_BOOTSTRAP", "127.0.0.1:19092")
    env["redis_url"] = os.getenv("AGORA_TEST_REDIS_URL", "redis://127.0.0.1:16379/0")
    return env


async def run_subprocess_json(cmd: list[str]) -> tuple[dict[str, Any] | None, str | None]:
    proc = await __import__("asyncio").create_subprocess_exec(
        *cmd,
        stdout=__import__("asyncio").subprocess.PIPE,
        stderr=__import__("asyncio").subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    output = stdout.decode().strip()
    if not output:
        err = stderr.decode().strip()
        return None, err.splitlines()[-1] if err else "subprocess produced no output"
    try:
        return json.loads(output.splitlines()[-1]), None
    except json.JSONDecodeError as exc:
        err = stderr.decode().strip()
        detail = err.splitlines()[-1] if err else f"JSONDecodeError: {exc}"
        return None, detail


def wait_for_tcp_endpoint(host: str, port: int, *, timeout_s: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError:
            time.sleep(0.25)
    raise SkipScenarioError(f"Service {host}:{port} is not reachable.")


def kafka_bootstrap() -> str:
    bootstrap = os.getenv("AGORA_TEST_KAFKA_BOOTSTRAP", "127.0.0.1:19092")
    for broker in bootstrap.split(","):
        host, port = broker.strip().rsplit(":", 1)
        wait_for_tcp_endpoint(host, int(port))
    return bootstrap


def redis_url() -> str:
    url = os.getenv("AGORA_TEST_REDIS_URL", "redis://127.0.0.1:16379/0")
    parsed = urlparse(url)
    wait_for_tcp_endpoint(parsed.hostname or "127.0.0.1", parsed.port or 6379)
    return url


async def run_plugin_with_measurement(
    family: str,
    scenario: str,
    records: list[dict[str, Any]],
    runner,
) -> PluginBenchmarkResult:
    payload = payload_mb(records)
    tracemalloc.start()
    t0 = time.monotonic()
    try:
        consumed, written = await runner()
    finally:
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    return PluginBenchmarkResult(
        family=family,
        scenario=scenario,
        status="ok",
        rows=consumed,
        records_written=written,
        elapsed_seconds=time.monotonic() - t0,
        payload_mb=payload,
        peak_py_heap_mb=peak / 1024 / 1024,
    )
