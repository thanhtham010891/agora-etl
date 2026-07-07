"""
agora/cli/commands/new.py
=========================
``agora new <project-name> [--preset <preset>]`` — scaffold a new agora project.

Presets
-------
    file-etl              CSV/JSONL file ingestion pipeline (default)
    kafka-consumer        Kafka → sink consumer pipeline
    postgres-incremental  PostgreSQL incremental extraction pipeline

Usage::

    agora new my-project
    agora new my-project --preset file-etl
    agora new my-project --preset kafka-consumer
    agora new my-project --preset postgres-incremental
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from agora.cli.commands.base import BaseCommand, CommandError
from agora.cli.console import console
from agora.core.packaging import first_party_plugin_requirement

if TYPE_CHECKING:
    import argparse

    from agora.cli.context import AgoraContext

# ======================================================================
# Preset registry
# ======================================================================

_PRESETS: dict[str, str] = {
    "file-etl": "file-etl",
    "kafka-consumer": "kafka-consumer",
    "postgres-incremental": "postgres-incremental",
}

_DEFAULT_PRESET = "file-etl"

WriteFn = Callable[[Path, str], None]


@dataclass(frozen=True)
class PresetMeta:
    key: str
    display: str
    description: str
    extras: list[str]


_PRESET_META: dict[str, PresetMeta] = {
    "file-etl": PresetMeta(
        key="file-etl",
        display="File ETL",
        description="Read CSV/JSONL files, transform, write to stdout or a sink.",
        extras=[],
    ),
    "kafka-consumer": PresetMeta(
        key="kafka-consumer",
        display="Kafka Consumer",
        description="Consume from a Kafka topic, transform, write to a sink.",
        extras=["kafka"],
    ),
    "postgres-incremental": PresetMeta(
        key="postgres-incremental",
        display="PostgreSQL Incremental",
        description="Incrementally extract rows from PostgreSQL using a cursor column.",
        extras=["postgres"],
    ),
}


# ======================================================================
# Command
# ======================================================================


class NewCommand(BaseCommand):
    """Scaffold a new agora project."""

    name = "new"
    description = "Scaffold a new agora project in a new directory."

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("name", help="Project name (used as directory name)")
        parser.add_argument(
            "--preset",
            default=_DEFAULT_PRESET,
            choices=list(_PRESETS),
            metavar="PRESET",
            help=(f"Project preset. Choices: {', '.join(_PRESETS)}. Default: {_DEFAULT_PRESET}"),
        )

    def execute(self, args: argparse.Namespace, ctx: AgoraContext) -> int:
        preset_key = getattr(args, "preset", _DEFAULT_PRESET)
        if preset_key not in _PRESETS:
            available = ", ".join(_PRESETS)
            raise CommandError(f"Unknown preset {preset_key!r}. Available presets: {available}")

        project_dir = Path(ctx.cwd) / args.name
        if project_dir.exists():
            raise CommandError(f"Directory '{args.name}' already exists.")

        meta = _PRESET_META[preset_key]
        console.header(f"Creating project '{args.name}' [{meta.display}]...")
        _scaffold(project_dir, args.name, preset_key)
        console.new_success(args.name)
        return 0


# ======================================================================
# Scaffold core
# ======================================================================


def _scaffold(root: Path, name: str, preset: str) -> None:
    def write(path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content.lstrip("\n"), encoding="utf-8")
        console.new_progress(str(path.relative_to(root.parent)))

    meta = _PRESET_META[preset]

    _write_common_files(write, root, name, meta)

    if preset == "file-etl":
        _write_file_etl(write, root, name)
    elif preset == "kafka-consumer":
        _write_kafka_consumer(write, root, name)
    elif preset == "postgres-incremental":
        _write_postgres_incremental(write, root, name)


# ======================================================================
# Common files (shared across all presets)
# ======================================================================


def _write_common_files(write: WriteFn, root: Path, name: str, meta: PresetMeta) -> None:
    extras_line = f'\n    "{first_party_plugin_requirement(*meta.extras)}",' if meta.extras else ""

    write(
        root / "agora.toml",
        f"""[project]
name = "{name}"
version = "0.1.0"
pipelines_package = "pipelines"
""",
    )

    write(
        root / "pyproject.toml",
        f"""[project]
name = "{name}"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "agora-etl",{extras_line}
]

[project.optional-dependencies]
dev = ["pytest==9.0.3", "pytest-asyncio==1.3.0"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
pythonpath = ["src"]
""",
    )

    write(root / "src" / "pipelines" / "__init__.py", "")
    write(root / "tests" / "__init__.py", "")


# ======================================================================
# file-etl preset
# ======================================================================


def _write_file_etl(write: WriteFn, root: Path, name: str) -> None:
    write(
        root / "README.md",
        f"""# {name}

A file ETL pipeline built with [agora-etl](https://pypi.org/project/agora-etl/).

## Setup

```bash
pip install -e '.[dev]'
```

## Run

```bash
# Dry-run against a sample file
agora run pipelines.ingest --dry-run

# Run with real data
agora run pipelines.ingest
```

## Test

```bash
pytest
```

## Architecture

- **Source**: `ArrowCsvSource` — reads CSV files in Arrow-native batches
- **Transform**: `MapMiddleware` — normalise/enrich each row
- **Sink**: `StdoutSink` — print results (replace with your real sink)

Checkpoint is saved after each batch so the pipeline resumes from where it
left off on restart.
""",
    )

    write(
        root / "src" / "pipelines" / "ingest.py",
        """\"\"\"
File ETL pipeline — reads CSV rows, transforms, writes to sink.

Run:
    agora run pipelines.ingest
\"\"\"
from __future__ import annotations

from agora import DeliveryConfig, MapMiddleware, Pipeline
from agora.core.pipeline import BoundPipeline
from agora.sources.file.csv import ArrowCsvSource
from agora.sinks.io.stdout import StdoutSink


def transform(record: dict) -> dict:
    \"\"\"Normalise a single CSV row.\"\"\"
    return {k: v.strip() for k, v in record.items()}


def build_pipeline() -> BoundPipeline:
    source = ArrowCsvSource("data/input.csv")
    return (
        Pipeline(source, id="file_etl")
        .pipe(MapMiddleware(transform, name="normalise"))
        .build(
            StdoutSink(),
            config=DeliveryConfig(batch_size=1_000, checkpoint_every=5),
        )
    )
""",
    )

    write(
        root / "tests" / "test_ingest.py",
        """\"\"\"Smoke test for the file ETL pipeline.\"\"\"
from __future__ import annotations

import pytest

from agora import DeliveryConfig, IterableSource, Pipeline
from agora.sinks.io.stdout import StdoutSink
from pipelines.ingest import transform


def test_transform_strips_whitespace() -> None:
    assert transform({"name": " alice ", "city": "NY "}) == {"name": "alice", "city": "NY"}


@pytest.mark.asyncio
async def test_pipeline_runs_with_iterable_source() -> None:
    records = [{"id": str(i), "value": str(i * 10)} for i in range(5)]
    sink = StdoutSink()
    summary = await (
        Pipeline(IterableSource(records), id="test_file_etl")
        .pipe(__import__("agora").MapMiddleware(transform, name="normalise"))
        .build(sink, config=DeliveryConfig(batch_size=10))
        .run()
    )
    assert summary.records_written == 5
""",
    )


# ======================================================================
# kafka-consumer preset
# ======================================================================


def _write_kafka_consumer(write: WriteFn, root: Path, name: str) -> None:
    write(
        root / "README.md",
        f"""# {name}

A Kafka consumer pipeline built with [agora-etl](https://pypi.org/project/agora-etl/).

## Setup

```bash
pip install -e '.[dev]'
```

## Required environment variables

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=my-topic
export KAFKA_GROUP_ID={name}-consumer
```

## Run

```bash
agora run pipelines.consumer
```

## Test

```bash
pytest
```

## Architecture

- **Source**: `KafkaSource` — consumes JSON messages from a Kafka topic
- **Transform**: `MapMiddleware` — parse/enrich each message
- **Sink**: `StdoutSink` — print results (replace with your real sink)

Checkpoint is managed via Kafka offset commits. The pipeline resumes from
the last committed offset on restart.
""",
    )

    write(
        root / "agora.env.example",
        f"""KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=my-topic
KAFKA_GROUP_ID={name}-consumer
""",
    )

    write(
        root / "src" / "pipelines" / "consumer.py",
        """\"\"\"
Kafka consumer pipeline — consumes JSON messages, transforms, writes to sink.

Run:
    agora run pipelines.consumer
\"\"\"
from __future__ import annotations

import json
import os

from agora import DeliveryConfig, MapMiddleware, Pipeline
from agora.core.pipeline import BoundPipeline
from agora.sinks.io.stdout import StdoutSink


def parse_message(record: dict) -> dict:
    \"\"\"Parse and enrich a Kafka message payload.\"\"\"
    raw = record.get("value", b"")
    if isinstance(raw, (bytes, bytearray)):
        return json.loads(raw.decode("utf-8"))
    return record


def build_pipeline() -> BoundPipeline:
    from agora_plugins.kafka import KafkaSource  # requires {kafka_requirement}

    source = KafkaSource(
        topics=[os.environ["KAFKA_TOPIC"]],
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        group_id=os.environ["KAFKA_GROUP_ID"],
    )
    return (
        Pipeline(source, id="kafka_consumer")
        .pipe(MapMiddleware(parse_message, name="parse"))
        .build(
            StdoutSink(),
            config=DeliveryConfig(batch_size=500),
        )
    )
""",
    )

    write(
        root / "tests" / "test_consumer.py",
        """\"\"\"Smoke test for the Kafka consumer pipeline.\"\"\"
from __future__ import annotations

import json

import pytest

from pipelines.consumer import parse_message


def test_parse_message_bytes() -> None:
    payload = json.dumps({"id": "a", "value": 1}).encode()
    result = parse_message({"value": payload})
    assert result == {"id": "a", "value": 1}


def test_parse_message_passthrough() -> None:
    record = {"id": "b", "value": 2}
    result = parse_message(record)
    assert result == record
""",
    )


# ======================================================================
# postgres-incremental preset
# ======================================================================


def _write_postgres_incremental(write: WriteFn, root: Path, name: str) -> None:
    write(
        root / "README.md",
        f"""# {name}

A PostgreSQL incremental extraction pipeline built with [agora-etl](https://pypi.org/project/agora-etl/).

## Setup

```bash
pip install -e '.[dev]'
```

## Required environment variables

```bash
export DATABASE_URL=postgresql://user:password@localhost:5432/mydb
```

## Run

```bash
agora run pipelines.extract
```

## Test

```bash
pytest
```

## Architecture

- **Source**: `PostgresSource` — incrementally extracts rows using an `updated_at` cursor
- **Transform**: `MapMiddleware` — normalise/enrich each row
- **Sink**: `StdoutSink` — print results (replace with your real sink)

Checkpoint stores the last `updated_at` value so the pipeline resumes from
where it left off on restart. Only new/updated rows are fetched on each run.
""",
    )

    write(
        root / "agora.env.example",
        """DATABASE_URL=postgresql://user:password@localhost:5432/mydb
""",
    )

    write(
        root / "src" / "pipelines" / "extract.py",
        """\"\"\"
PostgreSQL incremental extraction pipeline.

Run:
    agora run pipelines.extract
\"\"\"
from __future__ import annotations

import os

from agora import DeliveryConfig, MapMiddleware, Pipeline
from agora.core.pipeline import BoundPipeline
from agora.sinks.io.stdout import StdoutSink


def normalise_row(record: dict) -> dict:
    \"\"\"Normalise a database row.\"\"\"
    return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in record.items()}


def build_pipeline() -> BoundPipeline:
    from agora_plugins.postgres import PostgresSource  # requires {postgres_requirement}

    source = PostgresSource(
        dsn=os.environ["DATABASE_URL"],
        query="SELECT * FROM events WHERE updated_at > :cursor ORDER BY updated_at",
        cursor_column="updated_at",
    )
    return (
        Pipeline(source, id="postgres_incremental")
        .pipe(MapMiddleware(normalise_row, name="normalise"))
        .build(
            StdoutSink(),
            config=DeliveryConfig(batch_size=1_000, checkpoint_every=10),
        )
    )
""",
    )

    write(
        root / "tests" / "test_extract.py",
        """\"\"\"Smoke test for the PostgreSQL incremental pipeline.\"\"\"
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipelines.extract import normalise_row


def test_normalise_row_converts_datetime() -> None:
    dt = datetime(2026, 6, 4, 10, 0, 0, tzinfo=timezone.utc)
    result = normalise_row({"id": 1, "updated_at": dt, "value": "hello"})
    assert result["updated_at"] == dt.isoformat()
    assert result["id"] == 1


def test_normalise_row_passthrough_non_datetime() -> None:
    record = {"id": 42, "name": "alice"}
    assert normalise_row(record) == record
""",
    )
