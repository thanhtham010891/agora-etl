"""
agora/cli/commands/new.py
=========================
``agora new <project-name>`` — scaffold a new agora project.

Creates the conventional project layout that agora expects:

    <project-name>/
    ├── agora.toml
    ├── agora.env.example
    ├── pyproject.toml
    ├── src/
    │   ├── settings.py
    │   ├── pipelines/
    │   │   ├── __init__.py
    │   │   └── example.py
    │   ├── normalizers/
    │   │   └── __init__.py
    │   ├── models/
    │   │   └── __init__.py
    │   └── sinks/
    │       └── __init__.py
    └── tests/
        └── test_example.py
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from agora.cli.commands.base import BaseCommand, CommandError
from agora.cli.console import console

if TYPE_CHECKING:
    import argparse

    from agora.cli.context import AgoraContext


class NewCommand(BaseCommand):
    """Scaffold a new agora project."""

    name = "new"
    description = "Scaffold a new agora project in a new directory."

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("name", help="Project name (used as directory name)")
        parser.add_argument(
            "--template",
            default="default",
            help="Project template (default: default)",
        )

    def execute(self, args: argparse.Namespace, ctx: AgoraContext) -> int:
        project_dir = Path(ctx.cwd) / args.name
        if project_dir.exists():
            raise CommandError(f"Directory '{args.name}' already exists.")

        console.header(f"Creating project '{args.name}'...")
        _scaffold(project_dir, args.name)
        console.new_success(args.name)
        return 0


def _scaffold(root: Path, name: str) -> None:
    """Create all project files and directories."""

    def write(path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content.lstrip("\n"), encoding="utf-8")
        console.new_progress(str(path.relative_to(root.parent)))

    # agora.toml
    write(
        root / "agora.toml",
        f"""
[project]
name = "{name}"
version = "0.1.0"
pipelines_package = "pipelines"
""",
    )

    # agora.env.example
    write(
        root / "agora.env.example",
        """
LOG_LEVEL=INFO
AGORA_ENV=dev

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# PostgreSQL
DATABASE_URL=postgresql://localhost:5432/mydb
""",
    )

    # pyproject.toml
    write(
        root / "pyproject.toml",
        f"""
[project]
name = "{name}"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "agora-etl",
]

[project.optional-dependencies]
dev = ["pytest==9.0.3", "pytest-asyncio==1.3.0", "ruff==0.15.13"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
pythonpath = ["src"]
""",
    )

    # src/settings.py
    write(
        root / "src" / "settings.py",
        """
\"\"\"
Project settings — extend AgoraSettings with your own config.
\"\"\"
from __future__ import annotations
from functools import lru_cache
from agora.config import AgoraSettings


class Settings(AgoraSettings):
    pass


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
""",
    )

    # src/models/__init__.py
    write(root / "src" / "models" / "__init__.py", "")

    # src/normalizers/__init__.py
    write(root / "src" / "normalizers" / "__init__.py", "")

    # src/sinks/__init__.py
    write(root / "src" / "sinks" / "__init__.py", "")

    # src/pipelines/__init__.py
    write(root / "src" / "pipelines" / "__init__.py", "")

    # src/pipelines/example.py
    write(
        root / "src" / "pipelines" / "example.py",
        """
\"\"\"
Example pipeline — replace with your real pipeline.

Run with:
    agora run pipelines.example --dry-run
    agora run pipelines.example --max-records 100
\"\"\"
from __future__ import annotations

from dataclasses import dataclass

from agora.core.pipeline import BoundPipeline, Pipeline
from agora.core.source import IterableSource
from agora.sinks.io.stdout import StdoutSink


@dataclass
class SampleRecord:
    id: int
    message: str


async def build_pipeline() -> BoundPipeline:
    \"\"\"Factory function — agora run pipelines.example calls this.\"\"\"
    source = IterableSource(
        SampleRecord(id=i, message=f"Hello from record {i}")
        for i in range(10)
    )

    return (
        Pipeline(source, id="example")
        .filter(lambda r: r.id % 2 == 0, name="even_filter")
        .build(StdoutSink(prefix="📦 "))
    )
""",
    )

    # tests/test_example.py
    write(
        root / "tests" / "test_example.py",
        """
import pytest
from pipelines.example import build_pipeline


@pytest.mark.asyncio
async def test_example_pipeline_runs():
    pipeline = await build_pipeline()
    summary = await pipeline.run()
    assert summary.records_consumed == 10
    assert summary.records_written == 5   # even_filter keeps 0, 2, 4, 6, 8
    assert summary.records_dropped == 5
""",
    )
