# Getting Started

This guide takes you from installation to a runnable pipeline.

## Requirements

- Python 3.11+

## Install

Core package only:

```bash
pip install agora-etl
```

With file extras for Parquet support and faster JSONL paths:

```bash
pip install "agora-etl[file]"
```

With official integrations:

```bash
pip install "agora-etl-plugins[redis]"
pip install "agora-etl-plugins[kafka]"
pip install "agora-etl-plugins[postgres]"
```

## Scaffold a project

Create a new project with the built-in CLI:

```bash
agora new my-pipeline
cd my-pipeline
```

Generated layout:

```text
my-pipeline/
├── agora.toml
├── agora.env.example
├── pyproject.toml
├── src/
│   ├── settings.py
│   ├── pipelines/
│   │   └── example.py
│   ├── models/
│   ├── normalizers/
│   └── sinks/
└── tests/
    └── test_example.py
```

## Run the generated example

The scaffold includes `src/pipelines/example.py`. Run it:

```bash
agora run pipelines.example
```

Or dry-run it to stdout:

```bash
agora run pipelines.example --dry-run
```

You can cap record count during local testing:

```bash
agora run pipelines.example --max-records 100
```

## Your first pipeline

Here is a minimal pipeline built in pure Python:

```python
from dataclasses import dataclass

from agora import Pipeline
from agora.core.source import IterableSource
from agora.sinks.io.stdout import StdoutSink


@dataclass
class Event:
    id: int
    status: str


async def build_pipeline():
    source = IterableSource(
        [
            Event(id=1, status="new"),
            Event(id=2, status="done"),
            Event(id=3, status="new"),
        ]
    )

    return (
        Pipeline(source, id="events")
        .filter(lambda record: record.status == "new")
        .build(StdoutSink(prefix="[event] "))
    )
```

Run it by exposing `build_pipeline()` inside `src/pipelines/example.py` and then calling:

```bash
agora run pipelines.example
```

## Add real sources and sinks

Once the basics work, swap the in-memory source for real components:

- file ingestion: `JsonLinesSource`, `CsvSource`, `ParquetSource`
- HTTP polling: subclass `HTTPSource`
- official plugins: Redis, Kafka, PostgreSQL

See:

- [Sources](sources.md)
- [Sinks](sinks.md)
- [Plugins](plugins/index.md)

## Add validation or enrichment

Middlewares let you transform and protect records before they are written:

```python
from agora.middlewares.validate import ValidateMiddleware

pipeline = Pipeline(source).pipe(ValidateMiddleware(schema=MyModel)).build(sink)
```

Useful next reads:

- [Middlewares](middlewares.md)
- [Architecture](architecture.md)

## When to use declarative config

If you want operations-friendly pipeline definitions in TOML instead of Python wiring, Agora also supports config-driven pipelines:

```bash
agora run --config pipelines.toml --plan
agora run --config pipelines.toml
```

See [Configuration](configuration.md) for the full format.

Config-driven runs can import project callables via `import = "module:name"`.
Keep those configs in trusted source control rather than accepting them from
untrusted users.

## Next steps

- Learn config overlays with [Configuration](configuration.md)
- Explore worker processes in [Runner](runner.md)
- Browse the full command surface in [CLI Reference](cli.md)
