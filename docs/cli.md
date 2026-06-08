# CLI Reference

_When to read this: you want the exact command-line entry points for creating projects, running pipelines, operating workers, and replaying DLQ records._

## agora new

Scaffold a new project:

```bash
agora new <project-name>
```

Creates the following layout:

```
<project-name>/
├── pyproject.toml
├── agora.toml
├── agora.env.example
├── src/
│   ├── settings.py
│   ├── pipelines/
│   │   ├── __init__.py
│   │   └── example.py
│   ├── models/
│   │   └── __init__.py
│   ├── normalizers/
│   │   └── __init__.py
│   └── sinks/
│       └── __init__.py
└── tests/
    └── test_example.py
```

## agora run

Run a pipeline once:

```bash
agora run pipelines.example
agora run pipelines.example --max-records 1000
agora run --config pipelines.toml
agora run --config pipelines.toml --plan
agora run --config pipelines.toml --environment prod
```

`agora run` adds `cwd` and `cwd/src` to `sys.path` before importing the module.

With declarative configs, `AGORA_ENV` can supply the default environment overlay:

```bash
AGORA_ENV=production agora run --config pipelines.toml
```

When used with `--config`, Agora expects an `agora/v1` TOML document.

## agora worker

Start the `WorkerPool` from a `worker.py` module:

```bash
agora worker
agora worker --module pipelines.worker   # custom module path
agora worker --list                      # list registered pipelines
agora worker --health-auth-token <token> # override health auth token
```

The module must define `get_worker() -> WorkerPool`.

## agora pipelines

List all pipeline modules found under `src/pipelines/`:

```bash
agora pipelines list
```

## agora plugins

List registered sources, sinks, and middlewares:

```bash
agora plugins list
agora plugins list --kind sink
agora plugins list --json
```

`agora plugins list` is a diagnostics view, not a package installer. It now
covers every public plugin registry, including runners, dedup stores/strategies,
AI providers/caches, metrics exporters, and state backends.

Useful filters include:

```bash
agora plugins list --kind runner
agora plugins list --kind dedup_store
agora plugins list --kind state_backend
```

Incompatible entry-point plugins may appear with an incompatible status so
operators can see why Agora did not load them. `--json` includes the public
contract metadata for each row: category, entry-point group, registry name,
stability, compatibility, and manifest version.

## agora config

Show the resolved settings for the current project:

```bash
agora config show
```

Reads from `src/settings.py` which must define `get_settings() -> AgoraSettings`.

## Common local workflow

```bash
agora new my-project
cd my-project
agora run pipelines.example --dry-run
agora pipelines list
agora config show
```

## agora dlq

Replay dead-letter queue records using a declarative pipeline config:

```bash
agora dlq replay --config pipelines.toml
agora dlq replay --config pipelines.toml orders --stage sink_write
agora dlq replay --config pipelines.toml --mode sink
agora dlq replay --config pipelines.toml --limit 50
```

Replay modes:

- `--mode pipeline` replays the original payload back through the middleware chain
- `--mode sink` re-drives only the sink writer path using the processed payload

`--mode sink` only supports DLQ records captured at stage `sink_write`.

## agora checkpoint

Inspect or clear persisted checkpoint state:

```bash
agora checkpoint show nightly_events
agora checkpoint inspect nightly_events --store sqlite:/var/lib/myapp/checkpoints.db
agora checkpoint inspect nightly_events --store sqlite:/var/lib/myapp/checkpoints.db --json
agora checkpoint reset nightly_events --store sqlite:/var/lib/myapp/checkpoints.db --yes
```

`inspect` explains:

- raw checkpoint value
- resume key and granularity
- resume cost model
- warnings for expensive high-offset file resumes

`reset --yes` clears the saved resume position so the next run starts from the
beginning. `reset --json` is available for scripts.

## agora diagnose

Summarize the last visible failure and recovery posture:

```bash
agora diagnose nightly_events
agora diagnose nightly_events --checkpoint-store sqlite:/var/lib/myapp/checkpoints.db
agora diagnose nightly_events --checkpoint-store sqlite:/var/lib/myapp/checkpoints.db --dlq-path /var/lib/myapp/dlq.db
agora diagnose nightly_events --checkpoint-store sqlite:/var/lib/myapp/checkpoints.db --dlq-path /var/lib/myapp/dlq.db --json
```

`agora diagnose` combines:

- latest checkpoint state
- recovery contract for the source
- DLQ replayability
- most recent visible failure details

## agora doctor

Run pre-flight diagnostics for the current install or for one declarative
pipeline selection:

```bash
agora doctor
agora doctor --config pipelines.toml
agora doctor orders --config pipelines.toml --environment prod
```

When `--config` points at an `agora/v1` document, `agora doctor` now uses the
same project import path conventions as `agora run` and checks:

- pipeline selection / overlay resolution
- declarative import refs
- component construction
- recovery posture for the selected source
- whether the configured DLQ backend supports `agora dlq replay`
- public entry-point plugin compatibility, including incompatible MANIFEST versions

## agora version

Print the installed agora-etl version:

```bash
agora version
```

## Project layout conventions

`agora run` and `agora worker` expect:

```
my_project/
├── src/
│   ├── pipelines/        # agora pipelines list scans here
│   │   └── my_pipeline.py
│   └── settings.py       # agora config show imports get_settings()
└── worker.py             # agora worker imports get_worker()
```

Both `cwd` and `cwd/src` are added to `sys.path` automatically.

## Trust boundary

`agora run`, `agora worker`, and `agora dlq replay` import Python modules from
the current project. Config files may also resolve `import = "module:name"`
references. Treat project code and config as trusted input.
