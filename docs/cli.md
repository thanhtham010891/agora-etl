# CLI Reference

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
agora run --config pipeline.yaml
```

`agora run` adds `cwd` and `cwd/src` to `sys.path` before importing the module.

Set `AGORA_ENV` to load an environment-specific config overlay:

```bash
AGORA_ENV=production agora run pipelines.example
```

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

List all registered sources, sinks, and middlewares:

```bash
agora plugins list
```

## agora config

Show the resolved settings for the current project:

```bash
agora config show
```

Reads from `src/settings.py` which must define `get_settings() -> AgoraSettings`.

## agora dlq

Inspect and replay dead-letter queue records:

```bash
agora dlq list --db .dlq.db
agora dlq list --db .dlq.db --pipeline-id my_pipeline --limit 50
agora dlq replay --db .dlq.db --pipeline-id my_pipeline
agora dlq replay --db .dlq.db --pipeline-id my_pipeline --sink-mode
```

`--sink-mode` replays the processed record directly to the sink, bypassing the middleware chain.

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
