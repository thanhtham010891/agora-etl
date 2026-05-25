# Configuration

Agora has two complementary configuration layers:

- runtime settings via `AgoraSettings` and environment variables
- declarative pipeline definitions via `agora/v1` TOML

Use whichever fits your team. Many projects use both.

## Project settings with `AgoraSettings`

The scaffolded project creates `src/settings.py`:

```python
from functools import lru_cache

from agora.config import AgoraSettings


class Settings(AgoraSettings):
    pass


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
```

`AgoraSettings` reads from:

1. environment variables
2. `agora.env`
3. Python defaults

Core settings include:

```env
LOG_LEVEL=INFO
AGORA_ENV=dev
```

Extend `Settings` with your own project-specific fields for database URLs, API keys, feature flags, and service endpoints.

Inspect the resolved settings with:

```bash
agora config show
```

## Declarative pipeline configs

Agora supports TOML documents with `format = "agora/v1"`.

Minimal example:

```toml
format = "agora/v1"

[defaults]
pipeline = "users"

[pipelines.users]
pipeline_id = "users-import"

[pipelines.users.source]
type = "csv"
path = "data/users.csv"
encoding = "utf-8"
delimiter = ","
has_header = true
row_mapper = { import = "pipelines.mappers:user_from_csv" }

[[pipelines.users.middlewares]]
type = "validate"
schema = { import = "models:UserRecord" }

[[pipelines.users.sinks]]
type = "jsonl"
path = "output/users.jsonl"
```

Validate the resolved plan without running:

```bash
agora run --config pipelines.toml --plan
```

Run the pipeline:

```bash
agora run --config pipelines.toml
```

## Selecting pipelines

One config file can contain multiple pipelines:

```toml
format = "agora/v1"

[pipelines.users.source]
type = "iterable"
records = []

[pipelines.orders.source]
type = "iterable"
records = []
```

Select one by name:

```bash
agora run users --config pipelines.toml
agora run orders --config pipelines.toml
```

If you omit the name, `defaults.pipeline` is used when present.

## Profiles and environments

Config overlays let you keep one base definition and specialize it for local, staging, or production use.

```toml
format = "agora/v1"

[defaults]
pipeline = "orders"
environment = "local"

[pipelines.orders.source]
type = "jsonl"
path = "data/orders.jsonl"

[[pipelines.orders.sinks]]
type = "stdout"

[environments.local.pipelines.orders.dlq]
enabled = true
failure_policy = "log_only"

[environments.local.pipelines.orders.dlq.sink]
type = "sqlite_dlq"
path = ".orders.dlq.db"

[environments.prod.pipelines.orders.dlq]
enabled = true
failure_policy = "raise"
```

Select an environment explicitly:

```bash
agora run --config pipelines.toml --environment prod
```

Or rely on `AGORA_ENV`:

```bash
AGORA_ENV=prod agora run --config pipelines.toml
```

Profiles work the same way via `[profiles.<name>]` and `--profile`.

## Import references

Some config values can point to Python callables or classes using an import reference:

```toml
row_mapper = { import = "pipelines.mappers:user_from_csv" }
schema = { import = "models:UserRecord" }
```

That allows TOML configs to stay declarative while reusing project code.

Because import references resolve real Python modules from your project, treat
pipeline config as trusted input. Do not accept unreviewed config files from
untrusted users.

When you run:

- `agora run --config pipelines.toml`
- `agora run --config pipelines.toml --plan`
- `agora dlq replay --config pipelines.toml`

Agora prepends the project root and `src/` to `sys.path`, then resolves those
imports as normal Python objects. In practice, that means a declarative config
is operational code with a TOML wrapper.

Recommended operator posture:

- keep pipeline configs in version control and review them like application code
- keep imported callables in stable modules instead of ad-hoc scratch files
- do not let end users upload or edit `agora/v1` configs directly
- use `--plan` in CI to surface imports before promotion

## DLQ and tracing sections

Optional sections inside one pipeline:

```toml
[pipelines.orders.dlq]
enabled = true
failure_policy = "log_only"

[pipelines.orders.dlq.sink]
type = "sqlite_dlq"
path = ".orders.dlq.db"

[pipelines.orders.tracing]
enabled = true
backend = "in_memory"
service_name = "orders-local"
```

Supported tracing backends:

- `noop`
- `in_memory`
- `opentelemetry`

## Component type names

Declarative configs use the same registry keys shown by:

```bash
agora plugins list
```

Examples:

- built-in sources: `csv`, `jsonl`, `parquet`, `http`
- built-in sinks: `stdout`, `jsonl`, `csv`, `parquet`, `webhook`, `log`
- plugin sinks and sources: `redis`, `kafka`, `postgres`

## Recommended workflow

For community-facing projects, a good pattern is:

1. start in Python while shaping the pipeline
2. extract stable callables and schemas into importable modules
3. move operational wiring into `agora/v1` TOML
4. use `--plan` in CI to validate configs before deployment

## Security and operations notes

- `agora run --config ...` and `agora dlq replay --config ...` both import Python code from the project.
- `agora config show` imports `src/settings.py` and executes `get_settings()`.
- `agora run --plan` is read-only with respect to pipeline execution, but it still resolves trusted import references from the config.
- Health endpoints are intentionally lightweight. Keep them bound to private
  network interfaces or protect them with `AGORA_HEALTH_AUTH_TOKEN`.
- Treat the built-in health server as an internal probe surface, not as a public API edge.

## Related guides

- [Getting Started](getting-started.md)
- [CLI Reference](cli.md)
- [Plugins](plugins.md)
