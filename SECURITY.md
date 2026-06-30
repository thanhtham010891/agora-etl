# Security Policy

This policy covers security issues in the core `agora-etl` package.

Core includes the runtime, public contracts, CLI, checkpointing, DLQ behavior,
health/metrics/tracing surfaces, configuration loading, plugin discovery
diagnostics, and built-in sources/sinks/middlewares shipped by `agora-etl`.

Backend-specific issues in Redis, Kafka, PostgreSQL, cron, distributed
coordination, or Anthropic integrations belong to the
`agora-etl-plugins` security policy because those implementations ship in the
plugin package.

## Supported Versions

| Package line | Supported for security fixes | Notes |
|---|---:|---|
| `0.4.x` | Yes | Current production-stable `agora-etl` line. Supports Python `3.11`, `3.12`, and `3.13`. |
| `<0.4` | No | Upgrade to the current line before requesting security fixes. |

Security fixes should be released on the current supported line unless a
specific backport is explicitly announced.

## Reporting A Vulnerability

Do not disclose exploitable details in public GitHub issues, pull requests, or
discussions.

Preferred reporting path:

1. Use GitHub private vulnerability reporting for the repository if it is
   enabled.
2. If private reporting is not available, open a minimal public issue asking
   for a private security contact path. Do not include exploit details,
   credentials, payloads, production hostnames, private stack traces, or tenant
   data.

Please include privately:

- affected `agora-etl` version
- Python version and operating system
- whether `agora-etl-plugins` or other third-party plugins are installed
- a minimal reproduction or proof of impact
- whether records, DLQ payloads, checkpoint state, credentials, logs, traces,
  metrics, health responses, or plugin discovery output can be exposed or
  modified
- whether the issue can cause silent data loss, duplicate delivery,
  unauthorized replay, checkpoint corruption, or sink misrouting

## Vulnerability Handling

Maintainers should triage reports by impact:

- credential, token, DSN, URL, header, or secret-file exposure
- data exposure through DLQ payloads, logs, metrics, traces, health responses,
  exceptions, or replay records
- integrity failures that silently drop, duplicate, corrupt, reorder, or
  misroute records
- checkpoint advancement past unhandled failures
- replay acknowledgement before durable replay success
- unsafe plugin discovery, manifest diagnostics, or CLI behavior that exposes
  sensitive local state
- denial-of-service vectors in public parsing, configuration loading, recovery,
  replay, batching, process-isolated middleware, or health endpoints

Confirmed vulnerabilities should receive:

1. a private fix plan
2. regression coverage where practical
3. a patched release for the supported line
4. public disclosure notes after a fix is available

## Security Boundaries

### Core runtime

The core runtime is responsible for preserving documented failure, checkpoint,
DLQ, ordering, and replay semantics. A bug that breaks those guarantees in a
way that can cause data loss, unauthorized disclosure, or silent corruption is
in scope for this policy.

### Plugins

Installed plugin packages are executable Python code. Agora's discovery system
can report compatibility, collisions, and load failures, but it does not
sandbox arbitrary plugin code. Install plugins only from trusted sources.

Security issues in first-party backend integrations should be reported against
`agora-etl-plugins`. Security issues in third-party plugins should be reported
to the package owner unless the issue is caused by a core Agora contract.

### Secrets and sensitive data

Applications should treat records, DLQ payloads, prompts, completions,
checkpoints, logs, traces, and metrics as potentially sensitive. Use redaction
hooks, secret managers, private telemetry backends, and backend-specific access
controls in production deployments.

## Dependency Security

The core package depends on libraries such as `pydantic`, `pydantic-settings`,
`logstruct`, `rich`, `httpx`, and optional extras such as `pyarrow`, `orjson`,
`uvloop`, `pyinstrument`, and `agora-etl-rs`.

Security updates in those dependencies should be evaluated against the current
compatibility range before widening, pinning, or replacing versions.

For production deployments, keep `agora-etl`, Python, optional extras, and any
installed plugin packages patched within their supported version ranges.
