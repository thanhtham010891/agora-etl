# Plugins

Agora plugins extend the core runtime with integrations that are better kept
outside `agora-etl` itself.

This section focuses on the public plugin story:

- what the official plugin bundle includes
- when to use each plugin family
- what kind of system problem each family solves
- how to build your own plugin package

## Start here

- Want the official first-party integrations: [Official Bundle](official-bundle.md)
- Need Redis-backed state, streams, or replay: [Redis](redis.md)
- Need topic-based pipelines: [Kafka](kafka.md)
- Need relational extract/load workflows: [PostgreSQL](postgresql.md)
- Need calendar scheduling: [Scheduling](scheduling.md)
- Need multi-worker lease ownership: [Distributed Coordination](distributed.md)
- Want to build your own package: [Developing Plugins](developing.md)

## What counts as a plugin?

Agora discovers plugin packages through Python entry-points. A plugin may
provide:

- sources
- sinks
- middlewares
- AI providers
- caches
- state backends
- metrics exporters
- runner integrations

This keeps the core framework smaller and lets integrations evolve on their own
release cadence.

## Official first-party bundle

The public first-party plugin distribution is
[`agora-etl-plugins`](https://pypi.org/project/agora-etl-plugins/).

Current official coverage includes Redis, Kafka, PostgreSQL, cron scheduling,
and distributed worker coordination.

Install examples:

```bash
pip install "agora-etl-plugins[redis]"
pip install "agora-etl-plugins[kafka]"
pip install "agora-etl-plugins[postgres]"
pip install "agora-etl-plugins[all]"
```

## How to think about plugins

Use a plugin when:

- The capability depends on an external system
- The integration has its own dependency footprint
- The feature should evolve independently from the core runtime
- The people may want multiple interchangeable backends

Use the family pages in this section when the question is less about
"what is a plugin?" and more about "which backend story matches my pipeline?"

Keep work in the core when it is really part of Agora's execution model,
pipeline semantics, or stable framework contract.

## Discovery model

After installation, plugin components are available through Agora registries
and the CLI.

Examples:

```bash
agora plugins list
```

```python
from agora.sources import source_registry
from agora.sinks import sink_registry

source = source_registry.create("my_source", url="https://api.example.com")
sink = sink_registry.create("my_sink", dsn="postgresql://example/db")
```

For the full plugin contract and entry-point groups, see
[Developing Plugins](developing.md).
