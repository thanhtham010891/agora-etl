# Running Agora with uvloop

_When to read this: you want to run Agora on top of `uvloop` without turning event-loop choice into a core runtime dependency._

`uvloop` is an optional event-loop implementation for asyncio. Agora does not
require it, and the core runtime does not install or select it automatically.

That boundary is intentional:

- Agora owns pipeline semantics, recovery, and lifecycle behavior.
- The application entrypoint owns the process event loop.

If you want `uvloop`, enable it at the top-level script or worker launcher,
not inside sources, middlewares, sinks, or pipeline factories.

## Install

```bash
pip install uvloop
```

## Single pipeline script

For ordinary Python entrypoints, wrap the top-level coroutine with
`asyncio.Runner(...)` and pass `uvloop.new_event_loop` as the `loop_factory`.

```python
import asyncio

import uvloop

from agora import IterableSource, Pipeline


async def main() -> None:
    summary = await (
        Pipeline(IterableSource([{"id": 1}, {"id": 2}]), id="demo")
        .build()
        .run()
    )
    print(summary)


with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
    runner.run(main())
```

Agora requires Python `>=3.11`, so `asyncio.Runner(...)` is the right default
entrypoint shape for Agora applications that want a custom loop factory.

## WorkerPool entrypoint

The same pattern works for long-lived workers. Keep `get_worker()` focused on
building the pool, then choose the event loop in the launcher script.

```python
# worker.py
from agora.runner import Schedule, ScheduledPipeline, WorkerPool


async def build_pipeline():
    ...


def get_worker() -> WorkerPool:
    pool = WorkerPool(health_port=8080)
    pool.register(
        ScheduledPipeline(
            factory=build_pipeline,
            schedule=Schedule.continuous(),
            pipeline_id="events",
        )
    )
    return pool
```

```python
# run_worker.py
import asyncio

import uvloop

from worker import get_worker


def main() -> None:
    pool = get_worker()
    with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
        runner.run(pool.run())


if __name__ == "__main__":
    main()
```

This keeps worker construction and loop selection separate.

## CLI boundary

`agora run` and `agora worker` currently use the standard library asyncio
entrypoints internally. They do not expose a `--event-loop` or `--uvloop`
switch.

If you need `uvloop` today:

- run the pipeline from a Python script instead of `agora run`
- or create a tiny launcher script for `WorkerPool` instead of `agora worker`

That keeps Agora's CLI simple while still leaving room for process-level loop
selection in applications that care about it.

## Where not to configure it

Do not enable `uvloop` from:

- middleware constructors
- source or sink classes
- pipeline factories such as `build_pipeline()`
- library import side effects

The event loop is a process-level concern. Configuring it deep inside the
pipeline graph makes ownership unclear and can conflict with host frameworks.

## When it helps most

`uvloop` is most likely to matter in runtime shapes that create a lot of
asyncio scheduling pressure, such as:

- long-lived workers
- buffered execution with many in-flight tasks
- sink fan-out with concurrent writes

It may matter less when the bottleneck is dominated by external systems or
blocking work already delegated through `asyncio.to_thread()`.

Treat it as an optimization knob, not as part of Agora's correctness model.
