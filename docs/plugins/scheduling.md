# Scheduling Plugins

_When to read this: Agora's built-in interval schedules are not expressive enough and you need cron-style timing._

Use the scheduling family when simple fixed intervals are not expressive enough
for how your jobs should run.

In the official `agora-etl-plugins 0.4.x` line, cron scheduling is an official
helper surface. It is intentionally narrow: it calculates and validates
calendar timing, while Agora core owns the worker runtime and distributed
coordination owns multi-replica lease safety.

## Install

```bash
pip install "agora-etl-plugins[cron]"
```

## What it does

The scheduling plugin adds cron-expression support on top of Agora's worker and
runner model.

It provides:

- cron expression validation
- calculation of the next scheduled run time

That sounds small, but it matters when a job should follow calendar rules
instead of just “every N seconds.”

## When it is useful

- every weekday at a fixed local time
- every hour at minute 15
- first day of the month
- jobs that should align with business calendars instead of raw intervals

## Production checklist

- Give every scheduled pipeline a stable `pipeline_id`.
- Use cron expressions for wall-clock schedules and fixed intervals for simple
  recurring delays.
- Pair cron schedules with distributed coordination when the same schedule runs
  on more than one worker replica.
- Keep business-calendar exceptions in application code unless they become a
  reusable scheduling abstraction.

## Sample

This example validates a cron expression and computes the next delay before a
worker should trigger the pipeline again.

```python
import time

from agora_plugins.cron import seconds_until_next_run, validate_cron_expression


expression = "15 * * * *"

validate_cron_expression(expression)

delay_s = seconds_until_next_run(expression, time.time())
print(f"sleep for {delay_s:.1f} seconds before the next run")
```

What this shows:

- the cron plugin is about schedule calculation, not record movement
- validation happens before the worker loop trusts an expression
- it works well when jobs follow wall-clock rules instead of fixed intervals

## Worker example

In practice the cron plugin is most often used through `Schedule.cron(...)`
inside a scheduled worker.

```python
from agora.runner import Schedule, ScheduledPipeline, WorkerPool


async def build_daily_pipeline():
    return make_pipeline()


def get_worker() -> WorkerPool:
    pool = WorkerPool()
    pool.register(
        ScheduledPipeline(
            factory=build_daily_pipeline,
            schedule=Schedule.cron("0 2 * * 1-5"),
            pipeline_id="weekday-2am-sync",
        )
    )
    return pool
```

Use this when the question is not just "how long should I sleep?" but "how do I
embed calendar timing into a real long-lived Agora worker?"

## Why keep it as a plugin

Cron parsing pulls in calendar semantics and an extra dependency. That is
useful for scheduled jobs, but it does not need to live in the core runtime for
every installation.

## Boundary

Use the scheduling plugin when the question is “when should this run next?”

If the question becomes “which worker in the fleet should own this run?”, that
belongs to [Distributed Coordination](distributed.md).
