from __future__ import annotations

import asyncio
from typing import Any

from pipelines.kafka_raw_to_clean import build_pipeline as build_orders_normalize_pipeline
from pipelines.kafka_to_postgres import build_pipeline as build_orders_projection_pipeline
from settings import get_settings

from agora.runner import Schedule, ScheduledPipeline, WorkerPool


async def _log_run(record: Any) -> None:
    summary = getattr(record, "summary", None)
    if summary is None:
        return
    print(
        f"[{summary.pipeline_id}] consumed={summary.records_consumed} "
        f"written={summary.records_written} dropped={summary.records_dropped} "
        f"errors={summary.records_errored} elapsed={summary.elapsed_seconds:.2f}s"
    )


def get_worker() -> WorkerPool:
    settings = get_settings()
    pool = WorkerPool(
        health_port=settings.health_port,
        health_host=settings.health_host,
        health_auth_token=settings.health_auth_token or None,
    )
    pool.register(
        ScheduledPipeline(
            factory=build_orders_normalize_pipeline,
            schedule=Schedule.continuous(),
            pipeline_id="orders_normalize",
            on_run_complete=_log_run,
        )
    )
    pool.register(
        ScheduledPipeline(
            factory=build_orders_projection_pipeline,
            schedule=Schedule.continuous(),
            pipeline_id="orders_projection",
            on_run_complete=_log_run,
        )
    )
    return pool


async def main() -> None:
    await get_worker().run()


if __name__ == "__main__":
    asyncio.run(main())
