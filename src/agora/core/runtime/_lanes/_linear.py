"""Linear lane execution strategy."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from agora.core.constants import LINEAR_FLUSH_INTERVAL
from agora.core.runtime._delivery import CheckpointState, RunState, SourceRecord
from agora.core.runtime._hot_metrics import HotPathMetrics

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from agora.core.context import PipelineContext
    from agora.core.runtime._buffered import ExecutionCoordinator


@dataclass(slots=True)
class LinearLaneStrategy:
    coordinator: ExecutionCoordinator

    async def run(
        self,
        ctx: PipelineContext,
        source_records: AsyncGenerator[SourceRecord, None],
        checkpoint_state: CheckpointState,
    ) -> None:
        c = self.coordinator
        state = RunState(ctx=ctx, checkpoint_state=checkpoint_state, pending_writes=[])
        source_error: Exception | None = None

        batch_size = c.writer_batch_size
        source_name = c.source.source_name
        metrics = ctx.metrics

        if c.rust_available() and batch_size > 1:
            buf = c.make_linear_batch_buffer(batch_size, LINEAR_FLUSH_INTERVAL)
            use_direct_flush = c.plan.writer.direct_flush_eligible
            metrics.runtime.direct_flush_active = use_direct_flush

            try:
                async for source_record in source_records:
                    state.processed_count += 1
                    if buf.inc_consumed(source_name):
                        buf.flush_metrics(metrics)

                    result = await c.chain.process(source_record.raw, ctx)
                    if result.value is None:
                        await c.delivery.drop_record(
                            state,
                            source_record.checkpoint,
                            failure=result.failure,
                            on_success=source_record.on_success,
                        )
                        continue

                    if buf.push(
                        result.value,
                        source_record.raw,
                        source_record.checkpoint,
                        source_record.on_success,
                    ):
                        if use_direct_flush:
                            processed_list, raw_list, checkpoint_list, on_success_list = (
                                buf.take_flush_batch()
                            )
                            await c.delivery.flush_batch_direct(
                                state,
                                processed_list,
                                raw_list,
                                checkpoint_list,
                                on_success_list=on_success_list,
                            )
                        else:
                            batch = buf.take_batch()
                            for processed, raw, checkpoint, on_success in batch:
                                await c.delivery.queue_processed_record(
                                    state,
                                    processed,
                                    raw,
                                    checkpoint,
                                    batch_size,
                                    failure=None,
                                    on_success=on_success,
                                )

            except Exception as exc:
                source_error = exc

            if buf.len() > 0:
                if use_direct_flush:
                    processed_list, raw_list, checkpoint_list, on_success_list = (
                        buf.take_flush_batch()
                    )
                    await c.delivery.flush_batch_direct(
                        state,
                        processed_list,
                        raw_list,
                        checkpoint_list,
                        on_success_list=on_success_list,
                    )
                else:
                    remaining = buf.take_batch()
                    for processed, raw, checkpoint, on_success in remaining:
                        await c.delivery.queue_processed_record(
                            state,
                            processed,
                            raw,
                            checkpoint,
                            batch_size,
                            failure=None,
                            on_success=on_success,
                        )
                    await c.delivery.flush_pending_writes(state)
            buf.flush_metrics_final(metrics)
            if source_error is not None:
                raise source_error
            return

        hot = HotPathMetrics.for_source(source_name, metrics=metrics)

        try:
            async for source_record in source_records:
                if hot.inc_consumed():
                    hot.flush(metrics)
                state.processed_count += 1

                result = await c.chain.process(source_record.raw, ctx)
                if batch_size > 1:
                    await c.delivery.queue_processed_record(
                        state,
                        result.value,
                        source_record.raw,
                        source_record.checkpoint,
                        batch_size,
                        failure=result.failure,
                        on_success=source_record.on_success,
                    )
                else:
                    await c.delivery.write_processed_record(
                        state,
                        result.value,
                        source_record.raw,
                        source_record.checkpoint,
                        failure=result.failure,
                        on_success=source_record.on_success,
                    )
        except Exception as exc:
            source_error = exc

        hot.flush_final(metrics)

        await c.delivery.flush_pending_writes(state)
        if source_error is not None:
            raise source_error
