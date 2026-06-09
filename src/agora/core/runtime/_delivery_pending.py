"""Pending-write owner helpers for runtime delivery."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine

    from agora.core.runtime._delivery_types import RunState


def uses_pending_write_owner(
    *,
    writer_batch_size: int,
    batch_flush_interval_ms: int | None,
) -> bool:
    return (
        writer_batch_size > 1
        and batch_flush_interval_ms is not None
        and batch_flush_interval_ms > 0
    )


async def ensure_pending_write_owner(
    *,
    state: RunState,
    writer_batch_size: int,
    batch_flush_interval_ms: int | None,
    owner_name: str,
    owner_runner: Callable[[RunState], Coroutine[object, object, None]],
) -> None:
    if not uses_pending_write_owner(
        writer_batch_size=writer_batch_size,
        batch_flush_interval_ms=batch_flush_interval_ms,
    ):
        return
    if state.pending_write_owner_task is not None:
        return
    if batch_flush_interval_ms is None:
        return

    state.pending_write_batch_size = writer_batch_size
    state.pending_write_flush_interval_s = batch_flush_interval_ms / 1000.0
    state.pending_write_notify = asyncio.Event()
    state.pending_write_stop = asyncio.Event()
    state.pending_write_flushed = asyncio.Event()
    state.pending_write_flushed.set()
    state.pending_write_error = None
    state.pending_write_owner_task = asyncio.create_task(
        owner_runner(state),
        name=owner_name,
    )


async def run_pending_write_owner(
    *,
    state: RunState,
    flush_once: Callable[[RunState], Awaitable[None]],
) -> None:
    notify = state.pending_write_notify
    stop = state.pending_write_stop
    flushed = state.pending_write_flushed
    batch_size = state.pending_write_batch_size
    flush_interval_s = state.pending_write_flush_interval_s
    assert notify is not None
    assert stop is not None
    assert flushed is not None
    assert flush_interval_s is not None

    try:
        while True:
            if stop.is_set() and not state.pending_writes:
                return

            if len(state.pending_writes) >= batch_size:
                await flush_once(state)
                flushed.set()
                continue

            timeout: float | None = flush_interval_s if state.pending_writes else None
            timed_out = False
            try:
                if timeout is None:
                    await notify.wait()
                else:
                    await asyncio.wait_for(notify.wait(), timeout=timeout)
            except TimeoutError:
                timed_out = True
            finally:
                notify.clear()

            if stop.is_set() and state.pending_writes:
                await flush_once(state)
                flushed.set()
                continue

            if timed_out and state.pending_writes:
                await flush_once(state)
                flushed.set()
    except asyncio.CancelledError:
        raise
    except BaseException as exc:
        state.pending_write_error = exc
        flushed.set()
        raise


async def wait_for_pending_write_capacity(
    *,
    state: RunState,
    writer_batch_size: int,
) -> None:
    flushed = state.pending_write_flushed
    assert flushed is not None

    while len(state.pending_writes) >= writer_batch_size:
        if state.pending_write_error is not None:
            raise state.pending_write_error
        await flushed.wait()


async def close_pending_write_owner(state: RunState) -> None:
    task = state.pending_write_owner_task
    if task is None:
        return

    stop = state.pending_write_stop
    notify = state.pending_write_notify
    assert stop is not None
    assert notify is not None

    stop.set()
    notify.set()
    try:
        await task
    finally:
        state.pending_write_owner_task = None
        state.pending_write_notify = None
        state.pending_write_stop = None
        state.pending_write_flushed = None
        error = state.pending_write_error
        state.pending_write_error = None
        if error is not None:
            raise error
