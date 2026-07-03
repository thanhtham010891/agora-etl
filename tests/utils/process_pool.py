from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from functools import lru_cache


def _probe_identity(value: int) -> int:
    return value


@lru_cache(maxsize=1)
def process_pool_unavailable_reason() -> str | None:
    """Return a human-readable failure reason when process pools are blocked."""

    executor: ProcessPoolExecutor | None = None
    try:
        executor = ProcessPoolExecutor(max_workers=1)
        future = executor.submit(_probe_identity, 1)
        future.result(timeout=5)
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}"
    finally:
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
    return None
